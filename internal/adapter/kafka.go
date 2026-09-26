package adapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pavlo-v-chernykh/keystore-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafak-tools/internal/config"
)

// browseTimeout 浏览整体超时（空 topic 不应长时间卡住）。
const browseTimeout = 5 * time.Second

// kafkaAdapter 基于 franz-go 的 Kafka 只读适配器。
// 浏览使用独立 consumer + Assign/Seek，永不进消费组、永不提交 offset。
type kafkaAdapter struct {
	cfg     config.Kafka
	opts    []kgo.Opt
	brokers []string
}

// kafkaOpts 构造 franz-go 连接选项（种子过滤 + SSL），newKafka 与实时订阅共用。
func kafkaOpts(cfg config.Kafka) ([]kgo.Opt, []string, error) {
	brokers := strings.Split(cfg.Bootstrap, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
		brokers[i] = strings.Trim(brokers[i], `"`)
	}
	brokers = filter(brokers, cfg.Security) // 混合监听器：剔除协议不匹配的种子（结果带短缓存）
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.FetchMaxWait(500 * time.Millisecond),
		kgo.RequestTimeoutOverhead(10 * time.Second),
	}
	if cfg.Security == "ssl" {
		tlsCfg, err := buildTLS(cfg)
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	return opts, brokers, nil
}

func newKafka(cfg config.Kafka) (Adapter, error) {
	opts, brokers, err := kafkaOpts(cfg)
	if err != nil {
		return nil, err
	}
	return &kafkaAdapter{cfg: cfg, opts: opts, brokers: brokers}, nil
}

// ---- 多种子协议探测（解决 plaintext/ssl 混合监听器竞态） ----

type seedInfo struct {
	brokers []string
	at      time.Time
}

var seedCache sync.Map // "security|bootstrap" -> seedInfo

// filter 按配置的安全模式剔除协议不匹配的种子；仅单一种子时跳过探测。
// 判别原理：向端点发送垃圾字节——TLS 端点回 alert 记录(0x15/0x16)，
// 明文 Kafka 解析失败会断开(EOF)。无法判定(超时)则保留，行为与未过滤一致。
func filter(seeds []string, security string) []string {
	if len(seeds) <= 1 {
		return seeds
	}
	key := security + "|" + strings.Join(seeds, ",")
	if v, ok := seedCache.Load(key); ok {
		if info := v.(seedInfo); time.Since(info.at) < time.Minute {
			return info.brokers
		}
	}

	type result struct {
		addr string
		kind string // "tls" | "plain" | "unknown"
	}
	ch := make(chan result, len(seeds))
	for _, addr := range seeds {
		go func(addr string) {
			ch <- result{addr, classifySeed(addr)}
		}(addr)
	}
	want := "plain"
	if security == "ssl" {
		want = "tls"
	}
	var keep []string
	for range seeds {
		r := <-ch
		if r.kind == want || r.kind == "unknown" {
			keep = append(keep, r.addr)
		}
	}
	if len(keep) == 0 {
		keep = seeds // 全被剔除时保留原样，让后续报错保持原貌
	}
	seedCache.Store(key, seedInfo{brokers: keep, at: time.Now()})
	return keep
}

// classifySeed 探测单个种子的协议类型。
// 原理：主动发起 TLS 握手（跳过证书校验）——
//
//	TLS 端点握手成功（或证书阶段失败）→ "tls"；
//	明文 Kafka 收到 ClientHello 后解析失败断开 → "plain"；
//	拨号失败 → "unknown"。
func classifySeed(addr string) string {
	d := net.Dialer{Timeout: 700 * time.Millisecond}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return "unknown"
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	tc := tls.Client(conn, &tls.Config{
		InsecureSkipVerify: true, // 探测不校验证书
		ServerName:         "localhost",
		MinVersion:         tls.VersionTLS12,
	})
	err = tc.Handshake()
	if err == nil {
		return "tls"
	}
	// 握手推进到证书校验 = 对端确实是 TLS
	var cve *tls.CertificateVerificationError
	if errors.As(err, &cve) {
		return "tls"
	}
	// 对端回了 TLS alert = TLS 端点
	if strings.Contains(err.Error(), "remote error") {
		return "tls"
	}
	// EOF / 头部非法 / 超时 = 明文端点（收 ClientHello 后断开）
	return "plain"
}

// buildTLS 由 JKS 构建 TLS 配置。
// 与参考环境 ssl.endpoint.identification.algorithm=（关闭主机名校验）对齐：
// InsecureSkipVerify=true + 自定义链校验（仅当提供 truststore 时执行）。
func buildTLS(cfg config.Kafka) (*tls.Config, error) {
	t := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.Truststore == "" {
		// 无 truststore：完全跳过校验（等同只关主机名+不验链，标为安全热点）
		t.InsecureSkipVerify = true
		return t, nil
	}
	roots, err := loadJKSCerts(cfg.Truststore, cfg.SSLPassword)
	if err != nil {
		return nil, fmt.Errorf("读取 truststore: %w", err)
	}
	t.InsecureSkipVerify = true // 关闭主机名校验（对齐参考环境），链校验在回调中做
	t.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("服务器未提供证书")
		}
		pool := x509.NewCertPool()
		for _, c := range roots {
			pool.AddCert(c)
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		inter := x509.NewCertPool()
		for _, raw := range rawCerts[1:] {
			if c, err := x509.ParseCertificate(raw); err == nil {
				inter.AddCert(c)
			}
		}
		_, err = leaf.Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
		return err
	}
	if cfg.Keystore != "" {
		cert, err := loadJKSKeyPair(cfg.Keystore, cfg.SSLPassword)
		if err != nil {
			return nil, fmt.Errorf("读取 keystore: %w", err)
		}
		t.Certificates = []tls.Certificate{*cert}
	}
	return t, nil
}

// loadJKSCerts 读取 JKS truststore 中全部可信证书。
func loadJKSCerts(path, password string) ([]*x509.Certificate, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ks, err := keystore.Decode(f, []byte(password))
	if err != nil {
		return nil, err
	}
	var certs []*x509.Certificate
	for _, entry := range ks {
		// Decode 存的是指针；兼容值类型以防版本差异
		var tce *keystore.TrustedCertificateEntry
		switch e := entry.(type) {
		case *keystore.TrustedCertificateEntry:
			tce = e
		case keystore.TrustedCertificateEntry:
			tce = &e
		}
		if tce == nil {
			continue
		}
		c, err := x509.ParseCertificate(tce.Certificate.Content)
		if err != nil {
			return nil, err
		}
		certs = append(certs, c)
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("truststore 中没有证书: %s", path)
	}
	return certs, nil
}

// loadJKSKeyPair 读取 JKS keystore 中的客户端证书链与私钥（mTLS 用）。
func loadJKSKeyPair(path, password string) (*tls.Certificate, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ks, err := keystore.Decode(f, []byte(password))
	if err != nil {
		return nil, err
	}
	for _, entry := range ks {
		var pke *keystore.PrivateKeyEntry
		switch e := entry.(type) {
		case *keystore.PrivateKeyEntry:
			pke = e
		case keystore.PrivateKeyEntry:
			pke = &e
		}
		if pke == nil || len(pke.CertChain) == 0 {
			continue
		}
		key, err := parsePrivateKey(pke.PrivKey)
		if err != nil {
			return nil, err
		}
		var chain [][]byte
		for _, c := range pke.CertChain {
			chain = append(chain, c.Content)
		}
		return &tls.Certificate{PrivateKey: key, Certificate: chain}, nil
	}
	return nil, fmt.Errorf("keystore 中没有客户端私钥条目: %s", path)
}

// parsePrivateKey 兼容 PKCS#8（JKS 常见）、PKCS#1（RSA）与 SEC1（EC）。
func parsePrivateKey(der []byte) (any, error) {
	if k, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		return k, nil
	}
	if k, err := x509.ParseECPrivateKey(der); err == nil {
		return k, nil
	}
	return x509.ParsePKCS1PrivateKey(der)
}

func (a *kafkaAdapter) client() (*kgo.Client, error) {
	return kgo.NewClient(a.opts...)
}

func (a *kafkaAdapter) Test(ctx context.Context) error {
	cl, err := a.client()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if _, err := adm.Metadata(ctx); err != nil {
		return fmt.Errorf("元数据拉取失败: %w", err)
	}
	return nil
}

func (a *kafkaAdapter) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	cl, err := a.client()
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	details, err := kadm.NewClient(cl).ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	if err := details.Error(); err != nil {
		return nil, err
	}
	details.FilterInternal()
	out := make([]TopicInfo, 0, len(details))
	for _, t := range details.Sorted() {
		out = append(out, TopicInfo{Name: t.Topic, Partitions: len(t.Partitions), Depth: -1})
	}
	return out, nil
}

// Browse 零副作用浏览：独立 consumer、手动 Assign/Seek、无消费组、无提交。
func (a *kafkaAdapter) Browse(ctx context.Context, topic string, opt BrowseOptions) ([]Message, error) {
	if opt.Limit <= 0 {
		opt.Limit = 100
	}
	ctx, cancel := context.WithTimeout(ctx, browseTimeout)
	defer cancel()

	cl, err := a.client()
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	// 计算每个分区的起始 offset
	assign := make(map[int32]kgo.Offset)
	switch opt.Start {
	case "", "latest":
		ends, err := adm.ListEndOffsets(ctx, topic)
		if err != nil {
			return nil, err
		}
		if err := ends.Error(); err != nil {
			return nil, err
		}
		empty := true
		ends.Each(func(lo kadm.ListedOffset) {
			if lo.Topic != topic {
				return
			}
			start := lo.Offset - int64(opt.Limit) // 最新 N 条：从末尾回退
			if start < 0 {
				start = 0
			}
			if lo.Offset > 0 && start < lo.Offset {
				empty = false
			}
			assign[lo.Partition] = kgo.NewOffset().At(start)
		})
		if empty {
			return nil, nil // 空 topic 直接返回，不等超时
		}
	case "earliest":
		starts, err := adm.ListStartOffsets(ctx, topic)
		if err != nil {
			return nil, err
		}
		if err := starts.Error(); err != nil {
			return nil, err
		}
		starts.Each(func(lo kadm.ListedOffset) {
			if lo.Topic == topic {
				assign[lo.Partition] = kgo.NewOffset().At(lo.Offset)
			}
		})
		if !hasAvailable(ctx, adm, topic, assign) {
			return nil, nil
		}
	case "offset":
		starts, err := adm.ListStartOffsets(ctx, topic)
		if err != nil {
			return nil, err
		}
		if err := starts.Error(); err != nil {
			return nil, err
		}
		starts.Each(func(lo kadm.ListedOffset) {
			if lo.Topic != topic {
				return
			}
			at := opt.Offset
			if at < lo.Offset {
				at = lo.Offset
			}
			assign[lo.Partition] = kgo.NewOffset().At(at)
		})
		if !hasAvailable(ctx, adm, topic, assign) {
			return nil, nil
		}
	default:
		return nil, fmt.Errorf("未知起点: %q", opt.Start)
	}
	if len(assign) == 0 {
		return nil, nil // topic 不存在或无分区
	}

	bcl, err := kgo.NewClient(append(append([]kgo.Opt{}, a.opts...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}))...)
	if err != nil {
		return nil, err
	}
	defer bcl.Close()

	var filterRe *regexp.Regexp
	if opt.Filter != "" {
		if re, err := regexp.Compile(opt.Filter); err == nil {
			filterRe = re
		}
	}

	var msgs []Message
	for len(msgs) < opt.Limit {
		// 已有记录后：短空闲超时即返回，避免每次查询都等满整体超时
		pollCtx := ctx
		var pcancel context.CancelFunc
		if len(msgs) > 0 {
			pollCtx, pcancel = context.WithTimeout(ctx, 1200*time.Millisecond)
		}
		fs := bcl.PollFetches(pollCtx)
		if pcancel != nil {
			pcancel()
		}
		if err := fs.Err0(); err != nil {
			if pollCtx.Err() != nil {
				break // 超时/空闲：返回已收集部分
			}
			if len(fs.Records()) == 0 {
				if fe := fs.Errors(); len(fe) > 0 {
					return nil, fmt.Errorf("拉取失败: %v", fe[0].Err)
				}
				break
			}
		}
		recs := fs.Records()
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			if r.Topic != topic {
				continue
			}
			m := Message{
				Partition: r.Partition,
				Offset:    r.Offset,
				Time:      r.Timestamp,
				Key:       string(r.Key),
				Value:     string(r.Value),
			}
			for _, h := range r.Headers {
				m.Headers = append(m.Headers, Header{Key: h.Key, Value: string(h.Value)})
			}
			if filterRe != nil {
				if !filterRe.MatchString(m.Value) && !filterRe.MatchString(m.Key) {
					continue
				}
			} else if opt.Filter != "" {
				if !strings.Contains(m.Value, opt.Filter) && !strings.Contains(m.Key, opt.Filter) {
					continue
				}
			}
			msgs = append(msgs, m)
			if len(msgs) >= opt.Limit {
				break
			}
		}
	}

	sort.Slice(msgs, func(i, j int) bool {
		if !msgs[i].Time.Equal(msgs[j].Time) {
			return msgs[i].Time.Before(msgs[j].Time)
		}
		if msgs[i].Partition != msgs[j].Partition {
			return msgs[i].Partition < msgs[j].Partition
		}
		return msgs[i].Offset < msgs[j].Offset
	})
	return msgs, nil
}

// ListGroups 列出消费组与 lag（读 committed offset + end offset，只读操作）。
func (a *kafkaAdapter) ListGroups(ctx context.Context) ([]GroupRow, error) {
	cl, err := a.client()
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	groups, err := adm.ListGroups(ctx)
	if err != nil {
		return nil, err
	}

	var rows []GroupRow
	for _, g := range groups.Sorted() {
		if g.ProtocolType != "" && g.ProtocolType != "consumer" {
			continue
		}
		committed, err := adm.FetchOffsets(ctx, g.Group)
		if err != nil {
			continue // 单个组失败不影响整体
		}
		// 收集该组涉及的 topic，拉 end offset
		topicSet := map[string]bool{}
		committed.Each(func(o kadm.OffsetResponse) {
			if o.Err == nil && o.At >= 0 && o.Topic != "" {
				topicSet[o.Topic] = true
			}
		})
		var ends kadm.ListedOffsets
		if len(topicSet) > 0 {
			topics := make([]string, 0, len(topicSet))
			for t := range topicSet {
				topics = append(topics, t)
			}
			ends, err = adm.ListEndOffsets(ctx, topics...)
			if err != nil {
				continue
			}
		}
		committed.Each(func(o kadm.OffsetResponse) {
			if o.Err != nil || o.At < 0 || o.Topic == "" {
				return
			}
			end := int64(-1)
			if lo, ok := ends.Lookup(o.Topic, o.Partition); ok {
				end = lo.Offset
			}
			lag := int64(-1)
			if end >= 0 {
				lag = end - o.At
				if lag < 0 {
					lag = 0
				}
			}
			rows = append(rows, GroupRow{
				Group:     g.Group,
				Topic:     o.Topic,
				Partition: o.Partition,
				Committed: o.At,
				End:       end,
				Lag:       lag,
			})
		})
	}
	return rows, nil
}

// hasAvailable 判断 assign 的起始位置到末尾之间是否还有消息，避免空等超时。
func hasAvailable(ctx context.Context, adm *kadm.Client, topic string, assign map[int32]kgo.Offset) bool {
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return true // 拉不到就让消费阶段处理
	}
	avail := int64(0)
	ends.Each(func(lo kadm.ListedOffset) {
		if lo.Topic != topic {
			return
		}
		if o, ok := assign[lo.Partition]; ok {
			// kgo.Offset 内部起始值不可直接读，这里用 end>start 粗判：end>0 即可能有消息
			_ = o
			if lo.Offset > 0 {
				avail += lo.Offset
			}
		}
	})
	return avail > 0
}

// Send 发送一条 Kafka 消息。Partition < 0 时由分区器自动选择。
func (a *kafkaAdapter) Bounds(ctx context.Context, topic string) (int64, int64, error) {
	cl, err := a.client()
	if err != nil {
		return 0, 0, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, 0, err
	}
	if err := starts.Error(); err != nil {
		return 0, 0, err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, 0, err
	}
	if err := ends.Error(); err != nil {
		return 0, 0, err
	}
	var minStart int64 = -1
	var total int64
	starts.Each(func(lo kadm.ListedOffset) {
		if lo.Topic != topic || lo.Offset < 0 {
			return
		}
		if minStart < 0 || lo.Offset < minStart {
			minStart = lo.Offset
		}
		if e, ok := ends.Lookup(lo.Topic, lo.Partition); ok && e.Offset > lo.Offset {
			total += e.Offset - lo.Offset
		}
	})
	if minStart < 0 {
		return 0, 0, fmt.Errorf("topic 不存在或无分区: %q", topic)
	}
	return minStart, total, nil
}

func (a *kafkaAdapter) Send(ctx context.Context, opt SendOptions) (SendResult, error) {
	if opt.Target == "" {
		return SendResult{}, fmt.Errorf("目标 topic 不能为空")
	}
	opts := append([]kgo.Opt{}, a.opts...)
	if opt.Partition >= 0 {
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return SendResult{}, err
	}
	defer cl.Close()
	rec := &kgo.Record{
		Topic: opt.Target,
		Key:   []byte(opt.Key),
		Value: []byte(opt.Value),
	}
	if opt.Partition >= 0 {
		rec.Partition = opt.Partition
	}
	pr, err := cl.ProduceSync(ctx, rec).First()
	if err != nil {
		return SendResult{}, fmt.Errorf("发送失败: %w", err)
	}
	return SendResult{Partition: pr.Partition, Offset: pr.Offset}, nil
}

// Purge Kafka 无"清队列"语义，给出明确指引。
func (a *kafkaAdapter) Purge(context.Context, string) error {
	return fmt.Errorf("不支持清除 Kafka 队列消息；可调整 retention 或删除 topic（阶段 5+ 视需求提供）")
}

// SubscribeKafkaLatest Kafka 实时跟踪：从各分区最新位点起持续长轮询拉取。
// 零副作用：无消费组、不提交 offset（与 Browse 同一套 Assign/Seek 机制）。
// onMsg 在拉取 goroutine 回调，调用方负责线程安全（UI 应转 Synchronize）。
func SubscribeKafkaLatest(cfg config.Kafka, topic string, onMsg func(Message), onErr func(error)) (func() error, error) {
	opts, _, err := kafkaOpts(cfg)
	if err != nil {
		return nil, err
	}
	// 先取各分区末尾位点（起点=最新，只收订阅之后的新消息）
	probe, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	ends, err := kadm.NewClient(probe).ListEndOffsets(ctx, topic)
	cancel()
	probe.Close()
	if err != nil {
		return nil, fmt.Errorf("获取分区位点失败: %w", err)
	}
	assign := make(map[int32]kgo.Offset)
	ends.Each(func(lo kadm.ListedOffset) {
		if lo.Topic == topic && lo.Offset >= 0 {
			assign[lo.Partition] = kgo.NewOffset().At(lo.Offset)
		}
	})
	if len(assign) == 0 {
		return nil, fmt.Errorf("topic 不存在或无可用分区: %q", topic)
	}

	cl, err := kgo.NewClient(append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}))...)
	if err != nil {
		return nil, err
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			fs := cl.PollFetches(runCtx)
			if runCtx.Err() != nil {
				return // 主动停止
			}
			if err := fs.Err0(); err != nil {
				if onErr != nil {
					onErr(fmt.Errorf("实时拉取中断: %w", err))
				}
				return
			}
			for _, r := range fs.Records() {
				if r.Topic != topic || onMsg == nil {
					continue
				}
				m := Message{
					Partition: r.Partition,
					Offset:    r.Offset,
					Time:      r.Timestamp,
					Key:       string(r.Key),
					Value:     string(r.Value),
				}
				for _, h := range r.Headers {
					m.Headers = append(m.Headers, Header{Key: h.Key, Value: string(h.Value)})
				}
				onMsg(m)
			}
		}
	}()

	stopped := false
	stop := func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancelRun()
		<-done // 等拉取 goroutine 退出后再关连接，避免并发使用
		cl.Close()
		return nil
	}
	return stop, nil
}

// prettyJSON 若 value 是合法 JSON 则美化输出，否则原样返回。
func prettyJSON(s string) string {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return s
	}
	return string(b)
}
