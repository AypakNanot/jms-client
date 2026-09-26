package adapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafak-tools/internal/config"
)

// newFake 造一个进程内 mock 集群并预置 topic。
func newFake(t *testing.T, topic string) (*kfake.Cluster, config.Kafka) {
	t.Helper()
	cl, err := kfake.NewCluster(kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(func() { cl.Close() })
	cfg := config.Kafka{
		Bootstrap: cl.ListenAddrs()[0],
		Security:  "plaintext",
	}
	return cl, cfg
}

// produce 往 topic 发 n 条 "msg-i"。
func produce(t *testing.T, brokers []string, topic string, n int) {
	t.Helper()
	kcl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("produce client: %v", err)
	}
	defer kcl.Close()
	var rs []*kgo.Record
	for i := 0; i < n; i++ {
		rs = append(rs, &kgo.Record{
			Topic: topic,
			Value: []byte(fmt.Sprintf("msg-%d", i)),
		})
	}
	if err := kcl.ProduceSync(context.Background(), rs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func TestListAndBrowse(t *testing.T) {
	_, cfg := newFake(t, "t-alpha")
	brokers := []string{cfg.Bootstrap}
	produce(t, brokers, "t-alpha", 3)

	a, err := New(config.Connection{ID: "k", Name: "k", Type: config.TypeKafka, Kafka: cfg})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	ctx := context.Background()

	if err := a.Test(ctx); err != nil {
		t.Fatalf("test: %v", err)
	}

	topics, err := a.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(topics) != 1 || topics[0].Name != "t-alpha" || topics[0].Partitions != 1 {
		t.Fatalf("topics mismatch: %+v", topics)
	}

	// earliest 全量
	msgs, err := a.Browse(ctx, "t-alpha", BrowseOptions{Start: "earliest", Limit: 10})
	if err != nil {
		t.Fatalf("browse earliest: %v", err)
	}
	if len(msgs) != 3 || msgs[0].Value != "msg-0" || msgs[2].Value != "msg-2" {
		t.Fatalf("earliest mismatch: %+v", msgs)
	}

	// latest 回退 2 条
	msgs, err = a.Browse(ctx, "t-alpha", BrowseOptions{Start: "latest", Limit: 2})
	if err != nil {
		t.Fatalf("browse latest: %v", err)
	}
	if len(msgs) != 2 || msgs[0].Value != "msg-1" || msgs[1].Value != "msg-2" {
		t.Fatalf("latest mismatch: %+v", msgs)
	}

	// 指定 offset
	msgs, err = a.Browse(ctx, "t-alpha", BrowseOptions{Start: "offset", Offset: 1, Limit: 10})
	if err != nil {
		t.Fatalf("browse offset: %v", err)
	}
	if len(msgs) != 2 || msgs[0].Offset != 1 {
		t.Fatalf("offset mismatch: %+v", msgs)
	}

	// 过滤（正则）
	msgs, err = a.Browse(ctx, "t-alpha", BrowseOptions{Start: "earliest", Limit: 10, Filter: "msg-[12]$"})
	if err != nil {
		t.Fatalf("browse filter: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("filter mismatch: %+v", msgs)
	}

	// 零副作用：浏览后 broker 上不得出现消费组
	kcl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Bootstrap))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer kcl.Close()
	groups, err := kadm.NewClient(kcl).ListGroups(ctx)
	if err == nil && len(groups.Groups()) != 0 {
		t.Fatalf("browse leaked consumer groups: %v", groups.Groups())
	}
}

func TestBrowseEmptyTopicFast(t *testing.T) {
	_, cfg := newFake(t, "t-empty")
	a, err := New(config.Connection{ID: "k", Name: "k", Type: config.TypeKafka, Kafka: cfg})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	start := time.Now()
	msgs, err := a.Browse(context.Background(), "t-empty", BrowseOptions{Start: "latest", Limit: 10})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expect empty, got %d", len(msgs))
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("empty topic browse too slow: %v", d)
	}
}

// TestE2E_LocalBroker 对本机真实 broker（test-env）做端到端验证。
// 运行：KAFAK_E2E=1 go test ./internal/adapter/ -run E2E -v
func TestE2E_LocalBroker(t *testing.T) {
	if os.Getenv("KAFAK_E2E") != "1" {
		t.Skip("set KAFAK_E2E=1 to run against local broker")
	}
	cfg := config.Kafka{Bootstrap: "127.0.0.1:38132", Security: "plaintext"}
	a, err := New(config.Connection{ID: "e2e", Name: "e2e", Type: config.TypeKafka, Kafka: cfg})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()

	if err := a.Test(ctx); err != nil {
		t.Fatalf("test conn: %v", err)
	}

	topics, err := a.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	found := false
	for _, ti := range topics {
		if ti.Name == "OPTEL_Fault" {
			found = true
		}
	}
	if !found {
		t.Fatalf("OPTEL_Fault not found in %v", topics)
	}

	// 浏览前的组状态（基线）
	before := groupSnapshot(t, cfg)
	msgs, err := a.Browse(ctx, "OPTEL_Fault", BrowseOptions{Start: "earliest", Limit: 100})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if len(msgs) < 3 {
		t.Fatalf("expect >=3 messages, got %d", len(msgs))
	}
	if msgs[0].Offset != 0 {
		t.Fatalf("first offset expect 0: %+v", msgs[0])
	}
	// 中文与 JSON 内容完整
	if msgs[2].Value == "" {
		t.Fatalf("empty value: %+v", msgs[2])
	}

	// 浏览后组状态必须零变化（完成标志）
	after := groupSnapshot(t, cfg)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("consumer groups changed by browse!\nbefore=%v\nafter=%v", before, after)
	}
	t.Logf("browse ok: %d msgs, groups unchanged (%d groups), topics=%d", len(msgs), len(before), len(topics))
}

// groupSnapshot 抓取当前组及提交 offset 指纹。
func groupSnapshot(t *testing.T, cfg config.Kafka) map[string]string {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Bootstrap))
	if err != nil {
		t.Fatalf("snap client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	groups, err := adm.ListGroups(ctx)
	if err != nil {
		return map[string]string{"err": err.Error()}
	}
	out := map[string]string{}
	for _, g := range groups.Sorted() {
		offs, err := adm.FetchOffsets(ctx, g.Group)
		if err != nil {
			out[g.Group] = "fetch-err:" + err.Error()
			continue
		}
		offs.Each(func(o kadm.OffsetResponse) {
			out[fmt.Sprintf("%s|%s|%d", g.Group, o.Topic, o.Partition)] = fmt.Sprintf("at=%d err=%v", o.At, o.Err)
		})
	}
	return out
}

func TestSendAndBrowse(t *testing.T) {
	_, cfg := newFake(t, "t-send")
	a, err := New(config.Connection{ID: "k", Name: "k", Type: config.TypeKafka, Kafka: cfg})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()

	// 空目标报错
	if _, err := a.Send(ctx, SendOptions{Value: "x"}); err == nil {
		t.Fatal("empty target should fail")
	}

	// 自动分区发送
	res, err := a.Send(ctx, SendOptions{Target: "t-send", Key: "k1", Value: "hello-send"})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if res.Offset != 0 || res.Partition != 0 {
		t.Fatalf("send result: %+v", res)
	}

	// 指定分区发送
	res2, err := a.Send(ctx, SendOptions{Target: "t-send", Partition: 0, Value: "manual-0"})
	if err != nil {
		t.Fatalf("send manual: %v", err)
	}
	if res2.Offset != 1 {
		t.Fatalf("manual send offset: %+v", res2)
	}

	// 浏览回读两条
	msgs, err := a.Browse(ctx, "t-send", BrowseOptions{Start: "earliest", Limit: 10})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if len(msgs) != 2 || msgs[0].Value != "hello-send" || msgs[0].Key != "k1" || msgs[1].Value != "manual-0" {
		t.Fatalf("browse after send: %+v", msgs)
	}
}

// TestE2E_SendRoundTrip 真机发送→回读闭环（KAFAK_E2E=1）。
func TestE2E_SendRoundTrip(t *testing.T) {
	if os.Getenv("KAFAK_E2E") != "1" {
		t.Skip("set KAFAK_E2E=1")
	}
	cfg := config.Kafka{Bootstrap: "127.0.0.1:38132", Security: "plaintext"}
	a, err := New(config.Connection{ID: "e2e", Name: "e2e", Type: config.TypeKafka, Kafka: cfg})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()
	marker := "e2e-roundtrip-" + time.Now().Format("150405.000")
	res, err := a.Send(ctx, SendOptions{Target: "OPTEL_Performance", Key: "e2e", Value: marker})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	msgs, err := a.Browse(ctx, "OPTEL_Performance", BrowseOptions{Start: "latest", Limit: 5, Filter: marker})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Value != marker {
		t.Fatalf("roundtrip mismatch: sent=%+v got=%+v", res, msgs)
	}
	t.Logf("send roundtrip ok: partition=%d offset=%d", res.Partition, res.Offset)
}

// TestKafkaListGroupsUnit 消费组 lag 单测（kfake 支持组协调）。
func TestKafkaListGroupsUnit(t *testing.T) {
	_, cfg := newFake(t, "t-grp")
	produce(t, []string{cfg.Bootstrap}, "t-grp", 5)

	kcl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Bootstrap))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer kcl.Close()
	adm := kadm.NewClient(kcl)
	ctx := context.Background()
	// 提交 g1 在 t-grp p0 的 offset=3
	offs := kadm.Offsets{"t-grp": {0: kadm.Offset{At: 3}}}
	if _, err := adm.CommitOffsets(ctx, "g1", offs); err != nil {
		t.Fatalf("commit: %v", err)
	}

	a, err := New(config.Connection{ID: "k", Name: "k", Type: config.TypeKafka, Kafka: cfg})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	rows, err := a.ListGroups(ctx)
	if err != nil {
		t.Fatalf("groups: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: %+v", rows)
	}
	r := rows[0]
	if r.Group != "g1" || r.Topic != "t-grp" || r.Committed != 3 || r.End != 5 || r.Lag != 2 {
		t.Fatalf("lag mismatch: %+v", r)
	}
}

// TestSeedClassify 协议探测判定（明文关连接→plain；TLS alert→tls；死端口→unknown）。
func TestSeedClassify(t *testing.T) {
	// 明文模拟：读一下就断开（Kafka 遇垃圾的典型行为）
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln1.Close()
	go func() {
		for {
			c, err := ln1.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 64)
				_, _ = c.Read(buf)
				_ = c.Close()
			}(c)
		}
	}()
	if k := classifySeed(ln1.Addr().String()); k != "plain" {
		t.Fatalf("plain listener classify: %s", k)
	}

	// TLS 模拟：自签服务端握手，收到垃圾会回 alert
	ca := genCertPair(t)
	leaf, key := ca.signLeaf(t, x509.ExtKeyUsageServerAuth)
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{{
		Certificate: [][]byte{leaf, ca.caDER},
		PrivateKey:  key,
	}}}
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()
	go func() {
		for {
			c, err := ln2.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				tc := tls.Server(c, tlsCfg)
				_ = tc.Handshake() // 垃圾输入 → 发送 alert 后失败
				_ = tc.Close()
			}(c)
		}
	}()
	if k := classifySeed(ln2.Addr().String()); k != "tls" {
		t.Fatalf("tls listener classify: %s", k)
	}

	// 死端口 → unknown
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := dead.Addr().String()
	_ = dead.Close()
	if k := classifySeed(addr); k != "unknown" {
		t.Fatalf("dead listener classify: %s", k)
	}
}

// TestSubscribeKafkaLatest 实时跟踪：只收订阅后的新消息、零副作用、stop 幂等。
func TestSubscribeKafkaLatest(t *testing.T) {
	_, cfg := newFake(t, "t-live")
	produce(t, []string{cfg.Bootstrap}, "t-live", 2) // 订阅前的旧消息不应收到

	got := make(chan Message, 8)
	stop, err := SubscribeKafkaLatest(cfg, "t-live", func(m Message) { got <- m }, func(e error) { t.Logf("live err: %v", e) })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// 订阅后发新消息
	produce(t, []string{cfg.Bootstrap}, "t-live", 1)

	select {
	case m := <-got:
		// 订阅前有 2 条(0,1)；收到的必须是订阅后新产生的 offset=2
		if m.Offset != 2 {
			t.Fatalf("expect new message at offset 2, got %+v", m)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("no live message in 8s")
	}
	// 不能串入旧消息
	select {
	case m := <-got:
		t.Fatalf("unexpected extra/old message: %+v", m)
	case <-time.After(300 * time.Millisecond):
	}

	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := stop(); err != nil {
		t.Fatalf("stop idempotent: %v", err)
	}
	// 停止后再发，不应再回调
	produce(t, []string{cfg.Bootstrap}, "t-live", 1)
	select {
	case m := <-got:
		t.Fatalf("message after stop: %+v", m)
	case <-time.After(300 * time.Millisecond):
	}

	// 零副作用：整个过程不得产生消费组
	kcl, _ := kgo.NewClient(kgo.SeedBrokers(cfg.Bootstrap))
	defer kcl.Close()
	groups, err := kadm.NewClient(kcl).ListGroups(context.Background())
	if err == nil && len(groups.Groups()) != 0 {
		t.Fatalf("live subscribe leaked groups: %v", groups.Groups())
	}
}

// TestSubscribeKafkaLatestErrors 错误路径。
func TestSubscribeKafkaLatestErrors(t *testing.T) {
	_, cfg := newFake(t, "t-live-x")
	if _, err := SubscribeKafkaLatest(cfg, "no-such-topic", nil, nil); err == nil {
		t.Fatal("missing topic should fail")
	}
}

// TestBrowseHeaders 消息头透传。
func TestBrowseHeaders(t *testing.T) {
	_, cfg := newFake(t, "t-hdr")
	kcl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Bootstrap))
	if err != nil {
		t.Fatal(err)
	}
	defer kcl.Close()
	rec := &kgo.Record{
		Topic:   "t-hdr",
		Value:   []byte("with-headers"),
		Headers: []kgo.RecordHeader{{Key: "jms_type", Value: []byte("ALARM")}, {Key: "src", Value: []byte("ne01")}},
	}
	if err := kcl.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	a, err := New(config.Connection{ID: "k", Name: "k", Type: config.TypeKafka, Kafka: cfg})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := a.Browse(context.Background(), "t-hdr", BrowseOptions{Start: "earliest", Limit: 10})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("browse: %v %v", msgs, err)
	}
	m := msgs[0]
	if len(m.Headers) != 2 || m.Headers[0].Key != "jms_type" || m.Headers[0].Value != "ALARM" || m.Headers[1].Key != "src" {
		t.Fatalf("headers mismatch: %+v", m.Headers)
	}
}

// TestBounds Kafka 消息总数/最早位点。
func TestBounds(t *testing.T) {
	_, cfg := newFake(t, "t-bounds")
	produce(t, []string{cfg.Bootstrap}, "t-bounds", 5)
	a, err := New(config.Connection{ID: "k", Name: "k", Type: config.TypeKafka, Kafka: cfg})
	if err != nil {
		t.Fatal(err)
	}
	st, tot, err := a.Bounds(context.Background(), "t-bounds")
	if err != nil {
		t.Fatalf("bounds: %v", err)
	}
	if st != 0 || tot != 5 {
		t.Fatalf("bounds mismatch: start=%d total=%d", st, tot)
	}
	// 不存在的 topic
	if _, _, err := a.Bounds(context.Background(), "nope"); err == nil {
		t.Fatal("missing topic should error")
	}
}
