package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"kafak-tools/internal/config"
)

// amqAdapter 基于 Jolokia（JMX over HTTP）的 ActiveMQ 适配器。
// 列表/浏览/积压/发送/清除均走 Jolokia，不依赖 OpenWire；STOMP 实时订阅独立接入。
type amqAdapter struct {
	cfg    config.ActiveMQ
	base   string // jolokia URL，无尾斜杠
	origin string // 部分部署有 Origin 策略，需携带同源头
	hc     *http.Client
}

func newActiveMQ(cfg config.ActiveMQ) (Adapter, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(cfg.JolokiaURL), "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("非法的 Jolokia URL: %q", cfg.JolokiaURL)
	}
	return &amqAdapter{
		cfg:    cfg,
		base:   u.String(),
		origin: u.Scheme + "://" + u.Host,
		hc:     &http.Client{Timeout: 8 * time.Second},
	}, nil
}

// jolokiaResp Jolokia 响应包。
type jolokiaResp struct {
	Status    int             `json:"status"`
	Value     json.RawMessage `json:"value"`
	Error     string          `json:"error"`
	ErrorType string          `json:"error_type"`
}

// post 发送一个 Jolokia 请求（单请求模式），返回解析后的 value。
func (a *amqAdapter) post(ctx context.Context, req map[string]any) (json.RawMessage, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Origin", a.origin) // 绕过 Jolokia Origin 策略
	if a.cfg.User != "" {
		httpReq.SetBasicAuth(a.cfg.User, a.cfg.Password)
	}
	resp, err := a.hc.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("请求 Jolokia 失败: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("认证失败 (401)，请检查用户名/密码")
	}
	var jr jolokiaResp
	if err := json.Unmarshal(data, &jr); err != nil {
		return nil, fmt.Errorf("解析 Jolokia 响应失败 (HTTP %d): %s", resp.StatusCode, short(string(data), 200))
	}
	if jr.Status != 200 || jr.Error != "" {
		return nil, fmt.Errorf("调用 Jolokia 出错: %s", firstNonEmpty(jr.Error, jr.ErrorType, fmt.Sprintf("status %d", jr.Status)))
	}
	return jr.Value, nil
}

func (a *amqAdapter) Test(ctx context.Context) error {
	u := a.base + "/version"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	httpReq.Header.Set("Origin", a.origin)
	if a.cfg.User != "" {
		httpReq.SetBasicAuth(a.cfg.User, a.cfg.Password)
	}
	resp, err := a.hc.Do(httpReq)
	if err != nil {
		return fmt.Errorf("连接 Jolokia 失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("认证失败 (401)，请检查用户名/密码")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("探测 Jolokia 版本失败: HTTP %d", resp.StatusCode)
	}
	return nil
}

// isBrokerViewMBean 精确判定 BrokerView MBean（排除 Health 等同名 type=Broker 的 MBean）。
// 形如 org.apache.activemq:brokerName=localhost,type=Broker（属性恰好两个）。
func isBrokerViewMBean(n string) bool {
	if !strings.HasPrefix(n, "org.apache.activemq:") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(n, "org.apache.activemq:"), ",")
	if len(parts) != 2 {
		return false
	}
	okType, okName := false, false
	for _, p := range parts {
		if p == "type=Broker" {
			okType = true
		} else if strings.HasPrefix(p, "brokerName=") {
			okName = true
		}
	}
	return okType && okName
}

var destNameRe = regexp.MustCompile(`destinationName=([^,]+)`)

// searchDestinations 搜索 MBean。
// 返回 queue/topic 名、目的地 MBean 映射（"Q:名"/"T:名"→objectName）与 broker MBean。
func (a *amqAdapter) searchDestinations(ctx context.Context) (queues, topics []string, mbean map[string]string, broker string, err error) {
	val, err := a.post(ctx, map[string]any{
		"type":   "search",
		"mbean":  "org.apache.activemq:type=Broker,*",
		"config": map[string]any{"ignoreErrors": true},
	})
	if err != nil {
		return nil, nil, nil, "", err
	}
	var names []string
	if err := json.Unmarshal(val, &names); err != nil {
		return nil, nil, nil, "", fmt.Errorf("search 结果解析失败: %w", err)
	}
	mbean = map[string]string{}
	for _, n := range names {
		if !strings.Contains(n, "destinationType=") {
			if broker == "" && isBrokerViewMBean(n) {
				broker = n
			}
			continue
		}
		m := destNameRe.FindStringSubmatch(n)
		if m == nil {
			continue
		}
		dest := m[1]
		// 跳过 endpoint=Consumer/Producer 视图（订阅后会多出消费者 MBean，会覆盖目的地视图）
		if strings.Contains(n, "endpoint=") {
			continue
		}
		switch {
		case strings.Contains(n, "destinationType=Queue"):
			queues = append(queues, dest)
			mbean["Q:"+dest] = n
		case strings.Contains(n, "destinationType=Topic"):
			topics = append(topics, dest)
			mbean["T:"+dest] = n
		}
	}
	sort.Strings(queues)
	sort.Strings(topics)
	return queues, topics, mbean, broker, nil
}

func (a *amqAdapter) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	queues, topics, mbean, _, err := a.searchDestinations(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]TopicInfo, 0, len(queues)+len(topics))
	for _, q := range queues {
		info := TopicInfo{Name: q, Depth: -1}
		val, err := a.post(ctx, map[string]any{
			"type":      "read",
			"mbean":     mbean["Q:"+q],
			"attribute": []string{"QueueSize"},
		})
		if err == nil {
			var attrs map[string]int64
			if json.Unmarshal(val, &attrs) == nil {
				info.Depth = attrs["QueueSize"]
			}
		}
		out = append(out, info)
	}
	for _, t := range topics {
		out = append(out, TopicInfo{Name: t, Depth: -1})
	}
	return out, nil
}

// Browse 通过 QueueViewMBean.browse() 只读浏览队列消息。
// Jolokia 无法浏览 topic（无存储）→ 返回明确错误。
func (a *amqAdapter) Browse(ctx context.Context, dest string, opt BrowseOptions) ([]Message, error) {
	_, _, mbean, _, err := a.searchDestinations(ctx)
	if err != nil {
		return nil, err
	}
	qMBean, isQueue := mbean["Q:"+dest]
	if !isQueue {
		if _, isTopic := mbean["T:"+dest]; isTopic {
			return nil, fmt.Errorf("%q 是 topic：Jolokia 无存储可浏览，可用「实时订阅」（需服务端启用 STOMP）", dest)
		}
		return nil, fmt.Errorf("目的地不存在: %q", dest)
	}
	val, err := a.post(ctx, map[string]any{
		"type":      "exec",
		"mbean":     qMBean,
		"operation": "browse()",
		"arguments": []any{},
	})
	if err != nil {
		return nil, fmt.Errorf("browse 失败: %w", err)
	}
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(val, &items); err != nil {
		return nil, fmt.Errorf("browse 结果解析失败: %w", err)
	}

	if opt.Limit <= 0 {
		opt.Limit = 100
	}
	var filterRe *regexp.Regexp
	if opt.Filter != "" {
		if re, err := regexp.Compile(opt.Filter); err == nil {
			filterRe = re
		}
	}

	msgs := make([]Message, 0, len(items))
	for i, it := range items {
		m := Message{
			Offset: int64(i), // browse 位置序号（仅展示用）
			Time:   time.Now(),
		}
		if v, ok := it["JMSTimestamp"]; ok {
			var ms int64
			if json.Unmarshal(v, &ms) == nil && ms > 0 {
				m.Time = time.UnixMilli(ms)
			}
		}
		if v, ok := it["JMSCorrelationID"]; ok {
			var s string
			if json.Unmarshal(v, &s) == nil {
				m.Key = s
			}
		}
		m.Value = extractText(it)
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
	}
	// 起点语义：latest=最新 N 条（尾部），earliest=最早 N 条（头部）
	if opt.Start == "latest" && len(msgs) > opt.Limit {
		msgs = msgs[len(msgs)-opt.Limit:]
	} else if len(msgs) > opt.Limit {
		msgs = msgs[:opt.Limit]
	}
	return msgs, nil
}

// Bounds 队列返回积压条数；topic 无存储返回 0。
func (a *amqAdapter) Bounds(ctx context.Context, dest string) (int64, int64, error) {
	_, _, mbean, _, err := a.searchDestinations(ctx)
	if err != nil {
		return 0, 0, err
	}
	q, isQueue := mbean["Q:"+dest]
	if !isQueue {
		return 0, 0, nil
	}
	val, err := a.post(ctx, map[string]any{
		"type": "read", "mbean": q, "attribute": []string{"QueueSize"},
	})
	if err != nil {
		return 0, 0, err
	}
	var attrs map[string]int64
	if err := json.Unmarshal(val, &attrs); err != nil {
		return 0, 0, err
	}
	return 0, attrs["QueueSize"], nil
}

// Send 通过 DestinationView.sendTextMessage 发送；目的地不存在则先 addQueue 创建队列。
// Key 暂不支持（Jolokia 单参重载无 correlationId 入参）。
func (a *amqAdapter) Send(ctx context.Context, opt SendOptions) (SendResult, error) {
	if opt.Target == "" {
		return SendResult{}, fmt.Errorf("目标不能为空")
	}
	_, _, mbean, broker, err := a.searchDestinations(ctx)
	if err != nil {
		return SendResult{}, err
	}
	targetMBean := mbean["Q:"+opt.Target]
	if targetMBean == "" {
		targetMBean = mbean["T:"+opt.Target]
	}
	if targetMBean == "" {
		// 目的地不存在 → 通过 BrokerView 创建（Partition==-2 约定为创建 topic）
		if broker == "" {
			return SendResult{}, fmt.Errorf("找不到 broker MBean，无法创建目的地 %q", opt.Target)
		}
		// 未知目的地默认创建为队列（可浏览）；已存在的 topic 会直接命中无需创建
		if _, err := a.post(ctx, map[string]any{
			"type":      "exec",
			"mbean":     broker,
			"operation": "addQueue(java.lang.String)",
			"arguments": []any{opt.Target},
		}); err != nil {
			return SendResult{}, fmt.Errorf("创建目的地 %q 失败: %w", opt.Target, err)
		}
		_, _, mbean, _, err = a.searchDestinations(ctx)
		if err != nil {
			return SendResult{}, err
		}
		targetMBean = mbean["Q:"+opt.Target]
		if targetMBean == "" {
			targetMBean = mbean["T:"+opt.Target]
		}
		if targetMBean == "" {
			return SendResult{}, fmt.Errorf("目的地 %q 创建后仍未找到", opt.Target)
		}
	}
	val, err := a.post(ctx, map[string]any{
		"type":      "exec",
		"mbean":     targetMBean,
		"operation": "sendTextMessage(java.lang.String)",
		"arguments": []any{opt.Value},
	})
	if err != nil {
		return SendResult{}, fmt.Errorf("发送失败: %w", err)
	}
	var msgID string
	_ = json.Unmarshal(val, &msgID)
	return SendResult{Partition: -1, Offset: -1, MsgID: msgID}, nil
}

// Purge 清除队列全部消息。
func (a *amqAdapter) Purge(ctx context.Context, dest string) error {
	_, _, mbean, _, err := a.searchDestinations(ctx)
	if err != nil {
		return err
	}
	qMBean, isQueue := mbean["Q:"+dest]
	if !isQueue {
		return fmt.Errorf("%q 不是队列，无法清除", dest)
	}
	if _, err := a.post(ctx, map[string]any{
		"type":      "exec",
		"mbean":     qMBean,
		"operation": "purge",
		"arguments": []any{},
	}); err != nil {
		return fmt.Errorf("purge 失败: %w", err)
	}
	return nil
}

// extractText 从 CompositeData 中取消息正文，兼容大小写差异；找不到则序列化整条。
func extractText(it map[string]json.RawMessage) string {
	for _, k := range []string{"Text", "text", "Body", "body", "_value"} {
		if v, ok := it[k]; ok {
			var s string
			if json.Unmarshal(v, &s) == nil {
				return s
			}
		}
	}
	b, err := json.Marshal(it)
	if err != nil {
		return ""
	}
	return string(b)
}

// ListGroups ActiveMQ 无消费者组概念，返回空。
func (a *amqAdapter) ListGroups(context.Context) ([]GroupRow, error) {
	return nil, nil
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func short(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}
