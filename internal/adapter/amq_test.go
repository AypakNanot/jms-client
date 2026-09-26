package adapter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"kafak-tools/internal/config"
)

// fakeJolokia 造一个最小 Jolokia 服务端：
// Basic 认证 + Origin 策略 + search/read/exec(sendTextMessage|addQueue|purge|browse) 分发 + 可变队列表。
func fakeJolokia(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	queues := map[string]bool{} // 创建出来的队列
	queues["Q1"] = true

	handler := func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "admin" || pass != "admin" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Header.Get("Origin") == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": 403, "error": "Origin null is not allowed", "error_type": "java.lang.Exception",
			})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/version") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": 200, "value": map[string]any{"agent": "1.6.2", "protocol": "7.2"},
			})
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(body, &req)

		mu.Lock()
		defer mu.Unlock()

		switch req["type"] {
		case "search":
			names := []string{
				"org.apache.activemq:type=Broker,brokerName=localhost",
				"org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName=T1",
			}
			for q := range queues {
				names = append(names,
					"org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+q)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "value": names})
		case "read":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "value": map[string]any{"QueueSize": 3}})
		case "exec":
			op, _ := req["operation"].(string)
			switch {
			case strings.HasPrefix(op, "addQueue"):
				q, _ := req["arguments"].([]any)
				if len(q) == 1 {
					if name, ok := q[0].(string); ok {
						queues[name] = true
					}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "value": ""})
			case strings.HasPrefix(op, "sendTextMessage"):
				_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "value": "ID:fake-0001"})
			case op == "purge":
				_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "value": ""})
			case strings.HasPrefix(op, "browse"):
				_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "value": []map[string]any{
					{"JMSTimestamp": int64(1700000000000), "JMSCorrelationID": "c1", "Text": "payload-1"},
					{"JMSTimestamp": int64(1700000001000), "JMSCorrelationID": "c2", "Text": "payload-2"},
				}})
			default:
				_ = json.NewEncoder(w).Encode(map[string]any{"status": 400, "error": "unknown op " + op})
			}
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"status": 400, "error": "unknown type"})
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(srv.Close)
	return srv
}

func newAMQAdapter(t *testing.T, srv *httptest.Server) Adapter {
	t.Helper()
	cfg := config.ActiveMQ{
		JolokiaURL: srv.URL + "/api/jolokia",
		User:       "admin",
		Password:   "admin",
	}
	a, err := New(config.Connection{ID: "a", Name: "a", Type: config.TypeActiveMQ, ActiveMQ: cfg})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return a
}

func TestAMQJolokia(t *testing.T) {
	srv := fakeJolokia(t)
	a := newAMQAdapter(t, srv)
	ctx := context.Background()

	if err := a.Test(ctx); err != nil {
		t.Fatalf("test: %v", err)
	}

	topics, err := a.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(topics) != 2 || topics[0].Name != "Q1" || topics[0].Depth != 3 || topics[1].Name != "T1" {
		t.Fatalf("list mismatch: %+v", topics)
	}

	msgs, err := a.Browse(ctx, "Q1", BrowseOptions{Start: "earliest", Limit: 10})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if len(msgs) != 2 || msgs[0].Value != "payload-1" || msgs[0].Key != "c1" {
		t.Fatalf("browse mismatch: %+v", msgs)
	}
	if msgs[0].Time.UnixMilli() != 1700000000000 {
		t.Fatalf("timestamp mismatch: %v", msgs[0].Time)
	}

	msgs, err = a.Browse(ctx, "Q1", BrowseOptions{Start: "latest", Limit: 1})
	if err != nil {
		t.Fatalf("browse latest: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Value != "payload-2" {
		t.Fatalf("latest mismatch: %+v", msgs)
	}

	msgs, err = a.Browse(ctx, "Q1", BrowseOptions{Start: "earliest", Limit: 10, Filter: "payload-2"})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("filter mismatch: %+v", msgs)
	}

	if _, err := a.Browse(ctx, "T1", BrowseOptions{Start: "earliest"}); err == nil || !strings.Contains(err.Error(), "topic") {
		t.Fatalf("topic browse should error, got %v", err)
	}
	if _, err := a.Browse(ctx, "NOPE", BrowseOptions{Start: "earliest"}); err == nil || !strings.Contains(err.Error(), "不存在") {
		t.Fatalf("missing dest should error, got %v", err)
	}
}

func TestAMQSendAndPurge(t *testing.T) {
	srv := fakeJolokia(t)
	a := newAMQAdapter(t, srv)
	ctx := context.Background()

	// 发到已存在的队列
	res, err := a.Send(ctx, SendOptions{Target: "Q1", Value: "hello-amq"})
	if err != nil {
		t.Fatalf("send existing: %v", err)
	}
	if res.MsgID != "ID:fake-0001" {
		t.Fatalf("msg id: %+v", res)
	}

	// 发到不存在的队列 → 自动 addQueue 再发
	res, err = a.Send(ctx, SendOptions{Target: "BRAND_NEW_Q", Value: "x"})
	if err != nil {
		t.Fatalf("send new: %v", err)
	}
	if res.MsgID == "" {
		t.Fatalf("new queue send empty result: %+v", res)
	}
	topics, err := a.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list after create: %v", err)
	}
	found := false
	for _, ti := range topics {
		if ti.Name == "BRAND_NEW_Q" {
			found = true
		}
	}
	if !found {
		t.Fatalf("auto-created queue not listed: %+v", topics)
	}

	// purge 队列 OK / topic 报错
	if err := a.Purge(ctx, "Q1"); err != nil {
		t.Fatalf("purge queue: %v", err)
	}
	if err := a.Purge(ctx, "T1"); err == nil || !strings.Contains(err.Error(), "不是队列") {
		t.Fatalf("purge topic should error, got %v", err)
	}
}

// TestAMQAuthFail 认证失败路径。
func TestAMQAuthFail(t *testing.T) {
	srv := fakeJolokia(t)
	cfg := config.ActiveMQ{JolokiaURL: srv.URL + "/api/jolokia", User: "admin", Password: "wrong"}
	a, err := New(config.Connection{ID: "a", Name: "a", Type: config.TypeActiveMQ, ActiveMQ: cfg})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := a.Test(context.Background()); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("expect 401 error, got %v", err)
	}
}

// TestKafkaPurgeUnsupported Kafka 侧 purge 明确不支持。
func TestKafkaPurgeUnsupported(t *testing.T) {
	_, cfg := newFake(t, "t-purge")
	a, err := New(config.Connection{ID: "k", Name: "k", Type: config.TypeKafka, Kafka: cfg})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := a.Purge(context.Background(), "t-purge"); err == nil || !strings.Contains(err.Error(), "不支持") {
		t.Fatalf("expect unsupported error, got %v", err)
	}
}

// TestE2E_AMQJolokia 对本机产品实例（8161）做端到端验证。
// 运行：KAFAK_AMQ_E2E=1 go test ./internal/adapter/ -run E2E_AMQ -v
func TestE2E_AMQJolokia(t *testing.T) {
	if os.Getenv("KAFAK_AMQ_E2E") != "1" {
		t.Skip("set KAFAK_AMQ_E2E=1 to run against local ActiveMQ")
	}
	cfg := config.ActiveMQ{
		JolokiaURL: "http://127.0.0.1:8161/api/jolokia",
		User:       "admin",
		Password:   "admin",
	}
	a, err := New(config.Connection{ID: "e2e", Name: "e2e", Type: config.TypeActiveMQ, ActiveMQ: cfg})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()
	if err := a.Test(ctx); err != nil {
		t.Fatalf("test: %v", err)
	}
	topics, err := a.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	t.Logf("destinations: %d", len(topics))
	for _, ti := range topics {
		t.Logf("  %s depth=%d", ti.Name, ti.Depth)
	}

	// 发送→浏览→purge 全闭环（自动建队列）；先清场保证幂等
	const q = "KFK_TOOL_E2E"
	_ = a.Purge(ctx, q) // 队列不存在时忽略错误
	res, err := a.Send(ctx, SendOptions{Target: q, Value: "e2e-amq-roundtrip"})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	t.Logf("sent msgId=%s", res.MsgID)
	msgs, err := a.Browse(ctx, q, BrowseOptions{Start: "earliest", Limit: 10, Filter: "e2e-amq-roundtrip"})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Value != "e2e-amq-roundtrip" {
		t.Fatalf("roundtrip mismatch: %+v", msgs)
	}
	if err := a.Purge(ctx, q); err != nil {
		t.Fatalf("purge: %v", err)
	}
	msgs, err = a.Browse(ctx, q, BrowseOptions{Start: "earliest", Limit: 10})
	if err != nil {
		t.Fatalf("browse after purge: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("purge not effective: %d msgs left", len(msgs))
	}
	// 清理：直接调内部 post 删除测试队列（工具本身不提供删除功能）
	if amq, ok := a.(*amqAdapter); ok {
		_, _, mbean, broker, serr := amq.searchDestinations(ctx)
		if serr == nil && broker != "" {
			_, _ = amq.post(ctx, map[string]any{
				"type":      "exec",
				"mbean":     broker,
				"operation": "removeQueue(java.lang.String)",
				"arguments": []any{q},
			})
			_ = mbean
		}
	}
	t.Log("send/browse/purge roundtrip ok, test queue removed")
}

// TestAMQUtils 杂项工具函数。
func TestAMQUtils(t *testing.T) {
	// extractText：无 text 键 → 序列化整条
	v := extractText(map[string]json.RawMessage{"JMSTimestamp": json.RawMessage("123")})
	if !strings.Contains(v, "JMSTimestamp") {
		t.Fatalf("fallback: %s", v)
	}
	// 有 text 键
	v = extractText(map[string]json.RawMessage{"Text": json.RawMessage(`"abc"`)})
	if v != "abc" {
		t.Fatalf("text: %s", v)
	}
	if firstNonEmpty("", "x", "y") != "x" || firstNonEmpty("", "") != "" {
		t.Fatal("firstNonEmpty")
	}
	if short("abcdef", 3) != "abc..." || short("ab", 3) != "ab" {
		t.Fatalf("short: %q %q", short("abcdef", 3), short("ab", 3))
	}
	// ActiveMQ ListGroups 恒空
	srv := fakeJolokia(t)
	a := newAMQAdapter(t, srv)
	rows, err := a.ListGroups(context.Background())
	if err != nil || rows != nil {
		t.Fatalf("amq groups: %v %v", rows, err)
	}
}

// TestAMQBounds 队列积压总数。
func TestAMQBounds(t *testing.T) {
	srv := fakeJolokia(t)
	a := newAMQAdapter(t, srv)
	ctx := context.Background()
	_, total, err := a.Bounds(ctx, "Q1")
	if err != nil || total != 3 {
		t.Fatalf("queue bounds: total=%d err=%v", total, err)
	}
	_, total, err = a.Bounds(ctx, "T1") // topic 无存储
	if err != nil || total != 0 {
		t.Fatalf("topic bounds: total=%d err=%v", total, err)
	}
}
