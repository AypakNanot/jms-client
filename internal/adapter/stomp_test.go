package adapter

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"kafak-tools/internal/config"
)

// fakeSTOMP 极简 STOMP 服务端：CONNECT/STOMP→CONNECTED，SUBSCRIBE→推送一条 MESSAGE。
func fakeSTOMP(t *testing.T) (addr string, gotSub chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	gotSub = make(chan string, 1)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				for {
					frame, err := readSTOMPFrame(br)
					if err != nil {
						return
					}
					cmd, headers := parseSTOMPFrame(frame)
					switch cmd {
					case "CONNECT", "STOMP":
						if _, err := io.WriteString(c, "CONNECTED\nversion:1.2\n\n\x00"); err != nil {
							return
						}
					case "SUBSCRIBE":
						id := headers["id"]
						dest := headers["destination"]
						select {
						case gotSub <- dest:
						default:
						}
						body := "live-hello"
						msg := fmt.Sprintf("MESSAGE\nsubscription:%s\nmessage-id:1\ndestination:%s\ncontent-length:%d\n\n%s\x00",
							id, dest, len(body), body)
						if _, err := io.WriteString(c, msg); err != nil {
							return
						}
						// SUBSCRIBE 若带 receipt 也要回执
						if rid := headers["receipt"]; rid != "" {
							if _, err := io.WriteString(c, "RECEIPT\nreceipt-id:"+rid+"\n\n\x00"); err != nil {
								return
							}
						}
					case "UNSUBSCRIBE", "DISCONNECT":
						// go-stomp 默认等待 RECEIPT
						if rid := headers["receipt"]; rid != "" {
							if _, err := io.WriteString(c, "RECEIPT\nreceipt-id:"+rid+"\n\n\x00"); err != nil {
								return
							}
						}
						if cmd == "DISCONNECT" {
							return
						}
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), gotSub
}

// readSTOMPFrame 读到 NUL 结尾。
func readSTOMPFrame(br *bufio.Reader) (string, error) {
	var sb strings.Builder
	for {
		b, err := br.ReadByte()
		if err != nil {
			return "", err
		}
		if b == 0x00 {
			return sb.String(), nil
		}
		sb.WriteByte(b)
	}
}

func parseSTOMPFrame(frame string) (cmd string, headers map[string]string) {
	lines := strings.Split(frame, "\n")
	if len(lines) == 0 {
		return "", map[string]string{}
	}
	cmd = strings.TrimSpace(lines[0])
	headers = map[string]string{}
	for _, l := range lines[1:] {
		if l == "" {
			break // headers 结束
		}
		k, v, ok := strings.Cut(l, ":")
		if ok {
			headers[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return cmd, headers
}

func TestSubscribeTopicLive(t *testing.T) {
	addr, gotSub := fakeSTOMP(t)
	cfg := config.ActiveMQ{STOMPEnabled: true, STOMPAddr: addr}

	got := make(chan Message, 1)
	stop, err := SubscribeTopic(cfg, "Live.Topic", func(m Message) {
		got <- m
	}, func(error) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	select {
	case dest := <-gotSub:
		if dest != "/topic/Live.Topic" {
			t.Fatalf("dest: %s", dest)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no SUBSCRIBE seen")
	}
	select {
	case m := <-got:
		if m.Value != "live-hello" {
			t.Fatalf("msg: %+v", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no MESSAGE delivered")
	}

	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := stop(); err != nil {
		t.Fatalf("stop idempotent: %v", err)
	}
}

func TestSubscribeTopicErrors(t *testing.T) {
	// 未启用
	if _, err := SubscribeTopic(config.ActiveMQ{}, "X", nil, nil); err == nil || !strings.Contains(err.Error(), "未启用") {
		t.Fatalf("disabled: %v", err)
	}
	// 地址为空
	if _, err := SubscribeTopic(config.ActiveMQ{STOMPEnabled: true}, "X", nil, nil); err == nil || !strings.Contains(err.Error(), "地址为空") {
		t.Fatalf("empty addr: %v", err)
	}
	// 拒绝连接（拿一个已关闭的端口）
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	_ = ln.Close()
	cfg := config.ActiveMQ{STOMPEnabled: true, STOMPAddr: deadAddr}
	if _, err := SubscribeTopic(cfg, "X", nil, nil); err == nil || !strings.Contains(err.Error(), "STOMP 连接失败") {
		t.Fatalf("dial fail: %v", err)
	}
}

// TestE2E_STOMPLive Docker ActiveMQ 真机：Jolokia 建 topic 发送 → STOMP 订阅收到。
// 运行：KAFAK_STOMP_E2E=1 go test ./internal/adapter/ -run E2E_STOMP -v
func TestE2E_STOMPLive(t *testing.T) {
	if os.Getenv("KAFAK_STOMP_E2E") != "1" {
		t.Skip("set KAFAK_STOMP_E2E=1 (docker amq-stomp on 61613/8162)")
	}
	jolokia := config.ActiveMQ{
		JolokiaURL: "http://127.0.0.1:8162/api/jolokia",
		User:       "admin",
		Password:   "admin",
	}
	a, err := New(config.Connection{ID: "s", Name: "s", Type: config.TypeActiveMQ, ActiveMQ: jolokia})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()
	if err := a.Test(ctx); err != nil {
		t.Fatalf("jolokia: %v", err)
	}

	// 显式建 topic
	amq := a.(*amqAdapter)
	_, _, _, broker, err := amq.searchDestinations(ctx)
	if err != nil || broker == "" {
		t.Fatalf("broker mbean: %v", err)
	}
	if _, err := amq.post(ctx, map[string]any{
		"type": "exec", "mbean": broker,
		"operation": "addTopic(java.lang.String)",
		"arguments": []any{"E2E_LIVE"},
	}); err != nil {
		t.Fatalf("addTopic: %v", err)
	}

	// STOMP 订阅
	got := make(chan Message, 1)
	stompCfg := config.ActiveMQ{STOMPEnabled: true, STOMPAddr: "127.0.0.1:61613"}
	stop, err := SubscribeTopic(stompCfg, "E2E_LIVE", func(m Message) {
		got <- m
	}, func(error) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = stop() }()

	// Jolokia 发送 → 订阅端应收到
	marker := "stomp-e2e-" + time.Now().Format("150405.000")
	if _, err := a.Send(ctx, SendOptions{Target: "E2E_LIVE", Value: marker}); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case m := <-got:
		if m.Value != marker {
			t.Fatalf("got %q want %q", m.Value, marker)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("STOMP subscriber did not receive in 5s")
	}
	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	t.Logf("stomp live ok: received %q", marker)
}
