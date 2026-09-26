package adapter

import (
	"fmt"
	"time"

	"github.com/go-stomp/stomp"

	"kafak-tools/internal/config"
)

// SubscribeTopic 通过 STOMP 实时订阅 JMS topic（发布订阅，无消费副作用）。
// 返回 stop 函数（幂等）。onMsg 在 STOMP 读取线程回调，调用方自行负责线程安全。
// 队列不走此通道：STOMP 订阅队列会真正消费消息（有副作用），浏览请用 Jolokia。
func SubscribeTopic(cfg config.ActiveMQ, name string, onMsg func(Message), onErr func(error)) (func() error, error) {
	if !cfg.STOMPEnabled {
		return nil, fmt.Errorf("未启用 STOMP（连接配置中勾选后重试）")
	}
	if cfg.STOMPAddr == "" {
		return nil, fmt.Errorf("STOMP 地址为空")
	}
	var dialOpts []func(*stomp.Conn) error
	if cfg.User != "" {
		dialOpts = append(dialOpts, stomp.ConnOpt.Login(cfg.User, cfg.Password))
	}
	// 关闭心跳，避免测试环境/短连接场景下的误判
	dialOpts = append(dialOpts, stomp.ConnOpt.HeartBeat(0, 0))

	conn, err := stomp.Dial("tcp", cfg.STOMPAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("STOMP 连接失败（%s）: %w", cfg.STOMPAddr, err)
	}
	sub, err := conn.Subscribe("/topic/"+name, stomp.AckAuto)
	if err != nil {
		_ = conn.Disconnect()
		return nil, fmt.Errorf("订阅 %q 失败: %w", name, err)
	}

	done := make(chan struct{})
	var closed bool
	go func() {
		for {
			select {
			case <-done:
				return
			case msg, ok := <-sub.C:
				if !ok {
					return
				}
				if msg.Err != nil {
					if onErr != nil {
						onErr(msg.Err)
					}
					return
				}
				if onMsg != nil {
					onMsg(Message{
						Time:  time.Now(),
						Value: string(msg.Body),
					})
				}
			}
		}
	}()

	stop := func() error {
		if closed {
			return nil
		}
		closed = true
		// 顺序敏感：必须在 reader 仍消费 sub.C 时完成 Unsubscribe/Disconnect
		// （go-stomp 等待 RECEIPT 回执需要 reader 持续排水），最后才停 reader。
		errUnsub := sub.Unsubscribe()
		errDisc := conn.Disconnect()
		close(done)
		if errUnsub != nil {
			return nil // 回执超时常见于对端已断开，停止语义已达成为主
		}
		return errDisc
	}
	return stop, nil
}
