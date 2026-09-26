// Package adapter 定义 broker 统一访问接口（List/Browse/Send/Groups/Test）。
// 浏览操作必须零副作用：Kafka 不进消费组、不提交 offset；ActiveMQ browse 只读。
package adapter

import (
	"context"
	"fmt"
	"time"

	"kafak-tools/internal/config"
)

// TopicInfo 目的地概览。Partitions 用于 Kafka；Depth 用于 ActiveMQ 积压条数（无数据为 -1）。
type TopicInfo struct {
	Name       string
	Partitions int
	Depth      int64
}

// Header 消息头（Kafka record header；ActiveMQ 暂不提供）。
type Header struct {
	Key   string
	Value string
}

// Message 一条被浏览到的消息。
type Message struct {
	Partition int32
	Offset    int64
	Time      time.Time
	Key       string
	Value     string
	Headers   []Header
}

// GroupRow 消费者组 lag 行（ActiveMQ 无此概念，返回空）。
type GroupRow struct {
	Group     string
	Topic     string
	Partition int32
	Committed int64 // 未提交为 -1
	End       int64
	Lag       int64
}

// BrowseOptions 浏览参数。Start: earliest | latest（ActiveMQ 下 latest=最新 N 条）。
type BrowseOptions struct {
	Start  string
	Offset int64
	Limit  int
	Filter string // 子串或正则，空=不过滤
}

// SendOptions 发送参数。Partition < 0 表示自动分区。
type SendOptions struct {
	Target    string
	Partition int32
	Key       string
	Value     string
}

// SendResult 发送结果。Kafka 填分区/偏移；ActiveMQ 填 MsgID（broker 返回的 JMSMessageID）。
type SendResult struct {
	Partition int32
	Offset    int64
	MsgID     string
}

// Adapter broker 统一访问接口。
type Adapter interface {
	// Test 连通性测试。
	Test(ctx context.Context) error
	// ListTopics 列出目的地（Kafka=topic；ActiveMQ=queue+topic）。
	ListTopics(ctx context.Context) ([]TopicInfo, error)
	// Browse 零副作用浏览消息。
	Browse(ctx context.Context, topic string, opt BrowseOptions) ([]Message, error)
	// ListGroups 消费者组及其 lag（ActiveMQ 返回空）。
	ListGroups(ctx context.Context) ([]GroupRow, error)
	// Bounds 返回目的地可浏览消息总数（Kafka=Σ(高水位-最早位点)；ActiveMQ=队列积压）。
	// start 为最早位点（ActiveMQ 恒为 0）。
	Bounds(ctx context.Context, topic string) (start, total int64, err error)
	// Send 发送一条消息。
	Send(ctx context.Context, opt SendOptions) (SendResult, error)
	// Purge 清除目的地中的消息（ActiveMQ 队列；Kafka 返回不支持说明）。
	Purge(ctx context.Context, dest string) error
}

// New 按连接类型构造适配器。
func New(c config.Connection) (Adapter, error) {
	switch c.Type {
	case config.TypeKafka:
		return newKafka(c.Kafka)
	case config.TypeActiveMQ:
		return newActiveMQ(c.ActiveMQ)
	default:
		return nil, fmt.Errorf("未知连接类型: %q", c.Type)
	}
}
