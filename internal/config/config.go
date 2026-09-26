// Package config 负责连接配置的本地持久化与校验。
// 配置存放在 exe 同级 connections.json（便携跟随），密码字段落盘前 AES-GCM 加密。
package config

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	fileName   = "connections.json"
	secretPref = "enc:v1:"
	storeVer   = 1
	// 静态混淆密钥材料：防随手查看，不防逆向。交付时列入安全热点待审查。
	pepper = "kafak-tools.v1"
)

// BrokerType 连接类型。
type BrokerType string

const (
	TypeKafka    BrokerType = "kafka"
	TypeActiveMQ BrokerType = "activemq"
)

// Kafka Kafka 连接参数。
type Kafka struct {
	Bootstrap   string `json:"bootstrap"` // host:port,host:port
	Security    string `json:"security"`  // plaintext | ssl
	Truststore  string `json:"truststore"`
	Keystore    string `json:"keystore"`
	SSLPassword string `json:"sslPassword"`
	GroupID     string `json:"groupId"`
}

// ActiveMQ ActiveMQ(JMS) 连接参数。Jolokia 为主通道，STOMP 为可选实时通道。
type ActiveMQ struct {
	JolokiaURL   string `json:"jolokiaUrl"`
	User         string `json:"user"`
	Password     string `json:"password"`
	STOMPAddr    string `json:"stompAddr"`
	STOMPEnabled bool   `json:"stompEnabled"`
}

// Connection 一条连接配置（内存态，密码明文；落盘时加密）。
type Connection struct {
	ID       string     `json:"id"`
	Name     string     `json:"name"`
	Type     BrokerType `json:"type"`
	Kafka    Kafka      `json:"kafka,omitempty"`
	ActiveMQ ActiveMQ   `json:"activemq,omitempty"`
}

// Store 连接配置集合。
type Store struct {
	dir         string
	Version     int          `json:"version"`
	Connections []Connection `json:"connections"`
}

// NewStore 在 dir 下创建空 store（不读盘）。
func NewStore(dir string) *Store {
	return &Store{Version: storeVer, dir: dir}
}

// Load 读取 dir/connections.json；文件不存在返回空 store。
func Load(dir string) (*Store, error) {
	s := &Store{Version: storeVer, dir: dir}
	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, s); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	s.dir = dir
	for i := range s.Connections {
		c := &s.Connections[i]
		if c.Type == TypeKafka && c.Kafka.SSLPassword != "" {
			if c.Kafka.SSLPassword, err = decryptSecret(c.Kafka.SSLPassword); err != nil {
				return nil, fmt.Errorf("decrypt ssl password of %q: %w", c.Name, err)
			}
		}
		if c.Type == TypeActiveMQ && c.ActiveMQ.Password != "" {
			if c.ActiveMQ.Password, err = decryptSecret(c.ActiveMQ.Password); err != nil {
				return nil, fmt.Errorf("decrypt password of %q: %w", c.Name, err)
			}
		}
	}
	return s, nil
}

// Save 原子写盘（密码加密后落盘）。
func (s *Store) Save() error {
	cp := make([]Connection, len(s.Connections))
	copy(cp, s.Connections)
	for i := range cp {
		c := &cp[i]
		var err error
		if c.Type == TypeKafka && c.Kafka.SSLPassword != "" {
			if c.Kafka.SSLPassword, err = encryptSecret(c.Kafka.SSLPassword); err != nil {
				return err
			}
		}
		if c.Type == TypeActiveMQ && c.ActiveMQ.Password != "" {
			if c.ActiveMQ.Password, err = encryptSecret(c.ActiveMQ.Password); err != nil {
				return err
			}
		}
	}
	out := struct {
		Version     int          `json:"version"`
		Connections []Connection `json:"connections"`
	}{s.Version, cp}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	tmp := filepath.Join(s.dir, fileName+".tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, fileName)); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}

// Get 按 ID 查找。
func (s *Store) Get(id string) (Connection, bool) {
	for _, c := range s.Connections {
		if c.ID == id {
			return c, true
		}
	}
	return Connection{}, false
}

// Upsert 按 ID 插入或替换。
func (s *Store) Upsert(c Connection) {
	for i := range s.Connections {
		if s.Connections[i].ID == c.ID {
			s.Connections[i] = c
			return
		}
	}
	s.Connections = append(s.Connections, c)
}

// Remove 按 ID 删除。
func (s *Store) Remove(id string) {
	for i := range s.Connections {
		if s.Connections[i].ID == id {
			s.Connections = append(s.Connections[:i], s.Connections[i+1:]...)
			return
		}
	}
}

// DefaultKafka 预填参考环境（tmaster2000）默认值。
func DefaultKafka() Kafka {
	return Kafka{
		Bootstrap:   "127.0.0.1:38132", // 明文默认口；SSL 口 38131 由安全选项联动
		Security:    "plaintext",
		SSLPassword: "Optel-123456.",
		GroupID:     "opt-consumer",
	}
}

// DefaultActiveMQ 预填参考环境（tmaster2000）默认值。
func DefaultActiveMQ() ActiveMQ {
	return ActiveMQ{
		JolokiaURL:   "http://127.0.0.1:8161/api/jolokia",
		User:         "admin",
		Password:     "admin",
		STOMPAddr:    "127.0.0.1:61613",
		STOMPEnabled: false, // 参考环境 activemq.xml 中 stomp 连接器默认注释
	}
}

// NewID 生成 8 位随机连接 ID。
func NewID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(int64(os.Getpid()), 16)
	}
	return hex.EncodeToString(b)
}

// Validate 连接参数校验（测试连接与保存前均走此）。
func (c Connection) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return errors.New("名称不能为空")
	}
	switch c.Type {
	case TypeKafka:
		if strings.TrimSpace(c.Kafka.Bootstrap) == "" {
			return errors.New("bootstrap 地址不能为空")
		}
		for _, ep := range strings.Split(c.Kafka.Bootstrap, ",") {
			ep = strings.TrimSpace(ep)
			if ep == "" {
				continue
			}
			host, port, err := net.SplitHostPort(ep)
			if err != nil || host == "" {
				return fmt.Errorf("bootstrap 地址格式错误: %q（应为 host:port）", ep)
			}
			p, err := strconv.Atoi(port)
			if err != nil || p < 1 || p > 65535 {
				return fmt.Errorf("bootstrap 端口非法: %q", ep)
			}
		}
		if c.Kafka.Security != "plaintext" && c.Kafka.Security != "ssl" {
			return fmt.Errorf("安全协议仅支持 plaintext/ssl: %q", c.Kafka.Security)
		}
		if c.Kafka.Security == "ssl" && strings.TrimSpace(c.Kafka.Truststore) == "" {
			return errors.New("ssl 模式需要指定 truststore(JKS)")
		}
	case TypeActiveMQ:
		u, err := url.Parse(strings.TrimSpace(c.ActiveMQ.JolokiaURL))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("非法的 Jolokia URL: %q（应为 http(s)://host:port/...）", c.ActiveMQ.JolokiaURL)
		}
		if c.ActiveMQ.STOMPEnabled && strings.TrimSpace(c.ActiveMQ.STOMPAddr) != "" {
			if h, p, err := net.SplitHostPort(c.ActiveMQ.STOMPAddr); err != nil || h == "" || p == "" {
				return fmt.Errorf("STOMP 地址格式错误: %q（应为 host:port）", c.ActiveMQ.STOMPAddr)
			}
		}
	default:
		return fmt.Errorf("未知连接类型: %q", c.Type)
	}
	return nil
}

// ---- 落盘加密（AES-256-GCM，随机 nonce） ----

func encKey() []byte {
	sum := sha256.Sum256([]byte(pepper))
	return sum[:]
}

func encryptSecret(plain string) (string, error) {
	block, err := aes.NewCipher(encKey())
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ct := gcm.Seal(nonce, nonce, []byte(plain), nil)
	return secretPref + base64.StdEncoding.EncodeToString(ct), nil
}

func decryptSecret(enc string) (string, error) {
	if !strings.HasPrefix(enc, secretPref) {
		return enc, nil // 手工编辑的明文，兼容放行
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(enc, secretPref))
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(encKey())
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
