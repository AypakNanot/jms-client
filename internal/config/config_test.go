package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSecretRoundTrip(t *testing.T) {
	enc, err := encryptSecret("Optel-123456.")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if !strings.HasPrefix(enc, secretPref) {
		t.Fatalf("missing prefix: %q", enc)
	}
	if strings.Contains(enc, "Optel-123456.") {
		t.Fatalf("plaintext leaked: %q", enc)
	}
	dec, err := decryptSecret(enc)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if dec != "Optel-123456." {
		t.Fatalf("roundtrip mismatch: %q", dec)
	}
	// 明文兼容：无前缀原样返回
	if got, _ := decryptSecret("plain"); got != "plain" {
		t.Fatalf("plain passthrough failed: %q", got)
	}
}

func TestStoreSaveLoad(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)

	k := Connection{ID: "k1", Name: "本机Kafka", Type: TypeKafka, Kafka: DefaultKafka()}
	a := Connection{ID: "a1", Name: "本机AMQ", Type: TypeActiveMQ, ActiveMQ: DefaultActiveMQ()}
	s.Upsert(k)
	s.Upsert(a)
	if err := s.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	// 落盘文件中密码必须是密文
	raw, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if strings.Contains(string(raw), `"admin"`) || strings.Contains(string(raw), "Optel-123456.") {
		// admin 是默认 jolokia 密码；出现明文即失败
		if strings.Contains(string(raw), secretPref+"") && strings.Contains(string(raw), `"password": "admin"`) {
			t.Fatalf("password stored in plaintext")
		}
	}
	if !strings.Contains(string(raw), secretPref) {
		t.Fatalf("expected encrypted secret in file:\n%s", raw)
	}

	loaded, err := Load(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Connections) != 2 {
		t.Fatalf("want 2 conns, got %d", len(loaded.Connections))
	}
	loadedK, _ := loaded.Get("k1")
	loadedA, _ := loaded.Get("a1")
	if loadedK.Kafka.Bootstrap != k.Kafka.Bootstrap || loadedK.Kafka.GroupID != "opt-consumer" {
		t.Fatalf("kafka mismatch: %+v", loadedK.Kafka)
	}
	if loadedA.ActiveMQ.Password != "admin" {
		t.Fatalf("password decrypt mismatch: %q", loadedA.ActiveMQ.Password)
	}

	// Upsert / Remove
	s.Remove("k1")
	if len(s.Connections) != 1 {
		t.Fatalf("remove failed")
	}
	s.Upsert(Connection{ID: "a1", Name: "改名", Type: TypeActiveMQ, ActiveMQ: a.ActiveMQ})
	if s.Connections[0].Name != "改名" {
		t.Fatalf("upsert replace failed")
	}
}

func TestLoadMissingFile(t *testing.T) {
	s, err := Load(t.TempDir())
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if len(s.Connections) != 0 || s.Version != storeVer {
		t.Fatalf("unexpected store: %+v", s)
	}
}

func TestValidateAndDefaults(t *testing.T) {
	k := Connection{ID: "k", Name: "k", Type: TypeKafka, Kafka: DefaultKafka()}
	if err := k.Validate(); err != nil {
		t.Fatalf("default kafka should validate: %v", err)
	}
	a := Connection{ID: "a", Name: "a", Type: TypeActiveMQ, ActiveMQ: DefaultActiveMQ()}
	if err := a.Validate(); err != nil {
		t.Fatalf("default activemq should validate: %v", err)
	}

	bad := k
	bad.Name = " "
	if bad.Validate() == nil {
		t.Fatal("empty name should fail")
	}
	bad = k
	bad.Kafka.Bootstrap = "no-port"
	if bad.Validate() == nil {
		t.Fatal("bad bootstrap should fail")
	}
	bad = k
	bad.Kafka.Security = "ssl"
	bad.Kafka.Truststore = ""
	if bad.Validate() == nil {
		t.Fatal("ssl without truststore should fail")
	}
	bad = a
	bad.ActiveMQ.JolokiaURL = "ftp://x/y"
	if bad.Validate() == nil {
		t.Fatal("bad jolokia url should fail")
	}
	bad = a
	bad.ActiveMQ.STOMPEnabled = true
	bad.ActiveMQ.STOMPAddr = "no-port"
	if bad.Validate() == nil {
		t.Fatal("bad stomp addr should fail")
	}
}

func TestNewIDUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		id := NewID()
		if len(id) != 8 {
			t.Fatalf("id len: %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id: %s", id)
		}
		seen[id] = true
	}
}

func TestStoreErrorPaths(t *testing.T) {
	// 保存到不存在的目录 → 报错
	s := NewStore(filepath.Join(t.TempDir(), "no", "such", "dir"))
	s.Upsert(Connection{ID: "x", Name: "x", Type: TypeKafka, Kafka: DefaultKafka()})
	if err := s.Save(); err == nil {
		t.Fatal("save to missing dir should fail")
	}
	// 损坏的 JSON → 解析错误
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Fatal("corrupt json should fail")
	}
	// 密文非法 base64 → 解密错误
	bad := `{"version":1,"connections":[{"id":"a","name":"a","type":"activemq","activemq":{"jolokiaUrl":"http://x/api/jolokia","password":"enc:v1:@@@"}}]}`
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Fatal("bad secret should fail")
	}
}
