package adapter

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pavlo-v-chernykh/keystore-go"
	"github.com/twmb/franz-go/pkg/kfake"

	"kafak-tools/internal/config"
)

const testStorePass = "test-pass"

// certPair 持有测试 CA（证书 + 私钥），可签发叶子证书。
type certPair struct {
	caDER []byte
	caKey *ecdsa.PrivateKey
}

func genCertPair(t *testing.T) *certPair {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kafak-tools-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return &certPair{caDER: der, caKey: caKey}
}

// signLeaf 用 CA 签发叶子证书，返回 (DER, 私钥)。
func (c *certPair) signLeaf(t *testing.T, usage x509.ExtKeyUsage) ([]byte, any) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(c.caDER)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		DNSNames:     []string{"localhost"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, c.caKey)
	if err != nil {
		t.Fatal(err)
	}
	return der, key
}

// writeJKSTrust 仅写 truststore（CA）。
func writeJKSTrust(t *testing.T, path string, caDER []byte) {
	t.Helper()
	ks := keystore.KeyStore{}
	ks["ca"] = &keystore.TrustedCertificateEntry{
		Certificate: keystore.Certificate{Type: "X.509", Content: caDER},
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := keystore.Encode(f, ks, []byte(testStorePass)); err != nil {
		t.Fatalf("encode trust: %v", err)
	}
}

// writeJKSKeyPair 写 keystore（客户端证书链 + PKCS#8 私钥）。
func writeJKSKeyPair(t *testing.T, path string, caDER, leafDER []byte, leafKey any) {
	t.Helper()
	pkcs8, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	ks := keystore.KeyStore{}
	ks["client"] = &keystore.PrivateKeyEntry{
		PrivKey: pkcs8,
		CertChain: []keystore.Certificate{
			{Type: "PrivateKeyEntry", Content: leafDER},
			{Type: "PrivateKeyEntry", Content: caDER},
		},
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := keystore.Encode(f, ks, []byte(testStorePass)); err != nil {
		t.Fatalf("encode key: %v", err)
	}
}

func TestBuildTLS(t *testing.T) {
	dir := t.TempDir()
	ca := genCertPair(t)
	trustPath := filepath.Join(dir, "trust.jks")
	writeJKSTrust(t, trustPath, ca.caDER)

	clientLeaf, clientKey := ca.signLeaf(t, x509.ExtKeyUsageClientAuth)
	keyPath := filepath.Join(dir, "key.jks")
	writeJKSKeyPair(t, keyPath, ca.caDER, clientLeaf, clientKey)

	// 完整配置
	cfg := config.Kafka{Security: "ssl", Truststore: trustPath, Keystore: keyPath, SSLPassword: testStorePass}
	tlsCfg, err := buildTLS(cfg)
	if err != nil {
		t.Fatalf("buildTLS: %v", err)
	}
	if tlsCfg.VerifyPeerCertificate == nil || len(tlsCfg.Certificates) != 1 || tlsCfg.Certificates[0].PrivateKey == nil {
		t.Fatalf("tls cfg incomplete: cert=%d", len(tlsCfg.Certificates))
	}

	// 链校验：同 CA 签发 → 通过
	serverLeaf, _ := ca.signLeaf(t, x509.ExtKeyUsageServerAuth)
	if err := tlsCfg.VerifyPeerCertificate([][]byte{serverLeaf, ca.caDER}, nil); err != nil {
		t.Fatalf("trusted chain should pass: %v", err)
	}
	// 其他 CA 签发 → 拒绝
	other := genCertPair(t)
	badLeaf, _ := other.signLeaf(t, x509.ExtKeyUsageServerAuth)
	if err := tlsCfg.VerifyPeerCertificate([][]byte{badLeaf, other.caDER}, nil); err == nil {
		t.Fatal("foreign chain should be rejected")
	}
	// 空证书链
	if err := tlsCfg.VerifyPeerCertificate(nil, nil); err == nil {
		t.Fatal("empty chain should fail")
	}

	// 错误密码
	bad := cfg
	bad.SSLPassword = "wrong"
	if _, err := buildTLS(bad); err == nil {
		t.Fatal("wrong password should fail")
	}
	// 文件不存在
	bad2 := cfg
	bad2.Truststore = filepath.Join(dir, "nope.jks")
	if _, err := buildTLS(bad2); err == nil {
		t.Fatal("missing truststore should fail")
	}
	// 无 truststore → 完全跳过校验
	open := config.Kafka{Security: "ssl"}
	t2, err := buildTLS(open)
	if err != nil || t2.VerifyPeerCertificate != nil || !t2.InsecureSkipVerify {
		t.Fatalf("no-truststore path: %v", err)
	}
	// truststore 存的是"无证书条目" → 报错
	emptyPath := filepath.Join(dir, "empty.jks")
	empty := keystore.KeyStore{}
	f, _ := os.Create(emptyPath)
	if err := keystore.Encode(f, empty, []byte(testStorePass)); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	bad3 := cfg
	bad3.Truststore = emptyPath
	if _, err := buildTLS(bad3); err == nil {
		t.Fatal("empty truststore should fail")
	}
}

// TestSSLEndToEnd kfake TLS 集群全链路（Test/List/Send/Browse over TLS）。
func TestSSLEndToEnd(t *testing.T) {
	ca := genCertPair(t)
	serverLeaf, serverKey := ca.signLeaf(t, x509.ExtKeyUsageServerAuth)
	srvTLS := &tls.Config{Certificates: []tls.Certificate{{
		Certificate: [][]byte{serverLeaf, ca.caDER},
		PrivateKey:  serverKey,
	}}}

	cl, err := kfake.NewCluster(
		kfake.SeedTopics(1, "t-ssl"),
		kfake.TLS(srvTLS),
	)
	if err != nil {
		t.Fatalf("kfake tls: %v", err)
	}
	defer cl.Close()

	dir := t.TempDir()
	trustPath := filepath.Join(dir, "trust.jks")
	writeJKSTrust(t, trustPath, ca.caDER)

	cfg := config.Kafka{
		Bootstrap:   cl.ListenAddrs()[0],
		Security:    "ssl",
		Truststore:  trustPath,
		SSLPassword: testStorePass,
	}
	a, err := New(config.Connection{ID: "s", Name: "s", Type: config.TypeKafka, Kafka: cfg})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()
	if err := a.Test(ctx); err != nil {
		t.Fatalf("ssl test: %v", err)
	}
	topics, err := a.ListTopics(ctx)
	if err != nil || len(topics) != 1 || topics[0].Name != "t-ssl" {
		t.Fatalf("ssl list: %v %v", topics, err)
	}
	res, err := a.Send(ctx, SendOptions{Target: "t-ssl", Value: "ssl-hello"})
	if err != nil || res.Offset != 0 {
		t.Fatalf("ssl send: %v %+v", err, res)
	}
	msgs, err := a.Browse(ctx, "t-ssl", BrowseOptions{Start: "earliest", Limit: 10})
	if err != nil || len(msgs) != 1 || msgs[0].Value != "ssl-hello" {
		t.Fatalf("ssl browse: %v %v", msgs, err)
	}

	// 负例：信任别的 CA → 握手/链校验失败
	other := genCertPair(t)
	otherTrust := filepath.Join(dir, "other.jks")
	writeJKSTrust(t, otherTrust, other.caDER)
	badCfg := cfg
	badCfg.Truststore = otherTrust
	ba, err := New(config.Connection{ID: "b", Name: "b", Type: config.TypeKafka, Kafka: badCfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := ba.Test(ctx); err == nil {
		t.Fatal("untrusted CA should fail")
	}
}

// TestParsePrivateKey 私钥格式兼容与错误路径。
func TestParsePrivateKey(t *testing.T) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p8, _ := x509.MarshalPKCS8PrivateKey(k)
	if _, err := parsePrivateKey(p8); err != nil {
		t.Fatalf("pkcs8: %v", err)
	}
	pk, _ := x509.MarshalECPrivateKey(k)
	if _, err := parsePrivateKey(pk); err != nil {
		t.Fatalf("sec1: %v", err)
	}
	if _, err := parsePrivateKey([]byte("garbage")); err == nil {
		t.Fatal("garbage key should fail")
	}
}

// TestPrettyJSON 美化输出。
func TestPrettyJSON(t *testing.T) {
	if prettyJSON(`{"a":1}`) == `{"a":1}` {
		t.Fatal("should be indented")
	}
	if prettyJSON("not-json") != "not-json" {
		t.Fatal("passthrough")
	}
}
