# kafak-tools

Kafka / ActiveMQ 消息调试工具（Windows 桌面、单文件便携）。

双击即用：连接管理 → 列 topic/队列 → 浏览消息（零副作用）→ 发送 → 清空 → STOMP 实时订阅。

## 快速开始

```
build.bat                # 编译 → bin\kafak-tools.exe（单文件，无运行时依赖）
bin\kafak-tools.exe      # 双击运行
```

1. 「新建连接」——类型二选一，表单已按 tmaster2000 参考环境预填：
   - **Kafka**：`127.0.0.1:38131,127.0.0.1:38132`（SSL/PLAINTEXT 双监听，JKS 证书密码预填）
   - **ActiveMQ(JMS)**：`http://127.0.0.1:8161/api/jolokia` + admin/admin，STOMP 可选
2. 「测试连接」真实连通测试；选中连接自动加载目的地/积压/消费者组
3. 「消息浏览」查询（起点/数量/文本或正则过滤）→ 点行看详情（美化 JSON / 原文 / Hex）
4. 「发送消息」发送（Kafka 自动/手动分区；ActiveMQ 未知目的地自动建队列）
5. 「删除」= 清空 ActiveMQ 队列（二次确认）；「实时订阅」= STOMP topic 实时接收

## 测试

```
go test ./...                                          # 单元测试（kfake mock Kafka + 假 Jolokia + 假 STOMP）
KAFAK_E2E=1 go test ./internal/adapter/ -run E2E       # 本机 Kafka（test-env）真机
KAFAK_AMQ_E2E=1 go test ./internal/adapter/ -run E2E   # 产品 ActiveMQ (8161) 真机
KAFAK_STOMP_E2E=1 go test ./internal/adapter/ -run E2E # Docker AMQ STOMP (61613/8162)
```

覆盖率（新代码）：adapter **81.4%**、config **82.8%**（≥80% 达标）。

## 本地测试环境（test-env/）

| 组件 | 启停 | 端点 |
|---|---|---|
| Kafka 2.8.1（产品同款，配置 1:1） | `test-env\start-test-env.bat` / `stop-test-env.bat` | SSL 38131 / PLAINTEXT 38132，ZK 复用产品 38011 |
| ActiveMQ STOMP（Docker） | `docker run -d --name amq-stomp -p 61613:61613 -p 8162:8161 apache/activemq-classic` | STOMP 61613、Jolokia 8162 |

预置 topic：`OPTEL_Fault` `OPTEL_FileTransfer` `OPTEL_Inventory` `OPTEL_Protection` `OPTEL_Performance`（详见 `test-env/README.md`，含本机三大环境坑排障）。

## 平台

- Windows 10 / Windows Server 2016（Go ≥1.21 支持下限；manifest 内嵌 Common Controls v6）
- 编译：`build.bat`（`CGO_ENABLED=0` + `-H windowsgui`，无控制台黑窗）

## 安全热点（交付审查项）

1. `connections.json` 密码 AES-GCM 加密，**内嵌静态密钥**——防随手查看、不防逆向（`config.go` pepper）
2. Kafka SSL 关闭主机名校验（对齐参考环境 `ssl.endpoint.identification.algorithm=`）；提供 truststore 时仍做证书链校验（`kafka.go buildTLS`）
3. Jolokia/STOMP 走 HTTP Basic——凭据不出现在错误消息中；建议仅本机/内网使用
4. 实时订阅只接 topic：STOMP 订阅队列会真正消费消息（有副作用），产品内明确禁止

## 已知限制

- ActiveMQ topic 无存储：Jolokia 不可浏览，需 STOMP 实时订阅（服务端 activemq.xml 需启用 stomp 连接器，参考环境默认注释）
- ActiveMQ 发送暂不带 correlationId（Key）；Kafka 不支持删单条/清队列
- 界面为经典 Win32 风格（walk 零依赖取舍，见设计文档路线抉择）

## 结构

```
cmd/kafak-tools/        入口（-smoke 冒烟自检）
internal/config/        连接配置 CRUD + AES-GCM
internal/adapter/       统一接口：kafka(frangz-go) / amq(Jolokia) / stomp
internal/ui/            walk 界面（主窗口/面板/连接对话框）
test-env/               本机测试环境（产品同款 Kafka + 启停脚本）
docs/plans/             设计文档与评审记录
```
