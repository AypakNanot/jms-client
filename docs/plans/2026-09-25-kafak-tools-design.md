# kafak-tools 设计方案

- 日期：2026-09-25
- 状态：已评审通过（用户 OK），执行中
- 形态：Windows 便携式桌面工具（单 exe）

## 1. 需求

| # | 需求 | 来源 |
|---|---|---|
| R1 | 同时连接 Kafka 与 ActiveMQ 的消息调试工具 | 初衷 |
| R2 | 打开后自选连接类型（Kafka / ActiveMQ-JMS） | 用户指定 |
| R3 | 按类型展示对应连接配置表单，可保存多套、可测连通 | 用户指定 |
| R4 | 目标系统 Windows Server 2016 + Windows 10 | 用户指定 |
| R5 | 语言/形态：Go + 纯 GUI，不用浏览器；便携单 exe | 用户指定 |
| R6 | 端口、topic 等按 tmaster2000 参考环境配置，方便工程现场测试 | 用户指定 |

## 2. 参考环境真值（tmaster2000 / 产品实际部署）

来源：`application-cover.yaml`、`opt-boot-starter-jms:application-jms.yaml`、
产品 `E:\...\TMaster-cloud-core\activemq-win\conf\activemq.xml`、`kafka/config/server.properties`。

### ActiveMQ

| 项 | 值 |
|---|---|
| OpenWire | `tcp://127.0.0.1:38102`（唯一启用；SSL 38101 注释） |
| STOMP/AMQP/MQTT | **全部注释**（61613 不通）→ STOMP 为可选增强 |
| Jolokia/Web 控制台 | `http://127.0.0.1:8161/api/jolokia`，admin/admin |
| Broker 认证 | 无（plugins 空），凭据仅 web console |
| JMS Topic | `OPTEL_Fault` `OPTEL_FileTransfer` `OPTEL_Inventory` `OPTEL_Protection` `OPTEL_Performance`；内部枚举 `Fault` `FileTransfer` `Inventory` `Protection` `Performance` `Heartbeat` |
| 发送参数 | `jms.useAsyncSend=true` |

### Kafka

| 项 | 值 |
|---|---|
| 监听 | `SSL://127.0.0.1:38131` + `PLAINTEXT://127.0.0.1:38132`（advertised 均 127.0.0.1，仅本机） |
| SSL | JKS，密码 `Optel-123456.`，单向认证（client.auth=none），关闭主机名校验 |
| 客户端默认 | security-protocol SSL、group-id `opt-consumer`、String 序列化、snappy |
| 分区 | 单分区副本 1（发送固定 partition 0） |
| ZK | localhost:38011 |

## 3. 架构

```
kafak-tools.exe（单二进制，零运行时依赖）
├── GUI（github.com/lxn/walk，Win32 原生控件，纯 Go 免 CGO）
│   ├── 主窗口：左 TreeView（连接→broker→目的地）/ 右上消息 TableView / 右下详情面板
│   ├── 连接对话框：类型下拉（Kafka|ActiveMQ）→ 动态表单；测试连接 / 保存
│   └── 发送对话框、消息详情（JSON/文本/Hex 三视图）
├── internal/config    连接配置 CRUD，exe 同级 connections.json，AES-GCM 混淆
├── internal/adapter   统一接口 Adapter：List / Browse / Send / Describe / Test
│   ├── kafka/         franz-go：list、assign+seek 浏览（零副作用）、发送、消费者组
│   └── activemq/      Jolokia 主（列表/浏览/purge/DLQ/发送）+ STOMP 可选（实时订阅）
└── docs/              设计与操作文档
```

## 4. 关键设计决策

1. **Kafka 浏览零副作用**：独立 consumer → Assign → Seek(起点) → 拉取 → Close，永不进消费组、永不提交 offset。起点可选 earliest/latest/指定 offset。
2. **ActiveMQ 以 Jolokia 为主通道**（与工程环境零改动兼容）：列目的地/积压、Queue browse、purge、DLQ、发送（DestinationView.sendMessage）。**STOMP 为可选开关**：实时 topic 订阅需要，工程环境需取消注释 activemq.xml 中 stomp 一行（待用户拍板，不阻塞）。
3. **Kafka 安全**：PLAINTEXT / SSL(JKS) 二选一；JKS 直读（github.com/pavlo-v-chernykh/keystore-go 纯 Go），路径+密码预填参考值。
4. **凭据存储**：connections.json 中密码 AES-GCM 加密（内嵌静态密钥，防随手查看不防逆向）——交付时列入安全热点待审查。
5. **连接模板预填**：bootstrap `127.0.0.1:38131,127.0.0.1:38132`、Jolokia `http://127.0.0.1:8161/api/jolokia`、group `opt-consumer`、topic 快捷列表 OPTEL_* 五项——现场只改 host。
6. **测试环境 1:1 复刻参考配置**（Docker）：同端口、同 JKS 密码、同 topic 名、单分区；连接配置两边通用。
7. **消息分页**：100 条/页按 offset 递进，防大 topic 扫爆内存。

## 5. MVP 边界

- 做：连接管理（R2/R3）、Kafka 列 topic/详情/浏览/发送/消费者组 lag、ActiveMQ 列表/浏览/发送/purge/DLQ、连接测试
- 不做（YAGNI，二期）：Kafka 建删 topic/调分区、SASL、消息编辑重发、offset 重置、实时推送（手动刷新）、OpenWire（Go 无成熟客户端且 Jolokia 已覆盖管理面）

## 6. Windows 适配（R4）

- Go ≥1.21 支持下限即 Win10/Server 2016；本机 Win10+go1.26.1 已实证，**阶段 1 在 Server 2016 真机冒烟**
- walk 原生控件：无浏览器、无 WebView2、无 OpenGL → 服务器/RDP 会话可用
- 若 walk 受阻（P0 风险，2024 后低活跃）：备选 windigo，再备选 Wails（需装 WebView2）
- build.bat：`set CGO_ENABLED=0` + `go build -ldflags "-s -w"` → 单 exe

## 7. 测试策略

| 层 | 手段 |
|---|---|
| Kafka 单测 | franz-go `pkg/kfake` 进程内 mock broker |
| ActiveMQ 单测 | `httptest` 模拟 Jolokia 响应 + STOMP 帧编解码 |
| config 单测 | 临时目录存取 + 加解密往返 |
| 端到端 | Docker 复刻参考配置（端口/topic/证书 1:1）+ kcat、控制台交叉验证 |
| GUI | 手动冒烟（Win10 + Server 2016）；GUI 层薄，业务逻辑下沉 adapter/config |
| 覆盖率 | 新代码 ≥80%，测试文件 ≤10（新增模块整体） |

## 8. 阶段计划（每阶段 ≤5 步，汇报后继续）

| 阶段 | 内容 | 完成标志 |
|---|---|---|
| **1** | git init + go mod + walk 骨架 + 连接配置 CRUD/加密 + 连接对话框（类型自选/动态表单/测试桩）+ Win10 冒烟 | 窗口可开、可保存两类配置并回显 |
| 2 | Kafka 只读（list/详情/browse/组）+ kfake 单测 | 真实 topic 浏览且组 offset 零变化 |
| 3 | Kafka 发送 + ActiveMQ Jolokia 只读 + httptest 单测 | 两边都能看到消息 |
| 4 | ActiveMQ 发送/purge/DLQ + STOMP 实时（可选）+ 统一详情面板 | 调试闭环 |
| 5 | Docker 复刻环境 + 覆盖率 ≥80% + 全局 7 步自评审 + README/build.bat | 评审通过，等用户确认 |

## 9. 评审记录（对本方案的自评审）

- **P0-1** walk 低活跃（2024-01 后）→ vendor 可自修；阶段 1 编译即验证，受阻切 windigo/Wails
- **P0-2** Server 2016 未实证 → 阶段 1 真机冒烟；降级预案：go1.21~1.22 toolchain
- **P1-1** STOMP 在工程环境被注释 → 设计已改 Jolokia 主 + STOMP 可选；**待用户拍板**是否打开工程环境该行
- **P1-2** 凭据明文 → AES-GCM 混淆，交付列安全热点
- **P1-3** ActiveMQ 服务端配置差异（Jolokia/STOMP 未开）→ 连接测试按钮 + README 排障节
- **P2** 无构建链 UI lint（接受）；GUI 无 lint；127.0.0.1 advertised 限制远程（便携本机工具，无碍）
- **已核实**：go1.26.1/Win10 实证、franz-go 纯 Go、参考配置三源交叉验证、Jolokia 8161 存活（本机产品实例 401→需 admin/admin）

**建议修复顺序**：P0 冒烟 → P1-2 配置加密 → P1-3 测试按钮 → P1-1 等拍板

## 10. 待决事项

- [ ] 工程环境是否启用 STOMP（activemq.xml 注释行）——影响实时 topic 订阅能力，不阻塞开发


## 11. 执行状态（2026-09-25 收尾）

| 阶段 | 状态 | 证据 |
|---|---|---|
| 1 骨架+连接管理 | ✅ | smoke/结构枚举，walk manifest 内嵌 |
| 2 Kafka 只读 | ✅ | kfake 单测 + 真机 e2e（组 offset 零变化） |
| 3 Kafka 发送 + AMQ Jolokia 只读 | ✅ | 发送回读 e2e + httptest 全链路 |
| 4 AMQ 发送/purge + STOMP 实时 | ✅ | 产品实例闭环 e2e + Docker STOMP 实时 e2e |
| 5 收尾+评审 | ✅ | 覆盖 adapter 81.5% / config 82.8%；gofmt/vet 零告警；4 条 e2e 全绿；README/build.bat 交付；7 步自评审（见会话报告） |

评审中发现并修复：JKS 指针断言 P0、SEC1 私钥 P0、STOMP stop 顺序死锁 P1、Health/Consumer MBean 误匹配 P1、browse 重载签名 P1。
