# kafak-tools 评审报告（终审）

- 日期：2026-09-25
- 对象：全项目（14 个 Go 文件 / 4024 行，含测试）
- 依据：`~/.claude/CLAUDE.md`「代码评审流程（强制）」7 步法
- 结论：**通过** —— P0×2、P1×4、P2×2 全部修复并复验；无必修遗留

## 0. 评审方式（含局限披露）

| 轮次 | 方式 | 说明 |
|---|---|---|
| 方案评审 | 主线程自评（设计文档 §9） | 方案 OK 前完成，含 walk/Server2016/STOMP 三项预案 |
| 过程评审 | 每阶段门禁 | gofmt + `go vet` + 单测 + 真机 e2e + 子控件结构枚举 |
| **终审** | 分切面：协议正确性 / UI 线程 / 安全 | **局限①：计划的并行子代理评审因环境模型限制 3 次全部失败（Unsupported model），改为主线程分切面自评，独立性弱于理想流程** |
| 终审补强 | `staticcheck`（Go 界最接近 Sonar 的静态检查） | 首轮 11 条 → 修复 8 条 + 豁免 3 条 → **复跑 0 告警** |
| 独立验证 | 不采信任何自述 | 全部测试/e2e/覆盖率现场复跑；walk `Synchronize` 源码取证；窗口子控件枚举取证 |

**局限②**：未运行真 SonarQube（新项目未配扫描服务）；以 gofmt/vet/staticcheck + 人工 Sonar-way 清单（S106/S1128/S2259/S2095/S2077 等逐条核对）替代。

## 1. 发现与处置（P0 → P2，全部闭环）

| 级别 | 发现 | 处置 | 复验证据 |
|---|---|---|---|
| P0 | JKS 解码存指针、代码按值断言 → SSL 加载必挂 | 双类型兼容 | `TestSSLEndToEnd`（kfake TLS 全链路）过 |
| P0 | `parsePrivateKey` 不支持 SEC1(EC) | 补 `ParseECPrivateKey` | `TestParsePrivateKey` 过 |
| P1 | STOMP `stop()` 先停 reader 再等 RECEIPT → 活跃流量死锁窗口 | 调序 Unsubscribe→Disconnect→close | `TestSubscribeTopicLive` 过 |
| P1 | Jolokia search 误匹配 `service=Health` 假 Broker | 精确属性判定 `isBrokerViewMBean` | 产品实例真机 e2e 过 |
| P1 | 订阅产生的 `endpoint=Consumer` MBean 覆盖目的地视图 | 跳过 `endpoint=` | Docker STOMP e2e 过 |
| P1 | `browse` 重载未显式签名 | `browse()` | 产品实例真机 e2e 过 |
| P2 | README 代码围栏被 shell 反引号劫持执行 | Edit 工具重写 | 文档核对 |
| P2 | staticcheck ST1005 错误串首字符×8 | 中文动词前置改写 | staticcheck 复跑 0 |

## 2. 门禁违规项 / 豁免登记（须审查）

| 规则 | 位置 | 理由 | 处置 |
|---|---|---|---|
| ST1001 点导入 ×3 | `internal/ui/{app,panels,conn_dialog}.go` | walk/declarative **官方惯用写法**（walk 自带示例即 `. "github.com/lxn/walk/declarative"`）；全局改前缀为纯 churn | `staticcheck.conf` 豁免留痕 |

- lint 抑制标记（nolint/noqa/nozzle 等）：**全仓库 0 处**
- `gofmt` / `go vet`：零告警

## 3. 安全热点（交付审查清单）

1. `connections.json` 密码 AES-GCM + **内嵌静态密钥**（`config.go` pepper）——防查看不防逆向
2. Kafka SSL `InsecureSkipVerify=true`（对齐参考环境关闭主机名校验）；提供 truststore 时自定义回调**仍做完整链校验**（外国 CA 负例已测）
3. Jolokia/STOMP 使用 HTTP Basic；凭据不进入错误消息（headers 独立构造，已核实）
4. 实时订阅仅限 topic（STOMP 订阅队列=真实消费，适配器+UI 双层拦截）

## 4. 已核实无问题（证据）

- **跨线程 pending 字段无竞态**：walk `WindowGroup.Synchronize` 经 `syncMutex` 队列建立 happens-before（`windowgroup.go:195-201,218-225` 源码取证）
- **控件零越线**：全部控件操作位于 UI 线程（事件回调 / Synchronize 回调）；后台闭包只写数据字段
- **Kafka 浏览零副作用**：单测断言无组泄漏；真机 e2e 浏览前后消费者组指纹逐一比对一致
- **资源关闭**：kgo/kadm client、HTTP resp、STOMP conn 均 defer Close（含错误路径）
- **测试门禁**：单测全绿；4 条真机 e2e 全绿（Kafka 浏览/发送、AMQ 发送浏览清空、STOMP 实时）；覆盖率 adapter 81.5% / config 82.8%（≥80%）；测试文件 5 个（≤10）
- **平台**：Win10 实证（build/smoke）；manifest 内嵌 Common Controls v6；`staticcheck` 最终 0 告警

## 5. 建议修复顺序

无必修遗留。可选优化（不阻塞交付）：
1. `internal/ui` 单测补覆盖（当前靠冒烟+结构枚举+人工验收，GUI 层薄的既定取舍）
2. ActiveMQ 发送带 correlationId（Jolokia 多参重载）
3. Kafka 管理能力（建删 topic / retention）按需二期

## 6. 遗留事项（待用户/资源）

- [ ] Windows Server 2016 真机冒烟（拷 exe 双击）
- [ ] 用户对最终版的实机验收
- [ ] git 提交（等用户指令）
- [ ] 工程环境是否启用 STOMP 连接器（影响实时订阅，不阻塞其余功能）
