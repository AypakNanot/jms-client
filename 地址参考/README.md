# 地址参考

后续任务与本参考目录相关，真值与规则以此为准。

## 参考项目

**`D:\ai-workspace\1240615\tmaster2000`** — TMaster 2000（OPTEL 华拓光研）OTN/WDM/SDH(MSTP) 光传送网 EMS/NMS，版本 V09.00R26.04。

### 关键入口（tmaster2000 内）

| 路径 | 用途 |
|---|---|
| `CLAUDE.md` | 项目规则：分层架构、QX 领域语义、SonarQube 门禁、已定案协议真值 |
| `README.md` | 架构全景、模块清单、技术栈 |
| `openspec/project.md` | 项目约定；`openspec/specs/` 核心能力现状 |
| `guides/CONFIG-SYNC-FULL-REFERENCE.md` | **开发范围权威**（26 配置组 / 40 子功能） |
| `docs/detailed-design/DETAILED-DESIGN-INDEX.md` | 旧 EMS 逆向详细设计索引（33 篇 DD，需求来源） |
| `docs/qx-docx/output_md/` | QX 协议原文 C01~C08（有笔误，冲突时以旧系统为准） |
| `docs/cmdCode-mapping-table.md` | cmdCode 全量映射与实现状态 |
| `设备安装业务实现编写指南.md` | 新平台五层业务编写范式 |

### 外部真值源

- 旧系统全源码：`D:\ai-workspace\mtp\PROG\`（协议字段/业务规则歧义时查这里）

### 核心约定速记

- `neId` = 连接路由 key，禁止从中解析 OID；`oid` 一律冒号分割
- Set 成败只看报文头 `result`（偏移 22），成功应答无报文体
- 功能歧义 → 查 `docs/detailed-design/` → 旧源码 → 协议原文，禁止臆断
