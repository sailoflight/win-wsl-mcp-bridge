# 桥应具备的 MCP 行为轴与组合矩阵

本文件以 **十条行为轴 + 组合约束 + 证据门槛**说明桥如何适应环境：逐轴选择
行为，再验证组合。M0/M1/M2 仅是既有入口的组合示例与历史标签，不是所有环境
必须三选一的主模式。轴上行为包含可选策略、环境能力和硬约束，并非任意开关。
目标行为与已实现行为分别记录；轴编号不是新增 CLI、配置枚举或运行时接口。
开发顺序由 [未来开发总计划](DEVELOPMENT_PLAN.md) 维护：第 3 节把十轴映射到
既有 R/E/F 任务，第 4 节定义逐轴与组合验收；本矩阵不另建排期。

依据：源码基线 `2ddb028` 的三个 stdio 入口、已有专项/现场记录，以及十种宿主
的官方文档和源码研究。文档声明、代码链、问题报告、模型请求实测分别留证；
不得用产品名、SDK 支持或配置作用域替代当前环境的行为证据。

## 1. 十条行为轴

| 行为轴 | 轴上的行为模式 / 策略 | 证据与实现边界 |
|---|---|---|
| **X1 接入与协议** | 已支持原生连接、显式转换、不支持则报告 | 实际客户端/传输/协议交集决定可行性；不能猜 endpoint/command 或自动装扩展 |
| **X2 工具发现** | 宿主原生搜索、桥有界搜索、直接目录浏览 | 宿主搜索实际启用时优先复用；桥通用搜索属 E1，不遍历启动后端，不叠加重复发现层 |
| **X3 定义呈现** | 原名工具定义、结果内精确定义、按需配合 | 摘要不替代 schema；结果中的定义驻留历史，可重建面的卸载须请求级实证 |
| **X4 目录变更** | 稳定目录、仅增加、支持增加/移除/同名更新 | 协议合法且通知→重列→模型请求逐项生效；仅增加是部分能力，不认证折叠/更新 |
| **X5 选择粒度与提交** | 整库、明确工具批次；一次调用列表/事务提交 | 整库开关已实现；批量混合提交、版本冲突、no-op 不通知属 E2；自动分组未确认 |
| **X6 调用方式与保真** | 原名直接调用、固定入口转发 | 分别验证 _meta、取消、进度、错误、图片/结构化结果；包装不自动等价 |
| **X7 目录作用域** | 独立逻辑连接各自目录、同连接共享目录 | connectionId 不是 Agent 身份；已有独立 facade 隔离可验，viewId/COW 未实现且不预留 |
| **X8 后端共享** | 登记允许独立进程、登记要求共享后端 | 由 multiProcessAllowed 与所有权契约约束，不是为展示优化随意切换的策略 |
| **X9 生命周期与目录恢复** | 保持连接、按需重连；失效重取、有条件旧目录 | 展示/目录/连接/generation 分开；缓存不等于存活，业务结果不确定不重放 |
| **X10 说明与宿主保障** | 原生说明、同源兼容说明；保留保障或拒用不满足的组合 | 首次决策前说明可见、按名审批和确认各自验收；不以桥授权替代宿主保障 |

十轴由原七组轴拆分：A→X1，B→X2/X3/X5，C→X3/X4，D→X7/X8，
E→X9，F→X3/X10，G→X6/X10。这是职责拆分而非十种客户端各自一个轴。
费用与可观测性继续作为验收指标，不另造全局主模式或枚举全部组合。

**共用硬边界：** 两个运行时组件、标准库运行时、loopback、已登记 peer 范围、
脱敏元数据、不写业务专属逻辑。工具数、查询长度、分页、输出与时限应有明确
边界；不能把 deferred 的有界校验实现泛化成所有现有入口都同等严格。

### X5 的一次调用要求

| 粒度 | 目标动作 | 状态 / 门槛 |
|---|---|---|
| 整库 | 一次选择全部下游工具，持续使用期间保留 | 已实现，是当前 B′ 对照基线；重复 expand 仍通知 |
| 显式工具批次 | 一次加入/移除一组明确名称；整批验证并提交，未知项不部分应用 | 总计划 E1/E2 待开发与验证；名称不变但 schema 变化仍是新定义 |
| 语义分组或自动策略 | 按 group/risk/maintenance 元数据或意图自动选择 | 未确认候选；不自动纳入“批量名称选择” |

已确认的 E2 要求是**一次调用多目标展开、折叠或混合切换**：用事务操作列表或
单工具列表参数一次提交，不要求逐项调用或多轮 begin/commit。整批校验后发布
最终目录，no-op 不通知；多工具与跨 facade 多库的控制范围和原子性须分别设计
验证，不能默认存在跨连接全局事务。具体范围与成本验收见
[总计划 E2 一次调用要求](DEVELOPMENT_PLAN.md#e2-的一次调用批量展开折叠要求)。
目的是减少操作往返及未缓存输入；实际节省按请求与计费证据记录，当前尚未实现。

整库保留与细粒度选择需比较**完成同一任务的总成本**；不是越细越好。
原生宿主搜索、code-mode 或宿主自己的渐进发现是 X2 的宿主能力，
不据此为桥再增加“某客户端模式”或另一套搜索引擎。

## 2. 已有入口的组合示例（原 M0/M1/M2）

| 历史标签 / 入口 | 主要轴上行为 | 不由该标签决定的属性 |
|---|---|---|
| M0 原生目录协作 / connect 及相应原生路线 [B1] | X2 宿主搜索或目录浏览；X3 原名定义；X6 原名调用 | 宿主是否全量装入模型、是否支持后续刷新、目录作用域及后端共享均另验；共享后端会协议投影，不统称字节透明 |
| M1 动态选择披露 / deferred-mcp [B2] | X3 动态工具表；X4 整库增删；X5 当前整库；X6 原名调用 | 通用搜索、工具子集、版本冲突与 no-op 目标未实现；X7/X8/X9/X10 仍各自受约束 |
| M2 固定入口调用 / compatibility-mcp [B3] | X2 查询/分页；X3 结果内定义；X4 宿主入口稳定；X6 固定转发 | 下游目录仍可变化；旧目录、恢复、说明与审批不因入口稳定自动合格 |

这些组合不覆盖全部设计空间，不用于强制环境分类，也不代表运行时已经能自由
混配十轴。deferred/compatibility 现有入口仅支持 legacy stdio，不能从目标表
推导现代/HTTP 支持。固定转发须明确选用，不是未知环境的自动默认。

## 3. 逐轴决策与组合约束（设计目标）

| 已知条件 / 轴 | 应选择或限制的行为 | 组合边界与证据不足处置 |
|---|---|---|
| 无客户端或无可表示路线 / X1 | 报告所缺能力 | 不自动安装扩展，不进入不可用组合 |
| 宿主原生选择有效 / X2 | 优先复用宿主搜索 | 不由此固定 X3/X4/X6/X7/X8；额外桥搜索需证明必要性 |
| 定义进入工具结果 / X3 | 可按需返回精确定义 | 不宣称卸载历史；若需卸载须另验可重建面与 X4 移除 |
| 增删改分别实证 / X4 | 只启用有证据且协议允许的操作 | 只有增加通过时不启用折叠/替换；原生搜索可与刷新并存，不能因搜索存在而跳过刷新验收 |
| 一次展开/折叠多个 / X5×X7 | 统一校验后提交最终选择 | 同 facade 与跨 facade 所有权分开；单次调用不自动证明跨连接全局原子性 |
| 固定入口转发 / X6×X10 | 明确选用并验证包装语义和必需说明 | 原名保障不可替代或结果不可表示则拒用；不因动态未知自动包装 |
| 多 Agent 同连接 / X7 | 明确共享目录；稳定批次与冲突恢复 | 不接受模型自报 ID 作为隔离证据；已有独立连接可另验 |
| 共享后端 / X8×X7 | 按登记保持一个后端，允许不同逻辑连接各自目录 | 不为隔离目录创建违规进程，也不把共享后端等同共享目录 |
| 目录失效或连接回收 / X9 | 按所选入口契约恢复，旧目录有效性与存活分开 | 折叠不等于停止后端；不伪造初始化，不无条件采用旧目录，不重放未知结果 |
| 环境能力未知 | 保留已有支持的普通路线，逐轴补证 | 不自动启用未证动态行为或固定转发；X1/X8/X10 的硬门槛仍须满足 |

组合变更应有明确的连接/配置边界和回退点；不能仅返回“已回退 native”就假定
宿主工具表已改变。需要新连接或配置变更时按具体范围执行；试验中不静默换组合
后仍按原候选计分。逐轴能力通过后仍须对实际组合端到端验收。

### 不改写现有配置含义

`tool_exposure=native|auto|deferred` 与 `compatibility_route` 是独立配置维度。
现有 auto 只有在回执新鲜、环境/配置匹配、legacy 协议受支持、刷新与模型可见
均 supported 且原生搜索明确 unsupported 时才选 deferred；路线不适用则回退
native。显式 deferred 缺证据或非 native stdio 路线则报错。native 配合
constant-two-tool 仍可生成 `compatibility-mcp`，不必然生成 `connect` [B4]。

这些是投影检查；直接运行 `deferred-mcp` 不读取 enrollment 回执。目标矩阵
不改变该事实，也不把 M0/M1/M2 自动加入现有数据库或公共 schema。

## 4. 十种环境的逐轴验证入口

以下是上一轮只读研究的**验证入口**，不是生产组合映射表。简称：文=官方文档，
码=源码链，报=公开问题报告，实=本项目指定环境请求快照。源版本/配置改变后需重核。

| 环境与来源 | 对轴上行为最有用的发现 | 验证方向 |
|---|---|---|
| [Claude Code](https://code.claude.com/docs/en/mcp) | 原生 Tool Search、动态重列有文档；非第一方代理等配置可关闭默认搜索（文） | X2 搜索实际启用；X4 刷新与 X10 截断/按名审批各自验证 |
| [Claude Desktop](https://claude.com/docs/third-party/claude-desktop/extensions) | 本地/远程 MCP 与托管逐工具策略有文档；原生选择和动态模型刷新未知（文） | X1/X2/X4/X10 补证；不由 Claude Code 外推，不自动包装 |
| [Codex CLI](https://github.com/openai/codex/issues/33266) | 0.144.1 报告原生 deferred search 使用初始目录，但通知后未重列（报） | X2 优先复用可用搜索；X4 按版本复核，不能推断所有版本永久不支持刷新 |
| [Gemini CLI](https://github.com/google-gemini/gemini-cli/blob/ed2ac40df67a319bf348bd7e3d10494696b31b38/packages/core/src/tools/mcp-client-manager.ts) | 通知→重列→onContextUpdated→refreshMcpContext→setTools 的链路存在（码） | X4 请求级增删改验收；不是“只能换模型刷新”，也不是已经现场验证 |
| [Grok Build](https://docs.x.ai/build/features/mcp-servers.md) | 编码客户端有 stdio/HTTP 文档；原生选择、通知刷新、instructions 去向仍未知（文） | X1 普通路线基线；补 X2/X4/X10。xAI API Remote MCP 不能代替客户端证据 |
| [OpenCode](https://github.com/anomalyco/opencode/blob/486e8460b1401d1338a81c28cbfbf1b3fb1de2f1/packages/opencode/src/mcp/index.ts) | 通知重列、每 step 工具组装；目录级连接共享；有实验 code-mode（码） | 分别验证 X2 选择、X3 载体、X4 刷新和 X7 同目录并发影响 |
| [OpenClaw](https://docs.openclaw.ai/tools/mcp) | 原生 MCP、过滤、runtime 热更新；MCP list_changed 链路未证（文） | 配置热更新不算 X4 通知验收；核对 X7 实际 runtime/catalog 范围 |
| [Hermes Agent](https://github.com/NousResearch/hermes-agent/blob/b1ff8722a53ee223485ac9804945acf07ef5c601/website/docs/user-guide/features/mcp.md) | 有自动重列文档；另有 [三入口渐进发现源码](https://github.com/NousResearch/hermes-agent/blob/43717123c/tools/tool_search.py)，不同提交须核对启用状态 | X2 复用已启用宿主发现；X4 模型层、X9 idle 回收和 X10 原名过滤另验 |
| [Pi](https://github.com/badlogic/pi-mono/blob/main/packages/coding-agent/README.md) | 核心明确无 MCP；扩展可提供客户端（文） | X1 先确定实际接入扩展，再逐轴验证；不把扩展能力算桥或核心已有能力 |
| [DSH](MCP_TOOL_EXPOSURE.md#observed-dsh--ds4f-round-trip-2026-09-09) | 本地 0.1.1-rc.2 有重列代码；指定 Web/DS4F 54→134→54（实） | X4 整库增删为已观察基线；X5 批量、同名更新、X7 跨 Agent 与其他 profile 独立验收 |

## 5. 验收结果与收益记录

每个候选记录精确宿主 build、模型/代理配置摘要、桥源码/产物摘要、协议、十轴
选定行为、实现状态、连接拓扑及证据来源。只收集会改变行为的脱敏属性，不采集凭据。
逐轴记录通过/失败/未知/不适用，再验证实际组合；不由单轴通过外推所有组合。

| 验收组 | 必须分开记录的结果 |
|---|---|
| 接入/说明/执行 | initialize 与必需说明在首次工具决策前可见；schema/结果可表示；原名权限、取消、进度及确认规则各自通过/失败/未知 |
| 动态模式 | 桥状态、重列流量、实际模型请求三层；增加、移除、同名 schema 更新、突发变更、重连和共享交错分别记录 |
| 固定模式 | 不依赖目录变化；摘要/精确定义有界；缓存有效性、错误、包装身份及关键宿主保障分别验证；不声称能卸载历史 schema |
| 生命周期 | 启动失败、最后客户端退出、重连和宿主回收；旧目录与存活状态分离；未知结果不重放；临时客户端清理 |
| 任务收益 | 完成质量与来源正确性优先；再记请求/发现/切换/无效调用、未缓存输入、缓存读写、输出、耗时与失败恢复成本 |

费用和可观测性是验收指标，不增加模式。schema 更少不等于总费用更低；没有
可靠计费分项与价格就报告 token，不能套用固定“12.5 回合”阈值。源码、文档
或 tools/list 不能替代请求快照；观察不到保持未知，不由模型自述补证。

## 6. 当前实现与目标的差距

| 当前入口 / 关联轴 | `2ddb028` 已有行为 | 仍需开发/验证的目标 |
|---|---|---|
| 普通入口 / X1/X3/X6/X8 | 普通连接保持下游工具面；共享后端进行协议/ID/结果投影；连接器只重放允许的握手 | 当前候选全量/安装/现场门槛仍见总计划 R1/R2；不宣称有离线工具目录 |
| deferred / X3/X4/X5/X7/X9 | 初始化已连接后端；收起只显示入口、尚不抓目录；整库原子发现；名称级提示；两连接独立、同连接共享 | E1 搜索/复用索引，E2 批量/版本冲突/no-op，E3 宿主生效证据，E4–E6 真实任务与发布门槛 |
| compatibility / X2/X3/X6/X9/X10 | 固定双入口；查询/分页/精确定义；last-known-good 缓存及显式刷新；启动下游不可用时仍可提供入口 | E1/E3 复核缓存与调用语义及更完整有界处理；若从未获得下游 instructions，固定入口可启动不等于契约验收通过 |
| 作用域与恢复 / X7/X8/X9 | facade connectionId 跨按需重连保留；后端共享由登记决定，展示开关不改该策略 | 共享识别的环境证据、并发恢复与可用独立连接验证；不新增 Agent ID 适配 |
| 契约保真 / X6/X10 | deferred 保留原名/arguments/_meta，匹配 token 进度可转发；legacy result 投影；固定入口重新包装调用 | **deferred 不转发客户端取消；固定入口不转发取消/进度且内层丢外层 _meta**。任务需要这些语义时先修复/验证或拒绝路线，不能靠组合标签掩盖 |

当前行为还必须保留以下区别：

- 重复 expand 即使名称不变仍请求通知；重复 collapse 不重复通知；status
  不构成刷新确认。目标“no-op 不通知”不能写成 B′ 已完成。
- collapse 清目录缓存，但不主动关闭下游连接、停止后端或释放 lease。
- deferred 失效时保留展开意图、清旧 schema，状态可为 unknown；后续按需发现；
  下游协议或 instructions 变化要求新会话。保留名称仅用于恢复提示。
- 收起后通过历史名称调用会被拒绝，不下发业务调用；这是该入口执行条件，
  不替代宿主或下游权限。
- compatibility 被动读到变化后标 dirty，仍可返回旧目录；调用先查缓存成员，
  不会因 dirty 自动刷新。成功 result 与 JSON-RPC error 的包装方式不同。
- 现有通知路径可能产生多条通知；目录 revision 不是宿主 ACK，也不是现有
  expected-revision 输入参数。不能声称当前已有并发折叠冲突保护。

## 7. 实现证据与未选能力的去向

- **[B1]** [bridge_runtime.py](../bridge_runtime.py) 的 `SharedBackend`、owner open
  的 `multiProcessAllowed` 分支；[connector_engine.py](../connector_engine.py)；
  [Business MCP modes](ARCHITECTURE.md#business-mcp-modes)。
- **[B2]** [deferred_tools.py](../deferred_tools.py) 的 `DeferredSession`、`_Peer._receive`；
  [test_deferred_tools.py](../tests/test_deferred_tools.py)：初始目录、整库状态、参数、
  独立/共享连接、失效/重连、错误与 no-replay。
- **[B3]** [bridge_runtime.py](../bridge_runtime.py) 的 `compatibility_mcp`、
  `_CompatibilitySession`；[兼容韧性测试](../tests/test_compatibility_resilience.py)。
- **[B4]** [bridge_runtime.py](../bridge_runtime.py) 的 `_effective_tool_exposure`、
  `_desired_entry_descriptors`、`_agent_connect_entry`；
  [投影回执测试](../tests/test_client_enrollment_verification.py)、
  [探针测试](../tests/test_harness_verification.py)。

上一轮当前行为核对运行专项 **60 项，22.703s，OK**，不是本轮新测试或完整发布
验收；取消/进度细分主要据控制分支，未新增独立现场断言。复现命令：

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_deferred_tools tests.test_client_enrollment_verification tests.test_harness_verification tests.test_compatibility_resilience.CompatibilityFacadeResilienceTest -q
```

本轮仅验证文档的模式/轴覆盖、链接和目标/现状一致性，不修改运行时代码。

仍未确认的能力：语义 grouped、自动意图/前次调用触发、常驻只读/维护集合；
prompt-fold、warm-injection、per-Agent viewId/COW、scope 注册/释放及官方工具
统一治理；新增 catalogUnload/refreshScope/保真/身份/载体/成本探针字段。
这些分别属于 X5 待选策略、X7/X10 的条件性 E8 宿主协同或 E3 待选证据格式。
必须先说明新增了哪个桥动作、为什么现有组合不能满足，再单独选型；
不因画进模式矩阵而预留接口、迁移数据库或安装插件。
