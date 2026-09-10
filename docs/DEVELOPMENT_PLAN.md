# 未来开发总计划

本文件是项目未来工作的唯一排期入口：以 **十条行为轴上的能力与策略**组织开发，
根据精确环境证据分别选择轴上行为，再校验组合约束，完成真实任务比较与发布。
M0/M1/M2 仅保留为已有入口的组合示例和历史实验标签，不再是环境必须三选一的
顶层模式。专题矩阵定义轴与行为，本计划维护任务、依赖及退出条件。

## 1. 版本基线与状态规则

按 [Changelog](../CHANGELOG.md)，最近有发布日期的版本是 **0.3.1
（2026-08-31）**，**0.4.0 仍为 Unreleased**；本次整理时源码 HEAD 是
`2ddb028`。仓库没有版本 tag，不能构造一个不存在的正式发布提交。
本计划同时承接 0.3.1 之后尚未收口的验收项，以及当前源码之后的新增设想；
不把全部 0.4.0 功能重新列成待实现，也不在本次整理中决定下一版本号或发布日期。

| 状态层次 | 已有证据 | 规划含义 |
|---|---|---|
| 原始需求 | [Milestone design](MILESTONE_DESIGN.md) 保存 P6–P10、P5 外部接入及可选加固 | 历史状态段可能过时；只用于需求追踪，不能据此重复开发 |
| 已实现基线 | [Acceptance ledger](IMPLEMENTATION_STATUS.md)：传输投影、缓存 facade、生命周期、证据、双协议时代 tools 子集已实现 | 当前行为以 [MCP coverage](MCP_COVERAGE.md) 和 [Architecture](ARCHITECTURE.md) 为准 |
| 历史完整验收 | 506 项测试通过、旧候选包安装检查、真实双向源码夹具、SDK 2.0.0 五例互操作 | 属于对应源码/产物；不能替代当前 HEAD 的完整验收 |
| 当前 B′ 源码 | 专项 54 项通过；两次全量各运行 569 项但均未全绿；现代 HTTP 专项复测 13 项通过 | 完整回归、候选包及安装证据仍由 R1/R2 收口，不隐去失败 |
| 限定现场部署 | B′ 已在现有 DSH Web 两个业务 MCP 前端加载；[部署记录](../release-artifacts/deploy-b-prime-20260909T084545402105Z/RECEIPT.md) | 仅证明该次前端加载与稳定入口；不等于整个 wheel 升级或细粒度真实任务验收 |
| 未来工作 | 下方统一任务表中的未完成项 | 文档、原型、离线通过、现场通过、生产启用必须分别记录 |

完成项必须留下源版本、产物摘要、验证命令/结果及适用范围；从计划中移除文字
不算完成。缺失或失效证据保持 unknown/unverified，不根据客户端名称推断。

## 2. 目标、约束与统一顺序

目标是在现有双向桥能力上，先形成可验证、可回退的交付基线，再通过通用的
发现、搜索和批量披露降低完成真实任务的总成本，同时明确共享范围和实际隔离
能力。通用能力完成评估后再进行完整功能模块化；只有仍存在经过测量的宿主
能力缺口时，才考虑宿主协同。

以下顺序继承既有决定；其他支线只在满足自身进入条件时启动，不将所有远期
设想强行串成一个大版本：

```text
R0 基线与需求冻结
├─ R1 既有入口回归 ─→ R2 候选包/安装/现场验收 ─→ R3 基线发布决策
└─ E0 十轴契约、组合约束、复用与证据冻结
   ├─ 既有行为逐轴证据采集（可随 R1 开展）────────────┐
   └─ E1 发现/定义/执行与目录恢复（X2/X3/X6/X9）       │
      ├─ 无动态选择变更的候选组合 ────────────────────┤
      └─ E2 批量事务/刷新/并发（X4/X5/X7）─────────────┤
                                                    ↓
                   E3 十轴分项验收 + 选定组合的端到端验收
                    → E4 真实任务筛选 → E5 有界复核
                    → E6 按环境/行为组合发布决策
                    → E7 完整功能模块化 → E8 条件性宿主协同
```

R1 与 E0/E1 可在冻结各自源码后并行；R1 的问题仍必须在对应候选发布前解决。
E3 的既有原生/整库行为证据采集可以提前进行；使用新增发现、包装或目录恢复
能力的组合依赖 E1，使用批量动态变更的组合还依赖 E2；X1/X8/X10 的协议、后端
策略和保障贯穿两者。各组合通过适用断言后才进入 E4。E6 必须对其**最终候选源码**
重新通过 R1/R2 的适用门槛，不能复用旧包的绿灯；R3 不必等待远期 E7/E8。
如果 E5 结果不确定或候选被否决，E6 可以明确保留 B′ 基线，再评审是否启动 E7；
无须为了进入模块化而强行上线失败方案。

- 保持两个运行时组件、根目录共享模块、标准库运行时及 loopback 边界。
- 只使用已登记 peer 身份及脱敏元数据，不增加业务 MCP 专属规则。
- 区分物理后端共享、逻辑连接共享、目录共享；展示隔离不等于权限隔离。
  不为隔离目录而绕过 `multiProcessAllowed=false` 创建额外业务后端。
- 优先 MCP 协议和宿主已有能力；近期不引入插件，也不预留 DSH 接口、
  Agent 映射、viewId/COW 适配脚手架。E7 仍不得新增第三个运行时组件。
- 定义位于工具结果时会驻留消息历史，MCP 无撤回或触发 compact 的能力。
  可卸载性取决于宿主是否重建工具数组/提示段，不把固定入口称作无成本优化。
- 不自动按回合折叠，不以模型自述、tools/list 或通知已发送充当模型可见证据。
- 本次只整理文档；任务进入执行时仍按具体范围确认。付费模型批次、真实浏览器、
  安装、配置、现场操作和生产切换分别取得相应批准，已有批准不跨环境/候选泛化。

## 3. 统一任务表

### 已定方向与未确认候选

**`MCP_EXPOSURE_TAXONOMY.md` 以十条行为轴定义可选行为、能力门槛和组合约束，
并区分当前实现与目标。** 十轴是对原七组轴的拆分，不是十种客户端各自一个模式。
轴编号是文档追踪标识，不是新增 CLI/数据库字段或执行批准。批量披露遵循 E1–E6
门槛，分组/自动策略、提议字段及宿主协同仍须单独选型。

- 已有决定：先通用发现/搜索/批量披露及真实任务比较，再完整模块化，之后才
  **考虑**宿主协同；近期尽量通过既有协议/连接能力判断共享并隔离目录。
- 本次整理的 R/F 编号和跨线依赖是统一管理安排；R 是已有验收缺口，F 是暂缓
  需求池。它们不新增执行授权，也不承诺所有扩展都开发。
- 矩阵候选：`grouped`、prompt-fold、warm-injection、per-Agent viewId/COW、
  behavioral expansion、常驻只读/维护集合及新增探针字段，均**未确认实施**。
  E0/E3 只登记与核对相关问题；不据矩阵自动新增公共 API、注册表字段或插件。
- 候选转实施需单独形成决策：具体问题、可选方案、协议/宿主限制、对现有
  契约的影响、验收及成本；用户确认选型后，才绑定到相应任务并更新范围。
  实验资格与生产批准仍是后续独立门槛。未选中方案保留在矩阵中，不记作逾期任务。

### 按十条行为轴组织的交付范围

每个环境分别记录各轴的已证能力、选定行为、限制和来源，再组合成实际路线。
轴上既有可选策略，也有环境能力与硬约束；不将全部轴做成可任意切换的配置。
各轴可独立开发和留证，但组合仍需端到端验证，不枚举全部笛卡尔积。

| 行为轴 / 关联任务 | 轴上的行为选择与开发范围 | 退出条件 |
|---|---|---|
| **X1 接入与协议** / R1–R2、E0/E3；扩展 F1 | 已支持原生连接、显式协议转换或报告不支持；复用当前入口与协议投影 | 固定实际传输/协议交集；独立进程透明转发和共享后端投影分别验收；不自动安装客户端 |
| **X2 工具发现** / E0/E1/E3 | 宿主原生搜索、桥有界搜索或直接目录浏览；复用精确名称/关键词/别名/库过滤 | 宿主搜索按版本/模型/配置实证启用；桥搜索不披露、不调用业务、不遍历启动全部后端，不默认叠加搜索层 |
| **X3 定义呈现** / E0/E1/E3 | 原名工具定义、结果中的精确定义或按需配合；候选摘要与精确 schema 分离 | schema 不失真；定义载体与卸载边界实证，结果历史不可撤回；同源说明不能代替精确定义 |
| **X4 目录变更** / E0/E2/E3 | 稳定目录、经验证的仅增加能力、增加/移除/同名更新；原子发布后通知 | 通知、重列、模型请求三层分开；增删改分别取得资格，no-op 不通知；仅增加通过不准折叠/替换 |
| **X5 选择粒度与提交** / E0/E2/E3 | 整库、明确工具批次；一次调用多目标展开/折叠/混合切换 | 整批校验、冲突恢复、稳定选择与有界结果；多库所有权和跨连接原子性单独证明，见下方 E2 要求 |
| **X6 调用方式与保真** / R1、E0/E1/E2/E3 | 原名直接调用或固定入口转发；复用执行链，选定保真修复随所属工作包完成 | `_meta`、取消、进度、错误、图片/结构化结果逐项验证；不可接受差异使该组合拒用 |
| **X7 目录作用域** / E0/E2/E3 | 独立逻辑连接各自目录，或同连接共享目录；验证宿主已有连接隔离能力 | connectionId 不冒充 Agent 身份；共享冲突按版本恢复，不预留 viewId/COW 适配 |
| **X8 后端共享** / R1–R2、E0/E3 | 按登记使用独立进程或共享后端；保留所有权、ID 路由、序列化与 lease | 不能为目录隔离改写多进程策略；共享后端不推导目录共享，展示选择不决定进程数量 |
| **X9 生命周期与目录恢复** / R1、E0/E1/E3；持久化 F3 | 保持连接、按需重连；失效后重取或有条件使用旧目录，按入口契约选择 | 分离展示、目录、连接和 generation；冷启动/dirty/idle/最后客户端退出与清理留证，不重放不确定业务调用 |
| **X10 说明与宿主保障** / E0、R1/E1/E2、E3 | 原生说明呈现或同源兼容说明；保留按名审批/确认，必要保障不满足则拒用 | 必需说明首次决策前可见；记录截断和审批身份；不自建授权体系替代宿主，不因未知而开发插件 |

### 组合约束与已有入口复用

- 先过 X1 接入门，再分别选择各轴行为；X8 登记策略和 X10 必需保障是硬边界。
  已有普通路线可用但环境证据未知时保留普通路线，不自动启动动态选择或包装调用。
- X2 宿主搜索有效不意味着 X4 必须静态，也不决定 X7/X8。X3 的结果型定义
  不具备历史卸载能力；需要卸载时须证明重建面上的定义实际移除。
- X4 动态选择依赖协议与请求级证据；X5 一次批量提交须与 X7 控制范围一致。
  X7 独立目录可以与 X8 共享后端组合；X9 折叠、断线、停止后端不得混为一个动作。
- X6 包装调用必须通过 X10 宿主保障检查；“固定入口可启动”不能替代下游
  instructions 或业务契约验收。必需语义不满足时修复复验或拒用该组合。
- M0 原生目录协作、M1 动态选择披露、M2 固定入口调用仅用于指认普通 connect、
  deferred-mcp、compatibility-mcp 的已有组合与历史实验。它们不覆盖全部组合，
  不再作为 E0/E3 或自动路由的三选一。后两入口当前仅有 legacy stdio 支持，
  扩协议仍走 F1；不能把文档中可组合等同于运行时已支持任意组合。
- E0 输出轴到既有代码的复用图和不支持组合清单，避免第三套发现/执行引擎。
  `tool_exposure` 与 `compatibility_route` 含义保持不变；若需新配置或路由能力，
  另行冻结具体范围，不自动把十轴迁移为数据库字段。

已知缺口必须进入 E0 卡片：deferred 不转发客户端取消；compatibility 不转发
取消/进度、内层丢外层 `_meta`、JSON-RPC error 被包装为工具错误，且 dirty
不强制调用前刷新。E0 为各项确定“保持并声明差异 / 修复 / 该任务拒用”的
处置与验证责任，分别归入 X6/X9/X10；修复前不能把目标写成已具备能力。

### 统一阶段与任务编号

下表唯一维护先后关系和进入/退出条件。E0–E8 沿用原效率路线编号，避免既有
实验记录失去对应；R 为交付收口，F 为条件性扩展。角色表示工作责任，不授予权限。
Developer 负责仓库实现/文档，Verifier 负责隔离验证和证据，Operator 负责经批准
的安装与现场切换；用户决定范围、预算、取舍及生产启用。

| 编号 / 工作包 | 当前状态与前置 | 可审阅交付物 / 退出条件 |
|---|---|---|
| R0 — 基线与需求冻结 | 下一步；本次已完成文档层盘点，执行冻结未完成 | 固定源码、工作区差异、旧产物/部署摘要；逐项关联原需求、当前契约及未决门槛，不覆盖已有未提交修改 |
| R1 — 回归与稳定性收口 | R0；现有完整回归尚未全绿 | 对已记录目录残留和 HTTP BrokenPipe 失败取得复现/诊断证据；必要修复后完整离线通过，记录失败与复测而非只挑专项结果 |
| R2 — 构建、安装与现场验收 | R1；环境操作另批 | 新 sdist → wheel、逐字节/清单校验、两个安装入口；真实 Windows/WSL 已安装 runner 双向调用和产物传输、清理、目标客户端加载及恢复回执 |
| R3 — 基线发布决策 | R2 + 对目标部署的批准 | 一致源码/产物/配置版本、备份恢复点、变更窗口、停止条件、成对升级/回退方案；实际发布后才更新 release 状态 |
| E0 — 十轴契约与组合约束冻结 | R0；可与 R1 并行 | 十轴证据卡、既有原生/整库/固定入口复用图、保真缺口处置、可用/拒用/未实现组合清单与任务批准材料；不实现宿主预留 |
| E1 — 发现、定义、执行与目录恢复 | E0；主责 X2/X3/X6/X9 | 复用现有 facade/校验器的有界惰性索引、精确名称/关键词/别名/库过滤及精确 schema；缓存/dirty/执行资格与选定保真修复；搜索不披露、不调用业务、不遍历启动全部后端；失效/启动成本有测试 |
| E2 — 批量选择事务与动态目录 | E1；主责 X4/X5/X7，涉及 X6/X9/X10 | 一次调用多目标展开/折叠及混合切换（范围与事务规则见下）；原子选择、版本冲突恢复、空选择保留入口、同名 schema 更新、整库与批次 no-op 不通知、重连与并发及选定动态保真修复测试；不把未确认 grouped 纳入范围 |
| E3 — 逐轴能力与组合验收 | 既有行为证据采集可随 R1/E0；新增 E1 能力的组合需 E1，批量动态组合另需 E2 | 每个精确环境的十轴选择/能力回执；按适用行为验证通知/重列/请求、缓存/包装/说明/执行/恢复/拓扑；独立连接隔离与单连接共享分别证明，再验收目标组合端到端行为 |
| E4 — 真实模型筛选 | E3 合格 + 明确任务/模型/浏览器/预算批准 | 冻结 DS4F 首批任务、人工质量评分、完整成功/失败/恢复/费用记录；先淘汰不兼容候选，再付费比较 |
| E5 — 有界确认 | E4 + 入围决定 + 后续预算 | B′ 与至多一个入围方案重复配对；单独批准共享连接任务和 GLM-3.8F 复核；结果不确定则保留基线 |
| E6 — 效率方案发布决策 | E5 + 最终候选 R1/R2 门槛 | 质量、语义、兼容性、整任务收益和回退评审；仅为通过的环境/场景批准上线，不自动切全局默认 |
| E7 — 完整功能模块化 | E6 作出结论 + 独立架构任务 | 分离连接管理、目录/索引、裁剪/投影、搜索、披露、执行路由；明确依赖与共享状态所有权；行为/导入/打包回归通过，保持两组件 |
| E8 — 条件性宿主协同 | E7 完成 + 已证实通用能力缺口 + 单独批准 | 仅此时比较宿主原生演进与协同组件；必要时再设计 scope/view 绑定、注册/释放与隔离验收；没有必要则不做 |
| F1 — 协议能力扩展 | 具体需求 + 对应协议/所有权契约；独立于效率主线 | 按能力族扩展 legacy 字段/现代 HTTP schema、resumption、扩展语义；Tasks/MRTR/订阅/资源/提示及 roots/sampling/elicitation 分别验收，未实现不声明 |
| F2 — 更广 SDK / conformance / 双主机 CI | 有可固定版本的 runner/SDK、环境与预算 | 扩展 Python/TypeScript/C#/Java/Rust 互操作与官方 HTTP conformance；真实 Windows/WSL CI。单一 SDK 五例不称完整 conformance |
| F3 — 持久目录与更广证据关联 | 确认恢复/诊断需求 + 隐私/失效/保留契约 | 磁盘目录或资源/提示缓存；原生 HTTP 关联等按需扩展。限定容量、过期、损失与清理，不承诺无损全量追踪 |
| F4 — P5 外部能力仓库 | Operator 提供信任/签名/撤销/传输契约 + 客户端能力证据 | 稳定身份、去重、有界检索、精确 schema、显式 enrollment 映射；故障与回退测试。发现元数据不授予安装/启动权限 |
| F5 — 不可信本地用户加固 | 明确改变当前信任模型 + 架构批准 | ACL socket/pipe、目录句柄/reparse 安全、身份配额及真实 NTFS/ext4 对抗验收；不是当前可信本地配置的必做项 |

### E2 的一次调用批量展开/折叠要求

**已确认开发要求：一次工具调用可以展开多个目标、折叠多个目标，也可以在同一
请求中合并展开与折叠，完成一次选择切换。** 可采用事务操作列表，或单一工具
接收展开/折叠目标列表；具体工具名和参数 schema 在 E0 冻结，不要求模型逐项
调用，也不要求先 begin、再多次修改、最后 commit。宿主把多个独立调用并行
发出不能替代此要求。

目标可以表达明确工具批次或整库选择。E0 必须区分同一 facade 内的多工具与
分属多个 facade 的多库操作：先核对现有入口所有权，再确定一次调用的可控范围
及路由。跨独立 MCP 连接没有现成的全局事务；若需要统一批量入口，必须先形成
具体通用设计并确认范围，不借此安装宿主插件或扩大到未登记 peer。不得用
“只支持单库内列表”冒充已完成用户的一次多库展开/折叠需求。

- 一次请求先有界校验全部目标、目录版本与冲突，再原子发布最终选择；失败不
  发布部分选择，不暴露“先折叠后展开”的中间目录。预检可能发生的后端启动/
  发现另计成本，目录事务不等于业务调用或进程启动可以回滚。
- 展开/折叠列表重叠、未知目标、过期版本等必须有确定的整批拒绝与恢复规则；
  重试相同最终选择为 no-op，不发新通知。同名 schema 变化仍算定义变化。
- 一次提交返回完整有界结果和恢复入口；每个受影响的目录只为最终有效变更
  发一次通知，不为每个目标分别发布。跨连接能否原子提交必须单独证明，不能
  把逐连接成功结果拼装成“全局原子”。
- E2 验收覆盖：一次展开多个、一次折叠多个、一次混合切换、部分目标无效、
  重叠目标、过期版本、并发、重试及无变化；E3 验证宿主后续请求看到最终目录。
- E4/E5 记录批量调用相对逐项/先折叠再展开的模型往返、目录变更、未缓存输入
  与整任务费用。目标是避免两步操作额外造成的两轮未缓存输入；实际节省依赖
  宿主刷新与缓存行为，不能预先承诺固定减少两轮或完全没有刷新成本。

该要求归入已有 E2，不新增主模式；当前 B′ 整库开关不视为已具备批量事务。

### E3 的共享识别与隔离范围

共享判断先验证连接/进程拓扑与登记策略，不从相同工具名、业务 MCP 名或模型
传来的身份推断。多逻辑客户端共用一个业务后端不意味着目录必须共用；多个
Agent 共用同一 MCP 连接时，桥目前无法将目录更新定向到其中一个 Agent。

近期在无新增插件前提下验证宿主已有的独立连接/项目配置能力；具备条件时，
以多个 facade 连接实现目录隔离，并保持登记规定的同一个共享业务后端。
不具备条件时明确报告连接内共享，保留批量修改和冲突恢复，不能宣称已实现
per-Agent 隔离。当前 DSH Web 观察不能证明项目级配置已经造成物理连接隔离。

将 [桥行为轴矩阵](MCP_EXPOSURE_TAXONOMY.md) 的十轴行为、组合约束和目标/现状
差距作为 E0/E3 的检查参考，不按十个产品建立十套桥模式，也不强制三选一。
这不采纳其中的待选策略或提议字段。若后续确认新增探针字段并进入实现，须提升
`PROBE_VERSION` 并兼容旧回执；无证据保持 unknown。
可信调用身份本身也不证明定向披露，还需身份与实际请求目录作用域的完整绑定。
若目标客户端不能在第一次工具决策前呈现 canonical instructions，该环境不合格；
先核对原生支持或同源生成的兼容说明，不能据此提前启动 E8 插件。

## 4. 逐轴与组合验收、发布及回退

| 验收对象 | 最小证据与决策 | 阶段归属 |
|---|---|---|
| X1/X8 接入与后端 | 支持的协议路线、登记策略、所有权、共享/独立进程及 ID/结果投影分别验证 | R1/R2 + E3 |
| X2/X3 发现与呈现 | 宿主原生选择启用证据，或有界桥搜索；精确 schema 及其进入请求/历史的载体 | E1 + E3 |
| X4/X5 目录与提交 | 增加/移除/同名更新、一次批量展开/折叠/混合、no-op、冲突与失败原子性；逐项验模型请求 | E2 + E3 |
| X6/X10 执行与保障 | 原名或包装调用的语义、说明首次可见、审批/确认身份；必需项失败则组合拒用 | R1/E1/E2 + E3 |
| X7/X9 作用域与恢复 | 独立/共享目录、重连/dirty/旧目录/idle/退出与清理；不从进程拓扑推导目录隔离 | R1/E1/E2 + E3 |
| 目标组合 | 逐轴通过后测试实际互相影响：批量刷新×共享并发、重连×目录版本、包装×审批、载体×卸载等；不要求全部可能组合 | E3 |
| 真实任务与发布 | 同环境同任务先验证质量再比较总成本；记录完整十轴取值、改变的轴、收益、限制与回退 | E4–E6 |

E0 为每个候选形成有界组合记录：各轴的环境能力、选定行为、实现状态、证据与
失败处置。未知、不适用、未实现分别记录；单轴测试通过不等于任意组合可发布。
这些是验证材料要求，不是新增探针字段或公共配置 schema 的批准。

E4 依据问题选择少量组合，优先在其余条件固定时比较改变的轴；必要的多轴联动
如实记录，不能把联动收益归因于单轴。原生目录、整库保留、显式批量、固定入口
是原提案的四个参考条件，M0/M1/M2 仅是历史简称，不强制每个环境都跑这四组。
宿主原生搜索是否启用必须记录，不为偏向候选静默禁用。第 6 节原八会话预算仍
是待批准上限；新组合须在执行前重新冻结任务卡，不能自动扩大组合数或试验预算。
E5 仍以匹配环境的 B′ 与至多一个入围方案复核；若计划替换该环境已有普通原生
路线，E6 还需该路线的可比证据，不能只凭胜过 B′ 推广。

E6 按精确环境发布启用/保留/拒用的**行为组合**，列出十轴选择、源码/产物摘要、
通过的能力、未证限制、实际配置/入口及恢复动作。保持有效的轴上行为，只对
需变更的行为评审影响；组合切换可能仍需新连接或配置变更，必须验证实际恢复。
自动路由字段、默认策略或配置迁移若需改变，另列具体范围，不能由文档中的轴
自动生成。R3 收口基线；E6 只发布通过验收的候选，E7/E8 顺序不变。

仓库验证采用 [Verification](VERIFICATION.md) 的现有命令；当前没有
`docs/verification/MATRIX.md`，不引用不存在的矩阵。完整测试从仓库根执行：

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -t .
```

每项代码变更先跑相关测试，发布候选再完成完整回归、构建清单/source-byte、
隔离安装与现场门槛。本次纯文档整理仅检查链接、编号/依赖、需求覆盖及差异，
不产生新的 runtime 测试、安装或现场通过声明。

现场门槛统一包含精确主机/客户端版本、临时登记表/端口/工作区、双向所有权、
启动 pruning、断线重连、instructions 模型可见性、控制范围、generation 退出、
客户端配置回退及临时资源清理。真实 Windows 文件系统/进程行为不能由 Linux
角色模拟替代；DSH Web legacy 证据不能外推 TUI/headless、Codex/Claude、现代协议
或原生 HTTP。同一次现场测试可服务 R2/E3，但必须分别记录通过的断言。

[旧安装切换准备](../release-artifacts/DEPLOYMENT_ACCEPTANCE_PLAN.md) 是旧候选的
历史准备材料，执行前需重新核对哈希、目标和路径；不能复用其中旧进程/目录身份
直接操作。新候选发布须保存匹配的程序、配置及一致数据库恢复副本，并分别审批
安装验收与生产切换。失败、清理未完成、源码/配置漂移或业务结果不确定时停止
对应批次，不扩大预算、不自动重放业务调用。

## 5. 来源与需求覆盖

| 原有计划/来源 | 在本计划中的去向 | 保留边界 |
|---|---|---|
| P6 传输/投影/缓存、P7 控制、P8 证据 | 已实现部分为基线；剩余验收 R1–R3；扩展 F3 | 不重开已交付功能；不扩大缓存/关联语义 |
| P9 真实环境验收 | R2 + E3 | 安装、模型可见与恢复分别留证 |
| P10 双时代协议及兼容验证 | 已实现 tools 交集为基线；余项 F1/F2 | 历史设计的 roadmap-only 状态不覆盖当前实现 |
| 通用效率 E0–E8 及真实任务计划 | 保留编号并接入上方依赖图；第 6 节为详细合同 | 真实任务、预算与发布门槛不因整合而删除 |
| 十轴行为、组合约束、共享判断与无插件隔离需求 | E0/E3 的证据与可行性核对；[桥行为轴矩阵](MCP_EXPOSURE_TAXONOMY.md) | 逐轴区分目标/现状；M0/M1/M2 仅留组合示例，待选策略及新增字段不直接驱动迁移/默认行为 |
| grouped、分组/风险元数据和自动策略形态 | 仅留矩阵待选；选型确认后才关联实施任务 | 不自动并入 E2；不硬编码维护/浏览器/业务工具名单，不自动据意图折叠 |
| viewId/COW、prompt-fold、warm-injection、Agent scope 插件 | E8 条件候选 | 当前不预留、不开发；不作为未来必交付承诺 |
| P5 外部仓库及不可信本地加固 | F4/F5 | 外部信任和架构前提不具备时保持暂缓 |
| B′ 实验与限定部署记录 | E0 对照证据 + 第 6 节历史证据 | 不是通用效率、当前全量回归或全项目发布验收 |

**下一步执行任务：R0 + E0。** 先冻结可复核的基线、复用图和验收卡，随后推进
R1 与 E1。此处只确立完整路线，不启动代码实现、付费试验、真实浏览器或部署。

## 6. 通用效率方案详细合同

以下保留 2026-09-09 通用效率路线的详细要求与实验边界；它们约束候选评估，
不表示每个候选已被选中。排期以第 3 节为准，桥行为矩阵末尾的候选仍未确认。
其中 full native exposure、whole-library、batched disclosure、stable meta-tool
是历史参考组合，不是当前必须采用的固定候选集合；实际候选按第 4 节逐轴冻结。
动态断言适用于启用 X4 变更的组合，固定转发验证 X6/X10，目录与恢复验证 X9；
不依赖 M0/M1/M2 标签决定是否验收。原四条件与预算文字作为提案边界保留，
不能覆盖当前的轴驱动选型，也不授权新增组合试验。
本节保留的 DS4F/GLM-3.8F 与八会话方案是原实验提案，不是本次批准的新批次，
也不限制日常研究任务的模型选择；未来执行时核对具体模型路由及当次批准。

**Status: generic search/fine-grained work is planned, not implemented or
production-approved.** The B′ whole-library baseline is separately committed
and deployed to the existing DSH Web Onshape/Taobao frontends; that limited
acceptance does not extend to the candidates below. This track improves
universal bridge capabilities first. It does not authorize plugin installation,
production configuration changes, real browser access or business mutations.
The work order is binding:

1. Design and validate generic discovery, search, definition inspection and
   disclosure capabilities; use real model tasks before production acceptance.
2. Later, fully split the bridge implementation by functional responsibility
   (connection management, catalog/indexing, trimming/projection, search,
   folding/disclosure and execution routing), with explicit contracts and tests.
3. Only after that functional modularization, consider DSH-specific cooperation
   as a separate future development track. Do **not** reserve DSH interfaces,
   Agent mappings, view adapters or plugin scaffolding in the current work.

Functional modularization must preserve the existing two-runtime-component and
stdlib-only boundaries unless a separate architecture change is approved. The
current generic baseline remains logical-connection isolation with a shared
catalog within each connection. E3 may verify isolation through independently
provided connections; per-Agent views inside one shared connection are not a
current implementation goal.

### Design: Separate Discovery, Disclosure and Execution

- Search returns bounded candidates (identity, short purpose, owning library and
  necessary operation/risk metadata). It does not expand tools, warm all matches
  into the next request, or send `notifications/tools/list_changed`.
- Definition inspection returns the exact selected tool schema without changing
  the exposed directory. Search and inspection use a revisioned, invalidated
  catalog; stale entries are not evidence that a tool remains executable.
- Disclosure changes are explicit and batched. Keep selected tools visible for
  the current task phase; do not automatically prune on every turn, after every
  call, or after a small number of unused turns. One Agent's inactivity does not
  establish that another Agent sharing the connection no longer needs a tool.
- Reapplying an unchanged selection must not emit another change notification.
  Keep ordering, descriptions and schema serialization stable; do not add
  per-request timestamps or counters to the tool-definition prefix.
- Retain a small, directly discoverable library control entry and a clear route
  back from a collapsed state. The tested B′ approach (explicit added/removed
  names plus the reopening path) is the selected source wording; it is not a
  guarantee of model compliance or a production deployment claim.
- Start search with local exact-name/keyword matching, normalization, bounded
  aliases and library filters. Do not add an external rerank model by default;
  consider it only if measured discovery failures justify its extra cost.
- Reuse useful ideas from tool-search/meta-tool plugins without stacking duplicate
  discovery layers or changing business MCPs. Metadata and policies must come
  from generic contracts, not hard-coded browser/Onshape/Taobao behavior.

### Candidate Strategies and Compatibility Boundaries

Keep the existing whole-library approach as the baseline; finer granularity must
prove useful rather than become the automatic default.

| Candidate | Behavior | Benefit to verify | Cost or boundary to verify |
|---|---|---|---|
| Whole library, retained | Expand when needed and keep open during sustained use | Few discovery steps and stable repeated use | Initial full-library schema cost |
| Stable meta-tool dispatch | Fixed entries for discovery/definition/execution, available from initialization | Execution need not change the native tool list | Discovery/history cost; underlying tool-name approvals, audit and result fidelity may differ |
| Batched fine-grained disclosure | Add a selected group of tools, then call natively | Less unused schema while preserving native calls | Harness refresh behavior, switch frequency and cache invalidation |

Fixed meta-tool dispatch is not presumed equivalent to native execution. Verify
schema validation, confirmation, cancellation, errors, images/attachments and
structured results. A bridge-forwarded call does not by itself preserve a
harness policy keyed to the original tool name. Never claim that prompt hiding
is a permission boundary or that a wrapper transparently preserves host guards.

Universal dynamic disclosure depends heavily on how each harness responds to
MCP changes, especially `notifications/tools/list_changed`. Establish capabilities
for the exact harness version and mode; never infer them from the client name:

| Observed evidence | Eligible strategy |
|---|---|
| Only initial discovery is reliable | Fixed entries present at initialization, or stable full native exposure |
| Harness rereads tools/list, but request schemas are unverified | Do not enable dynamic fine-grained disclosure by default |
| Additions reach the relevant subsequent model requests | Eligible to test additive disclosure |
| Additions, removals and same-name schema updates reach model requests | Eligible to test full dynamic disclosure |
| Unknown version/mode or incomplete evidence | Treat dynamic model exposure as unverified |

Test additions, removals, same-name schema revisions, rapid consecutive updates,
reconnection and shared-connection Agent interleaving. Record bridge state,
harness refresh and actual request-bound tool definitions separately. A catalog
revision or sent notification is not a client acknowledgement; tools/list,
status/toolCount, GUI labels and model self-enumeration do not prove model-visible
exposure. DSH Web evidence must not be generalized to other harnesses, profiles
or transport modes. Do not introduce an assumed acknowledgement or a DSH-only
workaround as a universal MCP contract.

### Real-Task Model Evaluation — Mandatory Before Production

Protocol fixtures remain necessary, but isolated expand/collapse exercises are
insufficient to judge fine-grained efficiency. Prepare real tasks with verifiable
completion criteria and natural tool selection:

| Task family | Example scope | Main observation |
|---|---|---|
| Sustained single-library work | Find official information, navigate related pages, verify conditions and provide sources | Discovery overhead and reuse of already exposed tools |
| Multiple task stages | Locate an item, read details and cross-check another source | Whether selected batches avoid repeated incremental disclosure |
| Switch and return | Inspect source A, then B, then return to A to verify a detail | Repeated folding/reopening and cache churn |
| Shared-connection interleaving | Multiple Agents alternate approved real tasks on one connection | Conflicting needs and directory oscillation without per-Agent isolation |

Prompts specify the goal, limits and deliverable, not target tool names or a forced
search/expand/call sequence. Real browser use requires human approval of the
concrete task, sites, allowed operations, identities, budget and cleanup before
execution. Prefer public read-only tasks initially; approval for browsing is not
authorization for login, messages, purchases or other business writes.

- Use only **DS4F / GLM-3.8F** for model testing. DS4F is the primary screening
  route; use GLM-3.8F for bounded follow-up on harder selection behavior, accounting
  for its slower execution. Verify exact configured model identifiers before
  invocation; do not silently substitute models or change the default route.
- Compare full native exposure, whole-library expansion retained, stable meta-tool
  dispatch and batched fine-grained disclosure on the same tasks. Stratify cold
  discovery and sustained/warm work; balance run order and document changing web
  data, provider cache effects, model/version and sampling differences.
- Predeclare session/request/output/time budgets and stop conditions for each
  batch. Preserve failed, abandoned and retried work; do not silently exclude
  trials, repeatedly tune wording mid-batch, or add runs until a candidate passes.
- Measure task quality/completeness and source accuracy first, then model requests,
  discovery/definition calls, directory transitions, invalid calls, repeated
  expansion, early stopping, uncached input, cache reads/writes, reasoning/output
  and elapsed time. Capture actual tool definitions around each change.
- Use verified provider/adapter accounting and a verified rate table for financial
  comparisons. Otherwise report disjoint token categories without invented prices.
  Full schema size on every request is not the same as fully uncached input cost.
- Compare the total cost of completing equivalent work, including failures and
  recovery. A smaller exposed catalog or an early-aborted task is not a saving.
  If repeated discovery/changes invalidate caches enough to exceed the retained
  whole-library baseline, reject the strategy or restrict its applicable cases.

### Production Gates and Delivery Sequence

1. **Design/task specification:** fix generic contracts, harness evidence matrix,
   task acceptance criteria and real-browser approval materials. No DSH reserves.
2. **Isolated prototype/protocol checks:** validate catalog freshness, batched
   changes, no-change/no-notification behavior, reconnect and result fidelity.
3. **Budgeted real-model comparison:** screen candidates, then perform limited
   follow-up with DS4F/GLM-3.8F; do not auto-expand the approved test budget.
4. **Production decision:** require acceptable task quality, preserved execution
   and result semantics, request-level compatibility evidence, no persistent
   directory churn, and a demonstrated whole-task benefit over the existing
   retained-library baseline. Keep a concrete rollback to that baseline and obtain
   separate approval before production changes. A per-scenario strategy is valid;
   there need not be one universally best disclosure mode.
5. **Later architecture track:** complete functional modularization after the
   generic evaluation decision. Only if a remaining host-specific need is confirmed
   and E8 is separately approved may a DSH cooperation design be evaluated; no
   plugin, Agent-scoped view architecture or placeholder is selected by this plan.

### Implementation Contract and Failure Handling

These are requirements for the candidate implementation, not new public tool
schemas or permission grants. Freeze the exact API only after resolving this
contract; do not introduce reserved DSH/Agent identifiers.

| Concern | Required behavior | Acceptance example |
|---|---|---|
| Catalog ownership | Bind an index to the registered target, negotiated protocol, backend contract/generation and catalog revision/digest; reuse existing discovery/validation paths | A reconnect or same-name schema change cannot reuse an old executable definition |
| Cold discovery | Fetch a complete bounded catalog lazily for the selected target; count connection, initialization and discovery work separately from schema exposure | Searching a cold library may start its registered backend, but does not call business tools or silently expose it |
| Atomic publication | Publish a validated catalog/selection as one revision; never publish partial pages or partially apply a requested batch | One unknown name or invalid schema rejects the entire selection and preserves the prior valid view |
| Selection state | Distinguish the catalog of known tools from the connection's selected subset and explicit whole-library mode | Library controls remain reachable even when the downstream selected set is empty |
| Concurrent changes | Serialize connection updates; additions compose with current state; destructive replacement/removal checks the caller's expected revision | A stale removal returns the current revision and a recovery hint, without undoing another caller's newer addition |
| Repeated requests | A no-op selection returns changed=false and the current state without a fresh notification; a repeated request remains recognizable as success | A model that retries an already-applied addition does not trigger another schema change |
| Bounded input/output | Cap query length, candidate count, result bytes, requested names, catalog pages/bytes and refresh duration; report truncation explicitly | Oversized requests fail before state mutation; search never falls back to dumping the full schema catalog |
| Uncertainty | Separate known removal, invalid selection, stale catalog and pending harness refresh; preserve execution outcome uncertainty | A timeout after business dispatch never triggers automatic replay |

Retain exact downstream tool names and schemas for execution; short search
summaries are navigation aids, not replacement contracts. Search stays within
registered peer scope and never enumerates local private registries or accepts
an arbitrary endpoint/command. The first version uses bounded in-memory indexes;
persistent indexes or learned aliases need a separate privacy/invalidation design.
Do not probe every library by launching every backend just to create a global
index. Start cross-library discovery from existing redacted registry summaries.

Selection requests express desired directory state, not a requirement that the
model track counters correctly. A stale revision response must offer a bounded
recovery path through the stable entry; evaluate the extra conflict/retry cost.
No-op detection and additive behavior must include schema revision changes: the
same set of names does not mean the same definitions. Notify once per committed
exposure revision; do not require a one-to-one notification acknowledgement from
the harness or misinterpret coalesced notifications as lost state.

The current B′ runtime still requests a refresh on an explicit repeated expand.
No-op/no-notification behavior is a **future candidate change**, not a claim about
`2ddb028`. Keep it isolated from the production baseline during comparison.

Fallback is explicit. A failed dynamic trial may be stopped and subsequently
rerun as a separately labeled fixed-entry/native trial within an approved budget;
never silently switch strategies mid-trial and score the result as the original
candidate. Merely returning metadata saying "native fallback" does not change
a harness that has not re-read the catalog. Some transitions require a fresh
connection or separately approved configuration change.

### Reuse, Protocol Boundaries and Operational Costs

Before adding new discovery/dispatch machinery, map the existing constant
compatibility facade, deferred facade, catalog validators and initialization
handling to the candidate requirements. Record which path is reused and why a
new path is necessary; avoid a third competing catalog or execution engine.
This mapping is not the later full functional modularization.

Do not equate trimming with summarizing schemas. Native definitions remain exact;
compact search metadata must not remove required parameters, side-effect warnings
or confirmation requirements. Static dispatch must preserve the supported result
contract, including images/attachments, errors and structured content. If the
harness's original-tool guard semantics cannot be retained or explicitly accepted,
that route is ineligible for the affected task despite lower token use.

Dynamic connection-dependent disclosure remains constrained by the verified
legacy protocol contract. Do not advertise it on modern/native-HTTP routes whose
discovery rules differ. A fixed facade candidate must also retain the existing
route's protocol and authorization boundaries; this roadmap does not broaden
protocol support. Preserve the registered shared/dedicated backend policy and
never spawn a separate business process just to isolate a display choice.

Measure startup and reconnect as real costs. Closing the last client can stop a
shared backend; repeated discovery/reconnection can therefore cost more than a
schema change alone. Add offline acceptance for last-client teardown, leases,
failed initialization, cancellation and clean temporary-client removal. Any live
keepalive/reload procedure needs its own scope and cleanup review; the B′ frontend
deployment technique is not a generic permanent connection-retention policy.

### Harness Acceptance Matrix and Evidence Receipt

For every candidate environment, produce one bounded receipt containing the
common identity/contract evidence and the assertions applicable to its selected
axis behaviors. Dynamic transitions apply to X4-enabled combinations; native
catalog/host selection evidence covers X2/X3, and wrapper/cache evidence covers
X6/X9/X10 under section 4. Mark inapplicable assertions explicitly; do not require
a stable catalog to mutate merely to pass a dynamic-transition test:

- exact harness build/mode/config fingerprint, model route, protocol and bridge
  source/artifact digest;
- initial tool definitions, an addition, a removal, a same-name schema revision,
  a burst of updates and reconnect, each with expected and observed final state;
- bridge catalog/exposure revision, refresh traffic and request-bound schema
  fingerprint/name set, correlated by local sequence identifiers rather than
  relying solely on clocks;
- observed stale-request count, time to usable definitions, and handling of
  business calls attempted during the transition;
- verdict per capability: verified, failed or unknown, plus the actual evidence
  location and applicable fallback. One successful addition does not certify
  removals, concurrent changes, reconnect or another transport.

A temporary wait used in a test (such as the prior 150ms DSH fixture hold) must be
reported and compared with actual harness behavior. It cannot silently become a
universal fix or be required by a production strategy advertised as unassisted.
Observers may inspect logs; the tested model does not get a diagnostic tool solely
to read its request logs. The deployed user task should succeed using the normal
available tool surface. Never erase a stale-request trial as a mere test nuisance:
classify it as harness incompatibility or transport timing, separately from tool
selection behavior, and include its consumed budget in the test accounting.

### Executable Real-Task Test Cards and Budget Proposal

Before each approved batch, freeze task cards with: the exact user prompt, target
sites/objects, permitted reads/writes, expected deliverable, manual scoring rubric,
initial browser/session state, reset method, source capture time, model route,
strategy/config/source digest and per-run plus batch ceilings. Public read-only
browsing still needs the user's approval. Do not automatically clear a real browser
profile, log out, or create fresh business credentials to make runs comparable.

Initial task cards to make concrete before approval:

- **Card A — sustained work:** answer two related questions from one official
  documentation site, follow the relevant references, and provide verifiable
  citations with conditions/limitations. A later user follow-up reuses that site.
- **Card B — switch and return:** compare information from two approved official
  sources, resolve a concrete discrepancy, then revisit the first source for one
  additional condition. Score completeness and evidence, not just numeric output.
- **Card C — shared connection:** two model Agents execute separate approved tasks
  using one connection with controlled interleaving. Do not put a forced
  expand/collapse sequence into their prompts. Count both Agents' work and every
  directory change in one trial budget.

Suggested **first paid screening batch**, subject to the concrete task/route
approval: DS4F, Cards A/B, four strategies, one run per cell = eight sessions;
maximum ten model requests/session, 2,048 output tokens/request, five minutes per
session and 80 model requests across the entire batch. These are ceilings, not
required consumption or universal correctness thresholds. Validate task feasibility
before freezing them; a ceiling hit is a recorded failure, not permission to add
requests. All turns/follow-ups, retries and any auxiliary model calls count.
No separate model grader or rerank calls are budgeted.

This first pass is descriptive screening only. A later, separately budgeted batch
compares the retained-library baseline against at most one finalist with repeated,
order-balanced trials; Card C and GLM-3.8F are added only through that explicit
follow-up plan. GLM results stay separate from DS4F results. A different model's
slower execution is not a disclosure-strategy regression. Do not automatically
run all models across all candidates, or tune and retest indefinitely.

Balance strategy order across task cards and reverse it in confirmation runs.
Record cache usage rather than claiming a fresh process guarantees a cold provider
cache. Distinguish empty model history from actually observed uncached input, and
retain continuing sessions when measuring warm follow-up tasks. If web content or
login state changes between arms, preserve that run with its comparability flag;
do not quietly replace it with a favorable retry.

### Cost Decision Rule and Work Packages

First score final task success/quality independently from first-attempt cleanliness,
invalid calls and recovery. Then report both cost per attempted task and aggregate
cost per successful task **including the cost of failed attempts**; the latter is
undefined when no task succeeds. Do not discard failed runs from the numerator.
Show per-task paired differences and individual outliers alongside totals/medians.

With verified disjoint accounting and prices, use:

```text
batch_cost = uncached_input * input_rate
           + cache_read * read_rate + cache_write * write_rate
           + output_including_reasoning * output_rate
           + separately billed retrieval/rerank/tool costs, if authorized
```

Do not add reasoning twice if already included in output, or sum an inclusive
prompt count with its cache component. Report wall time separately. A measured
change in cache misses is an observation, not proof that every missed token was
caused by disclosure. No universal "12.5 rounds" or guessed token rate is a gate.

Freeze quality tolerance and a minimum worthwhile improvement before confirmation
runs, using baseline variance and the actual prices. The first screening sample
cannot establish those tolerances retrospectively. Reject or narrow a candidate
that degrades critical task correctness, loses execution safeguards, cannot prove
request-schema compatibility, or repeatedly exceeds the retained-library cost.
An inconclusive result keeps the current baseline; it does not authorize release.
Do not give a successful candidate credit for an untested scenario or harness.

E0–E8 的唯一任务/依赖表见第 3 节；本节只保留实现与实验合同。
执行起点为 R0 + E0，随后推进 R1 与 E1；不因专题设计已写完就跳过基线冻结。

### Prior Evidence and Its Limits

The isolated B′ versus A′ DS4F experiment recorded six successful sessions per
arm, 20 requests each and zero invalid calls/early abandonment. B′ had fewer
output tokens but more total input in that small sample. These are wording and
recovery observations, not evidence that fine-grained disclosure or any third-party
plugin is production-ready. Preserve the original and improved experiment records
under `release-artifacts/tool-guidance-ab/` and
`release-artifacts/tool-guidance-ab-v2/` (including REPORT.md, CAPTAIN_REVIEW.md,
PLAN_V2.md and final-results.json where applicable). Experimental artifacts may
be local and excluded from release packages; they are not production receipts.

The distinct B′ frontend deployment is recorded in
`release-artifacts/deploy-b-prime-20260909T084545402105Z/RECEIPT.md`: commit
`2ddb028`, existing DSH Web Onshape/Taobao frontends, backend generations retained,
new entry definitions observed in the current request, and idempotent collapse
returning B′ fields. It did not exercise fine-grained disclosure, new search,
live business tasks or a DSH cooperation plugin. Use this as the scoped deployed
baseline, not as completion of E0–E6.

## 7. 暂缓事项的重开规则

F1–F5 和矩阵候选只有在第 3 节的前置条件满足、具体范围得到确认后才进入
实施排期；不能因它们出现在总计划里就认定已批准。远期事项不阻塞当前 R/E
主线，除非选定候选实际依赖它们。任何重开决定都要回写统一任务表，并保留
未采纳方案及原因，避免重新形成独立而无关联的计划。
