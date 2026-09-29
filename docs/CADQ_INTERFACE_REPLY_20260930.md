# CadQ 接口答复：`.FCBak`、`multiProcessAllowed`、默认档 `expand` 断言（2026-09-30）

CadQ 就登记形态提了三点需要桥回答的问题。逐条给出结论、可复核依据，以及桥自身的边界。
所有行号指向 `bridge_runtime.py`（仓库根，sha256 `f83bd801b435aa8c…`，与两台主机已安装运行时一致）。

## 1. `.FCBak` 行为变更：桥无依赖，不必改回"先硬链接再 rename"

结论：桥不读、不写、也不假设任何旁备份文件，同时**不假设暂存目标预先存在**。CadQ 的新行为（暂存目标从不预先存在、不再留侧备份）与桥的约定同向，可以保留。

依据：

- 全仓（运行时、`installer/`、`tests/`、`docs/`）检索 `FCBak` / `.bak` / `sidecar` / `backupPath`：零命中。
- 桥从不枚举业务 MCP 的输出目录。唯一一次目录扫描是桥清理自己 spool 的残件（`5901-5946`）：限定在 `allowed_artifact_roots` 下的 `.mcp-artifacts`，且只处理 `.partial` 与 `.partial-*`。
- 产物投递是**显式点名推送**，不是扫目录：`publish_artifact` 在 `6511-6521` 先把 `relativePath` / `name` 过 `_safe_artifact_name`（`6456`：单个跨平台安全文件名，拒绝路径分隔符），然后 `source = artifact_stage / relative_name`。没有 glob，没有"取最新/取兜底文件"的分支，没有任何"目标已存在"的判断。
- 桥自己的暂存目录恰恰要求**不预先存在**：per-stream（`7897-7900`）与 shared-generation（`3081-3088`）都是 `mkdir(parents=True, mode=0o700, exist_ok=False)`，由桥创建，收尾用 `shutil.rmtree` 拆除（`3136-3138`、`3479-3481`、`7951-7952`、`9319-9320`）。
- 今天这条路径并未启用：CadQ 登记中 `artifactDelivery.enabled: false`。
- 同向印证：桥在 inbox 侧提交产物时用的是同一套模式——先写 `.partial-<hex>`（临时名由 `secrets.token_hex(8)` 生成，天然不预存），再 `os.rename` / `os.link` 到最终名（`8270`、`8599-8603`）。

边界：将来 CadQ 若开启 `artifactDelivery`（`2352-2373`；多对单下允许开启），桥仍然只取 MCP 点名的那一个文件。`.FCBak` 不会被发布，也不需要被发布。

## 2. `multiProcessAllowed` 维持 `false`：同意

- 权威字段是 `process.concurrency`，`multiProcessAllowed` 是派生镜像（`_normalize_concurrency`，`284`）。CadQ 现登记的 `concurrency: "many-to-one"` 派生出的正是 `multiProcessAllowed: false`，与 CadQ 的结论一致。
- 多对多（CadQ §5 的 ① 两实例并发 + 真实导出，以及 license / 端口 / 缓存）桥这边从未提升：`concurrencyEvidence` 里记录的只是多对单的观测，不含任何并发实例证据。
- 因此 §5 的四条在桥侧一律视为**未通过**，CadQ 不必为桥改动 `multiProcessAllowed`。

## 3. 默认档 `action=expand` 的 5 行断言：赞成，且只能由 CadQ 补

- 边界：桥把视图钉成 `sharedState.mode=fixed` 之后，`mcp_tool_view` 在桥侧就被拒（`shared_view_fixed`），CadQ 的默认档分支根本不会被这条路径触发。桥因此**无法**断言 CadQ 的默认档行为，这条断言只能落在 CadQ 侧（其 MATRIX）。
- 桥只能断言自己那一半，现已有三处：`tests/test_bridge.py:584`、`875-893`（`rejectCalls` 归一化）；`1060-1072`（`_reject_call_rule`）；`2861`、`2913` 与 `tests/test_protocol_projection.py:1022`（`shared_view_fixed`）。
- 供 CadQ 校准的对照数据（2026-09-30 实测，两个客户端同时连接）：

  | 调用 | 结果 |
  |---|---|
  | `mcp_tool_view{action:"status"}` | 拒绝，`shared_view_fixed` |
  | `mcp_tool_invoke{name:"mcp_tool_view"}` | 拒绝，`shared_view_fixed` |
  | `mcp_tool_invoke{name:"cadq_list_models"}` | **正常转发**，返回真实模型清单 |

  第三条是对照组：缺少它，前两条只能证明"桥一概拒绝"，不能证明被拒的是**视图**。CadQ 侧那条默认档断言正是它的镜像。

## 现状（供 CadQ 校准）

CadQ 已按多对单登记并实测可用：`connect cadq` → `tools/list` 9 个工具；`mcp_tool_view` 报 `mode=shared`、`concurrency=many-to-one`；两客户端观测 `activeClients=2`、`ownedGeneration=3`，8/8 交错调用无串扰。`artifactDelivery` 仍关闭，`inputDelivery` 在多对单下由桥拒绝（`2401-2405`）。
