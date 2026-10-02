# CadQ / MeshQ 登记 + 两侧运行时换码记录（2026-09-30）

## 结论先行

1. 两台主机的运行时已换到含**并发轴**的版本；两侧 `site-packages/bridge_runtime.py` 与工作区 HEAD `873e67a` 是同一份（sha256 `f83bd801b435aa8c…`）。
2. Windows 注册表新增两行：**cadq**（多对单，带实测依据）、**meshq**（单对单，尚未部署）。
3. 三个 DSH profile 的 overlay 各加 `mcp-cadq` / `mcp-meshq`；**要重启 profile 才在模型侧生效**（人工步骤）。
4. 旧运行时被原子拒掉的结构化 `rejectTools` 现已生效，且**已用对照实验证明它是按工具精确生效的**。
5. 备份与回滚点见 §6。

---

## §1 为什么"先换码再登记"是硬前置（实测，不是推断）

隔离探针库在 `C:\MCP\ci-repro\probe\`，**没有碰线上注册表**。旧运行时（9/10 那份）的实际行为：

| 写进清单的内容 | 旧运行时实际行为 |
|---|---|
| `{"concurrency":"one-to-one"}` | 跑成 **dedicated = 多对多**——正好是单对单的**反面** |
| 结构化 `rejectTools`（dict 那条） | **整份清单被原子拒**：`registry entry '…' sharedState is invalid`，连库都不建 |
| `{"multiProcessAllowed":false}` | shared = 多对单（两侧语义一致） |

原因在代码里就一行：旧版 `_lifecycle_mode()` 是 `multiProcessAllowed is False ? "shared" : "dedicated"`，它**没有** `concurrency` 这个概念，只把该键原样存进 `process_json`。

新代码的规则（`bridge_runtime.py:284 _normalize_concurrency`）：

- `concurrency` 是**权威字段**，`multiProcessAllowed` / `enforcement` 是派生镜像（每行读的时候现算）；
- `multiProcessAllowed:false` + `concurrency:"one-to-one"` 会被判为**冲突并报错**，所以**不存在**"新旧运行时都表示单对单"的兼容写法；
- 非单对单的显式声明必须给 `concurrencyEvidence`（≤320 字符，一句话点名所依据的观测）；只写历史布尔值 `multiProcessAllowed:false` 的行走 legacy 分支，落到多对单。

**单对单的准入闸门在 `SharedBackend._attach`（`bridge_runtime.py:3021-3028`）—— 它跑在"持有该业务进程的那一侧"。** onshape / CadQ 这类 Windows 侧服务由 Windows 节点持有共享后端，所以 **Windows 节点必须换码并重启**，绕不过去。

## §2 换了什么、怎么换的

| 项 | 值 |
|---|---|
| wheel | `dist/win_wsl_mcp_bridge-0.4.0-py3-none-any.whl`，sha256 `877576bcd457a9da…`（版本号仍是 0.4.0，靠 sha256 识别） |
| 协议 | 两侧都是 `win-wsl-mcp-bridge/0.2`，换码前后**未变**，可以混跑 |
| WSL 运行时 | `~/.local/share/win-wsl-mcp-bridge/runtime`，`pip install --force-reinstall --no-deps` |
| Windows 运行时 | `%LOCALAPPDATA%\WinWslMcpBridge\runtime`：先 robocopy 到 `runtime.next` 预装并验证 → 停栈 → 两次改名切换 |
| supervisor | `~/.local/share/win-wsl-mcp-bridge/dsh_node_registry_entry.py` 更新为仓库版（含 RC7 的 Windows 路径修复） |

换码本身的验证：预置运行时用**旧运行时拒掉的那份清单**跑 `registry-init` 成功，并把它归一化成 `rejectCalls` —— 这就是"新校验器已在 Windows 上生效"的直接证据。

### 踩到的三个坑（下次别再踩）

1. **pip 生成的 `.exe` 启动器把解释器的绝对路径写在文件里。** 把 venv 目录改名（`runtime.next` → `runtime`）会让启动器**直接退 1 且没有任何输出**，现象是 supervisor 报 `Windows bridge node exited during startup (1)`。取证方式：`strings runtime/Scripts/win-wsl-mcp-win.exe` 里能看到 `#!C:\...\runtime.next\Scripts\python.exe`。**修法：改名之后在新目录里原地重装一次 wheel，重生成启动器。**
2. **改名切换与 DSH 重连有竞态。** 停掉栈所有者后，6 个 profile 的 supervisor 会在 ~0.5s 内同时重连并抢 8769 / 8768 / 8770；抢输的一方各自绑端口失败退出，**赢家也可能被输家的对端链路顶掉**，结果是"WSL 节点在听、Windows 节点全退"。下次换码应让 DSH 侧先停（或只留一个入口）再切。
3. **文档里写的失败转移没有发生。** supervisor 的设计是：attach 模式的 `registry-mcp` 失去节点后退出，Harness 重连把这个条目重启成所有者。实测：所有者死后，attach 侧的 `registry-mcp` **没有退出**，而是回 `MCP error -32603 internal registry error`（控制面回 `internal control error`），栈在 24 秒后仍然全停；五个 attach supervisor 都还活着。**这很可能就是那组 `-32603` 的真正来源**：不是"控制面坏了"，是"栈的所有者不在了，而失效的包装器不退出、所以永远不会自我恢复"。要恢复必须让那些 `registry-mcp` 退出（它们退出 → supervisor 退出 → Harness 重连 → 端口空闲者成为所有者）。这是一条**待修**项。

## §3 登记行（Windows 注册表）

清单 `%LOCALAPPDATA%\WinWslMcpBridge\registry.manifest.json` 现在四行；`registry-init` **不加 `--replace`**，逐行 `INSERT … ON CONFLICT(id) DO UPDATE`，所以存量两行不受影响。新增的两行：

```json
{
  "id": "cadq",
  "name": "CadQ MCP",
  "command": "C:\\MCP\\CadQ\\.venv\\Scripts\\python.exe",
  "args": ["-X", "utf8", "-B", "-m", "cad_agent.mcp"],
  "cwd": "C:\\MCP\\CadQ",
  "env": {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONUTF8": "1"},
  "management": {"ownership": "bridge-managed",
                 "agentControl": {"enabled": true, "allowedActions": ["drain","restart","stop"]}},
  "process": {
    "concurrency": "many-to-one",
    "concurrencyEvidence": "2026-09-30 bridge observation: two clients (connect cadq) shared one CadQ backend; the bridge view reported mode=shared, concurrency=many-to-one, ownedGeneration=3, activeClients=2, and 8/8 interleaved calls returned on their own connection with no cross-talk.",
    "sharedState": {"mode": "fixed",
                    "rejectTools": ["mcp_tool_view",
                                    {"tool": "mcp_tool_invoke", "path": ["name"], "equals": ["mcp_tool_view"]}]}
  },
  "capabilityGroups": ["cad", "geometry", "cadquery", "freecad", "export"],
  "artifactDelivery": {"enabled": false}
}
```

```json
{
  "id": "meshq",
  "name": "MeshQ MCP",
  "command": "C:\\MCP\\MeshQ\\.venv\\Scripts\\python.exe",
  "args": ["-X", "utf8", "-B", "-m", "meshq.mcp"],
  "cwd": "C:\\MCP\\MeshQ",
  "env": {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONUTF8": "1"},
  "management": {"ownership": "bridge-managed",
                 "agentControl": {"enabled": true, "allowedActions": ["drain","restart","stop"]}},
  "process": {"concurrency": "one-to-one"},
  "capabilityGroups": ["mesh", "geometry", "blender", "export"],
  "artifactDelivery": {"enabled": false}
}
```

落库后 `process_json` 的样子（注册表是权威）：

```
cadq  {"multiProcessAllowed":false,"sharedState":{"mode":"fixed",
       "rejectTools":["mcp_tool_view"],
       "rejectCalls":[{"tool":"mcp_tool_invoke","path":["name"],"equals":["mcp_tool_view"]}]},
       "concurrency":"many-to-one","concurrencyEvidence":"…","…"}
meshq {"concurrency":"one-to-one","enforcement":"bridge-shared-backend","multiProcessAllowed":false}
```

**对端（红acted）视图不含拒绝规则内容**：`bridge_registry_describe cadq` 只显示 `sharedState: {"mode":"fixed"}`。

## §4 验收证据

- **CadQ 真的经桥可用**：`connect cadq` → `initialize` ok，`tools/list` 9 个工具（`mcp_tool_catalog` / `mcp_tool_view` / `mcp_tool_invoke` + 6 个 `cadq_*`）。
- **参数级拒绝按工具精确生效**（三条一组的对照）：
  - `mcp_tool_view{action:"status"}` → `{"code":"shared_view_fixed"}`（拒绝）
  - `mcp_tool_invoke{name:"mcp_tool_view"}` → `{"code":"shared_view_fixed"}`（拒绝，**这就是结构化规则在起作用**）
  - `mcp_tool_invoke{name:"cadq_list_models"}` → **正常转发**，返回真实模型清单（bracket / cube …）
  第三条第是**对照组**：如果桥对所有 `mcp_tool_invoke` 一概拒绝，前两条就不能证明规则生效。
- **多对单依据（写进 `concurrencyEvidence` 的那次观测）**：两个 `connect cadq` 客户端同时连着，桥自己的视图报 `mode=shared, concurrency=many-to-one, ownedGeneration=3, activeClients=2`；8 次交错调用 **8/8** 都在各自连接上返回，无串扰。
- **存量两行未受影响**：onshape 仍 25 个工具且在线；taobao 在线。
- **一处归一化需知悉**：新代码把 taobao 的 `enforcement` 从 `business-mcp` 改写为 `bridge-shared-backend`（该字段现在是派生镜像）；原本那句话说在 `note` 里，信息没丢。

## §5 客户端曝光

三个 profile 的 `cordis-bridge-overlay.json` 各追加两条 `@deepseek-ai/dsh-mcp-client` 条目（`mcp-cadq` / `mcp-meshq`，`deferred-mcp <id>`，`reconnect` 打开，`failOnStartupError:false`）。备份：同目录 `cordis-bridge-overlay.json.before-cadq-meshq`。

**生效需要人工重启对应 profile**（web / dsh-tui / headless）：overlay 只在会话启动时加载。

### §5.1 overlay 的形状契约（首次落地时踩坏过，勿重犯）

这三份 overlay 的所有者是桥的 projection 子系统（`installer/projection.py`；其自己的注释把 DSH overlay 描述为 "wholly Bridge-owned and regenerated deterministically"）。顶层每一项必须是 **mapping**：

```json
{"insert":[{"id":"mcp-cadq","name":"@deepseek-ai/dsh-mcp-client","config":{…}}]}
```

第一次追加时写成了**裸数组**（缺 `{"insert":…}` 外壳），后果是 **dsh-tui / web / headless 三个 profile 全部无法启动**：

```
dsh: overlay entry 3 in …/cordis-bridge-overlay.json must be a mapping (a loader patch entry)
  at parsePatchList (…/dsh-app-boot/lib/index.js:1200)
```

（条目编号是 1-based；entry 3 = 第一个裸数组。）修法 = 补外壳，并按 `json.dumps(..., sort_keys=True, separators=(",",":")) + "\n"` 写回，与 `_dsh_overlay_text()`（`projection.py:1999`）的产出字节格式一致。校验手段（不必启动会话）：`dsh --profile <p> [--patch <overlay>] --dump-config`；或直接调用加载器 `loadOverlayPatches(binName, file)`。

注意 `dsh --profile web --dump-config` **不带** `--patch` 时看不到这些条目——web 的 overlay 由 `~/.local/bin/dshweb` 传入，dsh-tui/headless 的由 `~/.local/bin/dsh` 传入。两个包装脚本才是"谁加载哪份 overlay"的权威。

### §5.2 手加条目能不能活过一次 reconcile（已实测）

`projection reconcile` 对 DSH 是**合并**而非整体重建：它读回现有 overlay（`_dsh_overlay_entries`），只移除**桥自己账上**且不再需要的条目（`agent_mcp_projections` + 指纹匹配，`projection.py:2535-2552`），再写入期望条目，最后整体重写。因此**手加的、非桥归属的条目会被保留**（只是被重新排序/规范化）。

验证方式（零生产影响）：把三份 overlay 与 `projection.sqlite3` 拷进 `/tmp`，把副本里 env 行的 `config_path` 改指副本、并停用 codex/claude 两个 env，然后 `projection reconcile --projection <副本>`。结果：`mcp-cadq` / `mcp-meshq` 在三个副本里**都存活**（web/headless 被重排为字母序，dsh-tui 字节不变）。

但**代价是账目不完整**：这两条不在桥的投影账里，所以 `projection status` 不报告它们、`unenroll --remove-entries` 也不会清理它们。

### §5.3 若改由桥的投影权威接管（尚未执行）

桥的投影镜像是**全局**的：`peer_projection_state` 只认 `servers.enabled=1`，`_desired_entry_descriptors` 对每个环境套用**全部**镜像条目（`projection.py:3423`），没有"按环境筛选 server"的开关。当前镜像仍是 2026-09-14 的内容（只有 onshape/taobao）。

`projection reconcile --dry-run --refresh-from registry-path --peer-registry <Windows 注册表>` 的预演结果：

- 镜像 → `['cadq','meshq','onshape','taobao']`
- **codex** 与 **claude** 两个环境也会被 `add-or-update` cadq / meshq（超出本轮批准的"三个 profile"范围）
- web / dsh-tui / headless 的 overlay 被规范化重写

即：一旦走正统路径，cadq/meshq 会同时进入 codex（`~/.codex/config.toml`）与 claude（`~/.claude.json`）。MeshQ 的 Windows 部署尚未完成，此时把空指针推进这两个客户端会带来启动噪声——**故暂缓，等 MeshQ 部署落地后再决定**。

**已定（Operator 决定）**：保持现状（cadq/meshq 只挂在三个 dsh profile，手加但已证明对 reconcile 稳定）；等 MeshQ 的 Windows 部署完成、`--doctor` 与 MCP 验收通过之后，再一次性让桥的投影权威接管这两条（届时 codex/claude 一并受益，不会出现空指针）。在那之前不运行 `projection sync`，以免把未部署的 meshq 推进镜像。

**前置条件已满足（2026-10-02）**：MeshQ 已在 Windows 部署（`C:\MCP\MeshQ` + `.venv`）并经桥实测可用（见缺口 1）。因此"一起接管"这一步现在可执行；它会把 cadq/meshq 一并写进 codex 与 claude 两个客户端，**待 Operator 点头后再动手**。

#### 2026-10-02 实测预演（dry-run，未落盘）

`reconcile --dry-run` 自带 `--refresh-from`：刷新与对账都只作用于一份临时副本，源库、outbox、权限、甚至缺失的源目录都不动（`projection.py:3944-3976`）。所以整条接管链可以在零写入的前提下预演：

```bash
PATH="$HOME/.local/bin:$PATH" \
python3 wsl-bridge-mcp/bridge.py projection reconcile --side wsl --dry-run \
  --refresh-from registry-remote --local-port 8769      # 8769 = WSL 节点本地控制口
```

`PATH` 不是可有可无：`official-cli` 适配器直接 exec `codex`/`claude`，这两个 CLI 不在 PATH 上时整个 reconcile 会以 `[Errno 2]` 中断（该缺陷已修，见本节末）。

| 环境 | 结论 | conflicts | 会写的动作 |
| --- | --- | --- | --- |
| `~/.claude.json` (claude) | **error** | onshape, taobao | add cadq, meshq |
| `~/.codex/config.toml` (codex) | **error** | — | — |
| dsh-tui overlay | next_session | — | 无（四条都已 configured） |
| headless overlay | next_session | — | 规范化全部四条（launcher 统一为 `/usr/bin/python3`） |
| web overlay | next_session | cadq, meshq | update onshape, taobao |

镜像 → `['cadq','meshq','onshape','taobao']`。两个 `error` 各有原因，**都不是桥的文档损坏**：

1. **codex 的 CLI 现在跑不起来（用户环境问题，与桥无关）**：`~/.local/bin/codex` → `~/.codex/packages/standalone/current/bin/codex` → `…/releases/0.155.0-alpha.16.3-x86_64-unknown-linux-musl/bin` → `~/.vscode-server/extensions/openai.chatgpt-26.917.62051-linux-x64/bin/linux-x86_64`，而该 VS Code 扩展已升到 `…-26.930.21537-…`，旧版本目录被删，整条符号链接链断掉（`shutil.which("codex")` 返回 None）。修它属于用户环境（重装 codex / 重指链接），或把 codex 环境 `unenroll`。
2. **claude 的 stdio 条目无法被官方 CLI 反证（已知设计，此前未记录）**：`_cli_parse_claude_text` 只精确解析单行 `URL:` 的 native-HTTP 文本块；stdio 的 `Args:` 是空格拼接、env 是自由文本，无法无损还原，于是返回 None → 名字进 `unverifiable` → 每次 reconcile 按 "unmanaged name collision" 报冲突（`projection.py:3678-3685`）。实测 `claude mcp get onshape` 输出完全正常（能看到 `WIN_WSL_MCP_BRIDGE_OWNED=1`），是**解析侧**取不到，不是文档丢了。推论：claude 环境在 `official-cli` 适配器下**永远不会收敛干净**，`ok` 恒为 False。可选出路是把 claude 环境改回 `bridge-file` 适配器（桥直接拥有 `~/.claude.json` 的 `mcpServers`，可读回校验——测试里跑的就是这条）。

顺带发现：`_cli_list_entry_names` 会把 `claude mcp list` 的首行 "Checking MCP server health…" 也当成条目名（实测返回 `['Checking','onshape','taobao']`）。name 不在镜像里所以不污染 desired 集合，但它会进 `unverifiable`，是处解析脆弱点。

#### 预演过程中修掉的两个缺陷（`installer/projection.py`）

- **一个客户端 CLI 装坏会中断整次 reconcile**：`_run_tool` 原先让 `FileNotFoundError` 直接冒泡，绕过 `_reconcile_one_environment` 的 per-environment `except BridgeError`，于是 codex 的断链把 claude 与三个 dsh 环境一起卡住。现改为抛 `BridgeError`（`OSError` 与 `TimeoutExpired` 都转），缺失/不可执行/超时都记到**那个环境自己**的 `errorDetail` 上。
- **`--dry-run` 把冲突和 drift 一律报成 configured**：干跑分支无条件写 `ENV_STATUS_CONFIGURED`，于是预演永远看不到 conflicts/drift，而实跑是 error/drift——正是预演该暴露的东西被它藏起来。现在干跑与实跑共用 `_environment_verdict()`，两侧结论必须一致。

**结论：接管仍未执行。** 除了需要 Operator 点头，现在还有两个前置要定：codex 的 CLI 要么修好要么 unenroll；claude 的适配器要么接受"每次 reconcile 常驻冲突"，要么改成 `bridge-file`。

## §6 已知缺口与回滚

**缺口**

1. ~~**MeshQ 现在是空指**~~ → **已关闭（2026-10-02 复核）**：`C:\MCP\MeshQ` 已在位并带 `.venv`，经桥实测可用——`connect meshq` → `initialize` 报 `meshq 0.1.0`，`tools/list` 11 个工具（8 个 `meshq_*`：`doctor`/`caps`/`inspect`/`describe`/`views`/`animate`/`run`/`audit`，加桥的 `mcp_tool_catalog`/`mcp_tool_view`/`mcp_tool_invoke`）。因此三个 profile 里的 `mcp-meshq` **不再需要摘除**。原始记录留痕：`C:\MCP\MeshQ` 不存在：`tools/deploy_windows_stdio.py --plan` 说 106 个文件待发、`--doctor` 0/2（无副本、无 `.venv`），Windows 侧也没有 Blender。登记行是**声明**；在它自己的部署完成前，调到它只会明确失败。
2. **CadQ 的 `artifactDelivery` 仍关闭**：CadQ 的输出根固定且故意不可配置，v1 不做产物回传，调用方按它自己声明的 `output_dir` 取件（WSL 侧走 `/mnt/c/...` 直读）。这不是桥的产物保证。
3. **失败转移缺陷**（§2 坑 3）未修。
4. 换码后 **Windows 侧运行时目录名换过**：以后要再原地升级，请按 §2 坑 1 在最终目录里重装一次。
5. **overlay 里的 cadq/meshq 是"手加、桥不知情"的**：它们能活过一次 reconcile（§5.2 已实测），但不在桥的投影账里——`projection status` 不报告、`unenroll --remove-entries` 不清理。消除这处不一致的唯一途径是让桥的投影权威接管，而那会波及 codex/claude（§5.3），故挂起。2026-10-02 预演证实了代价：web overlay 的这两条会被判为 conflict（`next_session` + `conflicts=['cadq','meshq']`），headless 的四条则会被规范化重写。
6. **`official-cli` 适配器下 stdio 条目永远无法反证**（§5.3 实测，`projection.py:2194-2217`）：claude 环境因此每次 reconcile 都报 onshape/taobao 冲突、`ok` 恒为 False。既未改适配器（→`bridge-file`），也未改解析。另：`_cli_list_entry_names` 会把 `claude mcp list` 的 "Checking MCP server health…" 首行当成条目名。

**回滚**

- 备份目录：`~/.local/share/win-wsl-mcp-bridge/backups/runtime-upgrade-20260929T090346Z/`
  （`wsl-runtime.tar.gz`、`win-runtime.tar.gz`、`win-registry.manifest.json`、`win-registry.sqlite3`）
- Windows 旧运行时仍在位：`%LOCALAPPDATA%\WinWslMcpBridge\runtime.prev-20260929T090620Z`
  回滚 = 停栈 → `runtime` 改名走开 → `runtime.prev-20260929T090620Z` 改名回 `runtime` → 重启栈
  （注意：旧运行时的启动器内嵌路径仍是 `…\runtime\Scripts\python.exe`，所以回滚不会踩 §2 坑 1。）
- 注册表回滚 = 用备份的 `win-registry.manifest.json` + 备份库，或把清单里 cadq/meshq 两行删掉再 `registry-init`（无 `--replace` 时不会删除行，需要 `--replace`）。
- overlay 回滚 = 还原三个 `cordis-bridge-overlay.json.before-cadq-meshq`。

## §7 提交

见本仓库同一提交。MeshQ 侧的动作（发文件、建 `.venv`、装 Blender、`--doctor`、MCP 验收）仍是人工/需批准，不在此记录范围内。
