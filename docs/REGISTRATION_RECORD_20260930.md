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

## §6 已知缺口与回滚

**缺口**

1. **MeshQ 现在是空指**。`C:\MCP\MeshQ` 不存在：`tools/deploy_windows_stdio.py --plan` 说 106 个文件待发、`--doctor` 0/2（无副本、无 `.venv`），Windows 侧也没有 Blender。登记行是**声明**；在它自己的部署完成前，调到它只会明确失败。把它提前加进三个 profile 意味着每次会话启动都会尝试拉一个起不来的服务（`failOnStartupError:false`，不会毁会话，但是噪声）。
2. **CadQ 的 `artifactDelivery` 仍关闭**：CadQ 的输出根固定且故意不可配置，v1 不做产物回传，调用方按它自己声明的 `output_dir` 取件（WSL 侧走 `/mnt/c/...` 直读）。这不是桥的产物保证。
3. **失败转移缺陷**（§2 坑 3）未修。
4. 换码后 **Windows 侧运行时目录名换过**：以后要再原地升级，请按 §2 坑 1 在最终目录里重装一次。

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
