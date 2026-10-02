# 事故与修复：半死栈卡死所有 DSH profile（2026-10-02）

## 症状

某个 DSH profile 的 MCP 客户端 `win-wsl-bridge-registry` 启动即失败，子进程 stderr 只有一句：

```
bridge supervisor: required port already accepts connections but the local registry does not answer: [8768, 8770]
```

同时桥的控制面全废：`bridge_control status` 与 `bridge_diagnostics` 都回 `MCP error -32603 internal control error`，注册表类工具（`bridge_registry_*`）在该会话中根本不存在。

**先排除的假设**：用户怀疑是某 infra 项目的端口管理器占用了一批端口。**不成立**。Windows 侧共 44 个监听，8700–8800 段只有三个：8767（vision toolkit，设计如此）、**8768 + 8770**。而 8768/8770 的占用者是**桥自己的 Windows 节点**：

```
win-wsl-mcp-win.exe serve --registry C:\Users\…\WinWslMcpBridge\registry.sqlite3 --local-port 8768 --link-port 8770
```

## 根因：半死栈（half-dead stack）

| 部件 | 状态 |
|---|---|
| Windows 节点 | **在跑且健康**（`lifecycle status --local-port 8768` 正常返回 4 条登记，全部 `state:exited`、`activeClients:0`） |
| WSL 节点 | **已死**（WSL 侧无 8769 监听） |
| 它的父进程 | 已不存在 → Windows 节点是**孤儿** |

supervisor 的判定（`installer/dsh_node_registry_entry.py` 的 `_plan`）：

```
occupied = [8768, 8770]      # 被孤儿占着
stack_ready(8769) = False    # 本地 registry 没人应答
=> "fail"                    # 既不能 own（端口占用）也不能 attach（本地无栈）
```

于是**任何 profile 都拿不到桥**，而旧的 `control-mcp` 子进程又不会自己退出，只回 `-32603`。

**孤儿是怎么来的**：13:11 有个 profile 的 supervisor 把栈拉起来（日志 `peer link established`），13:13 它随所属会话消失——WSL 节点被杀，**Windows 节点却活了下来**并继续占着 8768/8770。`_terminate()` 对 interop 启动的 Windows 子进程做的是 `process.terminate()`；会话被强杀时 `finally` 更是压根不执行，因此这个子进程没有任何人回收。

## 修复：新增 `reclaim` 角色

`_plan` 增加第三视角：端口被占、本地节点不应答时，**再问一次对端节点**（向 Windows 节点的 local 端口发一次 registry 查询）。对端应答 → 半死栈，只补起缺失的本地节点；对端也不应答 → 维持 `fail`（外来监听器，绝不猜）。

```python
def _plan(occupied, stack_ready, peer_ready=False):
    if not occupied:   return "own"
    if stack_ready:    return "attach"
    if peer_ready:     return "reclaim"
    return "fail"
```

- **只起本地节点**：`reclaim` 分支只 spawn WSL 节点，然后 `_wait_for_peer(None, wsl_process, 8769)`，再 `_serve_registry(...)`；退出时只 `_terminate(reclaimed)`。
- **绝不动对端节点**：它不是本进程启动的，端口占用本身也证明不了归属。因此**不杀进程、不依赖 Windows 命令行工具、不去猜 PID**。
- **不猜外来监听器**：`_peer_node_answers()` 用一次有界的 registry 查询做身份识别；超时/拒绝/垃圾回包/错误应答（`BridgeError`/`OSError`/`ValueError`）一律视为"不应答"→ 仍旧 `fail`。
- **零成本探测**：只有"端口被占且本地不应答"时才发这一次探测；端口空闲（`own`）或本地可 attach 时不探测。

## 附带修复：attach 侧的本节点失联看门狗

事故之所以拖成"永久"，是因为 `attach` 模式那半边坏了：模块文档承诺"owner 消失 → 挂在它上面的 `registry-mcp` 失去节点并退出 → Harness 重连 → 新 entry 成为 owner"，但**实际上它不退出**，只对每次调用回 `-32603`，于是 DSH 永远不重连、栈不会自愈。

attach 侧看不到自己借来的节点的进程句柄，所以补一个看门狗：每 `LOCAL_NODE_CHECK_SECONDS`（5s）向本节点发一次真实 registry 查询，连续 `LOCAL_NODE_LOSS_LIMIT`（2）次不应答就以非零码退出。健康时不退出（查询与轮询频率无关，不会给节点刷日志）。只有 attach 模式启用——`own`/`reclaim` 模式本来就有子进程句柄可判断。

## 附带修复 2：节点不可达的诊断（那组 `-32603` 的根因）

上一条只解决了"attach 侧不退出"。用户看到的字面症状——控制面 `-32603 internal control error`、注册表 `-32603 internal registry error`——来自另一处：两个前端把**节点不可达**（`ConnectionRefusedError` 之类的传输失败）和其他未预期异常混成同一个 `-32603`。`_registry_mcp_dispatch` 只捕获 `BridgeError`，所以"端口没人听"这种 `OSError` 直接漏到最外层 `except Exception`，连异常类型都被丢掉；`_control_mcp_dispatch` 更彻底，压根没有捕获。

改法（`bridge_runtime.py`）：

1. 新增 `NodeUnavailableError(BridgeError)`。`local_registry_query` / `local_control_query` 只把**传输段**（connect / send / 握手读 / JSON 解析）的失败包成它，并把原始异常挂在 `__cause__`；节点自己回 `ok:false` 的业务性拒绝仍然抛普通 `BridgeError`，于是"节点答了错"和"节点没答"不再混淆。
2. 两个前端把 `NodeUnavailableError` 变成**工具级 `isError` 结果**（与注册表前端既有约定一致），带 `code` / `endpoint` / `summary` / `detail` 四个字段：`node_absent`（连接被拒 = 端口没人听）与 `node_unreachable`（超时、重置、握手早关、回非本桥 JSON = 有东西占着端口但不服务）。
3. 真正的内部异常仍回 `-32603`（消息文字不变，事故记录里的原句仍成立），但加了 `data.exception` / `data.detail`；控制面补上 stderr 一行（注册表本来就有）。

离线测试：`tests/test_modern_control_frontend.py` 新增 5 项——控制面的两种故障码、节点自答错误保持自己的 code、注册表侧的 `node_absent`、以及 loop 级 `-32603` 带异常类型与 stderr 行。

## 验收（真实环境 A/B）

1. **造故障态**：用 supervisor 正常起栈（`own`，注册表 4 个工具、`bridge_registry_list` 返回真实对端数据）→ `kill -9` supervisor（模拟会话被强杀，无人执行 `finally`）→ 杀掉 WSL 节点 → 剩下孤儿的 Windows 节点占着 8768/8770、8769 空闲。
2. **旧代码**（部署前备份）：打出用户看到的那句，exit 1，MCP 客户端 `server closed stdout`。
3. **新代码**（已部署）：走 `reclaim`，`wsl-node.log` 出现标记行，随后 `[wsl-bridge] peer link established`；MCP 客户端拿到 4 个注册表工具，`bridge_registry_list` 正常返回。

```
[supervisor] reclaim: the peer node answers its own registry while the local node is gone; starting only the local node
[wsl-bridge] local control listening on 127.0.0.1:8769
[wsl-bridge] peer link established
```

离线测试：`tests/test_dsh_registry_supervisor.py` 19 项通过（新增 `reclaim` 计划、只起本地节点、外来占用仍旧 fail、空闲/attach 不探测、探测不吃异常六类，以及 attach 看门狗的两条：节点失联则退出、节点仍应答则继续服务）；全量离线套件 644 项通过。运行时（`bridge_runtime.py`）**未改动**——这是刻意的，避免为一次探测再动 wheel 换码。

**看门狗的真实环境验收**：在已成对的栈上，用一个 attach 模式的 supervisor 挂上去（`tools/list` 4 个注册表工具、`bridge_registry_list` 正常）→ 杀掉被借用的 WSL 节点 → 约 15s 后该 supervisor 自行退出：

```
bridge supervisor: RuntimeError: attached bridge node stopped answering; exiting so the Harness reconnect can take over
server exited on its own with code 1
```

即"owner 死 → attached entry 退出 → Harness 重连 → 新 entry own/reclaim"这条链现在真的闭合了。

## 部署与回滚

- 部署对象：`~/.local/share/win-wsl-mcp-bridge/dsh_node_registry_entry.py`（DSH 各 profile 的 `registry` MCP 入口所执行的文件）。
- 备份（两次改动各一份）：
  - `~/.local/share/win-wsl-mcp-bridge/backups/20261002T070923Z-supervisor-reclaim/dsh_node_registry_entry.py.before`
  - `~/.local/share/win-wsl-mcp-bridge/backups/20261002T071937Z-supervisor-attach-watchdog/dsh_node_registry_entry.py.before`
- 回滚 = 把对应备份覆盖回去（本改动只涉及这一个文件；`bridge_runtime.py` 与 wheel 均未动）。
- 生效时机：**下一次 profile 启动**（overlay/MCP 条目只在会话启动时加载）。已运行的会话不会自愈——实测 24 秒采样内 DSH 没有重试 `registry` 条目；但看门狗补上之后，只要 owner 出事，attach 侧的 entry 会自己退出并触发重连。

## 当前运行状态（2026-10-02 事故后）

为避免"人不在这段时间桥是断的"，已将栈以 **detached 的一对节点**恢复（不是 supervisor 拉起，即 attach-ready 状态）：

- Windows 节点：`win-wsl-mcp-win.exe serve --registry …\WinWslMcpBridge\registry.sqlite3 --local-port 8768 --link-port 8770`
- WSL 节点：`win-wsl-mcp-wsl serve --registry ~/.local/state/win-wsl-mcp-bridge/registry.sqlite3 --local-port 8769 --link-port 8770`
- 两侧日志均有 `peer link established`；经节点查询对端注册表得到 `['cadq','meshq','onshape','taobao']`。
- 任一 profile 下次启动时，supervisor 会看到端口被占且本地 registry 应答 → 走 `attach`，不再 `fail`。

## 仍未修的深层缺口

1. **孤儿 Windows 节点本身没人回收**。`reclaim` 让它重新可用，但那个 Windows 节点始终没有 supervisor 管辖；下次 owner 异常退出仍会再产生一个（只是现在不会卡死了）。彻底办法是让 Windows 节点在失去 link 对端后自行退出，或让 supervisor 主动接管——前者要动共享运行时，后者要依赖 Windows 侧工具，均未实施。
2. ~~**attach 侧的 `registry-mcp` 在 owner 死后不退出**~~ → **已修**（见"附带修复"）：attach 模式现在由 supervisor 监视借来的节点并主动退出。注意 `control-mcp` 是另一条独立 MCP 条目（由 home patch 提供），它仍不会自己退出；只是它是按调用连接、节点恢复后即可用（事故后实测恢复）。
3. ~~**`bridge_control status`/`bridge_diagnostics` 在这类故障下只给 `-32603`**，没有可区分的诊断信息。~~ → **已修**（见"附带修复 2"）：节点不可达现在是工具级 `isError`，带 `node_absent`/`node_unreachable` 与 endpoint；真内部异常仍回 `-32603` 但多了异常类型。
