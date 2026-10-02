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

## 验收（真实环境 A/B）

1. **造故障态**：用 supervisor 正常起栈（`own`，注册表 4 个工具、`bridge_registry_list` 返回真实对端数据）→ `kill -9` supervisor（模拟会话被强杀，无人执行 `finally`）→ 杀掉 WSL 节点 → 剩下孤儿的 Windows 节点占着 8768/8770、8769 空闲。
2. **旧代码**（部署前备份）：打出用户看到的那句，exit 1，MCP 客户端 `server closed stdout`。
3. **新代码**（已部署）：走 `reclaim`，`wsl-node.log` 出现标记行，随后 `[wsl-bridge] peer link established`；MCP 客户端拿到 4 个注册表工具，`bridge_registry_list` 正常返回。

```
[supervisor] reclaim: the peer node answers its own registry while the local node is gone; starting only the local node
[wsl-bridge] local control listening on 127.0.0.1:8769
[wsl-bridge] peer link established
```

离线测试：`tests/test_dsh_registry_supervisor.py` 17 项通过（新增 `reclaim` 计划、只起本地节点、外来占用仍旧 fail、空闲/attach 不探测、探测不吃异常六类）；全量离线套件 644 项通过。运行时（`bridge_runtime.py`）**未改动**——这是刻意的，避免为一次探测再动 wheel 换码。

## 部署与回滚

- 部署对象：`~/.local/share/win-wsl-mcp-bridge/dsh_node_registry_entry.py`（DSH 各 profile 的 `registry` MCP 入口所执行的文件）。
- 备份：`~/.local/share/win-wsl-mcp-bridge/backups/20261002T070923Z-supervisor-reclaim/dsh_node_registry_entry.py.before`。
- 回滚 = 把备份覆盖回去（本改动只涉及这一个文件；`bridge_runtime.py` 与 wheel 均未动）。
- 生效时机：**下一次 profile 启动**（overlay/MCP 条目只在会话启动时加载）。已运行的会话不会自愈——实测 24 秒采样内 DSH 没有重试 `registry` 条目。

## 仍未修的深层缺口

1. **孤儿 Windows 节点本身没人回收**。`reclaim` 让它重新可用，但那个 Windows 节点始终没有 supervisor 管辖；下次 owner 异常退出仍会再产生一个（只是现在不会卡死了）。彻底办法是让 Windows 节点在失去 link 对端后自行退出，或让 supervisor 主动接管——前者要动共享运行时，后者要依赖 Windows 侧工具，均未实施。
2. **attach 侧的 `registry-mcp` 在 owner 死后不退出**（只回 `-32603`），使 DSH 无法靠重连自愈。`dsh_node_registry_entry.py` 的文档承诺了"退出→Harness 重连→新 owner"，实际不成立。
3. **`bridge_control status`/`bridge_diagnostics` 在这类故障下只给 `-32603`**，没有可区分的诊断信息。
