# 交接：SakuraTools 1.20.1 → 1.21.8 + NeoForge

状态：**未完成**。构建通过、注入成功、端到端未通。
对应分支 `neoforge-1.21.8`，HEAD `a321d46`。

> 先读 `README.md`。那里有完整的中文使用说明（注入步骤、日志关键字、端口分工、
> 1.21.8 的协议差异）。本文只讲**当前进度和卡点**。

---

## 1. 这是什么

C++ Windows DLL，反射式注入 Minecraft Java 客户端，做包转发：

1. JVMTI `ClassFileLoadHook` 改写 `net.minecraft.network.Connection.channelActive`，
   拿到 A 的连接和 netty pipeline。
2. 往 pipeline 插一个匿名 netty handler 做中继，拦下 A 的收包。
3. 本机起假服务器，B 客户端连上来，A 的整个世界流镜像给 B。

原代码针对 **MC 1.20.1**，本次移植到 **1.21.8 + NeoForge 21.8.52**。
架构保留，改的是版本相关的 API 形状。

## 2. 东西在哪

| 位置 | 内容 |
|---|---|
| `github.com/ctcakes/SakuraTools` 分支 `neoforge-1.21.8` | 已推送，三个提交：移植本体 → 注入器常驻+UAC+端口拆分 → 本轮修复 |
| `D:\Desktop\SakuraTools-git` | 本地克隆，与远端同步 |
| `D:\Desktop\SakuraTools-1.21.8-neoforge\` | 打包产物：DLL、注入器、`starain_inject.dll`、README、脚本 |
| `D:\MCLDownload\Game\.minecraft\versions\1.21.8` | 用户客户端。启动器 KKCraft，**启动参数直接进服，不经主菜单** |
| `%APPDATA%\.minecraft\proxy.log` | 运行日志（找不到退到 `%TEMP%`，再退到 `C:\`） |

GitHub 直连会被 reset，推送要带代理：
`git -c http.proxy=http://127.0.0.1:7897 push`

## 3. 已确认能用的部分（实机日志，非推断）

```
RetransformIfLoaded: enumerated 34980 classes, found 1 Connection, retransformed 1
proto refs: enc=… dec=… replace=… bind=… playCB=… playSB=… loginCB=… cfgCB=… hsSB=…
cacheJavaRefs: configureSer=… send=… flowSB=… pipMid=… replace=… intentFid=… intentClasses=…
BServer: bound 127.0.0.1:25566 (loopback; injector bridges 25565 -> here)
```

- ✅ Java 21 的类文件（major 65）能被 `class_edit.cpp` 改写，retransform 命中成功。
- ✅ `cacheJavaRefs` 和 `proto refs` 两行**所有指针非空** —— 本次改的每一个 1.21.8
  签名在 NeoForge 运行时都解析到了。
- ✅ 假服务器在 `127.0.0.1:25566` 正常监听，注入器桥接可用。
- ✅ A 侧完整走完 `KeyPacket → LoginAcknowledged → SelectKnownPacks →
  FinishConfiguration → PLAY`。
- ✅ B 的握手包能解出来（`RX …ClientIntentionPacket`）。

## 4. 关键前提：NeoForge 暴露 Mojang 官方命名

代码按**类名**找 Minecraft 类，而官方 `1.21.8.jar` 是混淆的。
NeoForge / NeoForm 在运行时把它重映射成 Mojang 官方命名，所以能跑。
已用客户端自带的
`libraries/net/neoforged/neoform/1.21.8-*/…-mappings-merged.txt`
核实运行时确实存在 `net/minecraft/network/Connection`，成员名是 `channelActive`。

方法/字段**只按描述符找**（`findMethodByDesc` / `findFieldByDesc`），不看名字，
所以 SRG 命名不影响。受影响的只有类名和描述符。
**Fabric 不行**（运行时是 intermediary 名）。

## 5. 当前卡点

### 现象

B 连上、握手到达 DLL，然后**再无下文**。整份日志里 `RX` 只有一种包：

```
RX net.minecraft.network.protocol.handshake.ClientIntentionPacket   (×14)
— 没有任何其他包 —
```

最终表现是 B 连接超时。A 那边一切正常，早就进 PLAY 了。

### 已定位的部分

协议切换切错了对象。pipeline 状态随时间变化：

```
initChannel 结束时:  splitter, FlowControlHandler, decoder, prepender, outbound_config, bside
channelActive 时:    splitter, FlowControlHandler, decoder, prepender, outbound_config, bside
握手到达时:          splitter, FlowControlHandler, inbound_config, prepender, outbound_config, bside
                     ↑ 槽位从 decoder 变成了 UnconfiguredPipelineHandler$Inbound
```

即在 `initChannel` 返回到 B 发握手之间，`decoder` 槽位被换成了 vanilla 的
「未配置」占位符。**「谁换的」还没有确定答案 —— 这是接手后要查的第一个问题。**
`logPipeline()` 现在会连 handler 的运行时类名一起打（`pipeline[before swap]` 那种行），
就是专门为查这个加的。

### 本轮已修（已推送，未实测）

1. **换 handler 时改名会撞名。** 原来用 `replace(旧名, "encoder", 新handler)` 做归一化改名。
   netty 创建新 context 时旧 context 还在，目标名字被占用就抛
   `IllegalArgumentException: Duplicate handler name: encoder`。
   改成 `remove` 再 `addBefore(下一个 handler)`，不撞名且保住槽位
   （解码器必须排在 prepender/encoder 前面）。

2. **异常处理会弹模态框。** `LogAndClearException` 用的是 `Dbg`，而 `Dbg` 弹*模态*
   MessageBox。这个函数是在 **netty 事件循环线程**上被调的 —— 弹窗一出来，
   那条连接的事件循环就停在那里等人点确定。改成只写日志。

两条都有实机证据支撑，但修完还没跑过。

## 6. 下一步怎么测

注入器窗口必须常驻（它是桥，关了 B 就断）。
一个游戏进程里只能有一份 DLL，重测前务必先关干净。

1. 关掉游戏和注入器
2. `reflective_injector.exe "D:\Desktop\SakuraTools-1.21.8-neoforge\MinecraftProxy_msvc.dll"`
   → 点 UAC
3. 启动 A（默认标题匹配 `neoforge,KKCraft`，会在 NeoForge 加载画面就注入）
4. 看到 `matched Java window` 那行时，让 B 连 `127.0.0.1:25565`

期望看到：

```
proto: decoder slot refilled (inbound)                  ← 新写法，不该再有 Duplicate handler name
BServer: RX …login.ServerboundHelloPacket               ← 关键。之前只到 ClientIntentionPacket
login: B says name='…'
BServer: RX …login.ServerboundLoginAcknowledgedPacket   ← B 进配置阶段
keepalive: nudged B while it waits in CONFIGURATION
config-mirror: …  <-- ends configuration               ← A 进服后
config-mirror: B is in PLAY
```

如果还是只有 `ClientIntentionPacket`，说明 B 的登录包**压根没发出来**（而不是解码失败），
方向要转向「B 是不是在等我们的响应才发 Hello」。
这时看 `pipeline[before swap]` / `pipeline[after swap]` 里 `decoder` 槽位的类名，
确认装进去的是 `PacketDecoder` 而不是占位符。

## 7. 1.21.8 架构要点（改代码前必读）

### 两个端口

| 端口 | 谁在监听 | 作用 |
|---|---|---|
| `25565` | 注入器进程 | B 连这个。注入器一启动就占住，B 想多早连都行 |
| `25566` | 游戏内 DLL | 真正的假服务器，只监听回环，由注入器把 B 桥过来 |

必须先拆开：25566 是 DLL 用*游戏自己的 netty 类*开的，A 没起来就没有它；
而 A 启动即连服务器，B 来不及。改成注入器先接住 B、等 25566 起来再做 TCP 泵送，
消除竞态。实现在 `injector/relay.c`，有独立自测 `tests/relay_self_test.c`
（关键用例：上游还不存在时先接受，之后再转发）。

### 协议状态切换

1.20.1 的 `Connection.setProtocol()` + `ATTRIBUTE_PROTOCOL` +
`PacketEncoder/Decoder.setProtocol()` 在 1.21.8 **全部不存在**，
编解码器围绕不可变的 `ProtocolInfo` 构造。

现在的做法是直接换 pipeline 里 `encoder` / `decoder` 两个 handler
（`Connection.setupOutboundProtocol` 内部也这么干，但它要一个 `PacketListener`，我们没有）。

handler 名字要注意：一个方向是真 handler（`encoder`/`decoder`），
另一个是 `UnconfiguredPipelineHandler` 占位符（`outbound_config`/`inbound_config`），
代码两个名字都探。

`ProtocolInfo` 来源：login / configuration / status / handshake 是现成静态字段；
PLAY 要从 `GameProtocols` 模板 `bind()`，需要
`RegistryFriendlyByteBuf.decorator(RegistryAccess)`，SERVERBOUND 那个还要一个
`GameProtocols$Context` 实现 —— 运行时用 `ClassBuilder` 现造一个
（这是新加的 `ClassBuilder::addInterface` 的唯一用途）。

### CONFIGURATION 阶段（1.20.2 新增）

LOGIN 和 PLAY 之间多了一整个配置阶段。**不自己造注册表** ——
B 停在这个阶段，等 A 连上真实服务器后把 A 的
`net.minecraft.network.protocol.configuration.*` 原样镜像过去，
直到 A 的 `ClientboundFinishConfigurationPacket` 才放 B 进 PLAY。
这样 B 的注册表和 A 完全一致。

PLAY 的 `ProtocolInfo` 在**第一个 PLAY 包到达时**从 A 的 pipeline 里取现成的
（`ensureBPlayProtocol`）。必须用 A 的真实 `RegistryAccess`，
否则 chunk / entity / item 这类包编解码不了。

### 另外几处

- **不再阻塞 A 的渲染线程。** 1.20.1 靠卡住 A 等 B 进 PLAY，
  但 1.21.8 里 B 的配置阶段要靠 A 和服务器交互才能走完，
  卡住 A 会把两个客户端一起锁死。
- **B 采用 A 的身份**（`kGiveBOwnIdentity = false`）：A 自己的
  `ClientboundLoginPacket` 直接镜像给 B，不合成。旧那套「B 用离线身份 + 隐藏 A」
  的代码保留但关闭，里面的线上格式还是 1.20.1 的。
- **配置阶段保活**：B 卡在配置阶段时客户端 30 秒收不到东西会断开，
  所以有个看门狗每 10 秒推一个 `ClientboundKeepAlivePacket`
  （这个包在 CONFIGURATION 和 PLAY 都合法）。

## 8. 工具与构建

```bat
scripts\build_msvc.bat    :: 三个产物 → build\
scripts\test_msvc.bat     :: 全部自测，需要 PATH 上有 JDK
```

```
python tools\verify_signatures.py native\*.cpp --mappings <client.txt>
```

`verify_signatures.py` 校验源码里写死的 **43 个类名**、描述符里引用到的类、
以及 **37 个关键签名**。没有实机时这是唯一可靠的验证手段 ——
它在本次移植里确实抓出过一个描述符错误。
局限：ProGuard 映射不带访问标志，所以不校验 static，必要时用 `javap -v` 复核。

`tools/mcdump.py` 解析 Mojang 的 ProGuard 映射，`tools/report.py` 逐类对比新旧版本签名。
换版本先跑这两个。

MSVC 路径默认找 `D:\Microsoft Visual Studio\18\Community`，可用环境变量 `VCVARS` 覆盖。
本机没装 cmake，批处理是实际构建路径。
1.21.8 的官方映射可以从
`libraries/net/minecraft/client/1.21.8-*/client-1.21.8-*-mappings.txt` 直接拿。

## 9. 容易踩的坑

| 坑 | 说明 |
|---|---|
| 注入必须在 A 连服务器之前 | A 是启动器直接丢进服务器的，没有主菜单。晚一步配置阶段就过去了，B 永远等不到 |
| B 有 30 秒读超时 | 客户端自己的 `ReadTimeoutHandler`。在 25566 起来之前没有任何东西能发给 B |
| 注入器必须常驻 | 桥在它进程里。第一版 `main()` 注入完就返回，进程一退 25565 和所有 B 连接一起没 |
| 不要用 `inject.ps1` / `starain_inject.dll` | 进程内注入，没有桥。走这两条路 B 要直连 `25566` |
| 重测前先关干净 | 25566 被上一份 DLL 占着的话，新注入的副本绑不上会失败 |
| 日志采样 | `[C2S PASS]` 每 tick 刷屏，已改成只打前 20 条和之后每 1024 条。`DROP`/`ROUTE` 照常打 |
