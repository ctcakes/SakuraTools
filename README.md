# SakuraTools

包转发。把一个 Minecraft Java 客户端的收包流，镜像给另一个客户端。

注入后 DLL 会做三件事：

1. 用 JVMTI 的 `ClassFileLoadHook` 改写 `net.minecraft.network.Connection.channelActive`，
   在方法入口插入一次 native 调用，拿到 A 的连接和 netty pipeline。
2. 往 A 的 pipeline 里插一个匿名 netty handler 做中继，拦下 A 收到的每个包。
3. 在 `127.0.0.1:25566` 起一个假服务器：B 客户端连上来登录，然后 A 的整个世界流被转发给 B。

端口有两个，分工是刻意的：

| 端口 | 谁在监听 | 作用 |
|---|---|---|
| `25565` | **注入器进程** | B 连这个。注入器一开始就占住它，B 想什么时候连都行 |
| `25566` | 游戏内的 DLL | 真正的假服务器，只监听回环，由注入器把 B 桥过来 |

为什么要拆开：启动器是用启动参数把 A **直接丢进服务器**的，A 不经主菜单。
如果端口要等 A 注入后才开，B 就得跟 A 的配置阶段抢时间；拆开之后 B 可以先连上
在 `25565` 等着，等 DLL 起来再桥过去，没有竞态。

**当前目标版本：Minecraft Java 1.21.8 + NeoForge 21.8.52（Java 21）。**
1.20.1 的代码在 `main` 分支，这里是 `neoforge-1.21.8` 分支。

---

## 为什么必须跑在 NeoForge 上

代码是按**类名**找 Minecraft 类的（`net.minecraft.network.Connection` 这种），
而官方 `1.21.8.jar` 本身是混淆的。原版客户端里根本没有这些名字。

NeoForge / NeoForm 在运行时会把游戏重新映射成 **Mojang 官方命名**，
这时 `net.minecraft.network.Connection` 才存在。所以：

- ✅ NeoForge / Forge 客户端 —— 可以
- ❌ 原版客户端 —— 不行
- ❌ Fabric / 纯 Loader —— 运行时是 intermediary 名（`net.minecraft.class_2535`），不行

方法名不影响：代码里除了少数几个，绝大多数方法和字段都是**只按描述符**找的
（`findMethodByDesc` / `findFieldByDesc`），所以 SRG 命名也不影响。
受影响的只有**类名和描述符**。

---

## 编译

```bat
scripts\build_msvc.bat
```

需要 MSVC（默认找 `D:\Microsoft Visual Studio\18\Community`，可用环境变量 `VCVARS` 覆盖）。
产物在 `build\`：

| 文件 | 说明 |
|---|---|
| `MinecraftProxy_msvc.dll` | 代理本体，反射式注入用这个 |
| `reflective_injector.exe` | 配套注入器，会等客户端窗口出现 |
| `starain_inject.dll` | 另一个注入 DLL |

脚本最后会调 `scripts\strip_tls.py` 清掉 PE 的 TLS 目录。
反射式加载不会跑 TLS 回调，带 TLS 的 DLL 一用 CRT 就崩。
本项目 `/MT` 静态链接出来的 DLL 本来就没有 TLS 目录，所以这步通常是空操作，
但换个编译选项后就可能需要了。

`CMakeLists.txt` 保持同步，装了 CMake 也可以用 CMake 构建，只是本机没装，批处理是实际路径。

## 测试

```bat
scripts\test_msvc.bat
```

只测字节码层，不需要 Minecraft，但需要 PATH 上有 JDK：

- `edit_self_test` —— `class_edit.cpp` 改写 `Code` 属性后偏移是否正确
- `emit_samples` + `Verify` —— 用 `ClassBuilder` 现造类，再让 JVM 以 `-Xverify:all` 加载验证

## 注入

**先跑注入器，再启动 A。** 注入器会立刻占住 25565，然后等游戏窗口出现再注入。
因为 A 是启动器直接丢进服务器的，注入必须赶在它连服务器之前完成。

```powershell
# 推荐：交互式菜单
powershell -ExecutionPolicy Bypass -File proxy\launcher.ps1

# 或非交互
powershell -ExecutionPolicy Bypass -File scripts\auto_inject.ps1 `
    -Dll <路径>\MinecraftProxy_msvc.dll `
    -Injector <路径>\reflective_injector.exe

# 或直接指定进程（注入器已运行时用）
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\inject.ps1 `
    -ProcId <pid> -Dll <绝对路径>\MinecraftProxy_msvc.dll
```

顺序：

1. 跑注入器（默认匹配窗口标题含 `KKCraft`，可用第二个参数改，传 `""` 匹配任意 Java 窗口）。
   会弹一次 UAC——游戏通常是提权启动的，不提权就 `OpenProcess` 失败（Error=5）。
2. 看到 `listening on 0.0.0.0:25565` 后，**B 就连 `127.0.0.1:25565`**。
   这时 A 还没起来也没关系，B 会停在「正在连接」等着。
3. **启动 A**。注入器检测到窗口 → 注入 → DLL 在 25566 起来 → 注入器把 B 桥过去。
4. A 自动进服，配置阶段流镜像给 B，B 进世界。

不用 `reflective_injector.exe` 时（`starain_inject.dll`、`inject.ps1` 这类进程内注入），
没有桥，B 要直接连 `127.0.0.1:25566`。

## 日志

`%APPDATA%\.minecraft\proxy.log`（找不到就退到 `%TEMP%\MinecraftProxy.log`，再不行 `C:\MinecraftProxy.log`）。

排查时重点看这几行：

| 关键字 | 说明 |
|---|---|
| `cacheJavaRefs:` | 后面一串指针，**任何一个 null 都说明对应功能没了** |
| `proto refs:` | `enc` / `dec` / `replace` / `playCB` / `playSB` 是否为 null |
| `RetransformIfLoaded:` | `found 1 Connection, retransformed 1` 才算类改写成功 |
| `BServer: bound 127.0.0.1:25566` | 游戏内的假服务器起来了 |
| `retransformed 1` | `Connection` 的字节码改写成功 |
| `keepalive: nudged B` | B 在配置阶段等待中，保活生效 |
| `login: B authenticated` | B 登录成功，进入配置阶段 |
| `config-mirror:` | 把 A 的配置阶段包镜像给 B，最后一条会带 `ends configuration` |
| `config-mirror: B is in PLAY` | B 真正进世界了 |

## 1.21.8 上这套东西是怎么跑的

和 1.20.1 相比，**协议层几乎全换了**。几处关键差异：

### 协议状态不再存在 attribute 里

1.20.1 靠 `Connection.setProtocol()` + `ATTRIBUTE_PROTOCOL` + `PacketEncoder/Decoder.setProtocol()`。
这些在 1.21.8 全没了：编码器/解码器现在围绕一个不可变的 `ProtocolInfo` 构造。

现在改成**直接把 pipeline 里的 `encoder` / `decoder` 两个 handler 换掉**
（`Connection.setupOutboundProtocol` 内部就是这么干的，但它还要一个 `PacketListener`，
我们没有，所以自己做）。handler 名字要注意：一个方向是真 handler（`encoder`/`decoder`），
另一个方向是 `UnconfiguredPipelineHandler` 占位符（`outbound_config`/`inbound_config`），
代码两个名字都会试。

`ProtocolInfo` 的来源：login / configuration / status / handshake 是现成的静态字段；
PLAY 要从 `GameProtocols` 的模板 `bind()` 出来，需要 `RegistryFriendlyByteBuf.decorator(RegistryAccess)`，
SERVERBOUND 那个还要一个 `GameProtocols$Context` 实现——
于是运行时用 `ClassBuilder` 现造一个类实现它（这是 `ClassBuilder::addInterface` 的唯一用途）。

### 多了 CONFIGURATION 阶段

1.20.2 起，LOGIN 和 PLAY 之间多了一整个配置阶段：服务端要把注册表、enabled features、
tags、known packs 发下来，客户端才会进世界。

这里**不自己造注册表**，而是让 B 停在这个阶段，等 A 连上真实服务器后，
把 A 收到的 `net.minecraft.network.protocol.configuration.*` 原样镜像给 B，
直到 A 收到 `ClientboundFinishConfigurationPacket` 才放 B 进 PLAY。
这样 B 的注册表和 A 完全一致，后面镜像过来的 PLAY 包才解得开。

### B 的身份：直接采用 A 的

B 收到的是 A 自己的 `ClientboundLoginPacket`，不重建、不合成。
所以 **B 看起来就是 A**，Tab 列表里的也是 A。

1.20.1 那套「B 用自己的离线身份、把 A 从 Tab 列表隐藏」的逻辑还留在代码里，
但被 `native/b_server.cpp` 里的 `kGiveBOwnIdentity` 关掉了（默认 `false`）。
想切回去的话要注意：里面合成包用的线上格式还是 1.20.1 的，
`ClientboundLoginPacket` / `ClientboundPlayerPositionPacket` /
`ClientboundPlayerInfoUpdatePacket` 的构造器和字段都变了，得先改。

### 不再阻塞 A 的渲染线程

1.20.1 会把 A 的 Render thread 卡住，等 B 进了 PLAY 再放行。
1.21.8 不能这么做——B 的配置阶段要靠 A 去和服务器交互才能走完，
卡住 A 会把两个客户端一起锁死。现在只要求**手动保证 B 先连**。

### 其它

- `FriendlyByteBuf` → 包构造器要的是 `RegistryFriendlyByteBuf`（子类，带回注册表引用）
- `ClientboundAddPlayerPacket` 已删除 → 改用 `ClientboundAddEntityPacket` 的 `uuid` 字段
- keepalive / custom payload 移到 `net.minecraft.network.protocol.common.*`
- ping / pong 移到 `net.minecraft.network.protocol.ping.*`
- `ClientboundGameProfilePacket` 改名 `ClientboundLoginFinishedPacket`
- `ClientIntentionPacket.intention` 从 `ConnectionProtocol` 变 `ClientIntent` 枚举
- bundling 不再靠 `BundlerInfo.BUNDLER_PROVIDER` attribute；代码本来就会把
  `ClientboundBundlePacket` 拆成子包再转发，所以只要保证**永远不直接往 B 写 bundle 包**就行

## 下次换版本怎么办

`tools/` 下有两个脚本，是这次移植用的：

```bash
# 从 Mojang 下载目标版本的 client.txt（官方混淆映射），然后：
python tools/report.py <旧 client.txt> <新 client.txt> out.txt   # 逐类对比新旧签名
python tools/verify_signatures.py native/*.cpp --mappings <新 client.txt>  # 校验源码里写死的名字
```

`verify_signatures.py` 会检查 `native/` 里所有写死的 Minecraft 类名、
描述符里引用到的类、以及 37 个「一旦查不到就整个功能消失」的关键签名。
**没有实机的情况下这是唯一可靠的验证手段**，换版本先跑它。

它有个已知局限：ProGuard 映射里没有访问标志，所以**不校验 static**。
代码里 `findMethodByDesc(..., wantStatic)` 是要匹配 static 的，
必要时用 `javap -v` 对 jar 里对应的混淆类复核。

## 已知风险

- 配置阶段这套「停住 B 等 A」的流程是这次新写的，**没有实机验证过**，
  是整个移植里最可能出问题的地方。
- `ClassBuilder` 只能发 Java 8（major 52）的类，且不能发 StackMapTable。
  现有的生成类都是直线代码，没问题；以后要加分支就得先补这个能力。
- `data/` 之类的运行期目录不参与构建。
