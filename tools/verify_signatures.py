#!/usr/bin/env python3
"""Check every Minecraft class name and descriptor hardcoded in native/ against
a Mojang mappings file.

The proxy resolves Minecraft classes by *name* and everything else by
*descriptor* at runtime, so a stale name from an older version fails silently:
ClassFileLoadHook simply never fires, or cacheJavaRefs logs a null and the
feature disappears.  There is no compile-time link to the game, so this script
is the only thing standing between a port and a typo.

Usage:
    verify_signatures.py native/*.cpp --mappings <client.txt>

Exits non-zero if anything fails to resolve.
"""
import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from mcdump import Mappings  # noqa: E402

# "net.minecraft.network.Connection"  -- a dotted class name in a string literal
DOTTED_CLASS = re.compile(r'"(net\.minecraft\.[A-Za-z0-9_$.]+)"')

# "Lnet/minecraft/network/Connection;"  -- a class reference inside a descriptor
DESC_CLASS = re.compile(r"L(net/minecraft/[A-Za-z0-9_$/]+);")

# A descriptor literal: "(Lio/netty/channel/ChannelHandlerContext;)V"
DESCRIPTOR = re.compile(r'"\(([^"()]*)\)([^"]*)"')

# Class names that are deliberately allowed to be absent: types from other
# projects (authlib, netty) and our own generated trampolines.
IGNORED_PREFIXES = (
    "com.mojang.",
    "io.netty.",
)


# (class, "method"|"field", name, descriptor) -- every one of these is looked up
# by native/b_server.cpp at runtime.  ProGuard mappings do not carry access
# flags, so static-ness is not checked here; it was verified against the 1.21.8
# jar with `javap -v` and is called out in the comments at each lookup site.
CRITICAL = [
    # --- ClassFileLoadHook patch target: the whole thing starts here --------
    ("net.minecraft.network.Connection", "method", "channelActive",
     "(Lio/netty/channel/ChannelHandlerContext;)V"),

    # --- serialization / protocol switching --------------------------------
    ("net.minecraft.network.Connection", "method", "configureSerialization",
     "(Lio/netty/channel/ChannelPipeline;Lnet/minecraft/network/protocol/PacketFlow;Z"
     "Lnet/minecraft/network/BandwidthDebugMonitor;)V"),
    ("net.minecraft.network.Connection", "method", "send",
     "(Lnet/minecraft/network/protocol/Packet;)V"),
    ("net.minecraft.network.Connection", "field", "channel", "Lio/netty/channel/Channel;"),
    ("net.minecraft.network.PacketEncoder", "method", "<init>",
     "(Lnet/minecraft/network/ProtocolInfo;)V"),
    ("net.minecraft.network.PacketDecoder", "method", "<init>",
     "(Lnet/minecraft/network/ProtocolInfo;)V"),
    ("net.minecraft.network.RegistryFriendlyByteBuf", "method", "decorator",
     "(Lnet/minecraft/core/RegistryAccess;)Ljava/util/function/Function;"),
    ("net.minecraft.network.RegistryFriendlyByteBuf", "method", "<init>",
     "(Lio/netty/buffer/ByteBuf;Lnet/minecraft/core/RegistryAccess;)V"),
    ("net.minecraft.core.RegistryAccess", "field", "EMPTY",
     "Lnet/minecraft/core/RegistryAccess$Frozen;"),

    # --- protocol templates -------------------------------------------------
    ("net.minecraft.network.protocol.SimpleUnboundProtocol", "method", "bind",
     "(Ljava/util/function/Function;)Lnet/minecraft/network/ProtocolInfo;"),
    ("net.minecraft.network.protocol.UnboundProtocol", "method", "bind",
     "(Ljava/util/function/Function;Ljava/lang/Object;)Lnet/minecraft/network/ProtocolInfo;"),
    ("net.minecraft.network.protocol.game.GameProtocols", "field", "CLIENTBOUND_TEMPLATE",
     "Lnet/minecraft/network/protocol/SimpleUnboundProtocol;"),
    ("net.minecraft.network.protocol.game.GameProtocols", "field", "SERVERBOUND_TEMPLATE",
     "Lnet/minecraft/network/protocol/UnboundProtocol;"),
    ("net.minecraft.network.protocol.game.GameProtocols$Context", "method",
     "hasInfiniteMaterials", "()Z"),
    ("net.minecraft.network.protocol.handshake.HandshakeProtocols", "field", "SERVERBOUND",
     "Lnet/minecraft/network/ProtocolInfo;"),
    ("net.minecraft.network.protocol.login.LoginProtocols", "field", "CLIENTBOUND",
     "Lnet/minecraft/network/ProtocolInfo;"),
    ("net.minecraft.network.protocol.login.LoginProtocols", "field", "SERVERBOUND",
     "Lnet/minecraft/network/ProtocolInfo;"),
    ("net.minecraft.network.protocol.configuration.ConfigurationProtocols", "field",
     "CLIENTBOUND", "Lnet/minecraft/network/ProtocolInfo;"),
    ("net.minecraft.network.protocol.configuration.ConfigurationProtocols", "field",
     "SERVERBOUND", "Lnet/minecraft/network/ProtocolInfo;"),
    ("net.minecraft.network.protocol.status.StatusProtocols", "field", "CLIENTBOUND",
     "Lnet/minecraft/network/ProtocolInfo;"),
    ("net.minecraft.network.protocol.status.StatusProtocols", "field", "SERVERBOUND",
     "Lnet/minecraft/network/ProtocolInfo;"),

    # --- handshake / login / configuration ---------------------------------
    ("net.minecraft.network.protocol.handshake.ClientIntentionPacket", "field", "intention",
     "Lnet/minecraft/network/protocol/handshake/ClientIntent;"),
    ("net.minecraft.network.protocol.login.ServerboundHelloPacket", "field", "name",
     "Ljava/lang/String;"),
    ("net.minecraft.network.protocol.login.ClientboundLoginFinishedPacket", "method",
     "<init>", "(Lcom/mojang/authlib/GameProfile;)V"),
    ("net.minecraft.network.protocol.configuration.ClientboundFinishConfigurationPacket",
     "method", "<init>", "()V"),

    # --- PLAY packet synthesis ---------------------------------------------
    ("net.minecraft.network.protocol.game.ClientboundLoginPacket", "method", "<init>",
     "(IZLjava/util/Set;IIIZZZLnet/minecraft/network/protocol/game/CommonPlayerSpawnInfo;Z)V"),
    ("net.minecraft.network.protocol.game.ClientboundLoginPacket", "field",
     "commonPlayerSpawnInfo",
     "Lnet/minecraft/network/protocol/game/CommonPlayerSpawnInfo;"),
    ("net.minecraft.network.protocol.game.ClientboundPlayerPositionPacket", "method",
     "<init>", "(ILnet/minecraft/world/entity/PositionMoveRotation;Ljava/util/Set;)V"),
    ("net.minecraft.network.protocol.game.ClientboundPlayerInfoUpdatePacket", "method",
     "<init>", "(Lnet/minecraft/network/RegistryFriendlyByteBuf;)V"),
    ("net.minecraft.network.protocol.game.ClientboundPlayerInfoUpdatePacket", "method",
     "write", "(Lnet/minecraft/network/RegistryFriendlyByteBuf;)V"),
    ("net.minecraft.network.protocol.game.ClientboundSetPlayerTeamPacket", "method",
     "<init>", "(Lnet/minecraft/network/RegistryFriendlyByteBuf;)V"),
    ("net.minecraft.network.protocol.game.ClientboundAddEntityPacket", "field", "uuid",
     "Ljava/util/UUID;"),
    ("net.minecraft.network.protocol.common.ClientboundKeepAlivePacket", "method",
     "<init>", "(J)V"),
    ("net.minecraft.network.protocol.ping.ClientboundPongResponsePacket", "method",
     "<init>", "(J)V"),
    ("net.minecraft.network.protocol.ping.ServerboundPingRequestPacket", "field", "time",
     "J"),

    # --- client state we mirror from A -------------------------------------
    ("net.minecraft.client.multiplayer.ClientPacketListener", "method", "registryAccess",
     "()Lnet/minecraft/core/RegistryAccess$Frozen;"),
    ("net.minecraft.world.level.Level", "method", "dimensionTypeRegistration",
     "()Lnet/minecraft/core/Holder;"),
]


def collect(sources):
    dotted, descriptors = {}, {}
    for path in sources:
        text = path.read_text(encoding="utf-8", errors="replace")
        for lineno, line in enumerate(text.splitlines(), 1):
            for m in DOTTED_CLASS.finditer(line):
                dotted.setdefault(m.group(1), []).append((path, lineno))
            for m in DESCRIPTOR.finditer(line):
                descriptors.setdefault(m.group(0).strip('"'), []).append((path, lineno))
    return dotted, descriptors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="+", type=Path)
    ap.add_argument("--mappings", required=True, type=Path)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    mp = Mappings(str(args.mappings))
    named = set(mp.members)
    dotted, descriptors = collect(args.sources)

    failures = []

    print(f"== {len(dotted)} distinct net.minecraft.* class names ==")
    for name in sorted(dotted):
        if name.endswith("."):
            continue  # a package prefix used with rfind(), not a class
        if name in named:
            if args.verbose:
                print(f"  ok      {name}")
        else:
            # allow inner classes written with '.' instead of '$'
            if name.replace(".", "$", name.count(".") - 1) in named:
                continue
            where = ", ".join(f"{p.name}:{n}" for p, n in dotted[name][:3])
            failures.append(("class", name, where))
            print(f"  MISSING {name}   ({where})")

    # every class referenced from a descriptor
    print(f"\n== {len(descriptors)} distinct descriptors ==")
    seen = set()
    for desc in sorted(descriptors):
        for ref in DESC_CLASS.finditer(desc):
            internal = ref.group(1)
            if internal.startswith(IGNORED_PREFIXES):
                continue
            dot = internal.replace("/", ".")
            if dot in seen:
                continue
            seen.add(dot)
            if dot not in named:
                where = ", ".join(f"{p.name}:{n}" for p, n in descriptors[desc][:3])
                failures.append(("descriptor", dot, where))
                print(f"  MISSING {dot}   referenced by {desc}  ({where})")
    if args.verbose:
        print(f"  ({len(seen)} distinct class references checked)")

    # --- load-bearing signatures -------------------------------------------
    # These are the lookups that decide whether the proxy works at all: if one
    # of them stops resolving, cacheJavaRefs returns null and either the whole
    # B-side server or the protocol switch silently disappears.  Kept as an
    # explicit list because they are worth failing loudly over.
    print(f"\n== {len(CRITICAL)} load-bearing signatures ==")
    for cls, kind, name, desc in CRITICAL:
        members = mp.members.get(cls)
        if members is None:
            failures.append(("critical", f"{cls} (class absent)", ""))
            print(f"  MISSING class {cls}")
            continue
        want = desc if kind == "field" else desc
        if any(m[0] == name and m[1] == want for m in members):
            if args.verbose:
                print(f"  ok      {cls.split('.')[-1]}.{name}")
        else:
            failures.append(("critical", f"{cls}.{name}{want}", ""))
            print(f"  MISSING {cls}.{name}{want}")

    # --- the C2S suppress list ---------------------------------------------
    # relay_handler.cpp names these as bare simple names, so the dotted-class
    # scan above cannot see them.  A typo here silently stops suppressing an
    # input packet (harmless), but a name that has drifted out of the game
    # usually means the surrounding classification is stale too -- and the
    # 1.20.1 allow-list that this replaced is exactly what broke chunk loading.
    suppress_src = next(
        (p for p in args.sources if p.name == "relay_handler.cpp"), None)
    if suppress_src is not None:
        text = suppress_src.read_text(encoding="utf-8", errors="replace")
        if "kPlayerIntent[] = {" in text:
            block = text.split("kPlayerIntent[] = {", 1)[1].split("};", 1)[0]
            names = re.findall(r'"(Serverbound\w+)"', block)
            prefix = "net.minecraft.network.protocol.game."
            print(f"\n== {len(names)} player-intent packets ==")
            for n in names:
                if prefix + n in named:
                    if args.verbose:
                        print(f"  ok      {n}")
                else:
                    failures.append(("suppress-list", n, str(suppress_src)))
                    print(f"  MISSING {n}  ({suppress_src.name})")

    print()
    if failures:
        print(f"FAILED: {len(failures)} unresolved reference(s)")
        for kind, name, where in failures:
            print(f"  [{kind}] {name}  ({where})")
        return 1
    print(f"OK: all class names and descriptor references resolve against "
          f"{args.mappings.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
