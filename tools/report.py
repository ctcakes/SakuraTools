#!/usr/bin/env python3
"""Compare named signatures of every class the proxy touches, old vs new MC."""
import sys
from mcdump import Mappings

CLASSES = [
    "net.minecraft.network.Connection",
    "net.minecraft.network.ConnectionProtocol",
    "net.minecraft.network.PacketEncoder",
    "net.minecraft.network.PacketDecoder",
    "net.minecraft.network.ProtocolInfo",
    "net.minecraft.network.protocol.PacketFlow",
    "net.minecraft.network.protocol.BundlerInfo",
    "net.minecraft.network.FriendlyByteBuf",
    "net.minecraft.network.protocol.Packet",
    "net.minecraft.network.protocol.BundlePacket",
    "net.minecraft.network.protocol.game.ClientboundBundlePacket",
    "net.minecraft.network.protocol.game.ClientboundLoginPacket",
    "net.minecraft.network.protocol.game.CommonPlayerSpawnInfo",
    "net.minecraft.network.protocol.game.ClientboundPlayerPositionPacket",
    "net.minecraft.network.protocol.game.ClientboundPlayerInfoUpdatePacket",
    "net.minecraft.network.protocol.game.ClientboundPlayerInfoUpdatePacket$Entry",
    "net.minecraft.network.protocol.game.ClientboundAddPlayerPacket",
    "net.minecraft.network.protocol.game.ClientboundAddEntityPacket",
    "net.minecraft.network.protocol.game.ClientboundCustomPayloadPacket",
    "net.minecraft.network.protocol.game.ClientboundSetPlayerTeamPacket",
    "net.minecraft.network.protocol.game.ClientboundKeepAlivePacket",
    "net.minecraft.network.protocol.game.ClientboundGameEventPacket",
    "net.minecraft.network.protocol.game.ClientboundSetTimePacket",
    "net.minecraft.network.protocol.login.ClientboundGameProfilePacket",
    "net.minecraft.network.protocol.login.ServerboundHelloPacket",
    "net.minecraft.network.protocol.login.ClientboundLoginDisconnectPacket",
    "net.minecraft.network.protocol.handshake.ClientIntentionPacket",
    "net.minecraft.network.protocol.status.ServerboundStatusRequestPacket",
    "net.minecraft.network.protocol.status.ServerboundPingRequestPacket",
    "net.minecraft.network.protocol.status.ClientboundPongResponsePacket",
    "net.minecraft.network.protocol.status.ClientboundStatusResponsePacket",
    "net.minecraft.network.protocol.status.ServerStatus",
    "net.minecraft.client.Minecraft",
    "net.minecraft.client.User",
    "net.minecraft.client.multiplayer.ClientPacketListener",
    "net.minecraft.client.multiplayer.MultiPlayerGameMode",
    "net.minecraft.world.level.Level",
    "net.minecraft.world.level.GameType",
    "net.minecraft.core.RegistryAccess",
    "net.minecraft.resources.ResourceKey",
    "net.minecraft.world.entity.PositionMoveRotation",
    "com.mojang.authlib.GameProfile",
    "com.mojang.authlib.properties.PropertyMap",
]

# classes that only exist in the newer version
NEW_ONLY = [
    "net.minecraft.network.protocol.configuration.ClientboundFinishConfigurationPacket",
    "net.minecraft.network.protocol.configuration.ClientboundRegistryDataPacket",
    "net.minecraft.network.protocol.configuration.ServerboundFinishConfigurationPacket",
    "net.minecraft.network.protocol.configuration.ClientboundUpdateEnabledFeaturesPacket",
    "net.minecraft.network.protocol.configuration.ClientboundSelectKnownPacks",
    "net.minecraft.network.protocol.configuration.ServerboundSelectKnownPacks",
    "net.minecraft.network.protocol.configuration.ClientboundResetChatPacket",
    "net.minecraft.network.protocol.configuration.ClientboundCodeOfConductPacket",
    "net.minecraft.network.protocol.common.ClientboundKeepAlivePacket",
    "net.minecraft.network.protocol.common.ClientboundPingPacket",
]


def dump(out, mp, name, label):
    out.write(f"\n########## {label}: {name}\n")
    if name not in mp.members:
        out.write("  !! NOT PRESENT in this version\n")
        return
    out.write(f"  (obf {mp.class_n2o.get(name, '?')})\n")
    for mname, desc, obfname, kind in mp.members[name]:
        out.write(f"  {kind:6} {mname}{desc}\n")


def main():
    old_path, new_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
    old = Mappings(old_path)
    new = Mappings(new_path)
    with open(out_path, "w", encoding="utf-8") as out:
        for c in CLASSES:
            dump(out, old, c, "OLD")
            dump(out, new, c, "NEW")
        out.write("\n\n==================== NEW-ONLY CLASSES ====================\n")
        for c in NEW_ONLY:
            dump(out, new, c, "NEW")
    print("wrote", out_path)


if __name__ == "__main__":
    main()
