#pragma once
#include <cstring>

namespace sakura {
// Never forward B's signatures, last-seen updates or session key to A's server.
template<class Command, class Chat, class Forward>
bool routeBChat(const char* name, Command command, Chat chat, Forward forward) {
    if (!name) return false;
    if (std::strcmp(name, "net.minecraft.network.protocol.game.ServerboundChatCommandPacket") == 0 ||
        std::strcmp(name, "net.minecraft.network.protocol.game.ServerboundChatCommandSignedPacket") == 0) {
        command(); return true;
    }
    if (std::strcmp(name, "net.minecraft.network.protocol.game.ServerboundChatPacket") == 0) {
        chat(); return true;
    }
    if (std::strcmp(name, "net.minecraft.network.protocol.game.ServerboundCommandSuggestionPacket") == 0) {
        forward(); return true;
    }
    return false;
}
}
