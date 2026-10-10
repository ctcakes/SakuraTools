#include "b_chat_route.h"
#include <cstdio>
#include <cstdlib>
#include <initializer_list>

void check(bool value) { if (!value) std::abort(); }
int main() {
    int commands = 0, chats = 0, forwarded = 0;
    auto route = [&](const char* name) {
        return sakura::routeBChat(name, [&] { ++commands; }, [&] { ++chats; }, [&] { ++forwarded; });
    };
    check(route("net.minecraft.network.protocol.game.ServerboundChatCommandPacket"));
    check(commands == 1 && forwarded == 0);
    check(route("net.minecraft.network.protocol.game.ServerboundChatCommandSignedPacket"));
    check(commands == 2 && forwarded == 0);
    check(route("net.minecraft.network.protocol.game.ServerboundChatPacket"));
    check(chats == 1 && forwarded == 0);
    check(route("net.minecraft.network.protocol.game.ServerboundCommandSuggestionPacket"));
    check(forwarded == 1);
    for (const char* name : {"net.minecraft.network.protocol.game.ServerboundChatSessionUpdatePacket",
         "net.minecraft.network.protocol.game.ServerboundChatAckPacket",
         "net.minecraft.network.protocol.common.ServerboundKeepAlivePacket"}) check(!route(name));
    check(!route(nullptr));
    check(commands == 2 && chats == 1 && forwarded == 1);
    std::puts("B chat routing: commands=2 chats=1 suggestions=1 foreign session/ACK=0");
}
