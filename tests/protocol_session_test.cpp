#include "protocol_session.h"
#include <cstdio>

static int failures = 0;
#define CHECK(x) do { if (!(x)) { std::fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #x); ++failures; } } while (0)
using sakura::ProtocolSession;
using Phase = sakura::ProtocolPhase;

static void enterPlay(ProtocolSession& s) {
    CHECK(s.loginFinished());
    CHECK(s.bInbound == Phase::Login && s.bOutbound == Phase::Configuration);
    CHECK(!s.canSendConfiguration());
    CHECK(!s.finishAcknowledged());
    CHECK(s.loginAcknowledged());
    CHECK(!s.loginAcknowledged());
    CHECK(s.canSendConfiguration());
    CHECK(!s.finishConfiguration());
    s.aFinishedConfiguration = true;
    CHECK(!s.finishConfiguration());
    s.playCodecsReady = true;
    s.aPhase = Phase::Play;
    CHECK(s.finishConfiguration());
    CHECK(s.bInbound == Phase::Configuration && s.bOutbound == Phase::Play);
    CHECK(!s.active());
    CHECK(!s.canSendConfiguration());
    CHECK(s.finishAcknowledged());
    CHECK(!s.finishAcknowledged());
    CHECK(s.active());
}

int main() {
    ProtocolSession s;
    CHECK(!s.active());
    CHECK(!s.loginAcknowledged());
    CHECK(!s.configurationAcknowledged());
    CHECK(!s.finishConfiguration());
    s.beginA(); s.beginConfiguration(); s.attachB();
    enterPlay(s);
    auto connection = s.connectionGeneration;
    auto configuration = s.configurationGeneration;
    auto b = s.bGeneration;
    CHECK(s.current(connection, configuration, b));
    for (int i = 0; i < 3; ++i) {
        s.beginConfiguration();
        CHECK(!s.current(connection, configuration, b));
        CHECK(!s.aFinishedConfiguration && !s.playCodecsReady);
        CHECK(!s.active());
        CHECK(s.startConfiguration());
        CHECK(s.bInbound == Phase::Play && s.bOutbound == Phase::Configuration);
        CHECK(!s.canSendConfiguration());
        CHECK(!s.startConfiguration());
        CHECK(!s.finishConfiguration());
        CHECK(s.configurationAcknowledged());
        CHECK(!s.configurationAcknowledged());
        CHECK(s.canSendConfiguration());
        s.aFinishedConfiguration = s.playCodecsReady = true;
        s.aPhase = Phase::Play;
        CHECK(s.finishConfiguration());
        CHECK(!s.active());
        CHECK(s.finishAcknowledged());
        CHECK(s.active());
    }
    ProtocolSession replacement = s;
    replacement.beginA();
    CHECK(replacement.bInbound == Phase::Closed && replacement.bOutbound == Phase::Closed);
    CHECK(!replacement.waitingLoginAck && !replacement.waitingStartAck && !replacement.waitingFinishAck);
    CHECK(!replacement.current(connection, configuration, b));
    ProtocolSession pending;
    pending.beginA(); pending.attachB();
    CHECK(pending.loginFinished());
    pending.beginA();
    CHECK(!pending.waitingLoginAck && !pending.waitingStartAck && !pending.waitingFinishAck);
    CHECK(pending.bInbound == Phase::Closed && pending.bOutbound == Phase::Closed);
    b = s.bGeneration;
    configuration = s.configurationGeneration;
    const auto aConnection = s.connectionGeneration;
    const auto aConfiguration = s.configurationGeneration;
    s.detachB();
    CHECK(!s.active()); CHECK(!s.current(connection, configuration, b));
    CHECK(!s.finishAcknowledged());
    CHECK(s.connectionGeneration == aConnection && s.configurationGeneration == aConfiguration);
    CHECK(sakura::ProtocolSession::mayReplaceDisconnectedB(false));
    CHECK(!sakura::ProtocolSession::mayReplaceDisconnectedB(true));
    s.attachB();
    CHECK(s.bInbound == Phase::Login && s.bOutbound == Phase::Login);
    CHECK(s.connectionGeneration == aConnection && s.configurationGeneration == aConfiguration);
    s.disconnectA();
    CHECK(!s.active());
    CHECK(s.aPhase == Phase::Closed && s.bInbound == Phase::Closed && s.bOutbound == Phase::Closed);
    CHECK(!s.current(connection, configuration, s.bGeneration));
    CHECK(!s.loginAcknowledged() && !s.finishAcknowledged());
    s.beginA();
    CHECK(s.aPhase == Phase::Login && !s.aFinishedConfiguration && !s.playCodecsReady);
    if (failures) return 1;
    std::puts("protocol_session: all checks passed");
    return 0;
}
