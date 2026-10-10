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
        CHECK(!s.canStartConfiguration());
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
    ProtocolSession finishPending = s;
    finishPending.bInbound = Phase::Configuration;
    finishPending.bOutbound = Phase::Play;
    finishPending.waitingFinishAck = true;
    CHECK(!finishPending.canStartConfiguration());
    CHECK(!finishPending.startConfiguration());
    CHECK(finishPending.bOutbound == Phase::Play && finishPending.waitingFinishAck);

    ProtocolSession ordinary = s;
    CHECK(ordinary.canStartConfiguration());
    // Ordinary PLAY -> CONFIGURATION is the backend's StartConfiguration path.
    CHECK(ordinary.startConfiguration());
    CHECK(ordinary.bInbound == Phase::Play && ordinary.bOutbound == Phase::Configuration);
    CHECK(ordinary.configurationAcknowledged());
    CHECK(ordinary.canSendConfiguration());
    // A later A configuration generation can reuse B's already-installed config codecs.
    ordinary.beginConfiguration();
    CHECK(ordinary.startConfiguration());
    CHECK(ordinary.canSendConfiguration());
    ordinary.aFinishedConfiguration = ordinary.playCodecsReady = true;
    ordinary.aPhase = Phase::Play;
    CHECK(ordinary.finishConfiguration());
    CHECK(ordinary.finishAcknowledged());
    CHECK(ordinary.active());

    ProtocolSession midTransition = s;
    CHECK(midTransition.startConfiguration());
    midTransition.beginA(true);
    CHECK(midTransition.bInbound == Phase::Closed && midTransition.bOutbound == Phase::Closed);
    CHECK(!midTransition.waitingStartAck && !midTransition.configurationAcknowledged());

    ProtocolSession switched = s;
    switched.beginA(true);
    CHECK(switched.aPhase == Phase::Login);
    CHECK(switched.bInbound == Phase::Play && switched.bOutbound == Phase::Play);
    CHECK(switched.connectionGeneration != s.connectionGeneration);
    CHECK(!switched.active());
    switched.beginConfiguration();
    CHECK(switched.bInbound == Phase::Play && switched.bOutbound == Phase::Play);
    CHECK(switched.startConfiguration());
    CHECK(switched.bOutbound == Phase::Configuration && switched.waitingStartAck);
    CHECK(switched.configurationAcknowledged());
    CHECK(switched.canSendConfiguration());
    switched.aFinishedConfiguration = switched.playCodecsReady = true;
    switched.aPhase = Phase::Play;
    CHECK(switched.finishConfiguration());
    CHECK(switched.finishAcknowledged());
    CHECK(switched.active());

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
    CHECK(sakura::ProtocolSession::mayTakeoverB(true, true));
    CHECK(!sakura::ProtocolSession::mayTakeoverB(true, false));
    CHECK(!sakura::ProtocolSession::mayTakeoverB(false, true));
    s.attachB();
    CHECK(s.bInbound == Phase::Login && s.bOutbound == Phase::Login);
    CHECK(s.connectionGeneration == aConnection && s.configurationGeneration == aConfiguration);
    // A B disconnect/reconnect does not replace the retained A connection and generations.
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
