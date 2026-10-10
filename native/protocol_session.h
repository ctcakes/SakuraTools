#pragma once

#include <cstdint>

namespace sakura {
enum class ProtocolPhase { Closed, Handshake, Login, Configuration, Play };

struct ProtocolSession {
    using Phase = ProtocolPhase;
    std::uint64_t connectionGeneration = 0;
    std::uint64_t configurationGeneration = 0;
    std::uint64_t bGeneration = 0;
    Phase aPhase = Phase::Closed;
    Phase bInbound = Phase::Closed;
    Phase bOutbound = Phase::Closed;
    bool waitingLoginAck = false;
    bool waitingStartAck = false;
    bool waitingFinishAck = false;
    bool aFinishedConfiguration = false;
    bool playCodecsReady = false;

    void beginA(bool preserveB = false) {
        if (!preserveB || bInbound != Phase::Play || bOutbound != Phase::Play ||
            waitingStartAck || waitingFinishAck) detachB();
        ++connectionGeneration;
        ++configurationGeneration;
        aPhase = Phase::Login;
        aFinishedConfiguration = playCodecsReady = false;
        if (preserveB && bInbound == Phase::Play && bOutbound == Phase::Play)
            waitingStartAck = waitingFinishAck = false;
    }
    void beginConfiguration() {
        ++configurationGeneration;
        aPhase = Phase::Configuration;
        aFinishedConfiguration = playCodecsReady = false;
    }
    static bool mayReplaceDisconnectedB(bool previousChannelActive) {
        return !previousChannelActive;
    }
    static bool mayTakeoverB(bool previousChannelActive, bool snapshotReady) {
        return previousChannelActive && snapshotReady;
    }
    void attachB() {
        ++bGeneration;
        bInbound = bOutbound = Phase::Login;
        waitingLoginAck = waitingStartAck = waitingFinishAck = false;
    }
    bool loginFinished() {
        if (bInbound != Phase::Login || waitingLoginAck) return false;
        waitingLoginAck = true;
        bOutbound = Phase::Configuration;
        return true;
    }
    bool loginAcknowledged() {
        if (!waitingLoginAck || bInbound != Phase::Login) return false;
        waitingLoginAck = false;
        bInbound = Phase::Configuration;
        return true;
    }
    bool startConfiguration() {
        if (waitingStartAck || waitingFinishAck) return false;
        if (bInbound == Phase::Play && bOutbound == Phase::Play) {
            waitingStartAck = true;
            // StartConfiguration is encoded in PLAY; outbound changes after its write.
            bOutbound = Phase::Configuration;
            return true;
        }
        // A new A generation may supersede B's prior configuration. B is already
        // using the configuration codec, so no second start packet is legal/needed.
        return bInbound == Phase::Configuration && bOutbound == Phase::Configuration &&
               !waitingLoginAck;
    }
    bool configurationAcknowledged() {
        if (!waitingStartAck || bInbound != Phase::Play) return false;
        waitingStartAck = false;
        bInbound = Phase::Configuration;
        return true;
    }
    bool canSendConfiguration() const {
        return bInbound == Phase::Configuration && bOutbound == Phase::Configuration &&
               !waitingLoginAck && !waitingStartAck && !waitingFinishAck;
    }
    bool finishConfiguration() {
        if (!canSendConfiguration() || !aFinishedConfiguration || !playCodecsReady) return false;
        waitingFinishAck = true;
        bOutbound = Phase::Play;
        return true;
    }
    bool finishAcknowledged() {
        if (!waitingFinishAck || bInbound != Phase::Configuration) return false;
        waitingFinishAck = false;
        bInbound = Phase::Play;
        return true;
    }
    bool active() const {
        return aPhase == Phase::Play && bInbound == Phase::Play && bOutbound == Phase::Play &&
               !waitingStartAck && !waitingFinishAck;
    }
    bool current(std::uint64_t connection, std::uint64_t configuration,
                 std::uint64_t b) const {
        return connection == connectionGeneration && configuration == configurationGeneration &&
               b == bGeneration;
    }
    void detachB() {
        ++bGeneration;
        bInbound = bOutbound = Phase::Closed;
        waitingLoginAck = waitingStartAck = waitingFinishAck = false;
    }
    void disconnectA() {
        ++connectionGeneration;
        ++configurationGeneration;
        aPhase = Phase::Closed;
        aFinishedConfiguration = playCodecsReady = false;
        detachB();
    }
};
}
