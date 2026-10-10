#pragma once
#include <cstddef>

namespace sakura {
struct RegistryReplay {
    bool completePayloads = true;
    bool waitingKnownPacks = false;

    static std::size_t upstreamSelectionCount(std::size_t requested) {
        (void)requested;
        return 0;
    }
    static bool replaceUpstreamSelection(std::size_t requested) {
        return requested != 0;
    }
    static bool shouldForwardPreparedWrite(bool prepared) {
        return prepared;
    }
    void beginConfiguration() { completePayloads = true; waitingKnownPacks = false; }
    void attachB() { waitingKnownPacks = false; }
    bool offerB() {
        if (waitingKnownPacks) return false;
        waitingKnownPacks = true;
        return true;
    }
    bool selectB(std::size_t count) {
        if (!waitingKnownPacks || count != 0) return false;
        waitingKnownPacks = false;
        return true;
    }
    bool registryEntry(bool hasData) {
        completePayloads = completePayloads && hasData;
        return completePayloads;
    }
    bool canReplay() const { return completePayloads && !waitingKnownPacks; }
};
}
