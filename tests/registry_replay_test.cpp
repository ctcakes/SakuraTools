#include "registry_replay.h"
#include "protocol_session.h"
#include <cstdio>
#include <optional>
#include <string>
#include <vector>

static int failures = 0;
#define CHECK(x) do { if (!(x)) { std::fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #x); ++failures; } } while (0)

struct Entry { std::string id; std::optional<std::string> nbt; };
static std::vector<Entry> backendRegistries(std::size_t selected) {
    // RegistrySynchronization omits known-pack data only when that pack was selected.
    return {{"minecraft:banner_pattern/base", selected ? std::nullopt : std::optional<std::string>("{asset_id:base}")},
            {"minecraft:dimension_type/overworld", selected ? std::nullopt : std::optional<std::string>("{height:384}")},
            {"custom:dimension", "{height:256}"}};
}
static bool loadCrossVersion(const std::vector<Entry>& entries) {
    // B has no matching 1.21.8 local pack; absent payload reproduces FileNotFoundException.
    for (const auto& e : entries) if (!e.nbt) return false;
    return true;
}
int main() {
    CHECK(!loadCrossVersion(backendRegistries(1))); // Actual pre-fix omission failure.
    const std::vector<std::string> originalASelection = {"minecraft:core:1.21.8"};
    auto entries = backendRegistries(sakura::RegistryReplay::upstreamSelectionCount(originalASelection.size()));
    CHECK(loadCrossVersion(entries));
    CHECK(originalASelection.size() == 1); // Only the wire selection changes, never A's object/context.
    sakura::RegistryReplay r;
    for (const auto& e : entries) CHECK(r.registryEntry(e.nbt.has_value()));
    CHECK(r.canReplay());
    for (int generation = 0; generation < 3; ++generation) {
        r.beginConfiguration();
        CHECK(!r.selectB(0));
        CHECK(r.offerB());
        CHECK(!r.canReplay()); // Neither registry nor finish may overtake the local ACK.
        CHECK(!r.offerB());
        CHECK(!r.selectB(1));
        CHECK(r.waitingKnownPacks);
        CHECK(r.selectB(0));
        CHECK(!r.selectB(0)); // Duplicate answers cannot become duplicate upstream ACKs.
        CHECK(r.canReplay());
        r.attachB(); // Late B negotiates again from the full immutable journal.
        CHECK(r.offerB()); CHECK(r.selectB(0)); CHECK(r.canReplay());
    }
    r.beginConfiguration();
    CHECK(!r.registryEntry(false));
    CHECK(!r.registryEntry(true));
    r.attachB();
    CHECK(!r.canReplay()); // Reconnecting B cannot make a partial snapshot portable.
    r.beginConfiguration();
    CHECK(r.canReplay());
    CHECK(sakura::RegistryReplay::upstreamSelectionCount(0) == 0);
    CHECK(sakura::RegistryReplay::replaceUpstreamSelection(1)); // Non-empty JNI packet must enter replacement branch.
    CHECK(sakura::RegistryReplay::replaceUpstreamSelection(7));
    CHECK(!sakura::RegistryReplay::replaceUpstreamSelection(0));
    CHECK(sakura::RegistryReplay::shouldForwardPreparedWrite(true));
    CHECK(!sakura::RegistryReplay::shouldForwardPreparedWrite(false)); // Failed allocation cannot forward stale msg.
    if (failures) return 1;
    std::puts("registry_replay: omission reproduction, portable snapshots, local ACKs and repeated generations passed");
    return 0;
}
