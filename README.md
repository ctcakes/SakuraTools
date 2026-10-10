# SakuraTools — Minecraft 1.21.8 / protocol 772

This branch is an independent migration from `main`. It does not use
`origin/neoforge-1.21.8` as an implementation base.

## Runtime scope

- Windows x64 native DLL, JNI/JVMTI, Java 21 client runtime.
- JNI lookups currently require **Mojmap-named** Minecraft 1.21.8 classes and
  methods. A stock obfuscated client, Fabric intermediary mappings, and other
  remapped loaders are **not** supported by these lookups without adaptation.
- The user's client loader/runtime has not been identified. There is no claim
  that this DLL works on a particular Forge/NeoForge/Fabric installation.
- Protocol 772 is the target; this is not a cross-version protocol translator.
  The B-side local endpoint is `127.0.0.1:25565`; runtime binding and protocol
  setup must succeed before it can be used.

The relay observes decoded A packets in every protocol namespace. It is inserted
idempotently after `decoder` and before `packet_handler`; if these anchors are
missing or out of order, attachment is skipped rather than falling back to the
byte-stream head. Channel activation only installs the relay. A login intention
selects the A session, rather than blindly selecting every newly active channel.

Outbound routing is decided by `BServer_OnAWrite`. Configuration/common ACKs
must not be caught by a blanket game-packet filter. B-to-A forwarded writes carry
one-use identity bypass marks and skip this observer. Session replacement and
matching-channel disconnect must clear those marks; failed forwarding must
unmark its packet. `channelInactive` notifies the session owner and continues
propagation to the next Netty handler.

The session model tracks B inbound and outbound protocol phases independently:
Login ACK, StartConfiguration ACK, and FinishConfiguration ACK advance the
appropriate direction only. Connection, configuration, and B generations reject
stale work. Configuration completion requires the A finish and registry-aware
Play codecs, not merely receipt of a finish packet.

B chat and both command packet variants are re-issued through A's
`ClientPacketListener.sendChat/sendCommand` on the Minecraft executor. Only text
is taken from B: A owns signatures, last-seen tracking, chat ACKs and session
updates. A receives the same backend chat echoes as B and keeps processing them;
B's ACKs and session updates are not sent upstream. A's chat writes remain
allowed so its signing chain is never advanced for a deliberately dropped write.
Suggestions are forwarded unchanged (including request IDs); A's own suggestion
requests are suppressed while B controls the session. Pending text tasks are
bounded and checked against connection/configuration/B generations before use.

## Important limitations

- Fresh login/configuration is required. Injecting after A has entered Play is
  not a supported late-join path. No render-thread blocking gate is invoked.
- The old partial world-history cache is deliberately unused: observation and
  replay are disabled. It cannot reconstruct a complete registry/world snapshot,
  and no late-join, cross-respawn, or cross-session replay is claimed.
- Unit tests exercise the phase/generation model, not actual Netty event-loop
  timing, encryption/compression, registry contents, resource-pack policy, or
  mod-specific custom payload compatibility.
- JVM verification uses generated sample classes and minimal Java stubs. It
  validates classfile structure and patched bytecode, not JNI registrations or
  the complete generated B-side class in a Minecraft runtime.
- No client injection or live subserver-switch test has been performed. A real
  compatible named runtime must still verify initial login, repeated
  Play → Configuration → Play switches, B disconnect/reconnect, A reconnect,
  registry changes, and failure cleanup. Passing offline tests alone is not a
  claim of full switch compatibility.

## Build and offline tests (Windows)

Requires Visual Studio 2026 x64 C++ tools, CMake/Ninja, Python 3, and a Java 21 JDK
with `java`/`javac` on `PATH`. The script uses the installed VS paths below; edit
those paths for a different installation.

```bat
scripts\test-windows.cmd
```

The script configures `build-msvc`, builds the DLL/injector and test targets,
runs CTest, then runs `scripts/verify-windows.py` in the same `vcvars64` shell.
When several agents build in this checkout, use the shared `sakura-build` lock.

Offline checks:

1. `protocol_session_test`: directional transitions, premature/duplicate ACKs,
   readiness gates, repeated configuration, generation rejection, replacement,
   and disconnect.
2. `edit_self_test`: classfile editing/serialization checks.
3. `java -Xverify:all`: HookBridge, RelayHandler (including channelInactive), and
   a Connection sample with branches, exception handling, and stack maps.
4. Negative controls: an invalid operand-stack class must fail JVM verification;
   removing the finish-ACK guard in a **private header copy** must fail the same
   protocol-session test. The real working header is never mutated by the script.

The older `scripts/test.sh` remains a POSIX bytecode-test helper and assumes its
configured Zig toolchain; it is not the supported Windows entry point.
