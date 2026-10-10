"""Run after the MSVC build, in the vcvars64 environment. Writes only build-msvc/tests."""
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / 'build-msvc'
OUT = BUILD / 'tests'
CLASSES = OUT / 'classpath'
GEN = OUT / 'gen'
for directory in (CLASSES, GEN):
    directory.mkdir(parents=True, exist_ok=True)

def run(args, negative=False):
    result = subprocess.run([str(arg) for arg in args], cwd=OUT)
    if negative:
        if result.returncode == 0:
            raise RuntimeError('negative control unexpectedly passed')
        print('negative control rejected as expected', flush=True)
    elif result.returncode:
        raise RuntimeError(f'command failed: {args}')

run(['javac', '-d', CLASSES, *sorted((ROOT / 'tests/java').glob('*.java'))])
run([BUILD / 'emit_samples.exe', GEN, CLASSES / 'net/minecraft/network/Connection.class'])
cp = str(CLASSES) + ';' + str(GEN)
run(['java', '-Xverify:all', '-cp', cp, 'Verify',
     'HookBridge', GEN / 'HookBridge.class', 'RelayHandler', GEN / 'RelayHandler.class',
     'PatchedConnection', GEN / 'PatchedConnection.class'])
run(['java', '-Xverify:all', '-cp', cp, 'Verify', 'InvalidStack', GEN / 'InvalidStack.class'], negative=True)

run([BUILD / 'protocol_session_test.exe'])

# Mutate a private header copy, never the main coder's working file.
MUTANT = OUT / 'mutant'
MUTANT.mkdir(exist_ok=True)
header = (ROOT / 'native/protocol_session.h').read_text(encoding='utf-8')
needle = 'if (!waitingFinishAck || bInbound != Phase::Configuration) return false;'
if header.count(needle) != 1:
    raise RuntimeError('finish ACK guard changed; review negative control')
(MUTANT / 'protocol_session.h').write_text(header.replace(needle, 'if (false) return false;'), encoding='utf-8')
run(['cl', '/nologo', '/EHsc', '/std:c++17', '/I' + str(MUTANT),
     ROOT / 'tests/protocol_session_test.cpp', '/Fe:' + str(MUTANT / 'protocol_session_mutant.exe'),
     '/Fo:' + str(MUTANT / 'protocol_session_mutant.obj')])
run([MUTANT / 'protocol_session_mutant.exe'], negative=True)
run([BUILD / 'b_chat_route_test.exe'])
chat_header = (ROOT / 'native/b_chat_route.h').read_text(encoding='utf-8')
needle = '"net.minecraft.network.protocol.game.ServerboundChatCommandPacket"'
if chat_header.count(needle) != 1:
    raise RuntimeError('chat dispatcher changed; review negative control')
(MUTANT / 'b_chat_route.h').write_text(chat_header.replace(needle, '"disabled.command.route"'), encoding='utf-8')
run(['cl', '/nologo', '/EHsc', '/std:c++17', '/I' + str(MUTANT),
     ROOT / 'tests/b_chat_route_test.cpp', '/Fe:' + str(MUTANT / 'b_chat_route_mutant.exe'),
     '/Fo:' + str(MUTANT / 'b_chat_route_mutant.obj')])
run([MUTANT / 'b_chat_route_mutant.exe'], negative=True)
run([BUILD / 'registry_replay_test.exe'])
registry_header = (ROOT / 'native/registry_replay.h').read_text(encoding='utf-8')
needle = '        return 0;'
if registry_header.count(needle) != 1:
    raise RuntimeError('upstream pack policy changed; review negative control')
(MUTANT / 'registry_replay.h').write_text(registry_header.replace(needle, '        return requested;'), encoding='utf-8')
run(['cl', '/nologo', '/EHsc', '/std:c++17', '/I' + str(MUTANT), '/I' + str(ROOT / 'native'),
     ROOT / 'tests/registry_replay_test.cpp', '/Fe:' + str(MUTANT / 'registry_replay_mutant.exe'),
     '/Fo:' + str(MUTANT / 'registry_replay_mutant.obj')])
run([MUTANT / 'registry_replay_mutant.exe'], negative=True)
needle = 'return requested != 0;'
if registry_header.count(needle) != 1:
    raise RuntimeError('known-packs replacement guard changed; review negative control')
(MUTANT / 'registry_replay.h').write_text(registry_header.replace(needle, 'return false;'), encoding='utf-8')
run(['cl', '/nologo', '/EHsc', '/std:c++17', '/I' + str(MUTANT), '/I' + str(ROOT / 'native'),
     ROOT / 'tests/registry_replay_test.cpp', '/Fe:' + str(MUTANT / 'registry_route_mutant.exe'),
     '/Fo:' + str(MUTANT / 'registry_route_mutant.obj')])
run([MUTANT / 'registry_route_mutant.exe'], negative=True)
print('Windows verification and all five negative controls passed', flush=True)
