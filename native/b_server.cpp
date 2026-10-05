#include "b_server.h"

#include "classfile.h"
#include "random_name.h"
#include "relay_handler.h"
#include "world_cache.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

namespace {

constexpr const char* kInitSuper         = "io/netty/channel/ChannelInitializer";
constexpr const char* kHandlerSuper      = "io/netty/channel/ChannelInboundHandlerAdapter";

constexpr const char* kInitChannelDesc     = "(Lio/netty/channel/Channel;)V";
constexpr const char* kChannelReadDesc     = "(Lio/netty/channel/ChannelHandlerContext;Ljava/lang/Object;)V";
constexpr const char* kChannelActiveDesc   = "(Lio/netty/channel/ChannelHandlerContext;)V";
constexpr const char* kChannelInactiveDesc = "(Lio/netty/channel/ChannelHandlerContext;)V";
constexpr const char* kExceptionCaughtDesc =
    "(Lio/netty/channel/ChannelHandlerContext;Ljava/lang/Throwable;)V";

// 1.20.2 inserted CONFIGURATION between LOGIN and PLAY; B parks in
// AwaitConfiguration until A's own configuration stream has been mirrored over.
enum class BState { AwaitHandshake, AwaitLogin, AwaitConfiguration, Play };

// Two possible identities for B.
//   false (current) -- B mirrors A wholesale: A's own ClientboundLoginPacket and
//                      PlayerInfoUpdate/team packets reach B untouched, so B
//                      appears as A.  Nothing is synthesised, which is far less
//                      fragile across versions.
//   true            -- B gets its own offline identity, A is hidden from B's tab
//                      list, and team/player-info packets are rewritten.  This
//                      is what the 1.20.1 build did; the synthesised wire
//                      formats below are 1.20.1-shaped and would need updating
//                      for 1.21.8 before flipping this on.
constexpr bool kGiveBOwnIdentity = false;

struct BServer {

    jclass    initClass    = nullptr;
    jmethodID initCtor     = nullptr;
    jclass    handlerClass = nullptr;
    jmethodID handlerCtor  = nullptr;
    jclass    mainGateClass = nullptr;
    jmethodID mainGateCtor  = nullptr;

    jclass    connectionCls              = nullptr;
    jmethodID connectionConfigureSerMid  = nullptr;
    jmethodID connectionSendMid          = nullptr;

    jclass    clientIntentCls  = nullptr;
    jobject   intentLogin      = nullptr;
    jobject   intentStatus     = nullptr;

    jclass    statusResponsePacketCls   = nullptr;
    jmethodID statusResponsePacketCtor  = nullptr;
    jclass    serverStatusCls           = nullptr;
    jmethodID serverStatusCtor          = nullptr;
    jclass    pongResponsePacketCls     = nullptr;
    jmethodID pongResponsePacketCtor    = nullptr;
    jclass    statusRequestPacketCls    = nullptr;
    jclass    pingRequestPacketCls      = nullptr;
    jfieldID  pingRequestPacketTimeFid  = nullptr;
    jfieldID  intentionPacketIntentFid  = nullptr;

    jobject   flowServerbound  = nullptr;
    jobject   flowClientbound  = nullptr;

    jclass    channelCls              = nullptr;
    jmethodID channelPipelineMid      = nullptr;
    jmethodID channelWriteAndFlushMid = nullptr;
    jmethodID channelAttrMid          = nullptr;
    jmethodID channelConfigMid        = nullptr;
    jmethodID channelCloseMid         = nullptr;
    jfieldID  connectionChannelFid    = nullptr;

    jmethodID configSetOptionMid      = nullptr;
    jobject   tcpNoDelayOption        = nullptr;
    jobject   booleanTrue             = nullptr;
    jclass    pipelineCls             = nullptr;
    jmethodID pipelineAddLastMid      = nullptr;
    jmethodID pipelineRemoveNameMid   = nullptr;
    jmethodID pipelineGetHandlerMid   = nullptr;
    jmethodID pipelineReplaceMid      = nullptr;
    jmethodID pipelineNamesMid        = nullptr;
    jmethodID pipelineAddBeforeMid    = nullptr;
    jmethodID pipelineChannelMid      = nullptr;   // ChannelPipeline.channel()

    jclass    packetEncoderCls        = nullptr;
    jmethodID packetEncoderCtor       = nullptr;
    jclass    packetDecoderCls        = nullptr;
    jmethodID packetDecoderCtor       = nullptr;

    jmethodID configIsAutoReadMid     = nullptr;
    jclass    protocolInfoCls         = nullptr;
    jmethodID protocolInfoFlowMid     = nullptr;
    jmethodID simpleUnboundBindMid    = nullptr;
    jmethodID unboundBindMid          = nullptr;
    jmethodID rfbDecoratorMid         = nullptr;

    jclass    registryAccessCls       = nullptr;
    jfieldID  registryAccessEmptyFid  = nullptr;
    jobject   registryAccessRef       = nullptr;

    jclass    gameContextCls          = nullptr;
    jmethodID gameContextCtor         = nullptr;

    jobject   piHandshakeSbound       = nullptr;
    jobject   piLoginCbound           = nullptr;
    jobject   piLoginSbound           = nullptr;
    jobject   piConfigCbound          = nullptr;
    jobject   piConfigSbound          = nullptr;
    jobject   piPlayCbound            = nullptr;
    jobject   piPlaySbound            = nullptr;
    jobject   piStatusCbound          = nullptr;
    jobject   piStatusSbound          = nullptr;

    jclass    finishConfigPacketCls   = nullptr;
    jmethodID finishConfigPacketCtor  = nullptr;

    jclass    gameProfileCls          = nullptr;
    jmethodID gameProfileCtor         = nullptr;

    jclass    minecraftCls            = nullptr;
    jmethodID mcGetInstanceMid        = nullptr;
    jmethodID mcGetProfilePropsMid    = nullptr;
    jclass    friendlyBufCls          = nullptr;
    jclass    registryBufCls          = nullptr;
    jmethodID registryBufCtor         = nullptr;
    jmethodID fbbWriteByteMid         = nullptr;
    jmethodID fbbWriteBooleanMid      = nullptr;
    jmethodID fbbWriteVarIntMid       = nullptr;
    jmethodID fbbWriteUUIDMid         = nullptr;
    jmethodID fbbWriteUtfMid          = nullptr;
    jmethodID fbbWriteGpPropsMid      = nullptr;
    jclass    unpooledCls             = nullptr;
    jmethodID unpooledBufferMid       = nullptr;
    jmethodID unpooledWrappedMid      = nullptr;
    jclass    playerInfoUpdatePacketCls     = nullptr;
    jmethodID playerInfoUpdatePacketBufCtor = nullptr;
    jmethodID playerInfoUpdatePacketWriteMid = nullptr;

    jmethodID piuEntriesMidA          = nullptr;
    jmethodID piuEntriesMidB          = nullptr;
    jclass    piEntryCls              = nullptr;
    jmethodID piEntryProfileIdMid     = nullptr;
    jmethodID piEntryGameModeMid      = nullptr;
    jmethodID piEntryListedMid        = nullptr;
    jmethodID piEntryLatencyMid       = nullptr;
    jmethodID piEntryDisplayNameMid   = nullptr;
    jmethodID gameTypeGetIdMid        = nullptr;
    jmethodID fbbWriteComponentMid    = nullptr;
    jmethodID listSizeMid             = nullptr;
    jmethodID listGetMid              = nullptr;
    jmethodID byteBufGetByteMid       = nullptr;
    jobject   bUuidObj                = nullptr;

    jclass    addEntityPacketCls      = nullptr;
    jfieldID  addEntityPacketUuidFid  = nullptr;

    jclass    customPayloadPacketCls  = nullptr;

    jclass    setPlayerTeamPacketCls  = nullptr;
    jmethodID setPlayerTeamPacketBufCtor = nullptr;
    jfieldID  setPlayerTeamMethodFid  = nullptr;
    jfieldID  setPlayerTeamNameFid    = nullptr;
    jfieldID  setPlayerTeamPlayersFid = nullptr;
    jmethodID collectionContainsMid   = nullptr;

    jmethodID userGetGameProfileMid   = nullptr;
    jmethodID gameProfileGetNameMid   = nullptr;
    jstring   aName                   = nullptr;
    jstring   bName                   = nullptr;

    jmethodID byteBufReadableBytesMid = nullptr;
    jmethodID byteBufReaderIndexMid   = nullptr;
    jmethodID byteBufGetBytesMid      = nullptr;

    jobject   aRealUuid               = nullptr;
    unsigned char aUuidBytes[16]      = {0};
    bool      aUuidReady              = false;
    unsigned char bUuidBytes[16]      = {0};
    bool      bUuidReady              = false;

    jclass    userCls                 = nullptr;
    jmethodID userGetProfileIdMid     = nullptr;
    jmethodID mcGetUserMid            = nullptr;

    jmethodID uuidGetMsbMid           = nullptr;
    jmethodID uuidGetLsbMid           = nullptr;

    jclass    loginFinishedPacketCls  = nullptr;
    jmethodID loginFinishedPacketCtor = nullptr;

    jclass    helloPacketCls          = nullptr;
    jfieldID  helloPacketNameFid      = nullptr;

    jclass    intentPacketCls         = nullptr;

    jclass    playerPositionPacketCls = nullptr;
    jmethodID playerPositionPacketCtor = nullptr;
    jclass    setCls                  = nullptr;
    jmethodID setOfMid                = nullptr;
    jclass    keepAlivePacketCls      = nullptr;
    jmethodID keepAlivePacketCtor     = nullptr;

    jclass    uuidCls                 = nullptr;
    jmethodID uuidNameUuidFromBytesMid = nullptr;

    jclass    bundlePacketCls         = nullptr;
    jmethodID bundleSubPacketsMid     = nullptr;
    jmethodID iterableIteratorMid     = nullptr;
    jmethodID iteratorHasNextMid      = nullptr;
    jmethodID iteratorNextMid         = nullptr;

    std::mutex targetMu;
    jobject   targetAConnection = nullptr;

    std::mutex bMu;
    jobject   bChannel = nullptr;

    std::atomic<BState> bState{BState::AwaitHandshake};

    bool bServerBound = false;

    bool      playProtocolReady = false;   // clientbound PLAY lifted from A
    bool      playSboundReady   = false;   // serverbound PLAY lifted from A
    jmethodID protocolInfoIdMid      = nullptr;
    jobject   connectionProtocolPlay = nullptr;

    // Per-B-connection PLAY switch bookkeeping, guarded by playSwapMu.
    // bOutboundPlay: B's encoder has been moved to PLAY.
    // bInboundPlayPending: B already acknowledged the end of configuration (its
    // reads are paused by ProtocolSwapHandler) but A's PLAY ProtocolInfo was
    // not available yet, so the decoder swap waits for it.
    std::mutex playSwapMu;
    bool      bOutboundPlay       = false;
    bool      bInboundPlayPending = false;

    // Runs protocol swaps on B's event loop, in order with writes queued there.
    jclass    loopTaskCls             = nullptr;
    jmethodID loopTaskCtor            = nullptr;
    jmethodID channelEventLoopMid     = nullptr;
    jmethodID executorExecuteMid      = nullptr;
    jmethodID configSetAutoReadMid    = nullptr;
    jclass    loginAckPacketCls       = nullptr;
    jclass    finishConfigAckPacketCls = nullptr;
    jclass    startConfigPacketCls     = nullptr;   // clientbound: re-enter configuration
    jclass    configAckPacketCls       = nullptr;   // serverbound: B's answer to it
    std::atomic<bool> midSession{false};
    jmethodID mcGetConnectionMid      = nullptr;
    jclass    clientPacketListenerCls = nullptr;
    jmethodID cplGetConnectionMid     = nullptr;
    jmethodID cplLevelsMid            = nullptr;
    jmethodID cplRegistryAccessMid    = nullptr;
    jmethodID registryAccessFreezeMid = nullptr;
    jfieldID  mcPlayerFid             = nullptr;
    jfieldID  mcGameModeFid           = nullptr;
    jfieldID  mcLevelFid              = nullptr;
    jmethodID gameModeGetTypeMid      = nullptr;
    jclass    levelCls                = nullptr;
    jmethodID levelDimMidA            = nullptr;
    jmethodID levelDimMidB            = nullptr;
    jmethodID objToStringMid          = nullptr;
    jclass    loginPacketCls          = nullptr;
    jmethodID loginPacketCtor         = nullptr;
    jclass    optionalCls             = nullptr;
    jmethodID optionalEmptyMid        = nullptr;
};
BServer g_bs;

std::mutex              g_bConnMu;
std::condition_variable g_bConnCv;
bool                    g_bConnected = false;

jmethodID findMethodByDesc(jclass klass, const char* desc, bool wantStatic) {
    jint count = 0;
    jmethodID* mids = nullptr;
    if (g_jvmti->GetClassMethods(klass, &count, &mids) != JVMTI_ERROR_NONE) return nullptr;
    jmethodID hit = nullptr;
    for (jint i = 0; i < count && !hit; ++i) {
        char *n=nullptr, *s=nullptr, *g=nullptr;
        if (g_jvmti->GetMethodName(mids[i], &n, &s, &g) != JVMTI_ERROR_NONE) continue;
        jint mods = 0;
        g_jvmti->GetMethodModifiers(mids[i], &mods);
        bool isStatic = (mods & 0x0008) != 0;
        if (s && std::strcmp(s, desc) == 0 && isStatic == wantStatic) {
            hit = mids[i];
            LogTo("  findMethodByDesc(%s, static=%d): '%s'", desc, wantStatic?1:0, n?n:"?");
        }
        if (n) g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(n));
        if (s) g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(s));
        if (g) g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(g));
    }
    g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(mids));
    return hit;
}

int findMethodsByDesc(jclass klass, const char* desc, bool wantStatic,
                      jmethodID* out, int maxOut) {
    jint count = 0;
    jmethodID* mids = nullptr;
    if (g_jvmti->GetClassMethods(klass, &count, &mids) != JVMTI_ERROR_NONE) return 0;
    int n = 0;
    for (jint i = 0; i < count && n < maxOut; ++i) {
        char *nm=nullptr, *s=nullptr, *g=nullptr;
        if (g_jvmti->GetMethodName(mids[i], &nm, &s, &g) != JVMTI_ERROR_NONE) continue;
        jint mods = 0;
        g_jvmti->GetMethodModifiers(mids[i], &mods);
        bool isStatic = (mods & 0x0008) != 0;
        if (s && std::strcmp(s, desc) == 0 && isStatic == wantStatic) {
            out[n++] = mids[i];
            LogTo("  findMethodsByDesc(%s)[%d]: '%s'", desc, n-1, nm?nm:"?");
        }
        if (nm) g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(nm));
        if (s)  g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(s));
        if (g)  g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(g));
    }
    g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(mids));
    return n;
}

jmethodID findMethodByDescExcl(jclass klass, const char* desc, bool wantStatic,
                               const char* const* excl, int nExcl) {
    jint count = 0;
    jmethodID* mids = nullptr;
    if (g_jvmti->GetClassMethods(klass, &count, &mids) != JVMTI_ERROR_NONE) return nullptr;
    jmethodID hit = nullptr;
    for (jint i = 0; i < count && !hit; ++i) {
        char *nm=nullptr, *s=nullptr, *g=nullptr;
        if (g_jvmti->GetMethodName(mids[i], &nm, &s, &g) != JVMTI_ERROR_NONE) continue;
        jint mods = 0;
        g_jvmti->GetMethodModifiers(mids[i], &mods);
        bool isStatic = (mods & 0x0008) != 0;
        bool excluded = false;
        for (int e = 0; nm && e < nExcl; ++e)
            if (std::strcmp(nm, excl[e]) == 0) { excluded = true; break; }
        if (!excluded && s && std::strcmp(s, desc) == 0 && isStatic == wantStatic) {
            hit = mids[i];
            LogTo("  findMethodByDescExcl(%s): '%s'", desc, nm?nm:"?");
        }
        if (nm) g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(nm));
        if (s)  g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(s));
        if (g)  g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(g));
    }
    g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(mids));
    return hit;
}

jfieldID findFieldByDesc(jclass klass, const char* desc, bool wantStatic) {
    jint count = 0;
    jfieldID* fids = nullptr;
    if (g_jvmti->GetClassFields(klass, &count, &fids) != JVMTI_ERROR_NONE) return nullptr;
    jfieldID hit = nullptr;
    for (jint i = 0; i < count && !hit; ++i) {
        char *n=nullptr, *s=nullptr, *g=nullptr;
        if (g_jvmti->GetFieldName(klass, fids[i], &n, &s, &g) != JVMTI_ERROR_NONE) continue;
        jint mods = 0;
        g_jvmti->GetFieldModifiers(klass, fids[i], &mods);
        bool isStatic = (mods & 0x0008) != 0;
        if (s && std::strcmp(s, desc) == 0 && isStatic == wantStatic) {
            hit = fids[i];
            LogTo("  findFieldByDesc(%s, static=%d): '%s'", desc, wantStatic?1:0, n?n:"?");
        }
        if (n) g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(n));
        if (s) g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(s));
        if (g) g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(g));
    }
    g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(fids));
    return hit;
}

jclass findLoadedBySig(JNIEnv* env, const char* sig) {
    jint count = 0;
    jclass* classes = nullptr;
    if (g_jvmti->GetLoadedClasses(&count, &classes) != JVMTI_ERROR_NONE) return nullptr;
    jclass hit = nullptr;
    for (jint i = 0; i < count; ++i) {
        char* s = nullptr;
        if (g_jvmti->GetClassSignature(classes[i], &s, nullptr) != JVMTI_ERROR_NONE) continue;
        if (s && std::strcmp(s, sig) == 0) {
            hit = (jclass)env->NewLocalRef(classes[i]);
            g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(s));
            break;
        }
        if (s) g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(s));
    }
    for (jint i = 0; i < count; ++i) env->DeleteLocalRef(classes[i]);
    g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(classes));
    return hit;
}

jclass loadOrFind(JNIEnv* env, jobject mcLoader, const char* dot, const char* sig) {
    jclass c = LoadClassInLoader(env, mcLoader, dot);
    if (!c) c = findLoadedBySig(env, sig);
    if (!c) LogTo("BServer: cannot find class %s", dot);
    return c;
}

std::string classNameForB(JNIEnv* env, jobject o) {
    if (!o || !g_jvmti) return {};
    jclass c = env->GetObjectClass(o);
    if (!c) return {};
    char* sig = nullptr;
    jvmtiError rc = g_jvmti->GetClassSignature(c, &sig, nullptr);
    env->DeleteLocalRef(c);
    if (rc != JVMTI_ERROR_NONE || !sig) return {};
    std::string out;
    const char* p = sig;
    if (*p == 'L') { ++p; for (; *p && *p != ';'; ++p) out.push_back(*p == '/' ? '.' : *p); }
    else out = sig;
    g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(sig));
    return out;
}

// 1.20.5+ packet constructors and codecs take a RegistryFriendlyByteBuf, which
// carries the RegistryAccess the codecs resolve against.  Every buffer we
// synthesise by hand has to be built on one of these instead of a plain
// FriendlyByteBuf.  A's own RegistryAccess is picked up opportunistically (see
// BServer_TryCaptureLiveConnection); until then we fall back to
// RegistryAccess.EMPTY, which is enough for the login/status/handshake codecs.
jobject registryAccessForBuf(JNIEnv* env) {
    if (g_bs.registryAccessRef) return g_bs.registryAccessRef;
    if (!g_bs.registryAccessCls || !g_bs.registryAccessEmptyFid) return nullptr;
    jobject e = env->GetStaticObjectField(g_bs.registryAccessCls,
                                          g_bs.registryAccessEmptyFid);
    if (!e || env->ExceptionCheck()) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        return nullptr;
    }
    g_bs.registryAccessRef = env->NewGlobalRef(e);
    env->DeleteLocalRef(e);
    return g_bs.registryAccessRef;
}

jobject wrapRegistryBuf(JNIEnv* env, jobject byteBuf) {
    if (!byteBuf || !g_bs.registryBufCls || !g_bs.registryBufCtor) return nullptr;
    jobject ra = registryAccessForBuf(env);
    if (!ra) { LogTo("buf: no RegistryAccess available"); return nullptr; }
    jobject buf = env->NewObject(g_bs.registryBufCls, g_bs.registryBufCtor, byteBuf, ra);
    if (!buf || env->ExceptionCheck()) {
        LogAndClearException(env, "wrapRegistryBuf");
        return nullptr;
    }
    return buf;
}

jobject newWriteBuf(JNIEnv* env) {
    if (!g_bs.unpooledCls || !g_bs.unpooledBufferMid) return nullptr;
    jobject bb = env->CallStaticObjectMethod(g_bs.unpooledCls, g_bs.unpooledBufferMid);
    if (!bb || env->ExceptionCheck()) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        return nullptr;
    }
    jobject buf = wrapRegistryBuf(env, bb);
    env->DeleteLocalRef(bb);
    return buf;
}

// ---------------------------------------------------------------------------
// 1.21.8 protocol plumbing.
//
// 1.20.1 switched protocol state by poking Connection.ATTRIBUTE_PROTOCOL and
// calling setProtocol() on the encoder/decoder handlers.  Neither exists any
// more.  In 1.21.8 the encoder/decoder are built around an immutable
// ProtocolInfo, and the state change is done by *swapping those two handlers*
// in the pipeline -- which is exactly what Connection.setupOutboundProtocol /
// setupInboundProtocol do internally, except they also want a PacketListener.
// We have no listener (our B-side pipeline is ours alone), so we do the swap
// ourselves.
//
// Handler names: Connection.configureSerialization installs the two sides
// under different names depending on which one is "real" for that PacketFlow.
// A serverbound pipeline gets a real "decoder" and a placeholder named
// "outbound_config"; once a protocol is chosen the placeholder is renamed to
// "encoder".  We therefore probe both names and normalise to encoder/decoder.
// ---------------------------------------------------------------------------

enum class ProtoState { Handshake, Login, Configuration, Play, Status };

jobject bindProtocolInfoField(JNIEnv* env, const char* ownerDot, const char* fieldName) {
    jobject loader = GetMinecraftClassLoader(env, g_jvmti);
    if (!loader) return nullptr;
    jclass owner = LoadClassInLoader(env, loader, ownerDot);
    env->DeleteGlobalRef(loader);
    if (!owner) { LogTo("proto: class %s not found", ownerDot); return nullptr; }

    jfieldID f = env->GetStaticFieldID(owner, fieldName, "Lnet/minecraft/network/ProtocolInfo;");
    if (!f) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        LogTo("proto: %s.%s missing", ownerDot, fieldName);
        env->DeleteLocalRef(owner);
        return nullptr;
    }
    jobject v = env->GetStaticObjectField(owner, f);
    env->DeleteLocalRef(owner);
    if (!v || env->ExceptionCheck()) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        return nullptr;
    }
    jobject g = env->NewGlobalRef(v);
    env->DeleteLocalRef(v);
    return g;
}

// GameProtocols exposes *templates* rather than ready ProtocolInfos: the game
// codecs are parameterised by the registry access, so they must be bound.
jobject bindProtocolTemplate(JNIEnv* env, const char* ownerDot, const char* fieldName,
                             bool needsContext) {
    if (!g_bs.registryBufCls || !g_bs.rfbDecoratorMid) return nullptr;

    jobject loader = GetMinecraftClassLoader(env, g_jvmti);
    if (!loader) return nullptr;
    jclass owner = LoadClassInLoader(env, loader, ownerDot);
    env->DeleteGlobalRef(loader);
    if (!owner) { LogTo("proto: template class %s not found", ownerDot); return nullptr; }

    const char* tmplDesc = needsContext
        ? "Lnet/minecraft/network/protocol/UnboundProtocol;"
        : "Lnet/minecraft/network/protocol/SimpleUnboundProtocol;";
    jfieldID f = env->GetStaticFieldID(owner, fieldName, tmplDesc);
    if (!f) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        LogTo("proto: template %s.%s missing", ownerDot, fieldName);
        env->DeleteLocalRef(owner);
        return nullptr;
    }
    jobject tmpl = env->GetStaticObjectField(owner, f);
    env->DeleteLocalRef(owner);
    if (!tmpl || env->ExceptionCheck()) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        return nullptr;
    }

    jobject ra = g_bs.registryAccessRef;
    if (!ra && g_bs.registryAccessCls && g_bs.registryAccessEmptyFid) {
        jobject e = env->GetStaticObjectField(g_bs.registryAccessCls,
                                              g_bs.registryAccessEmptyFid);
        if (e && !env->ExceptionCheck()) {
            g_bs.registryAccessRef = env->NewGlobalRef(e);
            ra = g_bs.registryAccessRef;
        } else if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }
        if (e) env->DeleteLocalRef(e);
    }
    if (!ra) { LogTo("proto: no RegistryAccess available for %s", fieldName); env->DeleteLocalRef(tmpl); return nullptr; }

    jobject decorator = env->CallStaticObjectMethod(g_bs.registryBufCls,
                                                    g_bs.rfbDecoratorMid, ra);
    if (!decorator || env->ExceptionCheck()) {
        LogAndClearException(env, "proto/decorator");
        env->DeleteLocalRef(tmpl);
        return nullptr;
    }

    jobject pi = nullptr;
    if (needsContext) {
        jobject ctx = env->NewObject(g_bs.gameContextCls, g_bs.gameContextCtor);
        if (ctx && !env->ExceptionCheck()) {
            pi = env->CallObjectMethod(tmpl, g_bs.unboundBindMid, decorator, ctx);
        }
        if (env->ExceptionCheck()) LogAndClearException(env, "proto/bind(context)");
        if (ctx) env->DeleteLocalRef(ctx);
    } else {
        pi = env->CallObjectMethod(tmpl, g_bs.simpleUnboundBindMid, decorator);
        if (env->ExceptionCheck()) LogAndClearException(env, "proto/bind");
    }
    env->DeleteLocalRef(decorator);
    env->DeleteLocalRef(tmpl);
    if (!pi) return nullptr;

    jobject g = env->NewGlobalRef(pi);
    env->DeleteLocalRef(pi);
    return g;
}

jobject protocolInfoFor(ProtoState st, bool clientbound) {
    switch (st) {
        case ProtoState::Handshake:     return clientbound ? nullptr : g_bs.piHandshakeSbound;
        case ProtoState::Login:         return clientbound ? g_bs.piLoginCbound  : g_bs.piLoginSbound;
        case ProtoState::Configuration: return clientbound ? g_bs.piConfigCbound : g_bs.piConfigSbound;
        case ProtoState::Play:          return clientbound ? g_bs.piPlayCbound   : g_bs.piPlaySbound;
        case ProtoState::Status:        return clientbound ? g_bs.piStatusCbound : g_bs.piStatusSbound;
    }
    return nullptr;
}

// Dumps the live handler names.  netty installs the two protocol-sensitive
// handlers under names that depend on the PacketFlow (encoder/decoder vs
// outbound_config/inbound_config), and swapping the wrong one silently leaves
// the real codec on the old protocol -- which shows up much later as packets
// that fail to decode, so it is worth being able to see this.
void logPipeline(JNIEnv* env, jobject pipeline, const char* where) {
    if (!pipeline || !g_bs.pipelineNamesMid || !g_bs.listSizeMid || !g_bs.listGetMid)
        return;
    jobject names = env->CallObjectMethod(pipeline, g_bs.pipelineNamesMid);
    if (!names || env->ExceptionCheck()) { if (env->ExceptionCheck()) env->ExceptionClear(); return; }

    jint n = env->CallIntMethod(names, g_bs.listSizeMid);
    std::string all;
    for (jint i = 0; i < n; ++i) {
        jobject s = env->CallObjectMethod(names, g_bs.listGetMid, i);
        if (!s) continue;

        std::string entry;
        const char* c = env->GetStringUTFChars((jstring)s, nullptr);
        if (c) { entry = c; env->ReleaseStringUTFChars((jstring)s, c); }

        // Also resolve the handler's own class.  The pipeline name alone cannot
        // tell a real PacketDecoder from an UnconfiguredPipelineHandler
        // placeholder, and swapping the placeholder is silent.
        jobject h = env->CallObjectMethod(pipeline, g_bs.pipelineGetHandlerMid, s);
        if (h && !env->ExceptionCheck()) {
            jclass hc = env->GetObjectClass(h);
            static jmethodID getName = nullptr;
            if (!getName) {
                jclass classCls = env->FindClass("java/lang/Class");
                if (classCls) {
                    getName = env->GetMethodID(classCls, "getName", "()Ljava/lang/String;");
                    env->DeleteLocalRef(classCls);
                }
                if (env->ExceptionCheck()) env->ExceptionClear();
            }
            if (getName) {
                jobject cn = env->CallObjectMethod(hc, getName);
                if (cn) {
                    const char* cc = env->GetStringUTFChars((jstring)cn, nullptr);
                    if (cc) {
                        const char* slash = strrchr(cc, '.');
                        entry += "("; entry += (slash ? slash + 1 : cc); entry += ")";
                        env->ReleaseStringUTFChars((jstring)cn, cc);
                    }
                    env->DeleteLocalRef(cn);
                }
            }
            if (env->ExceptionCheck()) env->ExceptionClear();
            env->DeleteLocalRef(hc);
            env->DeleteLocalRef(h);
        } else if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }

        if (!all.empty()) all += ", ";
        all += entry;
        env->DeleteLocalRef(s);
    }
    if (env->ExceptionCheck()) env->ExceptionClear();
    env->DeleteLocalRef(names);
    LogTo("pipeline[%s]: %s", where, all.c_str());
}

// Swaps whichever of `names` is present for a freshly built handler, keeping
// its pipeline position and installing it under `installAs`.
//
// The canonical name matters: on every terminal packet vanilla's
// ProtocolSwapHandler does addBefore/addAfter(ctx.name(), "inbound_config" /
// "outbound_config", placeholder).  A real codec left sitting under the
// placeholder's name makes that throw "Duplicate handler name".
//
// Deliberately remove + addBefore rather than ChannelPipeline.replace():
// replace() rejects a new name that is already taken, and the old context is
// still registered while the new one is created -- so re-using the same name
// throws "Duplicate handler name", and renaming to a name that a previous swap
// already installed throws too.  Both happened in practice.  Removing first
// frees the name, and re-inserting before the old neighbour preserves position,
// which matters because the decoder must stay ahead of the prepender/encoder.
bool swapPipelineHandler(JNIEnv* env, jobject pipeline, const char* const* names,
                         int nNames, const char* installAs, jclass handlerCls,
                         jmethodID handlerCtor, jobject pi, const char* tag) {
    if (!pi || !handlerCls || !handlerCtor) return false;
    if (!g_bs.pipelineNamesMid || !g_bs.listSizeMid || !g_bs.listGetMid) return false;

    jobject list = env->CallObjectMethod(pipeline, g_bs.pipelineNamesMid);
    if (!list || env->ExceptionCheck()) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        return false;
    }
    jint count = env->CallIntMethod(list, g_bs.listSizeMid);

    // Locate the target by name, remembering what follows it.
    jint idx = -1;
    for (jint i = 0; i < count && idx < 0; ++i) {
        jobject s = env->CallObjectMethod(list, g_bs.listGetMid, i);
        if (!s) continue;
        const char* c = env->GetStringUTFChars((jstring)s, nullptr);
        if (c) {
            for (int k = 0; k < nNames; ++k) {
                if (std::strcmp(c, names[k]) == 0) { idx = i; break; }
            }
            env->ReleaseStringUTFChars((jstring)s, c);
        }
        env->DeleteLocalRef(s);
    }
    if (idx < 0) {
        LogTo("proto: no %s handler in pipeline", tag);
        env->DeleteLocalRef(list);
        return false;
    }

    jobject nextRef = env->CallObjectMethod(list, g_bs.listGetMid, (jint)(idx + 1));
    if (env->ExceptionCheck()) { env->ExceptionClear(); nextRef = nullptr; }

    jobject nameRef = env->CallObjectMethod(list, g_bs.listGetMid, idx);
    if (env->ExceptionCheck() || !nameRef) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        if (nextRef) env->DeleteLocalRef(nextRef);
        env->DeleteLocalRef(list);
        return false;
    }
    jstring newName = env->NewStringUTF(installAs);

    jobject handler = env->NewObject(handlerCls, handlerCtor, pi);
    if (!handler || env->ExceptionCheck()) {
        LogAndClearException(env, "proto/NewObject(handler)");
        env->DeleteLocalRef(newName);
        env->DeleteLocalRef(nameRef);
        if (nextRef) env->DeleteLocalRef(nextRef);
        env->DeleteLocalRef(list);
        return false;
    }

    jobject removed = env->CallObjectMethod(pipeline, g_bs.pipelineRemoveNameMid, nameRef);
    bool ok = !env->ExceptionCheck();
    if (!ok) LogAndClearException(env, "proto/pipeline.remove");
    else if (removed) env->DeleteLocalRef(removed);

    if (ok) {
        jobject ret = nullptr;
        if (nextRef) {
            ret = env->CallObjectMethod(pipeline, g_bs.pipelineAddBeforeMid,
                                        nextRef, newName, handler);
        } else {
            ret = env->CallObjectMethod(pipeline, g_bs.pipelineAddLastMid,
                                        newName, handler);
        }
        ok = !env->ExceptionCheck();
        if (!ok) LogAndClearException(env, "proto/pipeline.addBefore");
        else if (ret) env->DeleteLocalRef(ret);
    }

    if (ok) {
        const char* c = env->GetStringUTFChars((jstring)nameRef, nullptr);
        LogTo("proto: %s slot -> %s (%s)", c ? c : "?", installAs, tag);
        if (c) env->ReleaseStringUTFChars((jstring)nameRef, c);
    }

    env->DeleteLocalRef(handler);
    env->DeleteLocalRef(newName);
    env->DeleteLocalRef(nameRef);
    if (nextRef) env->DeleteLocalRef(nextRef);
    env->DeleteLocalRef(list);
    return ok;
}

// The PLAY ProtocolInfos must be bound against A's *real* RegistryAccess: the
// codecs resolve registry ids through the buffer, so one bound against
// RegistryAccess.EMPTY cannot encode a chunk section, an entity type or an item
// stack.  Rather than rebuild it, we lift the two ProtocolInfos straight out of
// A's live pipeline -- they are already bound with the right registry, and B
// runs the same version so they are exactly the ones B needs.
//
// This has to happen after A's own configuration has finished, because only
// then does A's pipeline hold the PLAY handlers.  The relay sees packets before
// Connection dispatches them to the listener, so when the finish-config packet
// goes past, A's swap has not happened yet; by the time the first PLAY packet
// goes past, it has.  Hence: steal lazily, on the first PLAY packet.
// Reads the ProtocolInfo out of one of A's codec handlers.  Returns null if the
// slot currently holds an UnconfiguredPipelineHandler placeholder, or -- when
// requirePlay -- if the codec is not on PLAY yet.
jobject liftAProtocolInfo(JNIEnv* env, jobject aCtx, const char* name, bool requirePlay) {
    if (!aCtx || !g_bs.pipelineGetHandlerMid) return nullptr;
    jobject aPipeline = env->CallObjectMethod(aCtx, g_relay.netty.pipelineMid);
    if (!aPipeline || env->ExceptionCheck()) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        return nullptr;
    }
    jstring nm = env->NewStringUTF(name);
    jobject h = env->CallObjectMethod(aPipeline, g_bs.pipelineGetHandlerMid, nm);
    env->DeleteLocalRef(nm);
    env->DeleteLocalRef(aPipeline);
    if (!h || env->ExceptionCheck()) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        return nullptr;
    }
    jclass hc = env->GetObjectClass(h);
    jfieldID f = findFieldByDesc(hc, "Lnet/minecraft/network/ProtocolInfo;", false);
    jobject pi = f ? env->GetObjectField(h, f) : nullptr;
    if (env->ExceptionCheck()) { env->ExceptionClear(); pi = nullptr; }
    env->DeleteLocalRef(hc);
    env->DeleteLocalRef(h);

    // Which handler A actually handed over, and which direction it carries.
    // The two names are not symmetric: a client's real handler is "encoder"
    // while its decoder side is vanilla's "inbound_config" placeholder, so a
    // lookup that silently misses is easy to mistake for a wrong direction.
    {
        const char* flow = "?";
        if (pi && g_bs.protocolInfoFlowMid) {
            jobject f = env->CallObjectMethod(pi, g_bs.protocolInfoFlowMid);
            if (f && !env->ExceptionCheck()) {
                flow = env->IsSameObject(f, g_bs.flowServerbound) ? "SERVERBOUND" : "CLIENTBOUND";
                env->DeleteLocalRef(f);
            }
            if (env->ExceptionCheck()) env->ExceptionClear();
        }
        LogTo("lift: A's \"%s\" -> %s (%s)", name,
              pi ? "found" : "MISSING", pi ? flow : "-");
    }

    if (pi && requirePlay && g_bs.protocolInfoIdMid && g_bs.connectionProtocolPlay) {
        jobject id = env->CallObjectMethod(pi, g_bs.protocolInfoIdMid);
        if (env->ExceptionCheck()) { env->ExceptionClear(); id = nullptr; }
        bool play = id && env->IsSameObject(id, g_bs.connectionProtocolPlay);
        if (id) env->DeleteLocalRef(id);
        if (!play) { env->DeleteLocalRef(pi); pi = nullptr; }
    }
    return pi;
}

// B's encoder needs PLAY *clientbound*, which is what A's decoder holds.  The
// relay sits after A's decoder, so by the first PLAY packet it is on PLAY.
void ensureBPlayProtocol(JNIEnv* env, jobject aCtx) {
    if (g_bs.playProtocolReady) return;
    jobject cb = liftAProtocolInfo(env, aCtx, "decoder", true);
    if (cb) {
        if (g_bs.piPlayCbound) env->DeleteGlobalRef(g_bs.piPlayCbound);
        g_bs.piPlayCbound = env->NewGlobalRef(cb);
        env->DeleteLocalRef(cb);
        LogTo("play-proto: lifted A's clientbound PLAY ProtocolInfo (B's encoder)");
    } else {
        LogTo("play-proto: WARNING could not lift A's clientbound PLAY ProtocolInfo; "
              "falling back to the template-bound one");
    }
    g_bs.playProtocolReady = true;
}

// B's decoder needs PLAY *serverbound*, which is A's encoder -- but A only moves
// its encoder to PLAY after sending its own FinishConfiguration, which can be
// later than the first PLAY packet it receives.  Lifting too early hands B a
// CONFIGURATION decoder and every game packet B sends fails to decode.  So
// retry until A's encoder actually reports PLAY.
bool tryLiftPlaySbound(JNIEnv* env, jobject aCtx) {
    if (g_bs.playSboundReady) return true;
    jobject sb = liftAProtocolInfo(env, aCtx, "encoder", true);
    if (!sb) return false;
    if (g_bs.piPlaySbound) env->DeleteGlobalRef(g_bs.piPlaySbound);
    g_bs.piPlaySbound = env->NewGlobalRef(sb);
    env->DeleteLocalRef(sb);
    g_bs.playSboundReady = true;
    LogTo("play-proto: lifted A's serverbound PLAY ProtocolInfo (B's decoder)");
    return true;
}

// The two directions switch at different moments, exactly like vanilla's
// server: the encoder right after the terminal clientbound packet has been
// written (LoginFinished, FinishConfiguration), the decoder when B's terminal
// serverbound packet arrives (intention, LoginAcknowledged, FinishConfiguration).
//
// Inbound also has to turn autoRead back on.  PacketDecoder, on decoding a
// terminal packet, calls ProtocolSwapHandler.handleInboundTerminalPacket, which
// sets autoRead=false and parks an UnconfiguredPipelineHandler$Inbound in its
// slot; vanilla's InboundConfigurationTask re-enables reads after installing
// the next decoder.  Skip that and the channel never reads again -- B's Hello
// sits in FlowControlHandler forever and B times out on "Connecting".
void setProtocolDirections(JNIEnv* env, jobject channel, ProtoState st,
                           bool outbound, bool inbound) {
    if (!channel || !g_bs.channelPipelineMid) return;

    jobject pipeline = env->CallObjectMethod(channel, g_bs.channelPipelineMid);
    if (!pipeline || env->ExceptionCheck()) { env->ExceptionClear(); return; }

    static const char* kOutboundNames[] = {"encoder", "outbound_config"};
    static const char* kInboundNames[]  = {"decoder", "inbound_config"};

    logPipeline(env, pipeline, "before swap");

    if (outbound) {
        swapPipelineHandler(env, pipeline, kOutboundNames, 2, "encoder",
                            g_bs.packetEncoderCls, g_bs.packetEncoderCtor,
                            protocolInfoFor(st, true), "outbound");
    }
    if (inbound) {
        bool ok = swapPipelineHandler(env, pipeline, kInboundNames, 2, "decoder",
                                      g_bs.packetDecoderCls, g_bs.packetDecoderCtor,
                                      protocolInfoFor(st, false), "inbound");
        if (ok && g_bs.channelConfigMid && g_bs.configSetAutoReadMid) {
            jobject cfg = env->CallObjectMethod(channel, g_bs.channelConfigMid);
            if (cfg && !env->ExceptionCheck()) {
                jobject r = env->CallObjectMethod(cfg, g_bs.configSetAutoReadMid, JNI_TRUE);
                if (env->ExceptionCheck()) LogAndClearException(env, "proto/setAutoRead");
                else if (r) env->DeleteLocalRef(r);
                env->DeleteLocalRef(cfg);
            } else if (env->ExceptionCheck()) env->ExceptionClear();
        }
    }

    logPipeline(env, pipeline, "after swap");

    // Two things decide whether B can still be heard: whether the channel is
    // reading at all (vanilla parks reads when it swaps a decoder), and whether
    // the two ProtocolInfos really are the directions B needs.  Getting either
    // wrong is silent from the outside -- B just never speaks.
    {
        bool autoRead = false;
        if (g_bs.channelConfigMid && g_bs.configIsAutoReadMid) {
            jobject cfg = env->CallObjectMethod(channel, g_bs.channelConfigMid);
            if (cfg && !env->ExceptionCheck()) {
                autoRead = env->CallBooleanMethod(cfg, g_bs.configIsAutoReadMid) == JNI_TRUE;
                env->DeleteLocalRef(cfg);
            }
            if (env->ExceptionCheck()) env->ExceptionClear();
        }
        auto flowName = [&](jobject pi) -> const char* {
            if (!pi || !g_bs.protocolInfoFlowMid) return "(none)";
            jobject f = env->CallObjectMethod(pi, g_bs.protocolInfoFlowMid);
            if (env->ExceptionCheck() || !f) { if (env->ExceptionCheck()) env->ExceptionClear(); return "(?)"; }
            bool sb = env->IsSameObject(f, g_bs.flowServerbound);
            env->DeleteLocalRef(f);
            return sb ? "SERVERBOUND" : "CLIENTBOUND";
        };
        LogTo("proto-state: autoRead=%d  encoderPI=%s  decoderPI=%s",
              autoRead ? 1 : 0,
              flowName(g_bs.piPlayCbound), flowName(g_bs.piPlaySbound));
    }
    env->DeleteLocalRef(pipeline);
}

void setProtocolState(JNIEnv* env, jobject channel, ProtoState st) {
    setProtocolDirections(env, channel, st, true, true);
}

// --- running swaps on B's event loop ----------------------------------------
// Packets A forwards are written from A's event loop, so netty queues them as
// tasks on B's loop.  A pipeline swap done directly from A's thread is *not* in
// that queue and can overtake writes still waiting in it -- e.g. B's encoder
// moved to PLAY before the queued FinishConfiguration got encoded.  Swaps
// requested from outside B's loop are therefore queued on it too.
struct LoopSwap { ProtoState st; bool outbound; bool inbound; };
std::mutex           g_loopSwapMu;
std::deque<LoopSwap> g_loopSwaps;

void JNICALL Native_LoopTask_run(JNIEnv* env, jobject) {
    LoopSwap op;
    {
        std::lock_guard<std::mutex> l(g_loopSwapMu);
        if (g_loopSwaps.empty()) return;
        op = g_loopSwaps.front();
        g_loopSwaps.pop_front();
    }
    jobject ch;
    { std::lock_guard<std::mutex> l(g_bs.bMu); ch = g_bs.bChannel ? env->NewLocalRef(g_bs.bChannel) : nullptr; }
    if (!ch) return;
    setProtocolDirections(env, ch, op.st, op.outbound, op.inbound);
    env->DeleteLocalRef(ch);
}

void postProtocolSwap(JNIEnv* env, jobject channel, ProtoState st,
                      bool outbound, bool inbound) {
    jobject loop = nullptr;
    jobject task = nullptr;
    if (g_bs.channelEventLoopMid && g_bs.executorExecuteMid &&
        g_bs.loopTaskCls && g_bs.loopTaskCtor) {
        loop = env->CallObjectMethod(channel, g_bs.channelEventLoopMid);
        if (env->ExceptionCheck()) { env->ExceptionClear(); loop = nullptr; }
        if (loop) task = env->NewObject(g_bs.loopTaskCls, g_bs.loopTaskCtor);
        if (env->ExceptionCheck()) { env->ExceptionClear(); task = nullptr; }
    }
    if (!loop || !task) {
        LogTo("proto: cannot reach B's event loop; swapping from this thread");
        if (loop) env->DeleteLocalRef(loop);
        setProtocolDirections(env, channel, st, outbound, inbound);
        return;
    }
    {
        std::lock_guard<std::mutex> l(g_loopSwapMu);
        g_loopSwaps.push_back({st, outbound, inbound});
    }
    env->CallVoidMethod(loop, g_bs.executorExecuteMid, task);
    if (env->ExceptionCheck()) {
        LogAndClearException(env, "proto/eventLoop.execute");
        std::lock_guard<std::mutex> l(g_loopSwapMu);
        if (!g_loopSwaps.empty()) g_loopSwaps.pop_back();
    }
    env->DeleteLocalRef(task);
    env->DeleteLocalRef(loop);
}

void JNICALL Native_ServerInit_initChannel(JNIEnv* env, jobject , jobject ch) {
    LogTo("BServer: initChannel for incoming ch=%p", (void*)ch);

    if (g_bs.channelConfigMid && g_bs.configSetOptionMid &&
        g_bs.tcpNoDelayOption && g_bs.booleanTrue) {
        jobject cfg = env->CallObjectMethod(ch, g_bs.channelConfigMid);
        if (cfg && !env->ExceptionCheck()) {
            env->CallBooleanMethod(cfg, g_bs.configSetOptionMid,
                                   g_bs.tcpNoDelayOption, g_bs.booleanTrue);
            if (env->ExceptionCheck()) env->ExceptionClear();
            else LogTo("BServer: TCP_NODELAY set on B channel");
            env->DeleteLocalRef(cfg);
        } else if (env->ExceptionCheck()) env->ExceptionClear();
    }

    jobject pipeline = env->CallObjectMethod(ch, g_bs.channelPipelineMid);
    if (env->ExceptionCheck() || !pipeline) {
        env->ExceptionClear(); LogTo("  pipeline() failed"); return;
    }

    // 1.21.8: configureSerialization grew a `local` flag and a bandwidth
    // monitor.  We are a real socket server, so local=false and no monitor.
    env->CallStaticVoidMethod(g_bs.connectionCls, g_bs.connectionConfigureSerMid,
                              pipeline, g_bs.flowServerbound, (jboolean)JNI_FALSE,
                              (jobject)nullptr);
    if (env->ExceptionCheck()) { LogAndClearException(env, "  configureSerialization"); }

    // configureSerialization installs the serverbound decoder for real and
    // leaves the outbound side as an UnconfiguredPipelineHandler placeholder;
    // setProtocolState swaps both for the handshake protocol below.
    setProtocolState(env, ch, ProtoState::Handshake);

    jobject handler = env->NewObject(g_bs.handlerClass, g_bs.handlerCtor);
    jstring name = env->NewStringUTF("bside");
    env->CallObjectMethod(pipeline, g_bs.pipelineAddLastMid, name, handler);
    if (env->ExceptionCheck()) LogAndClearException(env, "  addLast(bside)");
    env->DeleteLocalRef(name);
    env->DeleteLocalRef(handler);
    logPipeline(env, pipeline, "initChannel done");
    env->DeleteLocalRef(pipeline);

    {
        std::lock_guard<std::mutex> l(g_bs.bMu);
        if (g_bs.bChannel) env->DeleteGlobalRef(g_bs.bChannel);
        g_bs.bChannel = env->NewGlobalRef(ch);
        g_bs.bState.store(BState::AwaitHandshake, std::memory_order_release);
    }
    {
        std::lock_guard<std::mutex> l(g_bs.playSwapMu);
        g_bs.bOutboundPlay = false;
        g_bs.bInboundPlayPending = false;
    }

    LogTo("  B channel captured, state=AwaitHandshake");
}

void JNICALL Native_BSide_channelActive(JNIEnv* env, jobject , jobject ctx) {
    LogTo("BServer: B channelActive");
    if (ctx) {
        jobject p = env->CallObjectMethod(ctx, g_relay.netty.pipelineMid);
        if (p && !env->ExceptionCheck()) {
            logPipeline(env, p, "channelActive");
            env->DeleteLocalRef(p);
        } else if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }
    }
}

// B can sit in CONFIGURATION for as long as it takes A to join a server, and
// the client tears the connection down after 30 s of silence (ReadTimeoutHandler
// in Connection.connectToServer).  ClientboundKeepAlivePacket is valid in both
// CONFIGURATION and PLAY, so a slow tick keeps B alive without pushing it
// forward through the phase.
DWORD WINAPI BKeepAliveThread(LPVOID) {
    LogTo("keepalive: watchdog started");
    JniAttach attach;
    if (!attach) { LogTo("keepalive: could not attach thread"); return 0; }
    JNIEnv* env = attach.env;
    jlong seq = 0;

    for (;;) {
        Sleep(10000);
        if (g_bs.bState.load(std::memory_order_acquire) != BState::AwaitConfiguration)
            continue;
        if (!g_bs.keepAlivePacketCls || !g_bs.keepAlivePacketCtor) continue;

        jobject ch;
        {
            std::lock_guard<std::mutex> l(g_bs.bMu);
            ch = g_bs.bChannel ? env->NewLocalRef(g_bs.bChannel) : nullptr;
        }
        if (!ch) continue;

        jobject ka = env->NewObject(g_bs.keepAlivePacketCls, g_bs.keepAlivePacketCtor,
                                    (jlong)++seq);
        if (ka && !env->ExceptionCheck()) {
            env->CallObjectMethod(ch, g_bs.channelWriteAndFlushMid, ka);
            if (env->ExceptionCheck()) env->ExceptionClear();
            LogTo("keepalive: nudged B while it waits in CONFIGURATION (id=%lld)",
                  (long long)seq);
        } else if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }
        if (ka) env->DeleteLocalRef(ka);
        env->DeleteLocalRef(ch);
    }
    return 0;
}

void closeARemoteConnection(JNIEnv* env) {
    if (!g_bs.connectionChannelFid || !g_bs.channelCloseMid) return;
    jobject conn;
    {
        std::lock_guard<std::mutex> l(g_bs.targetMu);
        conn = g_bs.targetAConnection
                    ? env->NewLocalRef(g_bs.targetAConnection)
                    : nullptr;
    }
    if (!conn) { LogTo("[A-CLOSE] no target A connection cached; nothing to close"); return; }
    jobject aChannel = env->GetObjectField(conn, g_bs.connectionChannelFid);
    env->DeleteLocalRef(conn);
    if (!aChannel) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        LogTo("[A-CLOSE] Connection.channel is null (not yet channelActive?)");
        return;
    }
    jobject future = env->CallObjectMethod(aChannel, g_bs.channelCloseMid);
    if (env->ExceptionCheck()) LogAndClearException(env, "[A-CLOSE] channel.close");
    else LogTo("[A-CLOSE] A's netty channel to remote server closed directly");
    if (future) env->DeleteLocalRef(future);
    env->DeleteLocalRef(aChannel);

    std::lock_guard<std::mutex> l(g_bs.targetMu);
    if (g_bs.targetAConnection) {
        env->DeleteGlobalRef(g_bs.targetAConnection);
        g_bs.targetAConnection = nullptr;
    }
}

void JNICALL Native_BSide_channelInactive(JNIEnv* env, jobject , jobject ctx) {
    // B's multiplayer screen opens extra STATUS connections alongside the real
    // login one.  Only the current B channel closing tears B's state down --
    // a stale ping connection timing out must not orphan the login.
    {
        std::lock_guard<std::mutex> l(g_bs.bMu);
        bool current = false;
        if (g_bs.bChannel && ctx) {
            jobject p1 = env->CallObjectMethod(ctx, g_relay.netty.pipelineMid);
            if (env->ExceptionCheck()) { env->ExceptionClear(); p1 = nullptr; }
            jobject p2 = env->CallObjectMethod(g_bs.bChannel, g_bs.channelPipelineMid);
            if (env->ExceptionCheck()) { env->ExceptionClear(); p2 = nullptr; }
            current = p1 && p2 && env->IsSameObject(p1, p2);
            if (p1) env->DeleteLocalRef(p1);
            if (p2) env->DeleteLocalRef(p2);
        }
        if (!current) {
            LogTo("BServer: stale B channel inactive (not the current one) - ignored");
            return;
        }
        LogTo("BServer: B channelInactive");
        env->DeleteGlobalRef(g_bs.bChannel);
        g_bs.bChannel = nullptr;
        g_bs.bState.store(BState::AwaitHandshake, std::memory_order_release);
    }
    {
        std::lock_guard<std::mutex> l(g_bs.playSwapMu);
        g_bs.bOutboundPlay = false;
        g_bs.bInboundPlayPending = false;
    }
    {
        std::lock_guard<std::mutex> gateLock(g_bConnMu);
        g_bConnected = false;
    }

    if (g_bs.midSession.load(std::memory_order_acquire)) {
        LogTo("BServer: B gone; mid-session — leaving A's connection intact, A resumes control");
    } else {
        // 1.20.1 closed A here because A had been held back for B.  In 1.21.8
        // A is never held, so B leaving must not take A's session down.
        LogTo("BServer: B gone — A's connection left intact");
    }
}

void JNICALL Native_MainThreadGate_run(JNIEnv*, jobject ) {
    if (BServer_WaitForBConnected(0)) return;
    LogTo("[MAIN-GATE] blocking A Render thread until B reaches PLAY");
    BServer_WaitForBConnected(-1);
    LogTo("[MAIN-GATE] B reached PLAY; resuming A Render thread");
}

}
void BSide_OnPacket(JNIEnv* env, jobject ctx, jobject msg);
namespace {

void JNICALL Native_BSide_channelRead(JNIEnv* env, jobject , jobject ctx, jobject msg) {
    ::BSide_OnPacket(env, ctx, msg);
}

void JNICALL Native_BSide_exceptionCaught(JNIEnv* env, jobject , jobject, jobject cause) {
    if (!cause) return;
    jclass tc = env->FindClass("java/lang/Throwable");
    jmethodID toStr = tc ? env->GetMethodID(tc, "toString", "()Ljava/lang/String;") : nullptr;
    jstring s = toStr ? (jstring)env->CallObjectMethod(cause, toStr) : nullptr;
    const char* c = (s && !env->ExceptionCheck()) ? env->GetStringUTFChars(s, nullptr) : nullptr;
    LogTo("BServer: EXCEPTION on B channel: %s", c ? c : "<unprintable>");
    if (c) env->ReleaseStringUTFChars(s, c);
    if (s) env->DeleteLocalRef(s);
    if (tc) env->DeleteLocalRef(tc);
    if (env->ExceptionCheck()) env->ExceptionClear();
}

bool defineInitClass(JNIEnv* env, jobject mcLoader) {
    std::string simple   = GenerateRandomClassName(2, 3);
    std::string internal = MakeInternalName(GetTrampolinePackage(), simple);

    ClassBuilder cb(internal, kInitSuper, 52);
    u2 superInit = cb.methodRef(kInitSuper, "<init>", "()V");
    std::vector<u1> ctor = {
        0x2A, 0xB7, u1((superInit >> 8) & 0xFF), u1(superInit & 0xFF), 0xB1
    };
    cb.addCodedMethod("<init>", "()V", ACC_PUBLIC, ctor, 1, 1);
    cb.addNativeMethod("initChannel", kInitChannelDesc, ACC_PUBLIC | ACC_NATIVE);
    std::vector<u1> bytes = cb.build();

    jclass defined = env->DefineClass(internal.c_str(), mcLoader,
                                      reinterpret_cast<const jbyte*>(bytes.data()),
                                      static_cast<jsize>(bytes.size()));
    if (!defined) { LogAndClearException(env, "BServer/DefineInit"); return false; }
    JNINativeMethod nats[] = {
        {const_cast<char*>("initChannel"), const_cast<char*>(kInitChannelDesc),
         reinterpret_cast<void*>(&Native_ServerInit_initChannel)},
    };
    if (env->RegisterNatives(defined, nats, 1) != 0) {
        LogAndClearException(env, "BServer/RegisterInit"); env->DeleteLocalRef(defined); return false;
    }
    g_bs.initCtor = env->GetMethodID(defined, "<init>", "()V");
    g_bs.initClass = static_cast<jclass>(env->NewGlobalRef(defined));
    env->DeleteLocalRef(defined);
    LogTo("BServer: defined ServerChannelInit as %s", internal.c_str());
    return true;
}

bool defineHandlerClass(JNIEnv* env, jobject mcLoader) {
    std::string simple   = GenerateRandomClassName(2, 3);
    std::string internal = MakeInternalName(GetTrampolinePackage(), simple);

    ClassBuilder cb(internal, kHandlerSuper, 52);
    u2 superInit = cb.methodRef(kHandlerSuper, "<init>", "()V");
    std::vector<u1> ctor = {
        0x2A, 0xB7, u1((superInit >> 8) & 0xFF), u1(superInit & 0xFF), 0xB1
    };
    cb.addCodedMethod("<init>", "()V", ACC_PUBLIC, ctor, 1, 1);
    cb.addNativeMethod("channelActive",   kChannelActiveDesc,   ACC_PUBLIC | ACC_NATIVE);
    cb.addNativeMethod("channelInactive", kChannelInactiveDesc, ACC_PUBLIC | ACC_NATIVE);
    cb.addNativeMethod("channelRead",     kChannelReadDesc,     ACC_PUBLIC | ACC_NATIVE);
    // A decode failure upstream lands here.  Without this override netty hands
    // the throwable to slf4j -- the *game's* log -- and the proxy log shows
    // nothing at all, which is indistinguishable from B simply not sending.
    cb.addNativeMethod("exceptionCaught", kExceptionCaughtDesc, ACC_PUBLIC | ACC_NATIVE);
    std::vector<u1> bytes = cb.build();

    jclass defined = env->DefineClass(internal.c_str(), mcLoader,
                                      reinterpret_cast<const jbyte*>(bytes.data()),
                                      static_cast<jsize>(bytes.size()));
    if (!defined) { LogAndClearException(env, "BServer/DefineHandler"); return false; }
    JNINativeMethod nats[] = {
        {const_cast<char*>("channelActive"),   const_cast<char*>(kChannelActiveDesc),
         reinterpret_cast<void*>(&Native_BSide_channelActive)},
        {const_cast<char*>("channelInactive"), const_cast<char*>(kChannelInactiveDesc),
         reinterpret_cast<void*>(&Native_BSide_channelInactive)},
        {const_cast<char*>("channelRead"),     const_cast<char*>(kChannelReadDesc),
         reinterpret_cast<void*>(&Native_BSide_channelRead)},
        {const_cast<char*>("exceptionCaught"), const_cast<char*>(kExceptionCaughtDesc),
         reinterpret_cast<void*>(&Native_BSide_exceptionCaught)},
    };
    if (env->RegisterNatives(defined, nats, 4) != 0) {
        LogAndClearException(env, "BServer/RegisterHandler"); env->DeleteLocalRef(defined); return false;
    }
    g_bs.handlerCtor = env->GetMethodID(defined, "<init>", "()V");
    g_bs.handlerClass = static_cast<jclass>(env->NewGlobalRef(defined));
    env->DeleteLocalRef(defined);
    LogTo("BServer: defined BSideHandler as %s", internal.c_str());
    return true;
}

bool defineMainGateClass(JNIEnv* env, jobject mcLoader) {
    if (g_bs.mainGateClass && g_bs.mainGateCtor) return true;

    std::string simple   = GenerateRandomClassName(2, 3);
    std::string internal = MakeInternalName(GetTrampolinePackage(), simple);

    ClassBuilder cb(internal, "java/lang/Thread", 52);
    u2 superInit = cb.methodRef("java/lang/Thread", "<init>", "()V");
    std::vector<u1> ctor = {
        0x2A, 0xB7, u1((superInit >> 8) & 0xFF), u1(superInit & 0xFF), 0xB1
    };
    cb.addCodedMethod("<init>", "()V", ACC_PUBLIC, ctor, 1, 1);
    cb.addNativeMethod("run", "()V", ACC_PUBLIC | ACC_NATIVE);
    std::vector<u1> bytes = cb.build();

    jclass defined = env->DefineClass(internal.c_str(), mcLoader,
                                      reinterpret_cast<const jbyte*>(bytes.data()),
                                      static_cast<jsize>(bytes.size()));
    if (!defined) {
        LogAndClearException(env, "BServer/DefineMainGate");
        return false;
    }
    JNINativeMethod nats[] = {
        {const_cast<char*>("run"), const_cast<char*>("()V"),
         reinterpret_cast<void*>(&Native_MainThreadGate_run)},
    };
    if (env->RegisterNatives(defined, nats, 1) != 0) {
        LogAndClearException(env, "BServer/RegisterMainGate");
        env->DeleteLocalRef(defined);
        return false;
    }

    g_bs.mainGateCtor = env->GetMethodID(defined, "<init>", "()V");
    g_bs.mainGateClass = static_cast<jclass>(env->NewGlobalRef(defined));
    env->DeleteLocalRef(defined);
    if (!g_bs.mainGateCtor || !g_bs.mainGateClass) {
        LogAndClearException(env, "BServer/MainGateCtor");
        return false;
    }
    LogTo("BServer: defined A main-thread gate as %s", internal.c_str());
    return true;
}

bool defineLoopTaskClass(JNIEnv* env, jobject mcLoader) {
    if (g_bs.loopTaskCls && g_bs.loopTaskCtor) return true;

    std::string simple   = GenerateRandomClassName(2, 3);
    std::string internal = MakeInternalName(GetTrampolinePackage(), simple);

    ClassBuilder cb(internal, "java/lang/Object", 52);
    cb.addInterface("java/lang/Runnable");
    u2 objInit = cb.methodRef("java/lang/Object", "<init>", "()V");
    std::vector<u1> ctor = {
        0x2A, 0xB7, u1((objInit >> 8) & 0xFF), u1(objInit & 0xFF), 0xB1
    };
    cb.addCodedMethod("<init>", "()V", ACC_PUBLIC, ctor, 1, 1);
    cb.addNativeMethod("run", "()V", ACC_PUBLIC | ACC_NATIVE);
    std::vector<u1> bytes = cb.build();

    jclass defined = env->DefineClass(internal.c_str(), mcLoader,
                                      reinterpret_cast<const jbyte*>(bytes.data()),
                                      static_cast<jsize>(bytes.size()));
    if (!defined) { LogAndClearException(env, "BServer/DefineLoopTask"); return false; }
    JNINativeMethod nats[] = {
        {const_cast<char*>("run"), const_cast<char*>("()V"),
         reinterpret_cast<void*>(&Native_LoopTask_run)},
    };
    if (env->RegisterNatives(defined, nats, 1) != 0) {
        LogAndClearException(env, "BServer/RegisterLoopTask"); env->DeleteLocalRef(defined); return false;
    }
    g_bs.loopTaskCtor = env->GetMethodID(defined, "<init>", "()V");
    g_bs.loopTaskCls = static_cast<jclass>(env->NewGlobalRef(defined));
    env->DeleteLocalRef(defined);
    LogTo("BServer: defined B event-loop task as %s", internal.c_str());
    return true;
}

jboolean JNICALL Native_GameContext_hasInfiniteMaterials(JNIEnv*, jobject) {
    // Only the clientbound codecs consult this, to decide whether item stacks
    // carry the "infinite materials" component set.  We encode serverbound-side
    // packets and re-encode packets A already decoded, so the plain setting is
    // what we want.
    return JNI_FALSE;
}

// GameProtocols.SERVERBOUND_TEMPLATE is an UnboundProtocol, so binding it needs
// a GameProtocols$Context instance.  That interface has exactly one method, so
// we synthesise an implementation at runtime the same way HookBridge and the
// relay handler are synthesised.
bool defineGameContextClass(JNIEnv* env, jobject mcLoader) {
    if (g_bs.gameContextCls && g_bs.gameContextCtor) return true;

    std::string simple   = GenerateRandomClassName(2, 3);
    std::string internal = MakeInternalName(GetTrampolinePackage(), simple);

    ClassBuilder cb(internal, "java/lang/Object", 52);
    cb.addInterface("net/minecraft/network/protocol/game/GameProtocols$Context");

    u2 objInit = cb.methodRef("java/lang/Object", "<init>", "()V");
    std::vector<u1> ctor = {
        0x2A,
        0xB7, u1((objInit >> 8) & 0xFF), u1(objInit & 0xFF),
        0xB1,
    };
    cb.addCodedMethod("<init>", "()V", ACC_PUBLIC, ctor, 1, 1);
    cb.addNativeMethod("hasInfiniteMaterials", "()Z", ACC_PUBLIC | ACC_NATIVE);

    std::vector<u1> bytes = cb.build();
    jclass defined = env->DefineClass(internal.c_str(), mcLoader,
                                      reinterpret_cast<const jbyte*>(bytes.data()),
                                      static_cast<jsize>(bytes.size()));
    if (!defined) {
        LogAndClearException(env, "BServer/DefineGameContext");
        return false;
    }

    JNINativeMethod nats[] = {
        {const_cast<char*>("hasInfiniteMaterials"), const_cast<char*>("()Z"),
         reinterpret_cast<void*>(&Native_GameContext_hasInfiniteMaterials)},
    };
    if (env->RegisterNatives(defined, nats, 1) != 0) {
        LogAndClearException(env, "BServer/RegisterGameContext");
        env->DeleteLocalRef(defined);
        return false;
    }

    g_bs.gameContextCtor = env->GetMethodID(defined, "<init>", "()V");
    if (!g_bs.gameContextCtor) {
        LogAndClearException(env, "BServer/GameContextCtor");
        env->DeleteLocalRef(defined);
        return false;
    }
    g_bs.gameContextCls = static_cast<jclass>(env->NewGlobalRef(defined));
    env->DeleteLocalRef(defined);
    LogTo("BServer: defined GameProtocols$Context impl as %s", internal.c_str());
    return true;
}

bool cacheJavaRefs(JNIEnv* env, jobject mcLoader) {

    jclass connCls = loadOrFind(env, mcLoader,
        "net.minecraft.network.Connection", "Lnet/minecraft/network/Connection;");
    if (!connCls) return false;
    g_bs.connectionCls = static_cast<jclass>(env->NewGlobalRef(connCls));
    // 1.21.8: configureSerialization(ChannelPipeline, PacketFlow, boolean, BandwidthDebugMonitor)
    g_bs.connectionConfigureSerMid = findMethodByDesc(connCls,
        "(Lio/netty/channel/ChannelPipeline;Lnet/minecraft/network/protocol/PacketFlow;Z"
        "Lnet/minecraft/network/BandwidthDebugMonitor;)V", true);
    if (!g_bs.connectionConfigureSerMid) {
        // pre-1.20.5 shape, kept as a probe so the log tells us which we hit
        g_bs.connectionConfigureSerMid = findMethodByDesc(connCls,
            "(Lio/netty/channel/ChannelPipeline;Lnet/minecraft/network/protocol/PacketFlow;)V", true);
    }
    g_bs.connectionSendMid = findMethodByDesc(connCls,
        "(Lnet/minecraft/network/protocol/Packet;)V", false);

    g_bs.connectionChannelFid = findFieldByDesc(connCls,
        "Lio/netty/channel/Channel;", false);
    env->DeleteLocalRef(connCls);

    jclass intentCls = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.handshake.ClientIntent",
        "Lnet/minecraft/network/protocol/handshake/ClientIntent;");
    if (intentCls) {
        g_bs.clientIntentCls = static_cast<jclass>(env->NewGlobalRef(intentCls));
        auto readIntent = [&](const char* n) -> jobject {
            jfieldID f = env->GetStaticFieldID(intentCls, n,
                "Lnet/minecraft/network/protocol/handshake/ClientIntent;");
            if (!f) return nullptr;
            jobject v = env->GetStaticObjectField(intentCls, f);
            return v ? env->NewGlobalRef(v) : nullptr;
        };
        g_bs.intentLogin  = readIntent("LOGIN");
        g_bs.intentStatus = readIntent("STATUS");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(intentCls);
    } else {
        LogTo("BServer: ClientIntent not found — handshake intention won't be understood");
    }

    jclass flowCls = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.PacketFlow", "Lnet/minecraft/network/protocol/PacketFlow;");
    if (!flowCls) return false;
    jfieldID fSB = env->GetStaticFieldID(flowCls, "SERVERBOUND", "Lnet/minecraft/network/protocol/PacketFlow;");
    jfieldID fCB = env->GetStaticFieldID(flowCls, "CLIENTBOUND", "Lnet/minecraft/network/protocol/PacketFlow;");
    if (fSB) g_bs.flowServerbound = env->NewGlobalRef(env->GetStaticObjectField(flowCls, fSB));
    if (fCB) g_bs.flowClientbound = env->NewGlobalRef(env->GetStaticObjectField(flowCls, fCB));
    env->DeleteLocalRef(flowCls);

    jclass chCls = loadOrFind(env, mcLoader, "io.netty.channel.Channel", "Lio/netty/channel/Channel;");
    if (!chCls) return false;
    g_bs.channelCls = static_cast<jclass>(env->NewGlobalRef(chCls));
    g_bs.channelPipelineMid      = env->GetMethodID(chCls, "pipeline",      "()Lio/netty/channel/ChannelPipeline;");
    g_bs.channelWriteAndFlushMid = env->GetMethodID(chCls, "writeAndFlush", "(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;");
    g_bs.channelAttrMid          = env->GetMethodID(chCls, "attr",          "(Lio/netty/util/AttributeKey;)Lio/netty/util/Attribute;");
    g_bs.channelConfigMid        = env->GetMethodID(chCls, "config",        "()Lio/netty/channel/ChannelConfig;");
    g_bs.channelCloseMid         = env->GetMethodID(chCls, "close",         "()Lio/netty/channel/ChannelFuture;");
    g_bs.channelEventLoopMid     = env->GetMethodID(chCls, "eventLoop",     "()Lio/netty/channel/EventLoop;");
    if (env->ExceptionCheck()) env->ExceptionClear();
    env->DeleteLocalRef(chCls);

    jclass cfgCls = loadOrFind(env, mcLoader, "io.netty.channel.ChannelConfig",
                               "Lio/netty/channel/ChannelConfig;");
    if (cfgCls) {
        g_bs.configSetOptionMid = env->GetMethodID(cfgCls, "setOption",
            "(Lio/netty/channel/ChannelOption;Ljava/lang/Object;)Z");
        g_bs.configSetAutoReadMid = env->GetMethodID(cfgCls, "setAutoRead",
            "(Z)Lio/netty/channel/ChannelConfig;");
        g_bs.configIsAutoReadMid = env->GetMethodID(cfgCls, "isAutoRead", "()Z");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(cfgCls);
    }
    jclass optCls = loadOrFind(env, mcLoader, "io.netty.channel.ChannelOption",
                               "Lio/netty/channel/ChannelOption;");
    if (optCls) {
        jfieldID f = env->GetStaticFieldID(optCls, "TCP_NODELAY", "Lio/netty/channel/ChannelOption;");
        if (f) {
            jobject v = env->GetStaticObjectField(optCls, f);
            if (v) g_bs.tcpNoDelayOption = env->NewGlobalRef(v);
        }
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(optCls);
    }
    jclass execCls = env->FindClass("java/util/concurrent/Executor");
    if (execCls) {
        g_bs.executorExecuteMid = env->GetMethodID(execCls, "execute", "(Ljava/lang/Runnable;)V");
        env->DeleteLocalRef(execCls);
    }
    if (env->ExceptionCheck()) env->ExceptionClear();

    jclass boolCls = env->FindClass("java/lang/Boolean");
    if (boolCls) {
        jfieldID f = env->GetStaticFieldID(boolCls, "TRUE", "Ljava/lang/Boolean;");
        if (f) {
            jobject v = env->GetStaticObjectField(boolCls, f);
            if (v) g_bs.booleanTrue = env->NewGlobalRef(v);
        }
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(boolCls);
    }

    jclass pipCls = loadOrFind(env, mcLoader, "io.netty.channel.ChannelPipeline",
                               "Lio/netty/channel/ChannelPipeline;");
    if (!pipCls) return false;
    g_bs.pipelineCls = static_cast<jclass>(env->NewGlobalRef(pipCls));
    g_bs.pipelineAddLastMid = env->GetMethodID(pipCls, "addLast",
        "(Ljava/lang/String;Lio/netty/channel/ChannelHandler;)Lio/netty/channel/ChannelPipeline;");
    g_bs.pipelineRemoveNameMid = env->GetMethodID(pipCls, "remove",
        "(Ljava/lang/String;)Lio/netty/channel/ChannelHandler;");
    g_bs.pipelineGetHandlerMid = env->GetMethodID(pipCls, "get",
        "(Ljava/lang/String;)Lio/netty/channel/ChannelHandler;");
    g_bs.pipelineReplaceMid = env->GetMethodID(pipCls, "replace",
        "(Ljava/lang/String;Ljava/lang/String;Lio/netty/channel/ChannelHandler;)"
        "Lio/netty/channel/ChannelHandler;");
    g_bs.pipelineNamesMid = env->GetMethodID(pipCls, "names", "()Ljava/util/List;");
    g_bs.pipelineChannelMid = env->GetMethodID(pipCls, "channel",
        "()Lio/netty/channel/Channel;");
    g_bs.pipelineAddBeforeMid = env->GetMethodID(pipCls, "addBefore",
        "(Ljava/lang/String;Ljava/lang/String;Lio/netty/channel/ChannelHandler;)"
        "Lio/netty/channel/ChannelPipeline;");
    if (env->ExceptionCheck()) env->ExceptionClear();
    env->DeleteLocalRef(pipCls);

    // --- 1.21.8 protocol plumbing ------------------------------------------
    jclass rfbCls = loadOrFind(env, mcLoader,
        "net.minecraft.network.RegistryFriendlyByteBuf",
        "Lnet/minecraft/network/RegistryFriendlyByteBuf;");
    if (rfbCls) {
        g_bs.registryBufCls = static_cast<jclass>(env->NewGlobalRef(rfbCls));
        g_bs.registryBufCtor = env->GetMethodID(rfbCls, "<init>",
            "(Lio/netty/buffer/ByteBuf;Lnet/minecraft/core/RegistryAccess;)V");
        g_bs.rfbDecoratorMid = env->GetStaticMethodID(rfbCls, "decorator",
            "(Lnet/minecraft/core/RegistryAccess;)Ljava/util/function/Function;");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(rfbCls);
    }
    jclass raCls = loadOrFind(env, mcLoader, "net.minecraft.core.RegistryAccess",
                              "Lnet/minecraft/core/RegistryAccess;");
    if (raCls) {
        g_bs.registryAccessCls = static_cast<jclass>(env->NewGlobalRef(raCls));
        g_bs.registryAccessEmptyFid = env->GetStaticFieldID(raCls, "EMPTY",
            "Lnet/minecraft/core/RegistryAccess$Frozen;");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(raCls);
    }

    g_bs.protocolInfoCls = static_cast<jclass>(loadOrFind(env, mcLoader,
        "net.minecraft.network.ProtocolInfo", "Lnet/minecraft/network/ProtocolInfo;"));
    if (g_bs.protocolInfoCls)
        g_bs.protocolInfoFlowMid = env->GetMethodID(g_bs.protocolInfoCls, "flow",
            "()Lnet/minecraft/network/protocol/PacketFlow;");
    if (g_bs.protocolInfoCls) {
        g_bs.protocolInfoIdMid = findMethodByDesc(g_bs.protocolInfoCls,
            "()Lnet/minecraft/network/ConnectionProtocol;", false);
        if (env->ExceptionCheck()) env->ExceptionClear();
    }
    jclass cpCls = loadOrFind(env, mcLoader, "net.minecraft.network.ConnectionProtocol",
                              "Lnet/minecraft/network/ConnectionProtocol;");
    if (cpCls) {
        jfieldID f = env->GetStaticFieldID(cpCls, "PLAY", "Lnet/minecraft/network/ConnectionProtocol;");
        if (f) {
            jobject v = env->GetStaticObjectField(cpCls, f);
            if (v) { g_bs.connectionProtocolPlay = env->NewGlobalRef(v); env->DeleteLocalRef(v); }
        }
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(cpCls);
    }
    LogTo("proto: ProtocolInfo.id=%p ConnectionProtocol.PLAY=%p",
          (void*)g_bs.protocolInfoIdMid, (void*)g_bs.connectionProtocolPlay);
    jclass encCls = loadOrFind(env, mcLoader, "net.minecraft.network.PacketEncoder",
                               "Lnet/minecraft/network/PacketEncoder;");
    if (encCls) {
        g_bs.packetEncoderCls = static_cast<jclass>(env->NewGlobalRef(encCls));
        g_bs.packetEncoderCtor = env->GetMethodID(encCls, "<init>",
            "(Lnet/minecraft/network/ProtocolInfo;)V");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(encCls);
    }
    jclass decCls = loadOrFind(env, mcLoader, "net.minecraft.network.PacketDecoder",
                               "Lnet/minecraft/network/PacketDecoder;");
    if (decCls) {
        g_bs.packetDecoderCls = static_cast<jclass>(env->NewGlobalRef(decCls));
        g_bs.packetDecoderCtor = env->GetMethodID(decCls, "<init>",
            "(Lnet/minecraft/network/ProtocolInfo;)V");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(decCls);
    }
    {
        jclass subCls = loadOrFind(env, mcLoader,
            "net.minecraft.network.protocol.SimpleUnboundProtocol",
            "Lnet/minecraft/network/protocol/SimpleUnboundProtocol;");
        if (subCls) {
            g_bs.simpleUnboundBindMid = env->GetMethodID(subCls, "bind",
                "(Ljava/util/function/Function;)Lnet/minecraft/network/ProtocolInfo;");
            if (env->ExceptionCheck()) env->ExceptionClear();
            env->DeleteLocalRef(subCls);
        }
        jclass ubCls = loadOrFind(env, mcLoader, "net.minecraft.network.protocol.UnboundProtocol",
                                  "Lnet/minecraft/network/protocol/UnboundProtocol;");
        if (ubCls) {
            g_bs.unboundBindMid = env->GetMethodID(ubCls, "bind",
                "(Ljava/util/function/Function;Ljava/lang/Object;)"
                "Lnet/minecraft/network/ProtocolInfo;");
            if (env->ExceptionCheck()) env->ExceptionClear();
            env->DeleteLocalRef(ubCls);
        }
    }

    g_bs.piHandshakeSbound = bindProtocolInfoField(env,
        "net.minecraft.network.protocol.handshake.HandshakeProtocols", "SERVERBOUND");
    g_bs.piLoginCbound = bindProtocolInfoField(env,
        "net.minecraft.network.protocol.login.LoginProtocols", "CLIENTBOUND");
    g_bs.piLoginSbound = bindProtocolInfoField(env,
        "net.minecraft.network.protocol.login.LoginProtocols", "SERVERBOUND");
    g_bs.piConfigCbound = bindProtocolInfoField(env,
        "net.minecraft.network.protocol.configuration.ConfigurationProtocols", "CLIENTBOUND");
    g_bs.piConfigSbound = bindProtocolInfoField(env,
        "net.minecraft.network.protocol.configuration.ConfigurationProtocols", "SERVERBOUND");
    g_bs.piStatusCbound = bindProtocolInfoField(env,
        "net.minecraft.network.protocol.status.StatusProtocols", "CLIENTBOUND");
    g_bs.piStatusSbound = bindProtocolInfoField(env,
        "net.minecraft.network.protocol.status.StatusProtocols", "SERVERBOUND");
    g_bs.piPlayCbound = bindProtocolTemplate(env,
        "net.minecraft.network.protocol.game.GameProtocols", "CLIENTBOUND_TEMPLATE", false);
    g_bs.piPlaySbound = bindProtocolTemplate(env,
        "net.minecraft.network.protocol.game.GameProtocols", "SERVERBOUND_TEMPLATE", true);

    LogTo("proto refs: enc=%p dec=%p replace=%p bind=%p/%p playCB=%p playSB=%p "
          "loginCB=%p loginSB=%p cfgCB=%p cfgSB=%p hsSB=%p",
          (void*)g_bs.packetEncoderCtor, (void*)g_bs.packetDecoderCtor,
          (void*)g_bs.pipelineReplaceMid, (void*)g_bs.simpleUnboundBindMid,
          (void*)g_bs.unboundBindMid, (void*)g_bs.piPlayCbound, (void*)g_bs.piPlaySbound,
          (void*)g_bs.piLoginCbound, (void*)g_bs.piLoginSbound,
          (void*)g_bs.piConfigCbound, (void*)g_bs.piConfigSbound, (void*)g_bs.piHandshakeSbound);

    jclass gpCls = loadOrFind(env, mcLoader, "com.mojang.authlib.GameProfile",
                              "Lcom/mojang/authlib/GameProfile;");
    if (gpCls) {
        g_bs.gameProfileCls = static_cast<jclass>(env->NewGlobalRef(gpCls));
        g_bs.gameProfileCtor = env->GetMethodID(gpCls, "<init>", "(Ljava/util/UUID;Ljava/lang/String;)V");

        g_bs.gameProfileGetNameMid = env->GetMethodID(gpCls, "getName", "()Ljava/lang/String;");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(gpCls);
    }

    jclass mcCls = loadOrFind(env, mcLoader, "net.minecraft.client.Minecraft",
                              "Lnet/minecraft/client/Minecraft;");
    if (mcCls) {
        g_bs.minecraftCls = static_cast<jclass>(env->NewGlobalRef(mcCls));
        g_bs.mcGetInstanceMid = findMethodByDesc(mcCls, "()Lnet/minecraft/client/Minecraft;", true);
        g_bs.mcGetProfilePropsMid = findMethodByDesc(mcCls,
            "()Lcom/mojang/authlib/properties/PropertyMap;", false);
        g_bs.mcGetUserMid = findMethodByDesc(mcCls,
            "()Lnet/minecraft/client/User;", false);
        env->DeleteLocalRef(mcCls);
    }
    jclass userCls = loadOrFind(env, mcLoader, "net.minecraft.client.User",
                                "Lnet/minecraft/client/User;");
    if (userCls) {
        g_bs.userCls = static_cast<jclass>(env->NewGlobalRef(userCls));
        g_bs.userGetProfileIdMid = findMethodByDesc(userCls,
            "()Ljava/util/UUID;", false);

        g_bs.userGetGameProfileMid = findMethodByDesc(userCls,
            "()Lcom/mojang/authlib/GameProfile;", false);
        env->DeleteLocalRef(userCls);
    }
    jclass fbbCls = loadOrFind(env, mcLoader, "net.minecraft.network.FriendlyByteBuf",
                               "Lnet/minecraft/network/FriendlyByteBuf;");
    if (fbbCls) {
        g_bs.friendlyBufCls = static_cast<jclass>(env->NewGlobalRef(fbbCls));

        g_bs.fbbWriteByteMid    = env->GetMethodID(fbbCls, "writeByte",    "(I)Lio/netty/buffer/ByteBuf;");
        g_bs.fbbWriteBooleanMid = env->GetMethodID(fbbCls, "writeBoolean", "(Z)Lio/netty/buffer/ByteBuf;");
        if (env->ExceptionCheck()) env->ExceptionClear();
        g_bs.fbbWriteVarIntMid = findMethodByDesc(fbbCls,
            "(I)Lnet/minecraft/network/FriendlyByteBuf;", false);
        g_bs.fbbWriteUUIDMid = findMethodByDesc(fbbCls,
            "(Ljava/util/UUID;)Lnet/minecraft/network/FriendlyByteBuf;", false);
        g_bs.fbbWriteUtfMid = findMethodByDesc(fbbCls,
            "(Ljava/lang/String;I)Lnet/minecraft/network/FriendlyByteBuf;", false);
        g_bs.fbbWriteGpPropsMid = findMethodByDesc(fbbCls,
            "(Lcom/mojang/authlib/properties/PropertyMap;)V", false);
        env->DeleteLocalRef(fbbCls);
    }
    jclass byteBufCls = loadOrFind(env, mcLoader, "io.netty.buffer.ByteBuf",
                                   "Lio/netty/buffer/ByteBuf;");
    if (byteBufCls) {
        g_bs.byteBufReadableBytesMid = env->GetMethodID(byteBufCls, "readableBytes",  "()I");
        g_bs.byteBufReaderIndexMid   = env->GetMethodID(byteBufCls, "readerIndex",    "()I");
        g_bs.byteBufGetBytesMid      = env->GetMethodID(byteBufCls, "getBytes",       "(I[B)Lio/netty/buffer/ByteBuf;");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(byteBufCls);
    }
    jclass unpCls = loadOrFind(env, mcLoader, "io.netty.buffer.Unpooled",
                               "Lio/netty/buffer/Unpooled;");
    if (unpCls) {
        g_bs.unpooledCls = static_cast<jclass>(env->NewGlobalRef(unpCls));
        g_bs.unpooledBufferMid  = env->GetStaticMethodID(unpCls, "buffer",        "()Lio/netty/buffer/ByteBuf;");
        g_bs.unpooledWrappedMid = env->GetStaticMethodID(unpCls, "wrappedBuffer", "([B)Lio/netty/buffer/ByteBuf;");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(unpCls);
    }
    jclass piuCls = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.game.ClientboundPlayerInfoUpdatePacket",
        "Lnet/minecraft/network/protocol/game/ClientboundPlayerInfoUpdatePacket;");
    if (piuCls) {
        g_bs.playerInfoUpdatePacketCls = static_cast<jclass>(env->NewGlobalRef(piuCls));
        g_bs.playerInfoUpdatePacketBufCtor = env->GetMethodID(piuCls, "<init>",
            "(Lnet/minecraft/network/RegistryFriendlyByteBuf;)V");

        g_bs.playerInfoUpdatePacketWriteMid = findMethodByDesc(piuCls,
            "(Lnet/minecraft/network/RegistryFriendlyByteBuf;)V", false);

        jmethodID listMids[2] = {nullptr, nullptr};
        int nList = findMethodsByDesc(piuCls, "()Ljava/util/List;", false, listMids, 2);
        g_bs.piuEntriesMidA = listMids[0];
        g_bs.piuEntriesMidB = listMids[1];
        LogTo("  piu: %d ()List accessor(s) cached", nList);
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(piuCls);
    }

    jclass piEntryCls = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.game.ClientboundPlayerInfoUpdatePacket$Entry",
        "Lnet/minecraft/network/protocol/game/ClientboundPlayerInfoUpdatePacket$Entry;");
    if (piEntryCls) {
        g_bs.piEntryCls = static_cast<jclass>(env->NewGlobalRef(piEntryCls));
        g_bs.piEntryProfileIdMid   = findMethodByDesc(piEntryCls, "()Ljava/util/UUID;", false);
        g_bs.piEntryGameModeMid     = findMethodByDesc(piEntryCls,
            "()Lnet/minecraft/world/level/GameType;", false);
        g_bs.piEntryListedMid       = findMethodByDesc(piEntryCls, "()Z", false);
        static const char* const kHashExcl[] = { "hashCode" };
        g_bs.piEntryLatencyMid      = findMethodByDescExcl(piEntryCls, "()I", false, kHashExcl, 1);
        g_bs.piEntryDisplayNameMid  = findMethodByDesc(piEntryCls,
            "()Lnet/minecraft/network/chat/Component;", false);
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(piEntryCls);
    }

    jclass gameTypeCls = loadOrFind(env, mcLoader, "net.minecraft.world.level.GameType",
                                    "Lnet/minecraft/world/level/GameType;");
    if (gameTypeCls) {
        static const char* const kIdExcl[] = { "ordinal", "hashCode" };
        g_bs.gameTypeGetIdMid = findMethodByDescExcl(gameTypeCls, "()I", false, kIdExcl, 2);
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(gameTypeCls);
    }

    if (g_bs.friendlyBufCls) {
        g_bs.fbbWriteComponentMid = findMethodByDesc(g_bs.friendlyBufCls,
            "(Lnet/minecraft/network/chat/Component;)Lnet/minecraft/network/FriendlyByteBuf;", false);
    }

    jclass listCls = env->FindClass("java/util/List");
    if (listCls) {
        g_bs.listSizeMid = env->GetMethodID(listCls, "size", "()I");
        g_bs.listGetMid  = env->GetMethodID(listCls, "get", "(I)Ljava/lang/Object;");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(listCls);
    }

    {
        jclass bbCls = loadOrFind(env, mcLoader, "io.netty.buffer.ByteBuf",
                                  "Lio/netty/buffer/ByteBuf;");
        if (bbCls) {
            g_bs.byteBufGetByteMid = env->GetMethodID(bbCls, "getByte", "(I)B");
            if (env->ExceptionCheck()) env->ExceptionClear();
            env->DeleteLocalRef(bbCls);
        }
    }

    // 1.21.8 removed ClientboundAddPlayerPacket; players now arrive through
    // ClientboundAddEntityPacket, which carries the same UUID field.  Its
    // EntityType field distinguishes players, but the UUID is all we need.
    jclass appCls = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.game.ClientboundAddEntityPacket",
        "Lnet/minecraft/network/protocol/game/ClientboundAddEntityPacket;");
    if (appCls) {
        g_bs.addEntityPacketCls = static_cast<jclass>(env->NewGlobalRef(appCls));
        g_bs.addEntityPacketUuidFid = findFieldByDesc(appCls, "Ljava/util/UUID;", false);
        env->DeleteLocalRef(appCls);
    }

    jclass cpp = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.common.ClientboundCustomPayloadPacket",
        "Lnet/minecraft/network/protocol/common/ClientboundCustomPayloadPacket;");
    if (cpp) {
        g_bs.customPayloadPacketCls = static_cast<jclass>(env->NewGlobalRef(cpp));
        env->DeleteLocalRef(cpp);
    }

    jclass sptCls = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.game.ClientboundSetPlayerTeamPacket",
        "Lnet/minecraft/network/protocol/game/ClientboundSetPlayerTeamPacket;");
    if (sptCls) {
        g_bs.setPlayerTeamPacketCls = static_cast<jclass>(env->NewGlobalRef(sptCls));
        g_bs.setPlayerTeamPacketBufCtor = env->GetMethodID(sptCls, "<init>",
            "(Lnet/minecraft/network/RegistryFriendlyByteBuf;)V");
        g_bs.setPlayerTeamMethodFid  = findFieldByDesc(sptCls, "I", false);
        g_bs.setPlayerTeamNameFid    = findFieldByDesc(sptCls, "Ljava/lang/String;", false);
        g_bs.setPlayerTeamPlayersFid = findFieldByDesc(sptCls, "Ljava/util/Collection;", false);
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(sptCls);
    }
    jclass collCls = env->FindClass("java/util/Collection");
    if (collCls) {
        g_bs.collectionContainsMid = env->GetMethodID(collCls, "contains", "(Ljava/lang/Object;)Z");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(collCls);
    }

    jclass uuidClsL = env->FindClass("java/util/UUID");
    if (uuidClsL) {
        g_bs.uuidGetMsbMid = env->GetMethodID(uuidClsL, "getMostSignificantBits",  "()J");
        g_bs.uuidGetLsbMid = env->GetMethodID(uuidClsL, "getLeastSignificantBits", "()J");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(uuidClsL);
    }
    if (env->ExceptionCheck()) env->ExceptionClear();

    // 1.20.2 renamed ClientboundGameProfilePacket -> ClientboundLoginFinishedPacket
    jclass lfp = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.login.ClientboundLoginFinishedPacket",
        "Lnet/minecraft/network/protocol/login/ClientboundLoginFinishedPacket;");
    if (lfp) {
        g_bs.loginFinishedPacketCls = static_cast<jclass>(env->NewGlobalRef(lfp));

        g_bs.loginFinishedPacketCtor = findMethodByDesc(lfp,
            "(Lcom/mojang/authlib/GameProfile;)V", false);
        env->DeleteLocalRef(lfp);
    }

    // The configuration phase (new in 1.20.2) is ended by this packet.  B is
    // held in CONFIGURATION until A's own configuration stream has been
    // mirrored across, so this lookup is what ultimately releases B into PLAY.
    jclass fcp = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.configuration.ClientboundFinishConfigurationPacket",
        "Lnet/minecraft/network/protocol/configuration/ClientboundFinishConfigurationPacket;");
    if (fcp) {
        g_bs.finishConfigPacketCls = static_cast<jclass>(env->NewGlobalRef(fcp));
        g_bs.finishConfigPacketCtor = env->GetMethodID(fcp, "<init>", "()V");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(fcp);
    }

    jclass hello = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.login.ServerboundHelloPacket",
        "Lnet/minecraft/network/protocol/login/ServerboundHelloPacket;");
    if (hello) {
        g_bs.helloPacketCls = static_cast<jclass>(env->NewGlobalRef(hello));
        g_bs.helloPacketNameFid = findFieldByDesc(hello, "Ljava/lang/String;", false);
        env->DeleteLocalRef(hello);
    }

    jclass intent = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.handshake.ClientIntentionPacket",
        "Lnet/minecraft/network/protocol/handshake/ClientIntentionPacket;");
    if (intent) {
        g_bs.intentPacketCls = static_cast<jclass>(env->NewGlobalRef(intent));
        g_bs.intentionPacketIntentFid = findFieldByDesc(intent,
            "Lnet/minecraft/network/protocol/handshake/ClientIntent;", false);
        env->DeleteLocalRef(intent);
    }

    // B's two terminal serverbound packets after the handshake: each pauses
    // B's reads until we install the next decoder.
    jclass lAck = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.login.ServerboundLoginAcknowledgedPacket",
        "Lnet/minecraft/network/protocol/login/ServerboundLoginAcknowledgedPacket;");
    if (lAck) { g_bs.loginAckPacketCls = static_cast<jclass>(env->NewGlobalRef(lAck)); env->DeleteLocalRef(lAck); }
    jclass fAck = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.configuration.ServerboundFinishConfigurationPacket",
        "Lnet/minecraft/network/protocol/configuration/ServerboundFinishConfigurationPacket;");
    if (fAck) { g_bs.finishConfigAckPacketCls = static_cast<jclass>(env->NewGlobalRef(fAck)); env->DeleteLocalRef(fAck); }

    // Server transfer / reconfiguration: mid-PLAY the server sends
    // ClientboundStartConfigurationPacket, the client answers with
    // ServerboundConfigurationAcknowledgedPacket, and both sides walk back into
    // CONFIGURATION to re-sync registries before returning to PLAY.  Without
    // handling the re-entry B parks on "Reconfiguring..." forever.
    jclass sCfg = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.game.ClientboundStartConfigurationPacket",
        "Lnet/minecraft/network/protocol/game/ClientboundStartConfigurationPacket;");
    if (sCfg) { g_bs.startConfigPacketCls = static_cast<jclass>(env->NewGlobalRef(sCfg)); env->DeleteLocalRef(sCfg); }
    jclass cAck = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.game.ServerboundConfigurationAcknowledgedPacket",
        "Lnet/minecraft/network/protocol/game/ServerboundConfigurationAcknowledgedPacket;");
    if (cAck) { g_bs.configAckPacketCls = static_cast<jclass>(env->NewGlobalRef(cAck)); env->DeleteLocalRef(cAck); }

    jclass sReq = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.status.ServerboundStatusRequestPacket",
        "Lnet/minecraft/network/protocol/status/ServerboundStatusRequestPacket;");
    if (sReq) { g_bs.statusRequestPacketCls = static_cast<jclass>(env->NewGlobalRef(sReq)); env->DeleteLocalRef(sReq); }

    // the status ping packets moved to their own `ping` package in 1.20.2+
    jclass pReq = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.ping.ServerboundPingRequestPacket",
        "Lnet/minecraft/network/protocol/ping/ServerboundPingRequestPacket;");
    if (pReq) {
        g_bs.pingRequestPacketCls = static_cast<jclass>(env->NewGlobalRef(pReq));
        g_bs.pingRequestPacketTimeFid = findFieldByDesc(pReq, "J", false);
        env->DeleteLocalRef(pReq);
    }

    jclass pong = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.ping.ClientboundPongResponsePacket",
        "Lnet/minecraft/network/protocol/ping/ClientboundPongResponsePacket;");
    if (pong) {
        g_bs.pongResponsePacketCls = static_cast<jclass>(env->NewGlobalRef(pong));
        g_bs.pongResponsePacketCtor = findMethodByDesc(pong, "(J)V", false);
        env->DeleteLocalRef(pong);
    }

    jclass sResp = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.status.ClientboundStatusResponsePacket",
        "Lnet/minecraft/network/protocol/status/ClientboundStatusResponsePacket;");
    if (sResp) {
        g_bs.statusResponsePacketCls = static_cast<jclass>(env->NewGlobalRef(sResp));

        g_bs.statusResponsePacketCtor = findMethodByDesc(sResp,
            "(Lnet/minecraft/network/protocol/status/ServerStatus;)V", false);
        env->DeleteLocalRef(sResp);
    }
    jclass ss = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.status.ServerStatus",
        "Lnet/minecraft/network/protocol/status/ServerStatus;");
    if (ss) {
        g_bs.serverStatusCls = static_cast<jclass>(env->NewGlobalRef(ss));

        env->DeleteLocalRef(ss);
    }

    jclass pp = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.game.ClientboundPlayerPositionPacket",
        "Lnet/minecraft/network/protocol/game/ClientboundPlayerPositionPacket;");
    if (pp) {
        g_bs.playerPositionPacketCls = static_cast<jclass>(env->NewGlobalRef(pp));
        // 1.21.2 replaced the (x, y, z, yRot, xRot, relatives, id) constructor
        // with (id, PositionMoveRotation, relatives).  Only the placeholder
        // spawn in reconstructAndSendLoginToB uses this, and that whole path is
        // gated behind kGiveBOwnIdentity, so we resolve the new shape and leave
        // the (still 1.20.1-shaped) call site disabled.
        g_bs.playerPositionPacketCtor = findMethodByDesc(pp,
            "(ILnet/minecraft/world/entity/PositionMoveRotation;Ljava/util/Set;)V", false);
        env->DeleteLocalRef(pp);
    }

    jclass setC = env->FindClass("java/util/Set");
    if (setC) {
        g_bs.setCls = static_cast<jclass>(env->NewGlobalRef(setC));
        g_bs.setOfMid = env->GetStaticMethodID(setC, "of", "()Ljava/util/Set;");
        env->DeleteLocalRef(setC);
    }

    jclass kap = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.common.ClientboundKeepAlivePacket",
        "Lnet/minecraft/network/protocol/common/ClientboundKeepAlivePacket;");
    if (kap) {
        g_bs.keepAlivePacketCls = static_cast<jclass>(env->NewGlobalRef(kap));
        g_bs.keepAlivePacketCtor = findMethodByDesc(kap, "(J)V", false);
        env->DeleteLocalRef(kap);
    }

    jclass cbp = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.game.ClientboundBundlePacket",
        "Lnet/minecraft/network/protocol/game/ClientboundBundlePacket;");
    if (cbp) {
        g_bs.bundlePacketCls = static_cast<jclass>(env->NewGlobalRef(cbp));
        jclass bp = loadOrFind(env, mcLoader,
            "net.minecraft.network.protocol.BundlePacket",
            "Lnet/minecraft/network/protocol/BundlePacket;");
        if (bp) {
            g_bs.bundleSubPacketsMid = findMethodByDesc(bp, "()Ljava/lang/Iterable;", false);
            env->DeleteLocalRef(bp);
        }
        env->DeleteLocalRef(cbp);
    } else {
        LogTo("BServer: ClientboundBundlePacket not found — bundles won't be expanded");
    }
    jclass iterableCls = env->FindClass("java/lang/Iterable");
    if (iterableCls) {
        g_bs.iterableIteratorMid = env->GetMethodID(iterableCls, "iterator", "()Ljava/util/Iterator;");
        env->DeleteLocalRef(iterableCls);
    }
    jclass iteratorCls = env->FindClass("java/util/Iterator");
    if (iteratorCls) {
        g_bs.iteratorHasNextMid = env->GetMethodID(iteratorCls, "hasNext", "()Z");
        g_bs.iteratorNextMid    = env->GetMethodID(iteratorCls, "next", "()Ljava/lang/Object;");
        env->DeleteLocalRef(iteratorCls);
    }
    if (env->ExceptionCheck()) env->ExceptionClear();

    jclass uuidCls = env->FindClass("java/util/UUID");
    if (uuidCls) {
        g_bs.uuidCls = static_cast<jclass>(env->NewGlobalRef(uuidCls));
        g_bs.uuidNameUuidFromBytesMid = env->GetStaticMethodID(uuidCls,
            "nameUUIDFromBytes", "([B)Ljava/util/UUID;");
        env->DeleteLocalRef(uuidCls);
    }

    LogTo("cacheJavaRefs: configureSer=%p send=%p flowSB=%p pipMid=%p addLast=%p "
          "replace=%p gpCtor=%p lfpCtor=%p finishCfg=%p helloName=%p uuidFromBytes=%p "
          "intentFid=%p pingTimeFid=%p intentClasses=%p/%p",
          (void*)g_bs.connectionConfigureSerMid, (void*)g_bs.connectionSendMid,
          (void*)g_bs.flowServerbound, (void*)g_bs.channelPipelineMid,
          (void*)g_bs.pipelineAddLastMid, (void*)g_bs.pipelineReplaceMid,
          (void*)g_bs.gameProfileCtor, (void*)g_bs.loginFinishedPacketCtor,
          (void*)g_bs.finishConfigPacketCtor,
          (void*)g_bs.helloPacketNameFid, (void*)g_bs.uuidNameUuidFromBytesMid,
          (void*)g_bs.intentionPacketIntentFid, (void*)g_bs.pingRequestPacketTimeFid,
          (void*)g_bs.intentLogin, (void*)g_bs.intentStatus);
    if (env->ExceptionCheck()) env->ExceptionClear();

    if (g_bs.minecraftCls) {
        g_bs.mcGetConnectionMid = findMethodByDesc(g_bs.minecraftCls,
            "()Lnet/minecraft/client/multiplayer/ClientPacketListener;", false);
        g_bs.mcPlayerFid   = findFieldByDesc(g_bs.minecraftCls,
            "Lnet/minecraft/client/player/LocalPlayer;", false);
        g_bs.mcGameModeFid = findFieldByDesc(g_bs.minecraftCls,
            "Lnet/minecraft/client/multiplayer/MultiPlayerGameMode;", false);
        g_bs.mcLevelFid    = findFieldByDesc(g_bs.minecraftCls,
            "Lnet/minecraft/client/multiplayer/ClientLevel;", false);
        if (env->ExceptionCheck()) env->ExceptionClear();
    }
    jclass cplCls = loadOrFind(env, mcLoader,
        "net.minecraft.client.multiplayer.ClientPacketListener",
        "Lnet/minecraft/client/multiplayer/ClientPacketListener;");
    if (cplCls) {
        g_bs.clientPacketListenerCls = static_cast<jclass>(env->NewGlobalRef(cplCls));
        g_bs.cplGetConnectionMid  = findMethodByDesc(cplCls, "()Lnet/minecraft/network/Connection;", false);
        g_bs.cplLevelsMid         = findMethodByDesc(cplCls, "()Ljava/util/Set;", false);
        g_bs.cplRegistryAccessMid = findMethodByDesc(cplCls, "()Lnet/minecraft/core/RegistryAccess;", false);
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(cplCls);
    }
    {
        jclass frozenCls = loadOrFind(env, mcLoader, "net.minecraft.core.RegistryAccess",
                                      "Lnet/minecraft/core/RegistryAccess;");
        if (frozenCls) {
            g_bs.registryAccessFreezeMid = findMethodByDesc(frozenCls,
                "()Lnet/minecraft/core/RegistryAccess$Frozen;", false);
            if (env->ExceptionCheck()) env->ExceptionClear();
            env->DeleteLocalRef(frozenCls);
        }
    }
    jclass mpgmCls = loadOrFind(env, mcLoader,
        "net.minecraft.client.multiplayer.MultiPlayerGameMode",
        "Lnet/minecraft/client/multiplayer/MultiPlayerGameMode;");
    if (mpgmCls) {

        g_bs.gameModeGetTypeMid = findMethodByDesc(mpgmCls,
            "()Lnet/minecraft/world/level/GameType;", false);
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(mpgmCls);
    }
    jclass lvlCls = loadOrFind(env, mcLoader, "net.minecraft.world.level.Level",
                               "Lnet/minecraft/world/level/Level;");
    if (lvlCls) {
        g_bs.levelCls = static_cast<jclass>(env->NewGlobalRef(lvlCls));

        jmethodID dimMids[2] = {nullptr, nullptr};
        int nDim = findMethodsByDesc(lvlCls, "()Lnet/minecraft/resources/ResourceKey;", false, dimMids, 2);
        g_bs.levelDimMidA = dimMids[0];
        g_bs.levelDimMidB = dimMids[1];
        LogTo("  level: %d ()ResourceKey method(s)", nDim);
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(lvlCls);
    }
    jclass objCls = env->FindClass("java/lang/Object");
    if (objCls) {
        g_bs.objToStringMid = env->GetMethodID(objCls, "toString", "()Ljava/lang/String;");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(objCls);
    }
    jclass optionalClsL = env->FindClass("java/util/Optional");
    if (optionalClsL) {
        g_bs.optionalCls = static_cast<jclass>(env->NewGlobalRef(optionalClsL));
        g_bs.optionalEmptyMid = env->GetStaticMethodID(optionalClsL, "empty", "()Ljava/util/Optional;");
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(optionalClsL);
    }
    jclass loginCls = loadOrFind(env, mcLoader,
        "net.minecraft.network.protocol.game.ClientboundLoginPacket",
        "Lnet/minecraft/network/protocol/game/ClientboundLoginPacket;");
    if (loginCls) {
        g_bs.loginPacketCls = static_cast<jclass>(env->NewGlobalRef(loginCls));

        // 1.21.8 reshaped this packet around CommonPlayerSpawnInfo:
        //   (IZLjava/util/Set;IIIZZZLCommonPlayerSpawnInfo;Z)V
        // We do not synthesise it -- A's own ClientboundLoginPacket is mirrored
        // through to B instead -- so we only record whether it is reachable, to
        // decide later whether the synthetic path is even available.
        g_bs.loginPacketCtor = env->GetMethodID(loginCls, "<init>",
            "(IZLjava/util/Set;IIIZZZ"
            "Lnet/minecraft/network/protocol/game/CommonPlayerSpawnInfo;Z)V");
        if (env->ExceptionCheck()) env->ExceptionClear();
        LogTo("  login ctor %s (unused: B mirrors A's own login packet)",
              g_bs.loginPacketCtor ? "resolved" : "MISSING");
        env->DeleteLocalRef(loginCls);
    }

    return g_bs.connectionConfigureSerMid && g_bs.connectionSendMid &&
           g_bs.flowServerbound && g_bs.channelPipelineMid && g_bs.pipelineAddLastMid &&
           g_bs.channelWriteAndFlushMid && g_bs.pipelineReplaceMid &&
           g_bs.packetEncoderCtor && g_bs.packetDecoderCtor &&
           g_bs.piPlayCbound && g_bs.piPlaySbound;
}

bool bindServer(JNIEnv* env, jobject mcLoader) {
    jclass elgCls  = loadOrFind(env, mcLoader, "io.netty.channel.nio.NioEventLoopGroup",
                                "Lio/netty/channel/nio/NioEventLoopGroup;");
    jclass sbCls   = loadOrFind(env, mcLoader, "io.netty.bootstrap.ServerBootstrap",
                                "Lio/netty/bootstrap/ServerBootstrap;");
    jclass sscCls  = loadOrFind(env, mcLoader, "io.netty.channel.socket.nio.NioServerSocketChannel",
                                "Lio/netty/channel/socket/nio/NioServerSocketChannel;");
    jclass isaCls  = env->FindClass("java/net/InetSocketAddress");
    if (!elgCls || !sbCls || !sscCls || !isaCls) return false;

    jobject elg = env->NewObject(elgCls, env->GetMethodID(elgCls, "<init>", "()V"));
    if (env->ExceptionCheck()) { LogAndClearException(env, "NioELG"); return false; }

    jobject sb  = env->NewObject(sbCls, env->GetMethodID(sbCls, "<init>", "()V"));
    env->CallObjectMethod(sb,
        env->GetMethodID(sbCls, "group",
                         "(Lio/netty/channel/EventLoopGroup;)Lio/netty/bootstrap/ServerBootstrap;"),
        elg);
    env->CallObjectMethod(sb,
        env->GetMethodID(sbCls, "channel",
                         "(Ljava/lang/Class;)Lio/netty/bootstrap/AbstractBootstrap;"),
        sscCls);
    jobject init = env->NewObject(g_bs.initClass, g_bs.initCtor);
    env->CallObjectMethod(sb,
        env->GetMethodID(sbCls, "childHandler",
                         "(Lio/netty/channel/ChannelHandler;)Lio/netty/bootstrap/ServerBootstrap;"),
        init);
    env->DeleteLocalRef(init);

    // Loopback only, and on a different port from the one players connect to:
    // reflective_injector.exe owns 0.0.0.0:25565 from before the game starts and
    // TCP-bridges every client it accepts to us here.  That is what lets B
    // connect while A is still booting, instead of racing A's configuration
    // phase.  Running without the injector (starain_inject.dll, inject.ps1)
    // means pointing B straight at 25566.
    jstring host = env->NewStringUTF("127.0.0.1");
    jobject isa  = env->NewObject(isaCls,
        env->GetMethodID(isaCls, "<init>", "(Ljava/lang/String;I)V"),
        host, (jint)25566);
    env->DeleteLocalRef(host);

    jobject future = env->CallObjectMethod(sb,
        env->GetMethodID(sbCls, "bind",
                         "(Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;"),
        isa);
    env->DeleteLocalRef(isa);
    if (env->ExceptionCheck()) { LogAndClearException(env, "bind"); return false; }
    if (future) {
        jclass fCls = env->GetObjectClass(future);
        env->CallObjectMethod(future, env->GetMethodID(fCls, "sync", "()Lio/netty/channel/ChannelFuture;"));
        if (env->ExceptionCheck()) { LogAndClearException(env, "bind-sync"); return false; }
        env->DeleteLocalRef(fCls);
        env->DeleteLocalRef(future);
    }
    env->DeleteLocalRef(sb);
    env->DeleteLocalRef(elg);
    LogTo("BServer: bound 127.0.0.1:25566 (loopback; injector bridges 25565 -> here)");
    return true;
}

}

bool InstallBServer(JNIEnv* env) {
    if (g_bs.bServerBound) return true;
    jobject mcLoader = GetMinecraftClassLoader(env, g_jvmti);
    if (!mcLoader) return false;
    // defineGameContextClass must precede cacheJavaRefs: binding the PLAY
    // ProtocolInfo needs an instance of it.
    bool ok = defineInitClass(env, mcLoader) && defineHandlerClass(env, mcLoader)
           && defineGameContextClass(env, mcLoader)
           && defineLoopTaskClass(env, mcLoader)
           && cacheJavaRefs(env, mcLoader) && bindServer(env, mcLoader);
    env->DeleteGlobalRef(mcLoader);
    if (ok) {
        g_bs.bServerBound = true;
        HANDLE h = CreateThread(nullptr, 0, BKeepAliveThread, nullptr, 0, nullptr);
        if (h) CloseHandle(h);
    }
    return ok;
}

bool BServer_IsBActive() {
    return g_bs.bState.load(std::memory_order_acquire) == BState::Play;
}

bool BServer_ShouldMirror() {
    BState s = g_bs.bState.load(std::memory_order_acquire);
    return s == BState::Play || s == BState::AwaitConfiguration;
}

bool BServer_IsLoginIntention(JNIEnv* env, jobject packet) {
    if (!env || !packet) return false;
    if (!g_bs.intentPacketCls || !env->IsInstanceOf(packet, g_bs.intentPacketCls))
        return false;
    if (!g_bs.intentionPacketIntentFid || !g_bs.intentLogin) return false;
    jobject intent = env->GetObjectField(packet, g_bs.intentionPacketIntentFid);
    if (!intent) return false;
    bool isLogin = env->IsSameObject(intent, g_bs.intentLogin);
    env->DeleteLocalRef(intent);
    return isLogin;
}

bool BServer_WaitForBConnected(int timeoutMs) {
    std::unique_lock<std::mutex> lock(g_bConnMu);
    if (g_bConnected) return true;
    if (timeoutMs < 0) {
        g_bConnCv.wait(lock, [] { return g_bConnected; });
        return true;
    }
    g_bConnCv.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                       [] { return g_bConnected; });
    return g_bConnected;
}

bool BServer_BlockAMainThreadUntilBConnected(JNIEnv* env) {
    if (!env || !g_jvmti) return false;
    if (BServer_WaitForBConnected(0)) return true;

    jobject mcLoader = GetMinecraftClassLoader(env, g_jvmti);
    if (!mcLoader) {
        LogTo("[MAIN-GATE] Minecraft ClassLoader unavailable");
        return false;
    }
    if (!defineMainGateClass(env, mcLoader)) {
        env->DeleteGlobalRef(mcLoader);
        return false;
    }

    jclass minecraftCls = LoadClassInLoader(
        env, mcLoader, "net.minecraft.client.Minecraft");
    env->DeleteGlobalRef(mcLoader);
    if (!minecraftCls) {
        LogTo("[MAIN-GATE] Minecraft class unavailable");
        return false;
    }

    jmethodID getInstanceMid = findMethodByDesc(
        minecraftCls, "()Lnet/minecraft/client/Minecraft;", true);
    jmethodID executeMid = env->GetMethodID(
        minecraftCls, "execute", "(Ljava/lang/Runnable;)V");
    if (env->ExceptionCheck()) env->ExceptionClear();
    if (!getInstanceMid || !executeMid) {
        LogTo("[MAIN-GATE] Minecraft getInstance/execute lookup failed");
        env->DeleteLocalRef(minecraftCls);
        return false;
    }

    jobject minecraft = env->CallStaticObjectMethod(minecraftCls, getInstanceMid);
    jobject gate = env->NewObject(g_bs.mainGateClass, g_bs.mainGateCtor);
    if (!minecraft || !gate || env->ExceptionCheck()) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        LogTo("[MAIN-GATE] failed to create Minecraft gate task");
        if (gate) env->DeleteLocalRef(gate);
        if (minecraft) env->DeleteLocalRef(minecraft);
        env->DeleteLocalRef(minecraftCls);
        return false;
    }

    env->CallVoidMethod(minecraft, executeMid, gate);
    bool ok = !env->ExceptionCheck();
    if (!ok) env->ExceptionClear();
    LogTo("[MAIN-GATE] task %s on A Render thread", ok ? "queued" : "queue failed");

    env->DeleteLocalRef(gate);
    env->DeleteLocalRef(minecraft);
    env->DeleteLocalRef(minecraftCls);
    return ok;
}

void BServer_SetTargetConnection(JNIEnv* env, jobject connection) {
    std::lock_guard<std::mutex> l(g_bs.targetMu);
    if (g_bs.targetAConnection) env->DeleteGlobalRef(g_bs.targetAConnection);
    g_bs.targetAConnection = connection ? env->NewGlobalRef(connection) : nullptr;
    LogTo("BServer: target A connection = %p", (void*)g_bs.targetAConnection);
}

bool BServer_TryCaptureLiveConnection(JNIEnv* env) {
    if (!env) return false;
    if (!g_bs.minecraftCls || !g_bs.mcGetInstanceMid || !g_bs.mcGetConnectionMid ||
        !g_bs.cplGetConnectionMid || !g_bs.connectionChannelFid ||
        !g_bs.channelPipelineMid) {
        LogTo("mid-session: refs missing, cannot capture live connection");
        return false;
    }
    jobject mc = env->CallStaticObjectMethod(g_bs.minecraftCls, g_bs.mcGetInstanceMid);
    if (!mc || env->ExceptionCheck()) { env->ExceptionClear(); return false; }
    jobject cpl = env->CallObjectMethod(mc, g_bs.mcGetConnectionMid);
    env->DeleteLocalRef(mc);
    if (!cpl || env->ExceptionCheck()) { env->ExceptionClear(); return false; }
    jobject conn = env->CallObjectMethod(cpl, g_bs.cplGetConnectionMid);
    env->DeleteLocalRef(cpl);
    if (!conn || env->ExceptionCheck()) { env->ExceptionClear(); return false; }

    BServer_SetTargetConnection(env, conn);

    jobject channel = env->GetObjectField(conn, g_bs.connectionChannelFid);
    env->DeleteLocalRef(conn);
    if (!channel || env->ExceptionCheck()) { env->ExceptionClear(); LogTo("mid-session: connection.channel null"); return false; }
    jobject pipeline = env->CallObjectMethod(channel, g_bs.channelPipelineMid);
    env->DeleteLocalRef(channel);
    if (!pipeline || env->ExceptionCheck()) { env->ExceptionClear(); LogTo("mid-session: channel.pipeline() null"); return false; }
    RelayHandler_AttachToPipelineObject(env, pipeline);
    env->DeleteLocalRef(pipeline);

    g_bs.midSession.store(true, std::memory_order_release);
    LogTo("mid-session: captured A's live Connection + attached relay (A already in-game)");
    return true;
}

namespace {

// Write to one specific B connection.  Anything triggered by an inbound packet
// must use this with that packet's own channel -- see channelOfCtx().
void writeToChan(JNIEnv* env, jobject ch, jobject packet) {
    if (!ch || !packet || !g_bs.channelWriteAndFlushMid) return;
    env->CallObjectMethod(ch, g_bs.channelWriteAndFlushMid, packet);
    if (env->ExceptionCheck()) LogAndClearException(env, "writeAndFlush");
}

// The current B session, for callers with no packet to key off: the config-phase
// keep-alive watchdog, and A's relay deciding where to forward.
void writeToB(JNIEnv* env, jobject packet) {
    jobject ch;
    { std::lock_guard<std::mutex> l(g_bs.bMu); ch = g_bs.bChannel ? env->NewLocalRef(g_bs.bChannel) : nullptr; }
    if (!ch) return;
    writeToChan(env, ch, packet);
    env->DeleteLocalRef(ch);
}

void routeToA(JNIEnv* env, jobject packet) {
    jobject target;
    {
        std::lock_guard<std::mutex> l(g_bs.targetMu);
        target = g_bs.targetAConnection
            ? env->NewLocalRef(g_bs.targetAConnection)
            : nullptr;
    }
    if (!target) return;
    if (!g_bs.connectionSendMid) {
        env->DeleteLocalRef(target);
        return;
    }

    RelayFilter_MarkBypass(env, packet);
    env->CallVoidMethod(target, g_bs.connectionSendMid, packet);
    if (env->ExceptionCheck()) LogAndClearException(env, "routeToA");
    env->DeleteLocalRef(target);
}

// B drives, so B's *intent* is injected into A's connection and reaches the real
// server.  Only intent, though -- this used to forward everything except
// CustomPayload, which pushed B's connection upkeep into A's session and broke
// it in three separate ways:
//   ClientTickEnd          encoded on A's connection while it was still
//                          CONFIGURATION -> "Sending unknown packet
//                          'serverbound/minecraft:client_tick_end'"
//   AcceptTeleportation    carries B's teleport ids, meaningless on A's session
//   MovePlayer             sent while B was still on "Loading terrain", so the
//                          position disagreed with the server's -> A kicked for
//                          illegal player movement
// The predicate is shared with A's suppress list so the two can never drift.
bool shouldRouteBToA(const std::string& fqcn) {
    return IsPlayerIntentPacket(fqcn);
}

bool uuidToBytes(JNIEnv* env, jobject uuid, unsigned char out[16]) {
    if (!uuid || !g_bs.uuidGetMsbMid || !g_bs.uuidGetLsbMid) return false;
    jlong msb = env->CallLongMethod(uuid, g_bs.uuidGetMsbMid);
    if (env->ExceptionCheck()) { env->ExceptionClear(); return false; }
    jlong lsb = env->CallLongMethod(uuid, g_bs.uuidGetLsbMid);
    if (env->ExceptionCheck()) { env->ExceptionClear(); return false; }
    for (int i = 0; i < 8; ++i) out[i]     = (unsigned char)((msb >> ((7 - i) * 8)) & 0xFF);
    for (int i = 0; i < 8; ++i) out[8 + i] = (unsigned char)((lsb >> ((7 - i) * 8)) & 0xFF);
    return true;
}

void ensureARealUuid(JNIEnv* env) {
    if (g_bs.aUuidReady && g_bs.aName) return;
    if (!g_bs.minecraftCls || !g_bs.mcGetInstanceMid || !g_bs.mcGetUserMid) return;
    jobject mc = env->CallStaticObjectMethod(g_bs.minecraftCls, g_bs.mcGetInstanceMid);
    if (!mc || env->ExceptionCheck()) { env->ExceptionClear(); return; }
    jobject user = env->CallObjectMethod(mc, g_bs.mcGetUserMid);
    env->DeleteLocalRef(mc);
    if (!user || env->ExceptionCheck()) { env->ExceptionClear(); return; }

    if (!g_bs.aUuidReady && g_bs.userGetProfileIdMid) {
        jobject uuid = env->CallObjectMethod(user, g_bs.userGetProfileIdMid);
        if (uuid && !env->ExceptionCheck()) {
            unsigned char bytes[16];
            if (uuidToBytes(env, uuid, bytes)) {
                g_bs.aRealUuid = env->NewGlobalRef(uuid);
                std::memcpy(g_bs.aUuidBytes, bytes, 16);
                g_bs.aUuidReady = true;
                LogTo("mirror: cached A's real UUID for tab-list mirroring");
            }
            env->DeleteLocalRef(uuid);
        } else if (env->ExceptionCheck()) env->ExceptionClear();
    }
    if (!g_bs.aName && g_bs.userGetGameProfileMid && g_bs.gameProfileGetNameMid) {
        jobject profile = env->CallObjectMethod(user, g_bs.userGetGameProfileMid);
        if (profile && !env->ExceptionCheck()) {
            jstring nm = (jstring)env->CallObjectMethod(profile, g_bs.gameProfileGetNameMid);
            if (nm && !env->ExceptionCheck()) {
                g_bs.aName = (jstring)env->NewGlobalRef(nm);
                const char* utf = env->GetStringUTFChars(nm, nullptr);
                LogTo("team-mirror: cached A's name = '%s'", utf ? utf : "?");
                if (utf) env->ReleaseStringUTFChars(nm, utf);
                env->DeleteLocalRef(nm);
            } else if (env->ExceptionCheck()) env->ExceptionClear();
            env->DeleteLocalRef(profile);
        } else if (env->ExceptionCheck()) env->ExceptionClear();
    }
    env->DeleteLocalRef(user);
}

void sendSelfInfoToB(JNIEnv* env, jobject offlineUuid, jstring name) {
    if (!g_bs.minecraftCls || !g_bs.mcGetInstanceMid || !g_bs.mcGetProfilePropsMid ||
        !g_bs.registryBufCls || !g_bs.registryBufCtor || !g_bs.fbbWriteByteMid ||
        !g_bs.fbbWriteBooleanMid || !g_bs.fbbWriteVarIntMid ||
        !g_bs.fbbWriteUUIDMid || !g_bs.fbbWriteUtfMid || !g_bs.fbbWriteGpPropsMid ||
        !g_bs.unpooledCls || !g_bs.unpooledBufferMid ||
        !g_bs.playerInfoUpdatePacketCls || !g_bs.playerInfoUpdatePacketBufCtor) {
        LogTo("self-info: missing refs, skipping push");
        return;
    }
    jobject mc = env->CallStaticObjectMethod(g_bs.minecraftCls, g_bs.mcGetInstanceMid);
    if (!mc || env->ExceptionCheck()) { env->ExceptionClear(); LogTo("self-info: no Minecraft"); return; }
    jobject props = env->CallObjectMethod(mc, g_bs.mcGetProfilePropsMid);
    env->DeleteLocalRef(mc);
    if (!props || env->ExceptionCheck()) { env->ExceptionClear(); LogTo("self-info: no profile props"); return; }

    jobject buf = newWriteBuf(env);
    if (!buf) { env->DeleteLocalRef(props); return; }

    env->CallObjectMethod(buf, g_bs.fbbWriteByteMid, (jint)0x1D);
    env->CallObjectMethod(buf, g_bs.fbbWriteByteMid, (jint)0x01);
    env->CallObjectMethod(buf, g_bs.fbbWriteUUIDMid, offlineUuid);

    env->CallObjectMethod(buf, g_bs.fbbWriteUtfMid, name, (jint)16);
    env->CallVoidMethod  (buf, g_bs.fbbWriteGpPropsMid, props);

    env->CallObjectMethod(buf, g_bs.fbbWriteVarIntMid, (jint)0);

    env->CallObjectMethod(buf, g_bs.fbbWriteBooleanMid, (jboolean)JNI_TRUE);

    env->CallObjectMethod(buf, g_bs.fbbWriteVarIntMid, (jint)0);
    env->DeleteLocalRef(props);
    if (env->ExceptionCheck()) { LogAndClearException(env, "self-info: buffer build"); env->DeleteLocalRef(buf); return; }

    jobject pkt = env->NewObject(g_bs.playerInfoUpdatePacketCls,
                                 g_bs.playerInfoUpdatePacketBufCtor, buf);
    env->DeleteLocalRef(buf);
    if (!pkt || env->ExceptionCheck()) { LogAndClearException(env, "self-info: packet ctor"); return; }
    writeToB(env, pkt);
    env->DeleteLocalRef(pkt);
    LogTo("self-info: pushed B's own player-info to B (ADD_PLAYER+LISTED+GAME_MODE+LATENCY, A's skin)");
}

bool reconstructAndSendLoginToB(JNIEnv* env, jobject ch) {
    if (!g_bs.loginPacketCtor || !g_bs.minecraftCls || !g_bs.mcGetInstanceMid ||
        !g_bs.mcGetConnectionMid || !g_bs.cplLevelsMid || !g_bs.cplRegistryAccessMid ||
        !g_bs.registryAccessFreezeMid || !g_bs.mcLevelFid || !g_bs.mcGameModeFid ||
        !g_bs.gameModeGetTypeMid || !g_bs.levelDimMidA || !g_bs.objToStringMid ||
        !g_bs.optionalCls || !g_bs.optionalEmptyMid) {
        LogTo("mid-login: refs missing, cannot rebuild login");
        return false;
    }
    jobject mc = env->CallStaticObjectMethod(g_bs.minecraftCls, g_bs.mcGetInstanceMid);
    if (!mc || env->ExceptionCheck()) { env->ExceptionClear(); return false; }
    jobject cpl = env->CallObjectMethod(mc, g_bs.mcGetConnectionMid);
    if (!cpl || env->ExceptionCheck()) { env->ExceptionClear(); env->DeleteLocalRef(mc); return false; }

    jobject levels = env->CallObjectMethod(cpl, g_bs.cplLevelsMid);
    jobject ra     = (levels && !env->ExceptionCheck()) ? env->CallObjectMethod(cpl, g_bs.cplRegistryAccessMid) : nullptr;
    env->DeleteLocalRef(cpl);
    jobject frozen = (ra && !env->ExceptionCheck()) ? env->CallObjectMethod(ra, g_bs.registryAccessFreezeMid) : nullptr;
    if (ra) env->DeleteLocalRef(ra);
    if (!levels || !frozen || env->ExceptionCheck()) {
        env->ExceptionClear();
        LogTo("mid-login: levels/registry unavailable");
        if (levels) env->DeleteLocalRef(levels);
        if (frozen) env->DeleteLocalRef(frozen);
        env->DeleteLocalRef(mc);
        return false;
    }

    jobject gameMode = env->GetObjectField(mc, g_bs.mcGameModeFid);
    jobject gameType = (gameMode && !env->ExceptionCheck()) ?
        env->CallObjectMethod(gameMode, g_bs.gameModeGetTypeMid) : nullptr;
    if (gameMode) env->DeleteLocalRef(gameMode);
    if (env->ExceptionCheck()) env->ExceptionClear();

    jobject level = env->GetObjectField(mc, g_bs.mcLevelFid);
    env->DeleteLocalRef(mc);
    jobject dimKey = nullptr, dimTypeKey = nullptr;
    if (level && !env->ExceptionCheck()) {
        jmethodID mids[2] = { g_bs.levelDimMidA, g_bs.levelDimMidB };
        for (int i = 0; i < 2 && mids[i]; ++i) {
            jobject rk = env->CallObjectMethod(level, mids[i]);
            if (!rk || env->ExceptionCheck()) { env->ExceptionClear(); if (rk) env->DeleteLocalRef(rk); continue; }
            jstring s = (jstring)env->CallObjectMethod(rk, g_bs.objToStringMid);
            bool isDimType = false;
            if (s && !env->ExceptionCheck()) {
                const char* c = env->GetStringUTFChars(s, nullptr);
                if (c) { isDimType = std::strstr(c, "dimension_type") != nullptr; env->ReleaseStringUTFChars(s, c); }
                env->DeleteLocalRef(s);
            } else if (env->ExceptionCheck()) env->ExceptionClear();
            if (isDimType) { if (dimTypeKey) env->DeleteLocalRef(dimTypeKey); dimTypeKey = rk; }
            else           { if (dimKey)     env->DeleteLocalRef(dimKey);     dimKey = rk; }
        }
    }
    if (level) env->DeleteLocalRef(level);

    if (!gameType || !dimKey || !dimTypeKey) {
        LogTo("mid-login: incomplete (gameType=%p dim=%p dimType=%p), abort",
              (void*)gameType, (void*)dimKey, (void*)dimTypeKey);
        if (levels) env->DeleteLocalRef(levels);
        if (frozen) env->DeleteLocalRef(frozen);
        if (gameType) env->DeleteLocalRef(gameType);
        if (dimKey) env->DeleteLocalRef(dimKey);
        if (dimTypeKey) env->DeleteLocalRef(dimTypeKey);
        return false;
    }

    jobject empty = env->CallStaticObjectMethod(g_bs.optionalCls, g_bs.optionalEmptyMid);
    if (!empty || env->ExceptionCheck()) {

        env->ExceptionClear();
        LogTo("mid-login: Optional.empty() failed, abort");
        if (empty) env->DeleteLocalRef(empty);
        env->DeleteLocalRef(levels); env->DeleteLocalRef(frozen);
        env->DeleteLocalRef(gameType); env->DeleteLocalRef(dimKey); env->DeleteLocalRef(dimTypeKey);
        return false;
    }

    const jint     playerId    = 2000000000;
    const jboolean hardcore    = JNI_FALSE;
    const jlong    seed        = 0;
    const jint     maxPlayers  = 20;
    const jint     chunkRadius = 16;
    const jint     simDist     = 16;
    jobject login = env->NewObject(g_bs.loginPacketCls, g_bs.loginPacketCtor,
        playerId, hardcore, gameType, (jobject)nullptr, levels, frozen,
        dimTypeKey, dimKey, seed, maxPlayers, chunkRadius, simDist,
        (jboolean)JNI_FALSE, (jboolean)JNI_TRUE, (jboolean)JNI_FALSE, (jboolean)JNI_FALSE,
        empty, (jint)0);

    env->DeleteLocalRef(levels);
    env->DeleteLocalRef(frozen);
    env->DeleteLocalRef(gameType);
    env->DeleteLocalRef(dimKey);
    env->DeleteLocalRef(dimTypeKey);
    if (empty) env->DeleteLocalRef(empty);

    if (!login || env->ExceptionCheck()) { LogAndClearException(env, "mid-login: ctor"); return false; }
    env->CallObjectMethod(ch, g_bs.channelWriteAndFlushMid, login);
    bool sent = !env->ExceptionCheck();
    if (!sent) LogAndClearException(env, "mid-login: writeAndFlush");
    env->DeleteLocalRef(login);
    if (!sent) return false;
    LogTo("mid-login: rebuilt ClientboundLoginPacket sent to B");

    // Disabled with the rest of the identity-rewriting path.  The argument list
    // below is still 1.20.1's (x, y, z, yRot, xRot, relatives, id); 1.21.8 wants
    // (id, PositionMoveRotation, relatives), so this needs rewriting before
    // kGiveBOwnIdentity can be turned on.
    if (kGiveBOwnIdentity && g_bs.playerPositionPacketCtor && g_bs.setOfMid && g_bs.setCls) {
        jobject relSet = env->CallStaticObjectMethod(g_bs.setCls, g_bs.setOfMid);
        if (relSet && !env->ExceptionCheck()) {
            jobject pos = env->NewObject(g_bs.playerPositionPacketCls, g_bs.playerPositionPacketCtor,
                (jdouble)0.0, (jdouble)100.0, (jdouble)0.0, (jfloat)0.0f, (jfloat)0.0f, relSet, (jint)0);
            if (pos && !env->ExceptionCheck()) {
                env->CallObjectMethod(ch, g_bs.channelWriteAndFlushMid, pos);
                if (env->ExceptionCheck()) env->ExceptionClear();
                env->DeleteLocalRef(pos);
                LogTo("mid-login: sent placeholder spawn position to B");
            } else if (env->ExceptionCheck()) env->ExceptionClear();
            env->DeleteLocalRef(relSet);
        } else if (env->ExceptionCheck()) env->ExceptionClear();
    }
    return true;
}

void completeLogin(JNIEnv* env, jobject selfCh, jobject hello) {

    if (!g_bs.helloPacketNameFid) { LogTo("login: no hello.name field"); return; }
    jstring jname = (jstring)env->GetObjectField(hello, g_bs.helloPacketNameFid);
    if (!jname) { LogTo("login: hello.name is null"); return; }
    const char* utfName = env->GetStringUTFChars(jname, nullptr);
    LogTo("login: B says name='%s'", utfName ? utfName : "?");

    std::string composite = std::string("OfflinePlayer:") + (utfName ? utfName : "");
    if (utfName) env->ReleaseStringUTFChars(jname, utfName);
    jbyteArray arr = env->NewByteArray((jsize)composite.size());
    env->SetByteArrayRegion(arr, 0, (jsize)composite.size(),
                            reinterpret_cast<const jbyte*>(composite.data()));
    jobject uuid = env->CallStaticObjectMethod(
        g_bs.uuidCls, g_bs.uuidNameUuidFromBytesMid, arr);
    env->DeleteLocalRef(arr);
    if (!uuid || env->ExceptionCheck()) {
        LogAndClearException(env, "login: nameUUIDFromBytes"); env->DeleteLocalRef(jname); return;
    }
    LogTo("login: computed offline UUID");

    if (!g_bs.gameProfileCtor) { LogTo("login: no GameProfile ctor"); env->DeleteLocalRef(uuid); env->DeleteLocalRef(jname); return; }
    jobject profile = env->NewObject(g_bs.gameProfileCls, g_bs.gameProfileCtor, uuid, jname);
    if (!profile || env->ExceptionCheck()) {
        LogAndClearException(env, "login: GameProfile ctor"); env->DeleteLocalRef(uuid); env->DeleteLocalRef(jname); return;
    }

    if (!g_bs.loginFinishedPacketCtor) { LogTo("login: no LoginFinished ctor"); return; }
    jobject lfp = env->NewObject(g_bs.loginFinishedPacketCls, g_bs.loginFinishedPacketCtor, profile);
    env->DeleteLocalRef(profile);
    if (!lfp || env->ExceptionCheck()) {
        LogAndClearException(env, "login: LoginFinished ctor"); return;
    }

    writeToChan(env, selfCh, lfp);
    env->DeleteLocalRef(lfp);
    LogTo("login: sent ClientboundLoginFinishedPacket to B");

    // 1.20.2 inserted a whole CONFIGURATION phase between LOGIN and PLAY: the
    // server must send the registry contents, enabled features, tags and known
    // packs, and only ClientboundFinishConfigurationPacket moves the client on.
    // Rather than synthesise a registry from scratch, we hold B here and let
    // A's own configuration stream through (see BServer_ForwardToB) -- B then
    // ends up with exactly A's registries, which is what makes the mirrored
    // PLAY stream decodable on B's side.
    jobject ch;
    { std::lock_guard<std::mutex> l(g_bs.bMu); ch = g_bs.bChannel ? env->NewLocalRef(g_bs.bChannel) : nullptr; }
    if (ch) {
        // Encoder only.  The decoder stays on LOGIN until B's
        // LoginAcknowledged arrives (see BSide_OnPacket).
        setProtocolDirections(env, ch, ProtoState::Configuration, true, false);
        LogTo("login: encoder switched to CONFIGURATION (waiting for B's ack and A's config stream)");
        if (kGiveBOwnIdentity && g_bs.midSession.load(std::memory_order_acquire)) {
            LogTo("login: mid-session — rebuilding login for B from A's state");
            reconstructAndSendLoginToB(env, ch);
        }
        env->DeleteLocalRef(ch);
    }

    if (kGiveBOwnIdentity) {
        // The common keep-alive can be sent in CONFIGURATION or PLAY; sending it
        // now keeps A's connection from being reaped while B waits.
        if (g_bs.keepAlivePacketCtor && g_bs.bChannel) {
            jobject ka = env->NewObject(g_bs.keepAlivePacketCls,
                                        g_bs.keepAlivePacketCtor, (jlong)1);
            if (ka) { writeToChan(env, selfCh, ka); env->DeleteLocalRef(ka); }
        }
        sendSelfInfoToB(env, uuid, jname);
    }

    unsigned char bBytes[16];
    if (uuidToBytes(env, uuid, bBytes)) {
        std::memcpy(g_bs.bUuidBytes, bBytes, 16);
        g_bs.bUuidReady = true;
    }

    if (g_bs.bUuidObj) env->DeleteGlobalRef(g_bs.bUuidObj);
    g_bs.bUuidObj = env->NewGlobalRef(uuid);

    if (g_bs.bName) env->DeleteGlobalRef(g_bs.bName);
    g_bs.bName = (jstring)env->NewGlobalRef(jname);
    ensureARealUuid(env);

    env->DeleteLocalRef(uuid);
    env->DeleteLocalRef(jname);

    // Deliberately NOT switching to Play / releasing the gate here: B has to
    // finish its configuration phase first, and the packet that ends it comes
    // from A's real server.  See BServer_ForwardToB.
    g_bs.bState.store(BState::AwaitConfiguration, std::memory_order_release);
    LogTo("login: B authenticated, now in CONFIGURATION — A must connect and "
          "its configuration stream will release B into PLAY");
}

}

void hideAFromBTab(JNIEnv* env, jobject ch) {
    if (!g_bs.aRealUuid || !g_bs.registryBufCls || !g_bs.registryBufCtor ||
        !g_bs.unpooledCls || !g_bs.unpooledBufferMid || !g_bs.fbbWriteByteMid ||
        !g_bs.fbbWriteVarIntMid || !g_bs.fbbWriteUUIDMid || !g_bs.fbbWriteBooleanMid ||
        !g_bs.playerInfoUpdatePacketCls || !g_bs.playerInfoUpdatePacketBufCtor) return;

    jobject buf = newWriteBuf(env);
    if (!buf) return;

    env->CallObjectMethod(buf, g_bs.fbbWriteByteMid, (jint)0x08);
    env->CallObjectMethod(buf, g_bs.fbbWriteVarIntMid, (jint)1);
    env->CallObjectMethod(buf, g_bs.fbbWriteUUIDMid, g_bs.aRealUuid);
    env->CallObjectMethod(buf, g_bs.fbbWriteBooleanMid, (jboolean)JNI_FALSE);
    if (env->ExceptionCheck()) { env->ExceptionClear(); env->DeleteLocalRef(buf); return; }

    jobject pkt = env->NewObject(g_bs.playerInfoUpdatePacketCls,
                                 g_bs.playerInfoUpdatePacketBufCtor, buf);
    env->DeleteLocalRef(buf);
    if (!pkt || env->ExceptionCheck()) { LogAndClearException(env, "hide-A: ctor"); return; }
    env->CallObjectMethod(ch, g_bs.channelWriteAndFlushMid, pkt);
    if (env->ExceptionCheck()) LogAndClearException(env, "hide-A: writeAndFlush");
    env->DeleteLocalRef(pkt);
}

void mirrorPlayerInfoUpdateToB(JNIEnv* env, jobject ch, jobject packet) {
    if (!g_bs.playerInfoUpdatePacketCls || !g_bs.playerInfoUpdatePacketBufCtor ||
        !g_bs.playerInfoUpdatePacketWriteMid || !g_bs.friendlyBufCls ||
        !g_bs.registryBufCtor || !g_bs.unpooledCls || !g_bs.unpooledBufferMid ||
        !g_bs.byteBufGetByteMid || !g_bs.listSizeMid || !g_bs.listGetMid ||
        !g_bs.piuEntriesMidA || !g_bs.piEntryProfileIdMid ||
        !g_bs.fbbWriteByteMid || !g_bs.fbbWriteVarIntMid || !g_bs.fbbWriteUUIDMid ||
        !g_bs.fbbWriteBooleanMid) return;
    if (!g_bs.aUuidReady || !g_bs.bUuidReady || !g_bs.bUuidObj) return;
    if (!env->IsInstanceOf(packet, g_bs.playerInfoUpdatePacketCls)) return;

    jobject sbuf = newWriteBuf(env);
    if (!sbuf) return;
    env->CallVoidMethod(packet, g_bs.playerInfoUpdatePacketWriteMid, sbuf);
    if (env->ExceptionCheck()) { env->ExceptionClear(); env->DeleteLocalRef(sbuf); return; }
    jint bits = env->CallByteMethod(sbuf, g_bs.byteBufGetByteMid, (jint)0) & 0xFF;
    env->DeleteLocalRef(sbuf);
    if (env->ExceptionCheck()) { env->ExceptionClear(); return; }

    bool hasGameMode = bits & 0x04;
    bool hasListed   = bits & 0x08;
    bool hasLatency  = bits & 0x10;
    bool hasDisplay  = bits & 0x20;
    int outBits = (hasGameMode ? 0x04 : 0) | (hasListed ? 0x08 : 0) |
                  (hasLatency ? 0x10 : 0) | (hasDisplay ? 0x20 : 0);

    jobject listA = env->CallObjectMethod(packet, g_bs.piuEntriesMidA);
    if (env->ExceptionCheck()) { env->ExceptionClear(); listA = nullptr; }
    jobject listB = g_bs.piuEntriesMidB ? env->CallObjectMethod(packet, g_bs.piuEntriesMidB) : nullptr;
    if (env->ExceptionCheck()) { env->ExceptionClear(); listB = nullptr; }
    jint sizeA = -1, sizeB = -1;
    if (listA) { sizeA = env->CallIntMethod(listA, g_bs.listSizeMid);
                 if (env->ExceptionCheck()) { env->ExceptionClear(); sizeA = -1; } }
    if (listB) { sizeB = env->CallIntMethod(listB, g_bs.listSizeMid);
                 if (env->ExceptionCheck()) { env->ExceptionClear(); sizeB = -1; } }
    jobject entries = (sizeB > sizeA) ? listB : listA;
    jint    nEntries = (sizeB > sizeA) ? sizeB : sizeA;

    jobject aEntry = nullptr;
    for (jint i = 0; i < nEntries && !aEntry; ++i) {
        jobject e = env->CallObjectMethod(entries, g_bs.listGetMid, i);
        if (!e || env->ExceptionCheck()) { env->ExceptionClear(); if (e) env->DeleteLocalRef(e); continue; }
        jobject euuid = env->CallObjectMethod(e, g_bs.piEntryProfileIdMid);
        if (euuid && !env->ExceptionCheck()) {
            unsigned char eb[16];
            if (uuidToBytes(env, euuid, eb) && std::memcmp(eb, g_bs.aUuidBytes, 16) == 0)
                aEntry = env->NewLocalRef(e);
            env->DeleteLocalRef(euuid);
        } else if (env->ExceptionCheck()) env->ExceptionClear();
        env->DeleteLocalRef(e);
    }
    if (listA) env->DeleteLocalRef(listA);
    if (listB) env->DeleteLocalRef(listB);
    if (!aEntry) return;

    hideAFromBTab(env, ch);

    if (outBits == 0) { env->DeleteLocalRef(aEntry); return; }

    jobject buf = newWriteBuf(env);
    if (!buf) { env->DeleteLocalRef(aEntry); return; }

    env->CallObjectMethod(buf, g_bs.fbbWriteByteMid, (jint)outBits);
    env->CallObjectMethod(buf, g_bs.fbbWriteVarIntMid, (jint)1);
    env->CallObjectMethod(buf, g_bs.fbbWriteUUIDMid, g_bs.bUuidObj);

    if (hasGameMode && g_bs.piEntryGameModeMid && g_bs.gameTypeGetIdMid) {
        jobject gm = env->CallObjectMethod(aEntry, g_bs.piEntryGameModeMid);
        jint id = 0;
        if (gm && !env->ExceptionCheck()) id = env->CallIntMethod(gm, g_bs.gameTypeGetIdMid);
        if (gm) env->DeleteLocalRef(gm);
        if (env->ExceptionCheck()) env->ExceptionClear();
        env->CallObjectMethod(buf, g_bs.fbbWriteVarIntMid, id);
    }
    if (hasListed && g_bs.piEntryListedMid) {
        jboolean listed = env->CallBooleanMethod(aEntry, g_bs.piEntryListedMid);
        if (env->ExceptionCheck()) { env->ExceptionClear(); listed = JNI_TRUE; }
        env->CallObjectMethod(buf, g_bs.fbbWriteBooleanMid, listed);
    }
    if (hasLatency && g_bs.piEntryLatencyMid) {
        jint lat = env->CallIntMethod(aEntry, g_bs.piEntryLatencyMid);
        if (env->ExceptionCheck()) { env->ExceptionClear(); lat = 0; }
        env->CallObjectMethod(buf, g_bs.fbbWriteVarIntMid, lat);
    }
    if (hasDisplay) {
        jobject dn = g_bs.piEntryDisplayNameMid ?
            env->CallObjectMethod(aEntry, g_bs.piEntryDisplayNameMid) : nullptr;
        if (env->ExceptionCheck()) { env->ExceptionClear(); dn = nullptr; }

        if (dn && g_bs.fbbWriteComponentMid) {
            env->CallObjectMethod(buf, g_bs.fbbWriteBooleanMid, (jboolean)JNI_TRUE);
            env->CallObjectMethod(buf, g_bs.fbbWriteComponentMid, dn);
        } else {
            env->CallObjectMethod(buf, g_bs.fbbWriteBooleanMid, (jboolean)JNI_FALSE);
        }
        if (dn) env->DeleteLocalRef(dn);
        if (env->ExceptionCheck()) env->ExceptionClear();
    }
    env->DeleteLocalRef(aEntry);

    jobject pkt = env->NewObject(g_bs.playerInfoUpdatePacketCls,
                                 g_bs.playerInfoUpdatePacketBufCtor, buf);
    env->DeleteLocalRef(buf);
    if (!pkt || env->ExceptionCheck()) { LogAndClearException(env, "mirror: single-entry ctor"); return; }
    env->CallObjectMethod(ch, g_bs.channelWriteAndFlushMid, pkt);
    if (env->ExceptionCheck()) LogAndClearException(env, "mirror: writeAndFlush");
    env->DeleteLocalRef(pkt);
    LogTo("mirror: single-entry PlayerInfoUpdate to B (bits=0x%02x)", outBits);
}

void synthesiseMinimalPlayerInfoForUuid(JNIEnv* env, jobject ch,
                                        const unsigned char uuidBytes[16]) {
    if (!g_bs.unpooledCls || !g_bs.unpooledWrappedMid || !g_bs.registryBufCls ||
        !g_bs.registryBufCtor || !g_bs.playerInfoUpdatePacketCls ||
        !g_bs.playerInfoUpdatePacketBufCtor) return;

    // ADD_PLAYER payload: name, then the profile's property map.  Unchanged
    // since 1.19; the extra 1.21.2 Entry fields (showHat, listOrder) travel in
    // their own action bits, so this layout is still valid.
    unsigned char wire[20];
    wire[0] = 0x01;
    wire[1] = 0x01;
    std::memcpy(wire + 2, uuidBytes, 16);
    wire[18] = 0x00;
    wire[19] = 0x00;

    jbyteArray raw = env->NewByteArray(20);
    if (!raw) return;
    env->SetByteArrayRegion(raw, 0, 20, reinterpret_cast<const jbyte*>(wire));

    jobject wrapped = env->CallStaticObjectMethod(g_bs.unpooledCls,
                                                  g_bs.unpooledWrappedMid, raw);
    env->DeleteLocalRef(raw);
    if (!wrapped || env->ExceptionCheck()) { env->ExceptionClear(); return; }
    jobject buf = wrapRegistryBuf(env, wrapped);
    env->DeleteLocalRef(wrapped);
    if (!buf) return;
    jobject pkt = env->NewObject(g_bs.playerInfoUpdatePacketCls,
                                 g_bs.playerInfoUpdatePacketBufCtor, buf);
    env->DeleteLocalRef(buf);
    if (!pkt || env->ExceptionCheck()) {
        LogAndClearException(env, "synth-tab: packet ctor"); return;
    }
    env->CallObjectMethod(ch, g_bs.channelWriteAndFlushMid, pkt);
    if (env->ExceptionCheck()) LogAndClearException(env, "synth-tab: writeAndFlush");
    env->DeleteLocalRef(pkt);
}

void ensureTabEntryBeforeAddPlayer(JNIEnv* env, jobject ch, jobject packet) {
    if (!g_bs.addEntityPacketCls || !g_bs.addEntityPacketUuidFid) return;
    if (!env->IsInstanceOf(packet, g_bs.addEntityPacketCls)) return;
    jobject uuid = env->GetObjectField(packet, g_bs.addEntityPacketUuidFid);
    if (!uuid) { if (env->ExceptionCheck()) env->ExceptionClear(); return; }
    unsigned char bytes[16];
    bool ok = uuidToBytes(env, uuid, bytes);
    env->DeleteLocalRef(uuid);
    if (!ok) return;
    synthesiseMinimalPlayerInfoForUuid(env, ch, bytes);
}

void mirrorTeamPacketToB(JNIEnv* env, jobject ch, jobject packet) {
    if (!g_bs.setPlayerTeamPacketCls || !g_bs.setPlayerTeamPacketBufCtor ||
        !g_bs.setPlayerTeamMethodFid || !g_bs.setPlayerTeamNameFid ||
        !g_bs.setPlayerTeamPlayersFid || !g_bs.collectionContainsMid ||
        !g_bs.registryBufCls || !g_bs.registryBufCtor ||
        !g_bs.fbbWriteByteMid || !g_bs.fbbWriteVarIntMid || !g_bs.fbbWriteUtfMid ||
        !g_bs.unpooledCls || !g_bs.unpooledBufferMid) return;
    if (!g_bs.aName || !g_bs.bName) return;
    if (!env->IsInstanceOf(packet, g_bs.setPlayerTeamPacketCls)) return;

    jint method = env->GetIntField(packet, g_bs.setPlayerTeamMethodFid);

    if (method != 0 && method != 3 && method != 4) return;

    jobject players = env->GetObjectField(packet, g_bs.setPlayerTeamPlayersFid);
    if (!players) return;
    jboolean hasA = env->CallBooleanMethod(players, g_bs.collectionContainsMid, g_bs.aName);
    env->DeleteLocalRef(players);
    if (env->ExceptionCheck() || !hasA) { env->ExceptionClear(); return; }

    jstring teamName = (jstring)env->GetObjectField(packet, g_bs.setPlayerTeamNameFid);
    if (!teamName) return;

    jint outMethod = (method == 4) ? 4 : 3;

    jobject buf = newWriteBuf(env);
    if (!buf) { env->DeleteLocalRef(teamName); return; }

    env->CallObjectMethod(buf, g_bs.fbbWriteUtfMid, teamName, (jint)32767);
    env->CallObjectMethod(buf, g_bs.fbbWriteByteMid, (jint)outMethod);
    env->CallObjectMethod(buf, g_bs.fbbWriteVarIntMid, (jint)1);
    env->CallObjectMethod(buf, g_bs.fbbWriteUtfMid, g_bs.bName, (jint)32767);
    env->DeleteLocalRef(teamName);
    if (env->ExceptionCheck()) { LogAndClearException(env, "team-mirror: buffer build"); env->DeleteLocalRef(buf); return; }

    jobject pkt = env->NewObject(g_bs.setPlayerTeamPacketCls,
                                 g_bs.setPlayerTeamPacketBufCtor, buf);
    env->DeleteLocalRef(buf);
    if (!pkt || env->ExceptionCheck()) { LogAndClearException(env, "team-mirror: packet ctor"); return; }
    env->CallObjectMethod(ch, g_bs.channelWriteAndFlushMid, pkt);
    if (env->ExceptionCheck()) LogAndClearException(env, "team-mirror: writeAndFlush");
    env->DeleteLocalRef(pkt);
    LogTo("team-mirror: synthesised %s for B (server method=%d)",
          outMethod == 3 ? "JOIN" : "LEAVE", method);
}

void writeOneToBWithTabGuard(JNIEnv* env, jobject ch, jobject packet) {

    if (g_bs.customPayloadPacketCls &&
        env->IsInstanceOf(packet, g_bs.customPayloadPacketCls)) {
        static std::atomic<int> dropped{0};
        int n = dropped.fetch_add(1, std::memory_order_relaxed) + 1;
        if (n == 1 || (n & 0x3FF) == 0)
            LogTo("ForwardToB: skipping ClientboundCustomPayloadPacket (mod channel, count=%d)", n);
        return;
    }
    if (kGiveBOwnIdentity) ensureTabEntryBeforeAddPlayer(env, ch, packet);
    env->CallObjectMethod(ch, g_bs.channelWriteAndFlushMid, packet);
    if (env->ExceptionCheck()) LogAndClearException(env, "ForwardToB/writeOne");
    if (kGiveBOwnIdentity) {
        mirrorPlayerInfoUpdateToB(env, ch, packet);
        mirrorTeamPacketToB(env, ch, packet);
    }
}

bool forwardBundleExpanded(JNIEnv* env, jobject ch, jobject packet) {
    if (!g_bs.bundlePacketCls || !g_bs.bundleSubPacketsMid ||
        !g_bs.iterableIteratorMid || !g_bs.iteratorHasNextMid ||
        !g_bs.iteratorNextMid)
        return false;
    if (!env->IsInstanceOf(packet, g_bs.bundlePacketCls)) return false;

    jobject iterable = env->CallObjectMethod(packet, g_bs.bundleSubPacketsMid);
    if (env->ExceptionCheck() || !iterable) {
        LogAndClearException(env, "bundle.subPackets");
        return true;
    }
    jobject it = env->CallObjectMethod(iterable, g_bs.iterableIteratorMid);
    env->DeleteLocalRef(iterable);
    if (env->ExceptionCheck() || !it) {
        LogAndClearException(env, "bundle.iterator");
        return true;
    }

    int forwarded = 0, nulls = 0;
    while (true) {
        jboolean more = env->CallBooleanMethod(it, g_bs.iteratorHasNextMid);
        if (env->ExceptionCheck()) {
            LogAndClearException(env, "bundle.iter.hasNext");
            break;
        }
        if (!more) break;

        jobject sub = env->CallObjectMethod(it, g_bs.iteratorNextMid);
        if (env->ExceptionCheck()) {
            LogAndClearException(env, "bundle.iter.next");
            break;
        }
        if (!sub) { ++nulls; continue; }

        if (!forwardBundleExpanded(env, ch, sub)) {
            writeOneToBWithTabGuard(env, ch, sub);
        }
        env->DeleteLocalRef(sub);
        ++forwarded;
    }
    env->DeleteLocalRef(it);
    if (nulls > 0)
        LogTo("bundle: forwarded=%d, null-subs=%d (skipped)", forwarded, nulls);
    else
        LogTo("bundle: forwarded=%d sub-packets", forwarded);
    return true;
}

void BServer_ForwardToB(JNIEnv* env, jobject aCtx, jobject packet) {
    if (!packet) return;

    static constexpr const char kGame[]   = "net.minecraft.network.protocol.game.";
    static constexpr const char kConfig[] = "net.minecraft.network.protocol.configuration.";

    std::string cls = classNameForB(env, packet);
    BState state = g_bs.bState.load(std::memory_order_acquire);

    jobject ch;
    { std::lock_guard<std::mutex> l(g_bs.bMu);
      ch = g_bs.bChannel ? env->NewLocalRef(g_bs.bChannel) : nullptr; }
    if (!ch) return;

    // --- configuration phase -------------------------------------------------
    // B is parked here.  A's own configuration stream is what fills B's
    // registries, and A's ClientboundFinishConfigurationPacket is what finally
    // moves B into PLAY.
    //
    // Part of the configuration stream lives in protocol.common, not
    // protocol.configuration -- most importantly ClientboundUpdateTagsPacket.
    // Without it B's registries load with tags=0 and B disconnects with
    // "Unbound tags in registry" on FinishConfiguration.  KeepAlive / Ping /
    // Disconnect stay out: A answers the server's, and B has its own watchdog.
    if (state == BState::AwaitConfiguration) {
        static const char* kCommonMirrored[] = {
            "net.minecraft.network.protocol.common.ClientboundUpdateTagsPacket",
            "net.minecraft.network.protocol.common.ClientboundCustomPayloadPacket",
            "net.minecraft.network.protocol.common.ClientboundResourcePackPushPacket",
            "net.minecraft.network.protocol.common.ClientboundResourcePackPopPacket",
            "net.minecraft.network.protocol.common.ClientboundServerLinksPacket",
            "net.minecraft.network.protocol.common.ClientboundCustomReportDetailsPacket",
            "net.minecraft.network.protocol.common.ClientboundShowDialogPacket",
            "net.minecraft.network.protocol.common.ClientboundClearDialogPacket",
        };
        bool mirrored = cls.rfind(kConfig, 0) == 0;
        for (const char* c : kCommonMirrored) {
            if (!mirrored && cls == c) mirrored = true;
        }
        if (!mirrored) { env->DeleteLocalRef(ch); return; }

        bool finish = g_bs.finishConfigPacketCls &&
                      env->IsInstanceOf(packet, g_bs.finishConfigPacketCls);
        if (env->ExceptionCheck()) env->ExceptionClear();

        env->CallObjectMethod(ch, g_bs.channelWriteAndFlushMid, packet);
        if (env->ExceptionCheck()) LogAndClearException(env, "ForwardToB/config");
        LogTo("config-mirror: %s%s", cls.c_str(), finish ? "   <-- ends configuration" : "");

        if (finish) {
            // Deliberately *not* switching B's handlers here: A has not yet been
            // moved to the PLAY protocol at this point in its own pipeline (see
            // ensureBPlayProtocol).  B is marked as being in PLAY so the next
            // packet takes the game branch, which performs the swap first.
            g_bs.bState.store(BState::Play, std::memory_order_release);
            {
                std::lock_guard<std::mutex> l(g_bConnMu);
                g_bConnected = true;
            }
            g_bConnCv.notify_all();
            LogTo("config-mirror: B is in PLAY — A's live join stream now feeds B");
        }
        env->DeleteLocalRef(ch);
        return;
    }

    if (state != BState::Play) { env->DeleteLocalRef(ch); return; }
    if (cls.rfind(kGame, 0) != 0) { env->DeleteLocalRef(ch); return; }

    // Swap B onto the PLAY protocol before its first PLAY packet, using the
    // ProtocolInfos lifted from A's pipeline (correct registry access).
    {
        std::lock_guard<std::mutex> l(g_bs.playSwapMu);
        bool outbound = false, inbound = false;
        if (!g_bs.bOutboundPlay) {
            ensureBPlayProtocol(env, aCtx);
            g_bs.bOutboundPlay = true;
            outbound = true;
        }
        if (g_bs.bInboundPlayPending && tryLiftPlaySbound(env, aCtx)) {
            g_bs.bInboundPlayPending = false;
            inbound = true;
        }
        if (outbound || inbound)
            postProtocolSwap(env, ch, ProtoState::Play, outbound, inbound);
    }

    // Log the opening move of a server transfer / reconfiguration.  The state
    // change itself waits for B's acknowledgement (see BSide_OnPacket), because
    // that is the moment B actually leaves PLAY.
    if (g_bs.startConfigPacketCls && env->IsInstanceOf(packet, g_bs.startConfigPacketCls)) {
        LogTo("reconfigure: A was told to re-enter CONFIGURATION (server transfer)");
    }
    if (env->ExceptionCheck()) env->ExceptionClear();

    if (!forwardBundleExpanded(env, ch, packet)) {
        writeOneToBWithTabGuard(env, ch, packet);
    }

    env->DeleteLocalRef(ch);
}

// The channel this packet actually arrived on.
//
// This must never be g_bs.bChannel.  A Minecraft client pings the server list
// before connecting, and that ping is a *separate TCP connection* -- which our
// ChannelInitializer accepts and which used to overwrite the single global.  The
// client's "hello" then arrived on the real connection while the global still
// pointed at the ping, so login success was written down the wrong socket and
// the real connection never got it, failing as a client-side
// "Failed to decode packet 'clientbound/minecraft:hello'".
//
// (Locals returned here are reclaimed when the native call returns.)
jobject channelOfCtx(JNIEnv* env, jobject ctx) {
    if (!ctx || !g_relay.netty.pipelineMid || !g_bs.pipelineChannelMid) return nullptr;
    jobject pipeline = env->CallObjectMethod(ctx, g_relay.netty.pipelineMid);
    if (!pipeline || env->ExceptionCheck()) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        return nullptr;
    }
    jobject ch = env->CallObjectMethod(pipeline, g_bs.pipelineChannelMid);
    if (env->ExceptionCheck()) { env->ExceptionClear(); ch = nullptr; }
    env->DeleteLocalRef(pipeline);
    return ch;
}

void BSide_OnPacket(JNIEnv* env, jobject ctx, jobject msg) {
    std::string cls = classNameForB(env, msg);
    BState state = g_bs.bState.load(std::memory_order_acquire);
    LogTo("BServer: RX %s (state=%d)", cls.c_str(), (int)state);

    jobject selfCh = channelOfCtx(env, ctx);

    if (g_bs.intentPacketCls && env->IsInstanceOf(msg, g_bs.intentPacketCls)) {
        // 1.21.8: the intention field is a ClientIntent enum, not a
        // ConnectionProtocol, and TRANSFER joined LOGIN/STATUS in 1.20.5.
        bool wantStatus = false;
        if (g_bs.intentionPacketIntentFid) {
            jobject intent = env->GetObjectField(msg, g_bs.intentionPacketIntentFid);
            if (intent) {
                wantStatus = env->IsSameObject(intent, g_bs.intentStatus);
                env->DeleteLocalRef(intent);
            }
        }
        ProtoState nextProto = wantStatus ? ProtoState::Status : ProtoState::Login;
        if (selfCh) setProtocolState(env, selfCh, nextProto);
        // A LOGIN intention is the real session; a STATUS one is just the server
        // list ping and must not become the forwarding target.
        if (!wantStatus && selfCh) {
            std::lock_guard<std::mutex> l(g_bs.bMu);
            if (g_bs.bChannel) env->DeleteGlobalRef(g_bs.bChannel);
            g_bs.bChannel = env->NewGlobalRef(selfCh);
        }
        g_bs.bState.store(wantStatus ? BState::AwaitHandshake : BState::AwaitLogin,
                          std::memory_order_release);
        LogTo("BServer: intention → %s", wantStatus ? "STATUS" : "LOGIN");
        return;
    }

    if (g_bs.pingRequestPacketCls && env->IsInstanceOf(msg, g_bs.pingRequestPacketCls)) {
        jlong t = 0;
        if (g_bs.pingRequestPacketTimeFid)
            t = env->GetLongField(msg, g_bs.pingRequestPacketTimeFid);
        if (g_bs.pongResponsePacketCtor) {
            jobject pong = env->NewObject(g_bs.pongResponsePacketCls,
                                          g_bs.pongResponsePacketCtor, t);
            if (pong) { writeToChan(env, selfCh, pong); env->DeleteLocalRef(pong); }
            LogTo("BServer: replied Pong(%lld)", (long long)t);
        }
        return;
    }

    if (g_bs.helloPacketCls && env->IsInstanceOf(msg, g_bs.helloPacketCls)) {
        completeLogin(env, selfCh, msg);
        return;
    }

    // Reconfiguration.  B has just answered A's mirrored StartConfiguration and
    // switched itself into CONFIGURATION, so we have to follow it there and let
    // A's reconfiguration stream through -- otherwise B sits on "Reconfiguring...".
    // A rebinds its PLAY ProtocolInfos against the new registries while it is
    // reconfiguring, so the lifted ones have to be thrown away and taken again.
    if (g_bs.configAckPacketCls && env->IsInstanceOf(msg, g_bs.configAckPacketCls)) {
        if (selfCh) setProtocolDirections(env, selfCh, ProtoState::Configuration, true, true);
        {
            std::lock_guard<std::mutex> l(g_bs.playSwapMu);
            g_bs.bOutboundPlay = false;
            g_bs.bInboundPlayPending = false;
        }
        g_bs.playProtocolReady = false;
        g_bs.playSboundReady = false;
        g_bs.bState.store(BState::AwaitConfiguration, std::memory_order_release);
        LogTo("reconfigure: B acknowledged; both directions back on CONFIGURATION, "
              "PLAY ProtocolInfo dropped for re-lift");
        return;
    }

    if (g_bs.loginAckPacketCls && env->IsInstanceOf(msg, g_bs.loginAckPacketCls)) {
        if (selfCh) setProtocolDirections(env, selfCh, ProtoState::Configuration, false, true);
        LogTo("login: B acknowledged; decoder on CONFIGURATION");
        return;
    }

    if (g_bs.finishConfigAckPacketCls && env->IsInstanceOf(msg, g_bs.finishConfigAckPacketCls)) {
        // B's decoder needs PLAY's serverbound ProtocolInfo, which is lifted
        // from A on A's first PLAY packet.  If that has not happened yet, leave
        // B's reads paused; the PLAY switch in BServer_ForwardToB finishes it.
        {
            std::lock_guard<std::mutex> l(g_bs.playSwapMu);
            if (g_bs.playSboundReady) {
                if (selfCh) setProtocolDirections(env, selfCh, ProtoState::Play, false, true);
                LogTo("config: B acknowledged finish; decoder on PLAY");
            } else {
                g_bs.bInboundPlayPending = true;
                LogTo("config: B acknowledged finish; decoder waits for A's PLAY protocol");
            }
        }
        return;
    }

    if (g_bs.bState.load(std::memory_order_acquire) == BState::Play) {
        if (shouldRouteBToA(cls)) {
            routeToA(env, msg);
        }
    }
}
