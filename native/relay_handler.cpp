#include "relay_handler.h"

#include "b_server.h"
#include "classfile.h"
#include "registry_replay.h"
#include "random_name.h"


#include <cstdio>
#include <mutex>
#include <string>
#include <vector>

RelayHandler g_relay;

namespace {

constexpr const char* kSuperInternal     = "io/netty/channel/ChannelDuplexHandler";
constexpr const char* kContextInternal   = "io/netty/channel/ChannelHandlerContext";
constexpr const char* kPipelineInternal  = "io/netty/channel/ChannelPipeline";
constexpr const char* kHandlerInternal   = "io/netty/channel/ChannelHandler";
constexpr const char* kPromiseInternal   = "io/netty/channel/ChannelPromise";

constexpr const char* kChannelReadDesc =
    "(Lio/netty/channel/ChannelHandlerContext;Ljava/lang/Object;)V";
constexpr const char* kWriteDesc =
    "(Lio/netty/channel/ChannelHandlerContext;Ljava/lang/Object;Lio/netty/channel/ChannelPromise;)V";



std::mutex g_bypassMu;
std::vector<jobject> g_bypassPending;

bool consumeBypassMark(JNIEnv* env, jobject msg) {
    std::lock_guard<std::mutex> lock(g_bypassMu);
    for (size_t i = 0; i < g_bypassPending.size(); ++i) {
        if (env->IsSameObject(g_bypassPending[i], msg)) {
            env->DeleteGlobalRef(g_bypassPending[i]);
            g_bypassPending.erase(g_bypassPending.begin() + i);
            return true;
        }
    }
    return false;
}

std::string javaClassName(JNIEnv* env, jobject o) {
    if (!o) return "null";
    if (!g_jvmti) return "<no-jvmti>";
    jclass c = env->GetObjectClass(o);
    if (!c) return "<no-class>";

    char* sig = nullptr;
    jvmtiError rc = g_jvmti->GetClassSignature(c, &sig, nullptr);
    env->DeleteLocalRef(c);
    if (rc != JVMTI_ERROR_NONE || !sig) return "<?>";

    std::string out;
    const char* p = sig;
    if (*p == 'L') {
        ++p;
        for (; *p && *p != ';'; ++p) out.push_back(*p == '/' ? '.' : *p);
    } else {
        out = sig;
    }
    g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(sig));
    return out;
}

void JNICALL Native_RelayChannelRead(JNIEnv* env,
                                     jobject ,
                                     jobject ctx,
                                     jobject msg) {

    BServer_OnARead(env, ctx, msg);
    jobject next = env->CallObjectMethod(ctx, g_relay.netty.fireChannelReadMid, msg);
    if (next) env->DeleteLocalRef(next);
    if (env->ExceptionCheck()) env->ExceptionClear();
}

void JNICALL Native_RelayChannelInactive(JNIEnv* env, jobject, jobject ctx) {
    BServer_OnAInactive(env, ctx);
    jobject next = env->CallObjectMethod(ctx, g_relay.netty.fireChannelInactiveMid);
    if (next) env->DeleteLocalRef(next);
    if (env->ExceptionCheck()) env->ExceptionClear();
}

void JNICALL Native_RelayWrite(JNIEnv* env,
                               jobject ,
                               jobject ctx,
                               jobject msg,
                               jobject promise) {
    std::string cls = javaClassName(env, msg);

    bool bypass = consumeBypassMark(env, msg);
    bool allow = bypass || BServer_OnAWrite(env, ctx, msg);
    LogTo("[C2S %s] %s", bypass ? "ROUTE" : (allow ? "PASS" : "DROP"), cls.c_str());

    if (allow) {
        jobject wire = bypass ? msg : BServer_PrepareAWrite(env, ctx, msg);
        if (sakura::RegistryReplay::shouldForwardPreparedWrite(wire != nullptr)) {
            jobject future = env->CallObjectMethod(ctx, g_relay.netty.ctxWriteMid, wire, promise);
            if (future) env->DeleteLocalRef(future);
        } else if (promise && g_relay.netty.promiseSetSuccessMid) {
            // The original packet may contain stale negotiated packs; never fall back to it.
            jobject completed = env->CallObjectMethod(promise, g_relay.netty.promiseSetSuccessMid);
            if (completed) env->DeleteLocalRef(completed);
        }
        if (wire && wire != msg) env->DeleteLocalRef(wire);
    } else {
        if (promise && g_relay.netty.promiseSetSuccessMid) {
            jobject completed = env->CallObjectMethod(promise, g_relay.netty.promiseSetSuccessMid);
            if (completed) env->DeleteLocalRef(completed);
        }
    }
    if (env->ExceptionCheck()) env->ExceptionClear();
}

bool defineRelayClass(JNIEnv* env, jobject mcLoader) {
    std::string simple   = GenerateRandomClassName(2, 3);
    std::string internal = MakeInternalName(GetTrampolinePackage(), simple);
    std::string dotted   = internal;
    for (char& ch : dotted) if (ch == '/') ch = '.';

    ClassBuilder cb(internal, kSuperInternal, 52);

    u2 superInitRef = cb.methodRef(kSuperInternal, "<init>", "()V");
    std::vector<u1> ctorCode = {
        0x2A,
        0xB7, static_cast<u1>((superInitRef >> 8) & 0xFF), static_cast<u1>(superInitRef & 0xFF),
        0xB1,
    };
    cb.addCodedMethod("<init>", "()V", ACC_PUBLIC, ctorCode, 1, 1);

    cb.addNativeMethod("channelRead", kChannelReadDesc,
                       ACC_PUBLIC | ACC_NATIVE);
    cb.addNativeMethod("channelInactive", "(Lio/netty/channel/ChannelHandlerContext;)V", ACC_PUBLIC | ACC_NATIVE);
    cb.addNativeMethod("write", kWriteDesc,
                       ACC_PUBLIC | ACC_NATIVE);

    std::vector<u1> bytes = cb.build();

    jclass defined = env->DefineClass(
        internal.c_str(),
        mcLoader,
        reinterpret_cast<const jbyte*>(bytes.data()),
        static_cast<jsize>(bytes.size()));
    if (!defined) {
        LogAndClearException(env, "InstallRelayHandler/DefineClass");
        return false;
    }

    JNINativeMethod natives[] = {
        { const_cast<char*>("channelRead"),
          const_cast<char*>(kChannelReadDesc),
          reinterpret_cast<void*>(&Native_RelayChannelRead) },
        { const_cast<char*>("write"),
          const_cast<char*>(kWriteDesc),
          reinterpret_cast<void*>(&Native_RelayWrite) },
        { const_cast<char*>("channelInactive"), const_cast<char*>("(Lio/netty/channel/ChannelHandlerContext;)V"), reinterpret_cast<void*>(&Native_RelayChannelInactive) },
    };
    if (env->RegisterNatives(defined, natives, 3) != 0) {
        LogAndClearException(env, "InstallRelayHandler/RegisterNatives");
        env->DeleteLocalRef(defined);
        return false;
    }

    jmethodID ctor = env->GetMethodID(defined, "<init>", "()V");
    if (!ctor) {
        LogAndClearException(env, "InstallRelayHandler/ctor");
        env->DeleteLocalRef(defined);
        return false;
    }

    g_relay.klass = static_cast<jclass>(env->NewGlobalRef(defined));
    g_relay.ctor  = ctor;
    g_relay.internalName = std::move(internal);
    g_relay.dotName      = std::move(dotted);
    env->DeleteLocalRef(defined);
    return true;
}

bool cacheNettyRefs(JNIEnv* env, jobject mcLoader) {

    jclass ctxCls = LoadClassInLoader(env, mcLoader,
                                      "io.netty.channel.ChannelHandlerContext");
    if (!ctxCls) { Dbg("Relay: couldn't load ChannelHandlerContext"); return false; }
    g_relay.netty.contextCls = static_cast<jclass>(env->NewGlobalRef(ctxCls));
    g_relay.netty.pipelineMid = env->GetMethodID(
        ctxCls, "pipeline", "()Lio/netty/channel/ChannelPipeline;");
    g_relay.netty.fireChannelReadMid = env->GetMethodID(
        ctxCls, "fireChannelRead",
        "(Ljava/lang/Object;)Lio/netty/channel/ChannelHandlerContext;");
    g_relay.netty.fireChannelInactiveMid = env->GetMethodID(ctxCls, "fireChannelInactive", "()Lio/netty/channel/ChannelHandlerContext;");
    g_relay.netty.ctxWriteMid = env->GetMethodID(
        ctxCls, "write",
        "(Ljava/lang/Object;Lio/netty/channel/ChannelPromise;)Lio/netty/channel/ChannelFuture;");
    g_relay.netty.ctxWriteFlushMid = env->GetMethodID(
        ctxCls, "writeAndFlush",
        "(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;");
    env->DeleteLocalRef(ctxCls);

    jclass pipCls = LoadClassInLoader(env, mcLoader,
                                      "io.netty.channel.ChannelPipeline");
    if (!pipCls) { Dbg("Relay: couldn't load ChannelPipeline"); return false; }
    g_relay.netty.pipelineCls = static_cast<jclass>(env->NewGlobalRef(pipCls));
    g_relay.netty.getMid = env->GetMethodID(pipCls, "get", "(Ljava/lang/String;)Lio/netty/channel/ChannelHandler;");
    g_relay.netty.namesMid = env->GetMethodID(pipCls, "names", "()Ljava/util/List;");
    g_relay.netty.addBeforeMid = env->GetMethodID(
        pipCls, "addBefore",
        "(Ljava/lang/String;Ljava/lang/String;Lio/netty/channel/ChannelHandler;)Lio/netty/channel/ChannelPipeline;");
    env->DeleteLocalRef(pipCls);

    jclass promCls = LoadClassInLoader(env, mcLoader,
                                       "io.netty.channel.ChannelPromise");
    if (!promCls) { Dbg("Relay: couldn't load ChannelPromise"); return false; }
    g_relay.netty.promiseCls = static_cast<jclass>(env->NewGlobalRef(promCls));
    g_relay.netty.promiseSetSuccessMid = env->GetMethodID(
        promCls, "setSuccess", "()Lio/netty/channel/ChannelPromise;");
    env->DeleteLocalRef(promCls);

    if (!g_relay.netty.pipelineMid || !g_relay.netty.fireChannelReadMid ||
        !g_relay.netty.ctxWriteMid  || !g_relay.netty.getMid || !g_relay.netty.namesMid || !g_relay.netty.fireChannelInactiveMid ||
        !g_relay.netty.addBeforeMid || !g_relay.netty.promiseSetSuccessMid) {
        Dbg("Relay: one or more netty method IDs missing");
        return false;
    }
    return true;
}

}

void RelayFilter_MarkBypass(JNIEnv* env, jobject packet) {
    if (!env || !packet) return;
    jobject gref = env->NewGlobalRef(packet);
    if (!gref) return;
    std::lock_guard<std::mutex> lock(g_bypassMu);

    // A queued write must keep its mark until consumption, send failure, or session cleanup.
    g_bypassPending.push_back(gref);
}

void RelayFilter_UnmarkBypass(JNIEnv* env, jobject packet) {
    if (env && packet) consumeBypassMark(env, packet);
}

void RelayFilter_ClearBypass(JNIEnv* env) {
    if (!env) return;
    std::lock_guard<std::mutex> lock(g_bypassMu);
    for (jobject packet : g_bypassPending) env->DeleteGlobalRef(packet);
    g_bypassPending.clear();
}

bool InstallRelayHandler(JNIEnv* env) {
    if (g_relay.valid()) return true;
    if (!env) return false;

    jobject mcLoader = GetMinecraftClassLoader(env, g_jvmti);
    if (!mcLoader) return false;

    bool ok = cacheNettyRefs(env, mcLoader) && defineRelayClass(env, mcLoader);
    env->DeleteGlobalRef(mcLoader);
    return ok;
}

static void attachHandlerToPipeline(JNIEnv* env, jobject pipeline) {
    jstring name = env->NewStringUTF("sakura_protocol_relay");
    jobject existing = env->CallObjectMethod(pipeline, g_relay.netty.getMid, name);
    if (env->ExceptionCheck()) { env->ExceptionClear(); env->DeleteLocalRef(name); return; }
    if (existing) { env->DeleteLocalRef(existing); env->DeleteLocalRef(name); return; }
    jobject names = env->CallObjectMethod(pipeline, g_relay.netty.namesMid);
    jclass listCls = env->FindClass("java/util/List");
    jmethodID indexOf = env->GetMethodID(listCls, "indexOf", "(Ljava/lang/Object;)I");
    jstring decoder = env->NewStringUTF("decoder");
    jstring inboundConfig = env->NewStringUTF("inbound_config");
    jstring packetHandler = env->NewStringUTF("packet_handler");
    jint decoderIndex = names ? env->CallIntMethod(names, indexOf, decoder) : -1;
    if (decoderIndex < 0 && names && !env->ExceptionCheck())
        decoderIndex = env->CallIntMethod(names, indexOf, inboundConfig);
    jint handlerIndex = names ? env->CallIntMethod(names, indexOf, packetHandler) : -1;
    bool valid = !env->ExceptionCheck() && decoderIndex >= 0 && handlerIndex > decoderIndex;
    if (env->ExceptionCheck()) env->ExceptionClear();
    env->DeleteLocalRef(decoder); env->DeleteLocalRef(inboundConfig);
    env->DeleteLocalRef(packetHandler); env->DeleteLocalRef(listCls);
    if (names) env->DeleteLocalRef(names);
    if (!valid) { LogTo("Attach: codec anchors missing or out of order; skipped"); env->DeleteLocalRef(name); return; }

    jobject handler = env->NewObject(g_relay.klass, g_relay.ctor);
    if (!handler || env->ExceptionCheck()) {
        LogTo("Attach: NewObject(RelayHandler) FAILED");
        env->ExceptionClear();
        env->DeleteLocalRef(name);
        return;
    }

    jstring base = env->NewStringUTF("packet_handler");
    jobject unused = env->CallObjectMethod(
        pipeline, g_relay.netty.addBeforeMid, base, name, handler);
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
        LogTo("Attach: addBefore failed; no unsafe fallback");
    } else {
        LogTo("Attach: installed relay before packet_handler");
    }
    if (unused) env->DeleteLocalRef(unused);
    env->DeleteLocalRef(base);
    env->DeleteLocalRef(handler);
    env->DeleteLocalRef(name);
}

void RelayHandler_AttachToPipeline(JNIEnv* env, jobject ctx) {
    LogTo("Attach: entering ctx=%p", (void*)ctx);
    if (!ctx) return;
    if (!g_relay.valid()) {
        LogTo("Attach: g_relay invalid, installing");
        if (!InstallRelayHandler(env)) { LogTo("Attach: install failed"); return; }
    }

    jobject pipeline = env->CallObjectMethod(ctx, g_relay.netty.pipelineMid);
    if (!pipeline || env->ExceptionCheck()) {
        LogTo("Attach: ctx.pipeline() FAILED");
        env->ExceptionClear();
        return;
    }
    LogTo("Attach: got pipeline=%p", (void*)pipeline);
    attachHandlerToPipeline(env, pipeline);
    env->DeleteLocalRef(pipeline);
}

void RelayHandler_AttachToPipelineObject(JNIEnv* env, jobject pipeline) {
    if (!pipeline) return;
    if (!g_relay.valid()) {
        if (!InstallRelayHandler(env)) { LogTo("AttachObj: install failed"); return; }
    }
    attachHandlerToPipeline(env, pipeline);
}
