#include "b_server.h"
#include "b_chat_route.h"
#include "classfile.h"
#include "random_name.h"
#include "relay_handler.h"
#include "protocol_session.h"
#include "registry_replay.h"
#include "world_cache.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {
using Phase = sakura::ProtocolPhase;
constexpr const char* kContextDesc = "(Lio/netty/channel/ChannelHandlerContext;)V";
constexpr const char* kReadDesc = "(Lio/netty/channel/ChannelHandlerContext;Ljava/lang/Object;)V";
constexpr size_t kMaxPackets = 16384;
constexpr size_t kMaxConfigurationPackets = 4096;

struct PacketRef {
    jobject packet;
    std::uint64_t connection;
    std::uint64_t configuration;
};
struct Task {
    jobject object;
    std::uint64_t connection;
    std::uint64_t b;
};
struct ChatTask {
    jobject object;
    jobject text;
    bool command;
    std::uint64_t connection, configuration, b;
};
struct BServer {
    std::recursive_mutex mu;
    sakura::ProtocolSession session;
    sakura::RegistryReplay registries;
    std::atomic_bool bound{false};
    bool configurationComplete = false;
    bool historyComplete = true;
    bool loginSent = false;
    bool helloReceived = false;
    bool startPending = false;
    bool bWorldInitialized = false;
    size_t configCursor = 0;
    jobject aChannel = nullptr;
    jobject aConnection = nullptr;
    jobject bChannel = nullptr;
    jobject profile = nullptr;
    jobject aPlayInbound = nullptr;
    jobject aPlayOutbound = nullptr;
    jobject bPendingPlayInbound = nullptr;
    jobject handshake = nullptr;
    jobject loginIn = nullptr;
    jobject loginOut = nullptr;
    jobject configIn = nullptr;
    jobject configOut = nullptr;
    jobject statusIn = nullptr;
    jobject statusOut = nullptr;
    jobject flow = nullptr;
    jobject loginIntent = nullptr;
    jobject transferIntent = nullptr;
    jobject statusIntent = nullptr;
    jobject finish = nullptr;
    jobject startConfigurationPacket = nullptr;
    std::vector<PacketRef> configuration;
    std::deque<PacketRef> play;
    std::vector<PacketRef> history;
    std::vector<Task> tasks;
    std::vector<ChatTask> chatTasks;
    jclass minecraftClass = nullptr, listenerClass = nullptr, chatTaskClass = nullptr;
    jmethodID minecraftInstance = nullptr, minecraftExecute = nullptr, getListener = nullptr;
    jmethodID listenerConnection = nullptr, sendCommand = nullptr, sendChat = nullptr, chatTaskCtor = nullptr;
    jclass initClass = nullptr, handlerClass = nullptr, taskClass = nullptr;
    jmethodID initCtor = nullptr, handlerCtor = nullptr, taskCtor = nullptr;
    jclass connectionClass = nullptr, encoderClass = nullptr, decoderClass = nullptr;
    jclass intentClass = nullptr, loginFinishedClass = nullptr;
    jclass keepAliveClass = nullptr, pongClass = nullptr;
    jclass clientPacksClass = nullptr, serverPacksClass = nullptr, registryClass = nullptr;
    jclass listClass = nullptr, entryClass = nullptr;
    jmethodID clientPacksCtor = nullptr, serverPacksCtor = nullptr, knownPacks = nullptr;
    jmethodID emptyList = nullptr, listSize = nullptr, listGet = nullptr;
    jmethodID registryEntries = nullptr, entryData = nullptr, optionalPresent = nullptr;
    jmethodID configure = nullptr, send = nullptr, encoderCtor = nullptr, decoderCtor = nullptr;
    jmethodID channel = nullptr, pipeline = nullptr, write = nullptr, close = nullptr;
    jmethodID futureDone = nullptr, futureSuccess = nullptr, futureCause = nullptr, futureAddListener = nullptr;
    jclass channelFutureListenerClass = nullptr;
    jfieldID closeOnFailure = nullptr;
    jmethodID channelActive = nullptr;
    jmethodID addLast = nullptr, getHandler = nullptr, replace = nullptr;
    jmethodID eventLoop = nullptr, execute = nullptr, config = nullptr, autoRead = nullptr;
    jfieldID encoderProtocol = nullptr, decoderProtocol = nullptr;
    jmethodID intention = nullptr, protocolVersion = nullptr, profileAccessor = nullptr;
    jmethodID loginFinishedCtor = nullptr, keepAliveCtor = nullptr, pongCtor = nullptr;
    jclass statusResponseClass = nullptr, serverStatusClass = nullptr, versionClass = nullptr;
    jclass componentClass = nullptr, optionalClass = nullptr;
    jmethodID statusResponseCtor = nullptr, serverStatusCtor = nullptr, versionCtor = nullptr;
    jmethodID literal = nullptr, optionalOf = nullptr, optionalEmpty = nullptr;
    std::chrono::steady_clock::time_point lastKeepAlive{};
};
BServer g_bs;

std::string packetName(JNIEnv* env, jobject packet) {
    if (!packet) return {};
    jclass cls = env->GetObjectClass(packet);
    char* sig = nullptr;
    std::string name;
    if (g_jvmti->GetClassSignature(cls, &sig, nullptr) == JVMTI_ERROR_NONE && sig) {
        const char* p = sig + (*sig == 'L' ? 1 : 0);
        for (; *p && *p != ';'; ++p) name.push_back(*p == '/' ? '.' : *p);
        g_jvmti->Deallocate(reinterpret_cast<unsigned char*>(sig));
    }
    env->DeleteLocalRef(cls);
    return name;
}
bool ends(const std::string& name, const char* suffix) {
    size_t n = std::strlen(suffix);
    return name.size() >= n && name.compare(name.size() - n, n, suffix) == 0;
}
bool prefix(const std::string& name, const char* value) { return name.rfind(value, 0) == 0; }
void drop(JNIEnv* env, jobject& object) {
    if (object) env->DeleteGlobalRef(object);
    object = nullptr;
}
void clearPackets(JNIEnv* env, std::vector<PacketRef>& packets) {
    for (auto& p : packets) env->DeleteGlobalRef(p.packet);
    packets.clear();
}
void clearPlay(JNIEnv* env) {
    for (auto& p : g_bs.play) env->DeleteGlobalRef(p.packet);
    g_bs.play.clear();
}
void closeChannel(JNIEnv* env, jobject ch) {
    if (!ch) return;
    jobject future = env->CallObjectMethod(ch, g_bs.close);
    if (future) env->DeleteLocalRef(future);
    if (env->ExceptionCheck()) LogAndClearException(env, "BServer/close");
}
void detachB(JNIEnv* env) {
    jobject ch = g_bs.bChannel ? env->NewLocalRef(g_bs.bChannel) : nullptr;
    drop(env, g_bs.bChannel);
    drop(env, g_bs.bPendingPlayInbound);
    g_bs.session.detachB();
    g_bs.loginSent = false;
    g_bs.helloReceived = false;
    g_bs.startPending = false;
    g_bs.bWorldInitialized = false;
    g_bs.configCursor = 0;
    g_bs.registries.attachB();
    clearPlay(env);
    closeChannel(env, ch);
    if (ch) env->DeleteLocalRef(ch);
}
void failB(JNIEnv* env, const char* why) {
    LogTo("BServer: closing B: %s", why);
    detachB(env);
}
void invalidateConfiguration(JNIEnv* env) {
    clearPackets(env, g_bs.configuration);
    clearPlay(env);
    clearPackets(env, g_bs.history);
    drop(env, g_bs.finish);
    drop(env, g_bs.aPlayInbound);
    drop(env, g_bs.aPlayOutbound);
    g_bs.configurationComplete = false;
    g_bs.bWorldInitialized = false;
    g_bs.registries.beginConfiguration();
    g_bs.historyComplete = true;
    g_bs.configCursor = 0;
    g_cache.clear(env);
}
jobject channelFor(JNIEnv* env, jobject ctx) {
    return ctx ? env->CallObjectMethod(ctx, g_bs.channel) : nullptr;
}
bool isA(JNIEnv* env, jobject ctx) {
    jobject ch = channelFor(env, ctx);
    bool result = ch && g_bs.aChannel && env->IsSameObject(ch, g_bs.aChannel);
    if (ch) env->DeleteLocalRef(ch);
    return result;
}
bool isB(JNIEnv* env, jobject ch) {
    return ch && g_bs.bChannel && env->IsSameObject(ch, g_bs.bChannel);
}
jobject pipelineHandler(JNIEnv* env, jobject pipeline, const char* name) {
    jstring key = env->NewStringUTF(name);
    jobject handler = env->CallObjectMethod(pipeline, g_bs.getHandler, key);
    env->DeleteLocalRef(key);
    return handler;
}
// Called only on the corresponding channel's EventLoop.
bool swapProtocol(JNIEnv* env, jobject ch, jobject protocol, bool inbound) {
    if (!ch || !protocol) return false;
    jobject pipeline = env->CallObjectMethod(ch, g_bs.pipeline);
    const char* name = inbound ? "decoder" : "encoder";
    const char* placeholder = inbound ? "inbound_config" : "outbound_config";
    jobject existing = pipelineHandler(env, pipeline, name);
    const char* oldName = existing ? name : placeholder;
    if (existing) env->DeleteLocalRef(existing);
    jobject handler = env->NewObject(inbound ? g_bs.decoderClass : g_bs.encoderClass,
                                    inbound ? g_bs.decoderCtor : g_bs.encoderCtor, protocol);
    jstring oldKey = env->NewStringUTF(oldName);
    jstring newKey = env->NewStringUTF(name);
    jobject replaced = env->CallObjectMethod(pipeline, g_bs.replace, oldKey, newKey, handler);
    bool ok = !env->ExceptionCheck();
    if (!ok) LogAndClearException(env, "BServer/swapProtocol");
    if (replaced) env->DeleteLocalRef(replaced);
    env->DeleteLocalRef(oldKey); env->DeleteLocalRef(newKey);
    env->DeleteLocalRef(handler); env->DeleteLocalRef(pipeline);
    if (ok && inbound) {
        jobject config = env->CallObjectMethod(ch, g_bs.config);
        jobject result = env->CallObjectMethod(config, g_bs.autoRead, (jboolean)JNI_TRUE);
        if (result) env->DeleteLocalRef(result);
        env->DeleteLocalRef(config);
        if (env->ExceptionCheck()) { LogAndClearException(env, "BServer/autoRead"); ok = false; }
    }
    return ok;
}
std::string throwableText(JNIEnv* env, jthrowable error) {
    if (env->ExceptionCheck()) return "unknown (JNI exception pending)";
    if (!error) return "unknown";
    std::string result;
    jthrowable seen[8] = {};
    int count = 0;
    jthrowable current = (jthrowable)env->NewLocalRef(error);
    if (env->ExceptionCheck()) { env->ExceptionClear(); return "unknown"; }
    while (current && count < 8) {
        seen[count++] = current;
        std::string detail = "unknown";
        jclass cls = env->GetObjectClass(current);
        if (env->ExceptionCheck()) { env->ExceptionClear(); cls = nullptr; }
        jmethodID toString = cls ? env->GetMethodID(cls, "toString", "()Ljava/lang/String;") : nullptr;
        if (env->ExceptionCheck()) { env->ExceptionClear(); toString = nullptr; }
        jstring text = toString ? (jstring)env->CallObjectMethod(current, toString) : nullptr;
        if (env->ExceptionCheck()) { env->ExceptionClear(); text = nullptr; }
        const char* chars = text ? env->GetStringUTFChars(text, nullptr) : nullptr;
        if (env->ExceptionCheck()) { env->ExceptionClear(); chars = nullptr; }
        if (chars) {
            detail = chars;
            env->ReleaseStringUTFChars(text, chars);
            if (env->ExceptionCheck()) env->ExceptionClear();
        }
        if (text) env->DeleteLocalRef(text);
        jmethodID getCause = cls ? env->GetMethodID(cls, "getCause", "()Ljava/lang/Throwable;") : nullptr;
        if (env->ExceptionCheck()) { env->ExceptionClear(); getCause = nullptr; }
        jthrowable cause = getCause ? (jthrowable)env->CallObjectMethod(current, getCause) : nullptr;
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
            if (cause) env->DeleteLocalRef(cause);
            cause = nullptr;
        }
        if (cls) env->DeleteLocalRef(cls);
        if (!result.empty()) result += " <- caused by: ";
        result += detail;
        bool cycle = false;
        for (int i = 0; cause && i < count; ++i)
            if (env->IsSameObject(cause, seen[i])) { cycle = true; break; }
        if (cycle || (cause && count == 8)) {
            result += cycle ? " <- [cause cycle]" : " <- [cause depth limit]";
            env->DeleteLocalRef(cause);
            cause = nullptr;
        }
        current = cause;
    }
    for (int i = 0; i < count; ++i) env->DeleteLocalRef(seen[i]);
    return result.empty() ? "unknown" : result;
}
bool writePacket(JNIEnv* env, jobject ch, jobject packet) {
    jobject future = env->CallObjectMethod(ch, g_bs.write, packet);
    if (env->ExceptionCheck()) {
        jthrowable error = env->ExceptionOccurred();
        env->ExceptionClear();
        const std::string detail = throwableText(env, error);
        LogTo("BServer: synchronous write exception packet=%s detail=%s",
              packetName(env, packet).c_str(), detail.c_str());
        if (error) env->DeleteLocalRef(error);
        if (future) env->DeleteLocalRef(future);
        return false;
    }
    if (!future) {
        LogTo("BServer: write returned null ChannelFuture packet=%s", packetName(env, packet).c_str());
        return false;
    }
    const bool done = env->CallBooleanMethod(future, g_bs.futureDone) == JNI_TRUE;
    bool success = false;
    std::string cause = done ? "none" : "pending";
    if (env->ExceptionCheck()) {
        LogAndClearException(env, "BServer/write future inspection");
        cause = "future inspection exception";
    } else if (done) {
        success = env->CallBooleanMethod(future, g_bs.futureSuccess) == JNI_TRUE;
        if (!success && !env->ExceptionCheck()) {
            jobject error = env->CallObjectMethod(future, g_bs.futureCause);
            if (env->ExceptionCheck()) {
                LogAndClearException(env, "BServer/write future cause");
                cause = "future cause inspection exception";
            } else {
                cause = throwableText(env, (jthrowable)error);
            }
            if (error) env->DeleteLocalRef(error);
        }
        if (env->ExceptionCheck()) {
            LogAndClearException(env, "BServer/write future success");
            cause = "future success inspection exception";
        }
    } else {
        // Netty encodes the write on this EventLoop; its listener handles a later failure.
        jobject closeOnFailure = env->GetStaticObjectField(g_bs.channelFutureListenerClass,
                                                            g_bs.closeOnFailure);
        if (closeOnFailure && !env->ExceptionCheck()) {
            jobject listenerFuture = env->CallObjectMethod(future, g_bs.futureAddListener, closeOnFailure);
            if (listenerFuture) env->DeleteLocalRef(listenerFuture);
        }
        if (env->ExceptionCheck()) {
            LogAndClearException(env, "BServer/write failure listener");
            cause = "cannot register asynchronous failure listener";
        }
        if (closeOnFailure) env->DeleteLocalRef(closeOnFailure);
        // This means accepted by Netty, not completed successfully; CLOSE_ON_FAILURE
        // closes the channel if the asynchronous write later fails.
        success = cause == "pending";
    }
    if (!success)
        LogTo("BServer: ChannelFuture write failed packet=%s done=%d success=%d cause=%s",
              packetName(env, packet).c_str(), done ? 1 : 0, success ? 1 : 0, cause.c_str());
    env->DeleteLocalRef(future);
    return success;
}
void capturePlayCodecs(JNIEnv* env) {
    if (!g_bs.aChannel || g_bs.session.aPhase != Phase::Play) return;
    jobject pipeline = env->CallObjectMethod(g_bs.aChannel, g_bs.pipeline);
    for (bool inbound : {true, false}) {
        jobject& slot = inbound ? g_bs.aPlayInbound : g_bs.aPlayOutbound;
        if (slot) continue;
        jobject handler = pipelineHandler(env, pipeline, inbound ? "decoder" : "encoder");
        if (handler && env->IsInstanceOf(handler, inbound ? g_bs.decoderClass : g_bs.encoderClass)) {
            jobject protocol = env->GetObjectField(handler, inbound ? g_bs.decoderProtocol : g_bs.encoderProtocol);
            if (protocol) {
                // The A direction may still be CONFIGURATION while its acknowledgement is in flight.
                jclass cls = env->GetObjectClass(protocol);
                jmethodID id = env->GetMethodID(cls, "id", "()Lnet/minecraft/network/ConnectionProtocol;");
                jobject value = id ? env->CallObjectMethod(protocol, id) : nullptr;
                std::string text;
                if (value) {
                    jclass enumClass = env->FindClass("java/lang/Enum");
                    jmethodID enumName = env->GetMethodID(enumClass, "name", "()Ljava/lang/String;");
                    jstring str = (jstring)env->CallObjectMethod(value, enumName);
                    const char* raw = env->GetStringUTFChars(str, nullptr);
                    if (raw) { text = raw; env->ReleaseStringUTFChars(str, raw); }
                    env->DeleteLocalRef(str); env->DeleteLocalRef(enumClass); env->DeleteLocalRef(value);
                }
                if (text == "PLAY") slot = env->NewGlobalRef(protocol);
                env->DeleteLocalRef(cls); env->DeleteLocalRef(protocol);
            }
        }
        if (handler) env->DeleteLocalRef(handler);
    }
    env->DeleteLocalRef(pipeline);
    g_bs.session.playCodecsReady = g_bs.aPlayInbound && g_bs.aPlayOutbound;
    if (env->ExceptionCheck()) LogAndClearException(env, "BServer/capturePlayCodecs");
}
void routeToA(JNIEnv* env, jobject packet) {
    if (!g_bs.aConnection || !g_bs.session.active()) return;
    RelayFilter_MarkBypass(env, packet);
    env->CallVoidMethod(g_bs.aConnection, g_bs.send, packet);
    if (env->ExceptionCheck()) {
        RelayFilter_UnmarkBypass(env, packet);
        LogAndClearException(env, "BServer/routeToA");
    }
}
void JNICALL Native_Chat_run(JNIEnv* env, jobject self) {
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    for (auto it = g_bs.chatTasks.begin(); it != g_bs.chatTasks.end(); ++it) {
        if (!env->IsSameObject(it->object, self)) continue;
        ChatTask task = *it;
        g_bs.chatTasks.erase(it);
        env->PushLocalFrame(16);
        if (g_bs.session.active() && task.connection == g_bs.session.connectionGeneration &&
            task.configuration == g_bs.session.configurationGeneration && task.b == g_bs.session.bGeneration) {
            jobject minecraft = env->CallStaticObjectMethod(g_bs.minecraftClass, g_bs.minecraftInstance);
            jobject listener = minecraft ? env->CallObjectMethod(minecraft, g_bs.getListener) : nullptr;
            jobject connection = listener ? env->CallObjectMethod(listener, g_bs.listenerConnection) : nullptr;
            if (connection && env->IsSameObject(connection, g_bs.aConnection))
                env->CallVoidMethod(listener, task.command ? g_bs.sendCommand : g_bs.sendChat, task.text);
        }
        if (env->ExceptionCheck()) LogAndClearException(env, "BServer/A chat");
        env->PopLocalFrame(nullptr);
        env->DeleteGlobalRef(task.object);
        env->DeleteGlobalRef(task.text);
        return;
    }
}
void scheduleChat(JNIEnv* env, jobject packet, bool command) {
    if (g_bs.chatTasks.size() >= 256) { failB(env, "chat task limit"); return; }
    jclass cls = env->GetObjectClass(packet);
    jmethodID accessor = env->GetMethodID(cls, command ? "command" : "message", "()Ljava/lang/String;");
    jobject text = accessor ? env->CallObjectMethod(packet, accessor) : nullptr;
    jobject task = text ? env->NewObject(g_bs.chatTaskClass, g_bs.chatTaskCtor) : nullptr;
    if (!task || env->ExceptionCheck()) { LogAndClearException(env, "BServer/chat task"); return; }
    g_bs.chatTasks.push_back({env->NewGlobalRef(task), env->NewGlobalRef(text), command,
        g_bs.session.connectionGeneration, g_bs.session.configurationGeneration, g_bs.session.bGeneration});
    jobject minecraft = env->CallStaticObjectMethod(g_bs.minecraftClass, g_bs.minecraftInstance);
    if (minecraft) env->CallVoidMethod(minecraft, g_bs.minecraftExecute, task);
    if (!minecraft || env->ExceptionCheck()) {
        LogAndClearException(env, "BServer/chat enqueue");
        env->DeleteGlobalRef(g_bs.chatTasks.back().object);
        env->DeleteGlobalRef(g_bs.chatTasks.back().text);
        g_bs.chatTasks.pop_back();
    }
}
void pump(JNIEnv* env);
void schedulePump(JNIEnv* env) {
    if (!g_bs.bChannel) return;
    for (const auto& task : g_bs.tasks)
        if (task.connection == g_bs.session.connectionGeneration && task.b == g_bs.session.bGeneration) return;
    jobject task = env->NewObject(g_bs.taskClass, g_bs.taskCtor);
    if (!task) { LogAndClearException(env, "BServer/task"); return; }
    g_bs.tasks.push_back({env->NewGlobalRef(task), g_bs.session.connectionGeneration, g_bs.session.bGeneration});
    jobject loop = env->CallObjectMethod(g_bs.bChannel, g_bs.eventLoop);
    env->CallVoidMethod(loop, g_bs.execute, task);
    if (env->ExceptionCheck()) {
        LogAndClearException(env, "BServer/execute");
        env->DeleteGlobalRef(g_bs.tasks.back().object);
        g_bs.tasks.pop_back();
        failB(env, "cannot enqueue protocol task");
    }
    env->DeleteLocalRef(loop); env->DeleteLocalRef(task);
}
void JNICALL Native_Pump_run(JNIEnv* env, jobject self) {
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    for (auto it = g_bs.tasks.begin(); it != g_bs.tasks.end(); ++it) {
        if (!env->IsSameObject(it->object, self)) continue;
        bool current = it->connection == g_bs.session.connectionGeneration && it->b == g_bs.session.bGeneration;
        env->DeleteGlobalRef(it->object);
        g_bs.tasks.erase(it);
        if (current && g_bs.bChannel) pump(env);
        return;
    }
}
void pump(JNIEnv* env) {
    if (!g_bs.bChannel) return;
    if (g_bs.helloReceived && !g_bs.loginSent && g_bs.profile && g_bs.session.bInbound == Phase::Login) {
        jobject packet = env->NewObject(g_bs.loginFinishedClass, g_bs.loginFinishedCtor, g_bs.profile);
        if (!packet || !writePacket(env, g_bs.bChannel, packet) ||
            !swapProtocol(env, g_bs.bChannel, g_bs.configOut, false)) {
            if (packet) env->DeleteLocalRef(packet);
            failB(env, "login-finished codec transition failed"); return;
        }
        env->DeleteLocalRef(packet);
        g_bs.session.loginFinished();
        g_bs.loginSent = true;
    }
    if (g_bs.startPending && g_bs.session.bInbound == Phase::Play && !g_bs.session.waitingFinishAck) {
        const auto& session = g_bs.session;
        if (!session.canStartConfiguration()) {
            LogTo("BServer: start-configuration rejected stage=state_predicate a=%d b_in=%d b_out=%d login_ack=%d start_ack=%d finish_ack=%d",
                  (int)session.aPhase, (int)session.bInbound, (int)session.bOutbound,
                  session.waitingLoginAck ? 1 : 0, session.waitingStartAck ? 1 : 0,
                  session.waitingFinishAck ? 1 : 0);
            failB(env, "start-configuration state predicate failed"); return;
        }
        jobject start = env->NewLocalRef(g_bs.startConfigurationPacket);
        if (!start || env->ExceptionCheck()) {
            if (env->ExceptionCheck()) LogAndClearException(env, "BServer/start-configuration create");
            LogTo("BServer: start-configuration failed stage=packet_creation a=%d b_in=%d b_out=%d login_ack=%d start_ack=%d finish_ack=%d",
                  (int)session.aPhase, (int)session.bInbound, (int)session.bOutbound,
                  session.waitingLoginAck ? 1 : 0, session.waitingStartAck ? 1 : 0,
                  session.waitingFinishAck ? 1 : 0);
            if (start) env->DeleteLocalRef(start);
            failB(env, "start-configuration packet creation failed"); return;
        }
        if (!writePacket(env, g_bs.bChannel, start)) {
            env->DeleteLocalRef(start);
            LogTo("BServer: start-configuration failed stage=packet_write a=%d b_in=%d b_out=%d login_ack=%d start_ack=%d finish_ack=%d",
                  (int)session.aPhase, (int)session.bInbound, (int)session.bOutbound,
                  session.waitingLoginAck ? 1 : 0, session.waitingStartAck ? 1 : 0,
                  session.waitingFinishAck ? 1 : 0);
            failB(env, "start-configuration packet write failed"); return;
        }
        if (!swapProtocol(env, g_bs.bChannel, g_bs.configOut, false)) {
            env->DeleteLocalRef(start);
            LogTo("BServer: start-configuration failed stage=codec_swap a=%d b_in=%d b_out=%d login_ack=%d start_ack=%d finish_ack=%d",
                  (int)session.aPhase, (int)session.bInbound, (int)session.bOutbound,
                  session.waitingLoginAck ? 1 : 0, session.waitingStartAck ? 1 : 0,
                  session.waitingFinishAck ? 1 : 0);
            failB(env, "start-configuration codec swap failed"); return;
        }
        env->DeleteLocalRef(start);
        if (!g_bs.session.startConfiguration()) {
            LogTo("BServer: start-configuration rejected stage=state_commit a=%d b_in=%d b_out=%d login_ack=%d start_ack=%d finish_ack=%d",
                  (int)session.aPhase, (int)session.bInbound, (int)session.bOutbound,
                  session.waitingLoginAck ? 1 : 0, session.waitingStartAck ? 1 : 0,
                  session.waitingFinishAck ? 1 : 0);
            failB(env, "start-configuration state commit failed"); return;
        }
        g_bs.startPending = false;
    }
    if (g_bs.session.canSendConfiguration() && g_bs.registries.canReplay()) {
        while (g_bs.configCursor < g_bs.configuration.size()) {
            const PacketRef& p = g_bs.configuration[g_bs.configCursor++];
            if (p.connection != g_bs.session.connectionGeneration ||
                p.configuration != g_bs.session.configurationGeneration) continue;
            if (!writePacket(env, g_bs.bChannel, p.packet)) { failB(env, "configuration write failed"); return; }
            if (env->IsInstanceOf(p.packet, g_bs.clientPacksClass)) {
                if (!g_bs.registries.offerB()) { failB(env, "overlapping known-packs offer"); return; }
                return;
            }
        }
        capturePlayCodecs(env);
        if (g_bs.finish && g_bs.session.aFinishedConfiguration && g_bs.session.playCodecsReady) {
            drop(env, g_bs.bPendingPlayInbound);
            g_bs.bPendingPlayInbound = env->NewGlobalRef(g_bs.aPlayOutbound);
            if (!writePacket(env, g_bs.bChannel, g_bs.finish) ||
                !swapProtocol(env, g_bs.bChannel, g_bs.aPlayInbound, false) ||
                !g_bs.session.finishConfiguration()) {
                failB(env, "finish-configuration transition failed"); return;
            }
        }
    }
    if (g_bs.session.active()) {
        while (!g_bs.play.empty()) {
            PacketRef p = g_bs.play.front();
            g_bs.play.pop_front();
            bool current = p.connection == g_bs.session.connectionGeneration &&
                           p.configuration == g_bs.session.configurationGeneration;
            bool ok = !current || writePacket(env, g_bs.bChannel, p.packet);
            if (current && ok && ends(packetName(env, p.packet), ".ClientboundLoginPacket"))
                g_bs.bWorldInitialized = true;
            env->DeleteGlobalRef(p.packet);
            if (!ok) { failB(env, "play write failed"); return; }
        }
    }
    auto now = std::chrono::steady_clock::now();
    if (g_bs.loginSent && (g_bs.session.bOutbound == Phase::Configuration ||
        g_bs.session.bOutbound == Phase::Play) && now - g_bs.lastKeepAlive > std::chrono::seconds(10)) {
        jobject packet = env->NewObject(g_bs.keepAliveClass, g_bs.keepAliveCtor, (jlong)0x53414B555241);
        if (packet) { writePacket(env, g_bs.bChannel, packet); env->DeleteLocalRef(packet); }
        g_bs.lastKeepAlive = now;
    }
}

void sendStatus(JNIEnv* env, jobject ch) {
    jstring desc = env->NewStringUTF("SakuraTools 1.21.8 relay");
    jobject component = env->CallStaticObjectMethod(g_bs.componentClass, g_bs.literal, desc);
    jstring versionName = env->NewStringUTF("1.21.8");
    jobject version = env->NewObject(g_bs.versionClass, g_bs.versionCtor, versionName, (jint)772);
    jobject empty = env->CallStaticObjectMethod(g_bs.optionalClass, g_bs.optionalEmpty);
    jobject versionOptional = env->CallStaticObjectMethod(g_bs.optionalClass, g_bs.optionalOf, version);
    jobject status = env->NewObject(g_bs.serverStatusClass, g_bs.serverStatusCtor,
                                    component, empty, versionOptional, empty, (jboolean)JNI_FALSE);
    jobject packet = env->NewObject(g_bs.statusResponseClass, g_bs.statusResponseCtor, status);
    if (packet) writePacket(env, ch, packet);
    if (env->ExceptionCheck()) LogAndClearException(env, "BServer/status");
    for (jobject ref : { (jobject)desc, component, (jobject)versionName, version, empty,
                         versionOptional, status, packet }) if (ref) env->DeleteLocalRef(ref);
}
void JNICALL Native_BSide_channelActive(JNIEnv*, jobject, jobject) {}
bool hasCapturedAPlayBootstrap(JNIEnv* env) {
    if (!g_bs.aChannel || !g_bs.aConnection || !g_bs.configurationComplete ||
        !g_bs.historyComplete || g_bs.session.aPhase != Phase::Play ||
        env->CallBooleanMethod(g_bs.aChannel, g_bs.channelActive) != JNI_TRUE ||
        env->ExceptionCheck()) {
        if (env->ExceptionCheck()) LogAndClearException(env, "BServer/A snapshot readiness");
        return false;
    }
    for (const auto& p : g_bs.history) {
        if (p.connection == g_bs.session.connectionGeneration &&
            p.configuration == g_bs.session.configurationGeneration &&
            ends(packetName(env, p.packet), ".ClientboundLoginPacket")) return true;
    }
    return false;
}
void JNICALL Native_BSide_channelInactive(JNIEnv* env, jobject, jobject ctx) {
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    jobject ch = channelFor(env, ctx);
    if (isB(env, ch)) {
        const bool midSession = hasCapturedAPlayBootstrap(env);
        jobject aChannel = g_bs.aChannel ? env->NewLocalRef(g_bs.aChannel) : nullptr;
        jobject aConnection = g_bs.aConnection ? env->NewLocalRef(g_bs.aConnection) : nullptr;
        LogTo("BServer: B channel inactive; releasing owner mid_session=%d a=%d config=%d history=%d",
              midSession ? 1 : 0, aChannel && aConnection ? 1 : 0,
              g_bs.configurationComplete ? 1 : 0, g_bs.historyComplete ? 1 : 0);
        detachB(env);
        RelayFilter_ClearBypass(env);
        const bool stillOwnedA = aChannel && aConnection && g_bs.aChannel && g_bs.aConnection &&
            env->IsSameObject(aChannel, g_bs.aChannel) &&
            env->IsSameObject(aConnection, g_bs.aConnection) &&
            env->CallBooleanMethod(aChannel, g_bs.channelActive) == JNI_TRUE && !env->ExceptionCheck();
        if (!midSession && stillOwnedA) {
            // Netty Channel.close() is thread-safe and schedules close on the channel EventLoop.
            LogTo("BServer: closing current A backend channel to trigger the client's reconnect workflow");
            closeChannel(env, aChannel);
        } else if (!midSession) {
            if (env->ExceptionCheck()) LogAndClearException(env, "BServer/A close ownership check");
            LogTo("BServer: not closing A backend channel; current ownership/activity could not be confirmed");
        }
        if (aChannel) env->DeleteLocalRef(aChannel);
        if (aConnection) env->DeleteLocalRef(aConnection);
    } else if (ch) {
        LogTo("BServer: ignored inactive callback from non-owner B channel");
    }
    if (ch) env->DeleteLocalRef(ch);
}
void JNICALL Native_BSide_exceptionCaught(JNIEnv* env, jobject, jobject ctx, jobject error) {
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    const char* stage = "HANDSHAKE";
    jobject ch = channelFor(env, ctx);
    if (isB(env, ch)) {
        stage = g_bs.session.bInbound == Phase::Login ? "LOGIN" :
                g_bs.session.bInbound == Phase::Configuration ? "CONFIGURATION" :
                g_bs.session.bInbound == Phase::Play ? "PLAY" : "OWNER";
    }
    const std::string detail = throwableText(env, (jthrowable)error);
    LogTo("BServer: B HANDSHAKE stage=%s action=close reason=exception detail=%s",
          stage, detail.c_str());
    closeChannel(env, ch);
    if (ch) env->DeleteLocalRef(ch);
}
void JNICALL Native_BSide_channelRead(JNIEnv* env, jobject, jobject ctx, jobject packet) {
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    env->PushLocalFrame(64);
    jobject ch = channelFor(env, ctx);
    const std::string name = packetName(env, packet);
    if (env->IsInstanceOf(packet, g_bs.intentClass)) {
        jobject intent = env->CallObjectMethod(packet, g_bs.intention);
        bool status = intent && env->IsSameObject(intent, g_bs.statusIntent);
        jint version = env->CallIntMethod(packet, g_bs.protocolVersion);
        if (status) {
            if (!swapProtocol(env, ch, g_bs.statusOut, false) ||
                !swapProtocol(env, ch, g_bs.statusIn, true)) {
                LogTo("BServer: B HANDSHAKE stage=STATUS action=close reason=codec_transition_failed");
                closeChannel(env, ch);
            }
        } else if (version != 772 || !intent ||
                   (!env->IsSameObject(intent, g_bs.loginIntent) && !env->IsSameObject(intent, g_bs.transferIntent))) {
            LogTo("BServer: B HANDSHAKE stage=HANDSHAKE action=reject reason=invalid_intention_or_protocol version=%d", (int)version);
            closeChannel(env, ch);
        } else {
            bool ownerActive = g_bs.bChannel &&
                env->CallBooleanMethod(g_bs.bChannel, g_bs.channelActive) == JNI_TRUE;
            if (g_bs.bChannel && !ownerActive) {
                LogTo("BServer: B HANDSHAKE stage=LOGIN owner=inactive action=reclaim");
                detachB(env);
            }
            const bool snapshotReady = g_bs.historyComplete &&
                !(g_bs.session.aPhase == Phase::Play && !g_bs.configurationComplete);
            const bool takeoverReady = snapshotReady && g_bs.session.aPhase == Phase::Play &&
                                       g_bs.configurationComplete;
            if (g_bs.bChannel && sakura::ProtocolSession::mayTakeoverB(ownerActive, takeoverReady)) {
                LogTo("BServer: B HANDSHAKE stage=LOGIN owner=active action=takeover old_generation=%llu",
                      (unsigned long long)g_bs.session.bGeneration);
                detachB(env);
            }
            if (g_bs.bChannel) {
                LogTo("BServer: B HANDSHAKE stage=LOGIN action=reject reason=owner_active_or_unavailable");
                closeChannel(env, ch);
            } else if (!snapshotReady) {
                LogTo("BServer: B HANDSHAKE stage=LOGIN action=reject reason=snapshot_unavailable history=%d configuration=%d a_phase=%d",
                      g_bs.historyComplete ? 1 : 0, g_bs.configurationComplete ? 1 : 0,
                      (int)g_bs.session.aPhase);
                closeChannel(env, ch);
            } else if (swapProtocol(env, ch, g_bs.loginOut, false) &&
                       swapProtocol(env, ch, g_bs.loginIn, true)) {
                g_bs.bChannel = env->NewGlobalRef(ch);
                g_bs.session.attachB();
                LogTo("BServer: B HANDSHAKE stage=LOGIN action=attached version=%d generation=%llu intent=%s",
                      (int)version, (unsigned long long)g_bs.session.bGeneration,
                      env->IsSameObject(intent, g_bs.transferIntent) ? "TRANSFER" : "LOGIN");
                g_bs.loginSent = false;
                g_bs.helloReceived = false;
                g_bs.bWorldInitialized = false;
                g_bs.startPending = false;
                g_bs.configCursor = 0;
                g_bs.registries.attachB();
                g_bs.lastKeepAlive = std::chrono::steady_clock::now();
                clearPlay(env);
                for (const auto& p : g_bs.history)
                    g_bs.play.push_back({env->NewGlobalRef(p.packet), p.connection, p.configuration});
            } else closeChannel(env, ch);
        }
        env->PopLocalFrame(nullptr); return;
    }
    if (ends(name, ".ServerboundStatusRequestPacket")) {
        sendStatus(env, ch);
        env->PopLocalFrame(nullptr); return;
    }
    if (ends(name, ".ServerboundPingRequestPacket")) {
        jclass cls = env->GetObjectClass(packet);
        jmethodID time = env->GetMethodID(cls, "getTime", "()J");
        jlong value = time ? env->CallLongMethod(packet, time) : 0;
        jobject pong = env->NewObject(g_bs.pongClass, g_bs.pongCtor, value);
        if (pong) writePacket(env, ch, pong);
        closeChannel(env, ch);
        env->PopLocalFrame(nullptr); return;
    }
    if (!isB(env, ch)) { closeChannel(env, ch); env->PopLocalFrame(nullptr); return; }
    if (ends(name, ".ServerboundHelloPacket")) {
        if (g_bs.session.bInbound != Phase::Login || g_bs.loginSent || g_bs.helloReceived) failB(env, "unexpected login hello");
        else { g_bs.helloReceived = true; pump(env); }
    } else if (ends(name, ".ServerboundLoginAcknowledgedPacket")) {
        if (!g_bs.session.loginAcknowledged() || !swapProtocol(env, ch, g_bs.configIn, true))
            failB(env, "unexpected login acknowledgement");
        else pump(env);
    } else if (ends(name, ".ServerboundConfigurationAcknowledgedPacket")) {
        if (!g_bs.session.configurationAcknowledged() || !swapProtocol(env, ch, g_bs.configIn, true))
            failB(env, "unexpected start-configuration acknowledgement");
        else pump(env);
    } else if (env->IsInstanceOf(packet, g_bs.serverPacksClass)) {
        jobject packs = env->CallObjectMethod(packet, g_bs.knownPacks);
        jint count = packs ? env->CallIntMethod(packs, g_bs.listSize) : -1;
        if (g_bs.session.bInbound != Phase::Configuration || count < 0 ||
            !g_bs.registries.selectB(static_cast<size_t>(count))) failB(env, "unexpected known-packs selection");
        else pump(env);
    } else if (ends(name, ".ServerboundFinishConfigurationPacket")) {
        if (!g_bs.session.finishAcknowledged() || !swapProtocol(env, ch, g_bs.bPendingPlayInbound, true))
            failB(env, "unexpected finish-configuration acknowledgement");
        else pump(env);
    } else if (g_bs.session.active()) {
        bool handled = sakura::routeBChat(name.c_str(),
            [&] { scheduleChat(env, packet, true); },
            [&] { scheduleChat(env, packet, false); },
            [&] { routeToA(env, packet); });
        if (!handled && BServer_ShouldRoutePlayerPacket(name.c_str())) routeToA(env, packet);
    }
    // A owns backend protocol/common/cookie answers. B's local answers terminate here.
    if (env->ExceptionCheck()) { LogAndClearException(env, "BServer/B packet"); failB(env, "packet handler exception"); }
    env->PopLocalFrame(nullptr);
}
void JNICALL Native_ServerInit_initChannel(JNIEnv* env, jobject, jobject ch) {
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    env->PushLocalFrame(32);
    jobject pipeline = env->CallObjectMethod(ch, g_bs.pipeline);
    env->CallStaticVoidMethod(g_bs.connectionClass, g_bs.configure, pipeline, g_bs.flow,
                             (jboolean)JNI_FALSE, nullptr);
    bool ok = !env->ExceptionCheck();
    if (ok) ok = swapProtocol(env, ch, g_bs.handshake, true);
    jobject handler = env->NewObject(g_bs.handlerClass, g_bs.handlerCtor);
    jstring key = env->NewStringUTF("bside");
    if (ok) env->CallObjectMethod(pipeline, g_bs.addLast, key, handler);
    if (env->ExceptionCheck()) { LogAndClearException(env, "BServer/initChannel"); ok = false; }
    if (!ok) closeChannel(env, ch);
    // Accepted sockets (including STATUS) are not session owners until LOGIN intention.
    env->PopLocalFrame(nullptr);
}

jclass defineNativeClass(JNIEnv* env, jobject loader, const char* super,
                         const std::vector<JNINativeMethod>& methods) {
    std::string internal = MakeInternalName(GetTrampolinePackage(), GenerateRandomClassName(2, 3));
    ClassBuilder builder(internal, super, 52);
    u2 superInit = builder.methodRef(super, "<init>", "()V");
    builder.addCodedMethod("<init>", "()V", ACC_PUBLIC,
                          {0x2A, 0xB7, u1(superInit >> 8), u1(superInit), 0xB1}, 1, 1);
    for (const auto& method : methods)
        builder.addNativeMethod(method.name, method.signature, ACC_PUBLIC | ACC_NATIVE);
    std::vector<u1> bytes = builder.build();
    jclass cls = env->DefineClass(internal.c_str(), loader,
                                  reinterpret_cast<const jbyte*>(bytes.data()), (jsize)bytes.size());
    if (!cls || env->RegisterNatives(cls, methods.data(), (jint)methods.size()) != JNI_OK) {
        LogAndClearException(env, "BServer/defineNativeClass"); return nullptr;
    }
    jclass global = (jclass)env->NewGlobalRef(cls);
    env->DeleteLocalRef(cls);
    return global;
}

bool cacheJavaRefs(JNIEnv* env, jobject loader) {
    bool ok = true;
    auto cls = [&](const char* name) -> jclass {
        jclass local = LoadClassInLoader(env, loader, name);
        if (!local) { LogTo("BServer: missing Mojmap class %s", name); ok = false; return nullptr; }
        jclass result = (jclass)env->NewGlobalRef(local);
        env->DeleteLocalRef(local);
        return result;
    };
    auto method = [&](jclass c, const char* name, const char* desc, bool isStatic = false) -> jmethodID {
        jmethodID result = c ? (isStatic ? env->GetStaticMethodID(c, name, desc) : env->GetMethodID(c, name, desc)) : nullptr;
        if (!result) { LogTo("BServer: missing binding %s %s", name, desc); ok = false; }
        if (env->ExceptionCheck()) LogAndClearException(env, "BServer/binding");
        return result;
    };
    auto field = [&](jclass c, const char* name, const char* desc) -> jfieldID {
        jfieldID result = c ? env->GetFieldID(c, name, desc) : nullptr;
        if (!result) { LogTo("BServer: missing field %s %s", name, desc); ok = false; }
        if (env->ExceptionCheck()) LogAndClearException(env, "BServer/field");
        return result;
    };
    auto staticObject = [&](const char* owner, const char* name, const char* desc) -> jobject {
        jclass c = cls(owner);
        jfieldID f = c ? env->GetStaticFieldID(c, name, desc) : nullptr;
        jobject local = f ? env->GetStaticObjectField(c, f) : nullptr;
        jobject result = local ? env->NewGlobalRef(local) : nullptr;
        if (!result) { LogTo("BServer: missing constant %s.%s", owner, name); ok = false; }
        if (local) env->DeleteLocalRef(local);
        if (c) env->DeleteGlobalRef(c);
        if (env->ExceptionCheck()) LogAndClearException(env, "BServer/constant");
        return result;
    };
    g_bs.minecraftClass = cls("net.minecraft.client.Minecraft");
    g_bs.listenerClass = cls("net.minecraft.client.multiplayer.ClientPacketListener");
    g_bs.minecraftInstance = method(g_bs.minecraftClass, "getInstance", "()Lnet/minecraft/client/Minecraft;", true);
    g_bs.minecraftExecute = method(g_bs.minecraftClass, "execute", "(Ljava/lang/Runnable;)V");
    g_bs.getListener = method(g_bs.minecraftClass, "getConnection", "()Lnet/minecraft/client/multiplayer/ClientPacketListener;");
    g_bs.listenerConnection = method(g_bs.listenerClass, "getConnection", "()Lnet/minecraft/network/Connection;");
    g_bs.sendCommand = method(g_bs.listenerClass, "sendCommand", "(Ljava/lang/String;)V");
    g_bs.sendChat = method(g_bs.listenerClass, "sendChat", "(Ljava/lang/String;)V");
    g_bs.connectionClass = cls("net.minecraft.network.Connection");
    g_bs.configure = method(g_bs.connectionClass, "configureSerialization",
        "(Lio/netty/channel/ChannelPipeline;Lnet/minecraft/network/protocol/PacketFlow;ZLnet/minecraft/network/BandwidthDebugMonitor;)V", true);
    g_bs.send = method(g_bs.connectionClass, "send", "(Lnet/minecraft/network/protocol/Packet;)V");
    g_bs.encoderClass = cls("net.minecraft.network.PacketEncoder");
    g_bs.decoderClass = cls("net.minecraft.network.PacketDecoder");
    g_bs.encoderCtor = method(g_bs.encoderClass, "<init>", "(Lnet/minecraft/network/ProtocolInfo;)V");
    g_bs.decoderCtor = method(g_bs.decoderClass, "<init>", "(Lnet/minecraft/network/ProtocolInfo;)V");
    g_bs.encoderProtocol = field(g_bs.encoderClass, "protocolInfo", "Lnet/minecraft/network/ProtocolInfo;");
    g_bs.decoderProtocol = field(g_bs.decoderClass, "protocolInfo", "Lnet/minecraft/network/ProtocolInfo;");
    jclass channel = cls("io.netty.channel.Channel");
    jclass ctx = cls("io.netty.channel.ChannelHandlerContext");
    jclass pipeline = cls("io.netty.channel.ChannelPipeline");
    jclass executor = cls("io.netty.util.concurrent.EventExecutorGroup");
    jclass config = cls("io.netty.channel.ChannelConfig");
    jclass future = cls("io.netty.util.concurrent.Future");
    g_bs.futureDone = method(future, "isDone", "()Z");
    g_bs.futureSuccess = method(future, "isSuccess", "()Z");
    g_bs.futureCause = method(future, "cause", "()Ljava/lang/Throwable;");
    g_bs.futureAddListener = method(future, "addListener", "(Lio/netty/util/concurrent/GenericFutureListener;)Lio/netty/util/concurrent/Future;");
    g_bs.channelFutureListenerClass = cls("io.netty.channel.ChannelFutureListener");
    g_bs.closeOnFailure = g_bs.channelFutureListenerClass
        ? env->GetStaticFieldID(g_bs.channelFutureListenerClass, "CLOSE_ON_FAILURE", "Lio/netty/channel/ChannelFutureListener;") : nullptr;
    if (!g_bs.closeOnFailure) {
        LogTo("BServer: missing ChannelFutureListener.CLOSE_ON_FAILURE binding");
        ok = false;
        if (env->ExceptionCheck()) LogAndClearException(env, "BServer/failure listener binding");
    }
    g_bs.channelActive = method(channel, "isActive", "()Z");
    g_bs.channel = method(ctx, "channel", "()Lio/netty/channel/Channel;");
    g_bs.pipeline = method(channel, "pipeline", "()Lio/netty/channel/ChannelPipeline;");
    g_bs.write = method(channel, "writeAndFlush", "(Ljava/lang/Object;)Lio/netty/channel/ChannelFuture;");
    g_bs.close = method(channel, "close", "()Lio/netty/channel/ChannelFuture;");
    g_bs.config = method(channel, "config", "()Lio/netty/channel/ChannelConfig;");
    g_bs.autoRead = method(config, "setAutoRead", "(Z)Lio/netty/channel/ChannelConfig;");
    g_bs.eventLoop = method(channel, "eventLoop", "()Lio/netty/channel/EventLoop;");
    g_bs.execute = method(executor, "execute", "(Ljava/lang/Runnable;)V");
    g_bs.addLast = method(pipeline, "addLast", "(Ljava/lang/String;Lio/netty/channel/ChannelHandler;)Lio/netty/channel/ChannelPipeline;");
    g_bs.getHandler = method(pipeline, "get", "(Ljava/lang/String;)Lio/netty/channel/ChannelHandler;");
    g_bs.replace = method(pipeline, "replace", "(Ljava/lang/String;Ljava/lang/String;Lio/netty/channel/ChannelHandler;)Lio/netty/channel/ChannelHandler;");
    for (jclass c : {channel, ctx, pipeline, executor, config, future}) if (c) env->DeleteGlobalRef(c);
    g_bs.flow = staticObject("net.minecraft.network.protocol.PacketFlow", "SERVERBOUND", "Lnet/minecraft/network/protocol/PacketFlow;");
    g_bs.handshake = staticObject("net.minecraft.network.protocol.handshake.HandshakeProtocols", "SERVERBOUND", "Lnet/minecraft/network/ProtocolInfo;");
    g_bs.loginIn = staticObject("net.minecraft.network.protocol.login.LoginProtocols", "SERVERBOUND", "Lnet/minecraft/network/ProtocolInfo;");
    g_bs.loginOut = staticObject("net.minecraft.network.protocol.login.LoginProtocols", "CLIENTBOUND", "Lnet/minecraft/network/ProtocolInfo;");
    g_bs.configIn = staticObject("net.minecraft.network.protocol.configuration.ConfigurationProtocols", "SERVERBOUND", "Lnet/minecraft/network/ProtocolInfo;");
    g_bs.configOut = staticObject("net.minecraft.network.protocol.configuration.ConfigurationProtocols", "CLIENTBOUND", "Lnet/minecraft/network/ProtocolInfo;");
    g_bs.statusIn = staticObject("net.minecraft.network.protocol.status.StatusProtocols", "SERVERBOUND", "Lnet/minecraft/network/ProtocolInfo;");
    g_bs.statusOut = staticObject("net.minecraft.network.protocol.status.StatusProtocols", "CLIENTBOUND", "Lnet/minecraft/network/ProtocolInfo;");
    g_bs.loginIntent = staticObject("net.minecraft.network.protocol.handshake.ClientIntent", "LOGIN", "Lnet/minecraft/network/protocol/handshake/ClientIntent;");
    g_bs.transferIntent = staticObject("net.minecraft.network.protocol.handshake.ClientIntent", "TRANSFER", "Lnet/minecraft/network/protocol/handshake/ClientIntent;");
    g_bs.statusIntent = staticObject("net.minecraft.network.protocol.handshake.ClientIntent", "STATUS", "Lnet/minecraft/network/protocol/handshake/ClientIntent;");
    g_bs.intentClass = cls("net.minecraft.network.protocol.handshake.ClientIntentionPacket");
    g_bs.intention = method(g_bs.intentClass, "intention", "()Lnet/minecraft/network/protocol/handshake/ClientIntent;");
    g_bs.protocolVersion = method(g_bs.intentClass, "protocolVersion", "()I");
    g_bs.loginFinishedClass = cls("net.minecraft.network.protocol.login.ClientboundLoginFinishedPacket");
    g_bs.loginFinishedCtor = method(g_bs.loginFinishedClass, "<init>", "(Lcom/mojang/authlib/GameProfile;)V");
    g_bs.profileAccessor = method(g_bs.loginFinishedClass, "gameProfile", "()Lcom/mojang/authlib/GameProfile;");
    g_bs.startConfigurationPacket = staticObject("net.minecraft.network.protocol.game.ClientboundStartConfigurationPacket",
        "INSTANCE", "Lnet/minecraft/network/protocol/game/ClientboundStartConfigurationPacket;");
    g_bs.keepAliveClass = cls("net.minecraft.network.protocol.common.ClientboundKeepAlivePacket");
    g_bs.keepAliveCtor = method(g_bs.keepAliveClass, "<init>", "(J)V");
    g_bs.pongClass = cls("net.minecraft.network.protocol.ping.ClientboundPongResponsePacket");
    g_bs.pongCtor = method(g_bs.pongClass, "<init>", "(J)V");
    g_bs.statusResponseClass = cls("net.minecraft.network.protocol.status.ClientboundStatusResponsePacket");
    g_bs.statusResponseCtor = method(g_bs.statusResponseClass, "<init>", "(Lnet/minecraft/network/protocol/status/ServerStatus;)V");
    g_bs.serverStatusClass = cls("net.minecraft.network.protocol.status.ServerStatus");
    g_bs.serverStatusCtor = method(g_bs.serverStatusClass, "<init>", "(Lnet/minecraft/network/chat/Component;Ljava/util/Optional;Ljava/util/Optional;Ljava/util/Optional;Z)V");
    g_bs.versionClass = cls("net.minecraft.network.protocol.status.ServerStatus$Version");
    g_bs.versionCtor = method(g_bs.versionClass, "<init>", "(Ljava/lang/String;I)V");
    g_bs.componentClass = cls("net.minecraft.network.chat.Component");
    g_bs.literal = method(g_bs.componentClass, "literal", "(Ljava/lang/String;)Lnet/minecraft/network/chat/MutableComponent;", true);
    g_bs.optionalClass = cls("java.util.Optional");
    g_bs.optionalOf = method(g_bs.optionalClass, "of", "(Ljava/lang/Object;)Ljava/util/Optional;", true);
    g_bs.optionalEmpty = method(g_bs.optionalClass, "empty", "()Ljava/util/Optional;", true);
    g_bs.optionalPresent = method(g_bs.optionalClass, "isPresent", "()Z");
    g_bs.listClass = cls("java.util.List");
    g_bs.emptyList = method(g_bs.listClass, "of", "()Ljava/util/List;", true);
    g_bs.listSize = method(g_bs.listClass, "size", "()I");
    g_bs.listGet = method(g_bs.listClass, "get", "(I)Ljava/lang/Object;");
    g_bs.clientPacksClass = cls("net.minecraft.network.protocol.configuration.ClientboundSelectKnownPacks");
    g_bs.serverPacksClass = cls("net.minecraft.network.protocol.configuration.ServerboundSelectKnownPacks");
    g_bs.clientPacksCtor = method(g_bs.clientPacksClass, "<init>", "(Ljava/util/List;)V");
    g_bs.serverPacksCtor = method(g_bs.serverPacksClass, "<init>", "(Ljava/util/List;)V");
    g_bs.knownPacks = method(g_bs.serverPacksClass, "knownPacks", "()Ljava/util/List;");
    g_bs.registryClass = cls("net.minecraft.network.protocol.configuration.ClientboundRegistryDataPacket");
    g_bs.registryEntries = method(g_bs.registryClass, "entries", "()Ljava/util/List;");
    g_bs.entryClass = cls("net.minecraft.core.RegistrySynchronization$PackedRegistryEntry");
    g_bs.entryData = method(g_bs.entryClass, "data", "()Ljava/util/Optional;");
    return ok;
}

bool bindServer(JNIEnv* env, jobject loader) {
    env->PushLocalFrame(48);
    jclass groupClass = LoadClassInLoader(env, loader, "io.netty.channel.nio.NioEventLoopGroup");
    jclass bootstrapClass = LoadClassInLoader(env, loader, "io.netty.bootstrap.ServerBootstrap");
    jclass socketClass = LoadClassInLoader(env, loader, "io.netty.channel.socket.nio.NioServerSocketChannel");
    jclass addressClass = env->FindClass("java/net/InetSocketAddress");
    if (!groupClass || !bootstrapClass || !socketClass || !addressClass) {
        env->PopLocalFrame(nullptr); return false;
    }
    jobject group = env->NewObject(groupClass, env->GetMethodID(groupClass, "<init>", "()V"));
    jobject bootstrap = env->NewObject(bootstrapClass, env->GetMethodID(bootstrapClass, "<init>", "()V"));
    env->CallObjectMethod(bootstrap, env->GetMethodID(bootstrapClass, "group",
        "(Lio/netty/channel/EventLoopGroup;)Lio/netty/bootstrap/ServerBootstrap;"), group);
    env->CallObjectMethod(bootstrap, env->GetMethodID(bootstrapClass, "channel",
        "(Ljava/lang/Class;)Lio/netty/bootstrap/AbstractBootstrap;"), socketClass);
    jobject init = env->NewObject(g_bs.initClass, g_bs.initCtor);
    env->CallObjectMethod(bootstrap, env->GetMethodID(bootstrapClass, "childHandler",
        "(Lio/netty/channel/ChannelHandler;)Lio/netty/bootstrap/ServerBootstrap;"), init);
    jstring host = env->NewStringUTF("127.0.0.1");
    jobject address = env->NewObject(addressClass,
        env->GetMethodID(addressClass, "<init>", "(Ljava/lang/String;I)V"), host, (jint)25565);
    jobject future = env->CallObjectMethod(bootstrap, env->GetMethodID(bootstrapClass, "bind",
        "(Ljava/net/SocketAddress;)Lio/netty/channel/ChannelFuture;"), address);
    bool ok = future && !env->ExceptionCheck();
    if (ok) {
        jclass futureClass = env->GetObjectClass(future);
        env->CallObjectMethod(future, env->GetMethodID(futureClass, "sync", "()Lio/netty/channel/ChannelFuture;"));
        ok = !env->ExceptionCheck();
    }
    if (!ok) {
        LogAndClearException(env, "BServer/bind");
        jmethodID shutdown = env->GetMethodID(groupClass, "shutdownGracefully", "()Lio/netty/util/concurrent/Future;");
        if (shutdown) env->CallObjectMethod(group, shutdown);
        if (env->ExceptionCheck()) LogAndClearException(env, "BServer/group shutdown");
    }
    env->PopLocalFrame(nullptr);
    if (ok) LogTo("BServer: listening on 127.0.0.1:25565, Mojmap 1.21.8 protocol 772");
    return ok;
}
void selectA(JNIEnv* env, jobject ctx, bool transfer) {
    jobject ch = channelFor(env, ctx);
    if (!ch) return;
    if (g_bs.aChannel && env->IsSameObject(ch, g_bs.aChannel)) { env->DeleteLocalRef(ch); return; }
    jobject pipeline = env->CallObjectMethod(ch, g_bs.pipeline);
    jobject connection = pipelineHandler(env, pipeline, "packet_handler");
    if (!connection || !env->IsInstanceOf(connection, g_bs.connectionClass)) {
        LogTo("BServer: ignored intention on non-client Connection pipeline");
        if (connection) env->DeleteLocalRef(connection);
        env->DeleteLocalRef(pipeline); env->DeleteLocalRef(ch); return;
    }
    const bool firstA = !g_bs.aChannel && g_bs.session.aPhase == Phase::Closed;
    const bool waitingB = firstA && g_bs.bChannel && g_bs.session.bInbound == Phase::Login && !g_bs.loginSent;
    const bool keepB = g_bs.bChannel && g_bs.session.active() &&
                       (transfer || g_bs.session.aPhase == Phase::Play);
    if (!waitingB && !keepB) detachB(env);
    RelayFilter_ClearBypass(env);
    invalidateConfiguration(env);
    drop(env, g_bs.profile);
    drop(env, g_bs.aChannel);
    drop(env, g_bs.aConnection);
    g_bs.session.beginA(keepB);
    if (waitingB) g_bs.session.attachB();
    g_bs.aChannel = env->NewGlobalRef(ch);
    g_bs.aConnection = env->NewGlobalRef(connection);
    LogTo("BServer: selected LOGIN/TRANSFER A generation=%llu", (unsigned long long)g_bs.session.connectionGeneration);
    env->DeleteLocalRef(connection); env->DeleteLocalRef(pipeline); env->DeleteLocalRef(ch);
}
void observeA(JNIEnv* env, jobject packet) {
    std::string name = packetName(env, packet);
    if (ends(name, ".ClientboundBundlePacket")) {
        jclass bundleClass = env->GetObjectClass(packet);
        jmethodID subPackets = env->GetMethodID(bundleClass, "subPackets", "()Ljava/lang/Iterable;");
        if (!subPackets) {
            LogAndClearException(env, "BServer/bundle accessor");
            failB(env, "bundle accessor unavailable"); env->DeleteLocalRef(bundleClass); return;
        }
        jobject iterable = env->CallObjectMethod(packet, subPackets);
        jclass iterableClass = env->FindClass("java/lang/Iterable");
        jobject iterator = env->CallObjectMethod(iterable, env->GetMethodID(iterableClass, "iterator", "()Ljava/util/Iterator;"));
        jclass iteratorClass = env->FindClass("java/util/Iterator");
        jmethodID hasNext = env->GetMethodID(iteratorClass, "hasNext", "()Z");
        jmethodID next = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
        while (!env->ExceptionCheck() && env->CallBooleanMethod(iterator, hasNext)) {
            jobject sub = env->CallObjectMethod(iterator, next);
            observeA(env, sub);
            env->DeleteLocalRef(sub);
        }
        for (jobject ref : {(jobject)bundleClass, iterable, (jobject)iterableClass, iterator, (jobject)iteratorClass})
            if (ref) env->DeleteLocalRef(ref);
        return;
    }
    if (env->IsInstanceOf(packet, g_bs.loginFinishedClass)) {
        drop(env, g_bs.profile);
        jobject profile = env->CallObjectMethod(packet, g_bs.profileAccessor);
        if (profile) { g_bs.profile = env->NewGlobalRef(profile); env->DeleteLocalRef(profile); }
        schedulePump(env);
        return;
    }
    if (ends(name, ".ClientboundStartConfigurationPacket")) {
        // Clear the old registry/world before retaining anything in the new configuration.
        invalidateConfiguration(env);
        g_bs.session.beginConfiguration();
        if (g_bs.bChannel) {
            if (g_bs.session.waitingFinishAck || (g_bs.session.bInbound == Phase::Play &&
                g_bs.session.bOutbound == Phase::Play)) {
                g_bs.startPending = true;
                schedulePump(env);
            } else if (g_bs.session.waitingStartAck ||
                       (g_bs.session.bInbound == Phase::Configuration &&
                        g_bs.session.bOutbound == Phase::Configuration &&
                        !g_bs.session.waitingLoginAck)) {
                // B's configuration codec is already installed; retain it while the newest
                // A generation is journaled, then replay that generation from its beginning.
                g_bs.startPending = false;
                schedulePump(env);
            } else {
                failB(env, "backend reconfigured during an unsupported B protocol transition");
            }
        }
        return;
    }
    if (ends(name, ".ClientboundFinishConfigurationPacket")) {
        drop(env, g_bs.finish);
        g_bs.finish = env->NewGlobalRef(packet);
        g_bs.session.aFinishedConfiguration = true;
        g_bs.configurationComplete = g_bs.registries.completePayloads;
        if (!g_bs.configurationComplete) { failB(env, "registry snapshot contains omitted NBT"); return; }
        schedulePump(env);
        return;
    }
    if (ends(name, ".ClientboundKeepAlivePacket") || ends(name, ".ClientboundPingPacket") ||
        ends(name, ".ClientboundCookieRequestPacket") || ends(name, ".ClientboundTransferPacket")) return;
    if (ends(name, ".ClientboundDisconnectPacket")) {
        failB(env, "backend disconnected"); return;
    }
    bool configurationPacket = prefix(name, "net.minecraft.network.protocol.configuration.") ||
        ((prefix(name, "net.minecraft.network.protocol.common.") || prefix(name, "net.minecraft.network.protocol.cookie.")) &&
         g_bs.session.aPhase == Phase::Configuration);
    if (configurationPacket) {
        if (g_bs.configuration.size() >= kMaxConfigurationPackets) {
            g_bs.historyComplete = false;
            failB(env, "configuration journal overflow (no partial registry replay)"); return;
        }
        jobject replay = packet;
        if (env->IsInstanceOf(packet, g_bs.clientPacksClass)) {
            // B never depends on A's version-specific local data packs.
            jobject empty = env->CallStaticObjectMethod(g_bs.listClass, g_bs.emptyList);
            replay = empty ? env->NewObject(g_bs.clientPacksClass, g_bs.clientPacksCtor, empty) : nullptr;
            if (empty) env->DeleteLocalRef(empty);
            if (!replay || env->ExceptionCheck()) {
                g_bs.historyComplete = false;
                failB(env, "cannot create portable known-packs offer"); return;
            }
        } else if (env->IsInstanceOf(packet, g_bs.registryClass)) {
            jobject entries = env->CallObjectMethod(packet, g_bs.registryEntries);
            jint count = entries ? env->CallIntMethod(entries, g_bs.listSize) : -1;
            bool valid = count >= 0;
            for (jint i = 0; valid && i < count && !env->ExceptionCheck(); ++i) {
                jobject entry = env->CallObjectMethod(entries, g_bs.listGet, i);
                jobject data = entry ? env->CallObjectMethod(entry, g_bs.entryData) : nullptr;
                valid = data && env->CallBooleanMethod(data, g_bs.optionalPresent);
                if (data) env->DeleteLocalRef(data);
                if (entry) env->DeleteLocalRef(entry);
            }
            if (entries) env->DeleteLocalRef(entries);
            if (!g_bs.registries.registryEntry(valid && !env->ExceptionCheck())) {
                g_bs.historyComplete = false;
                LogTo("BServer: omitted registry NBT rejected; reconnect A after injection for full configuration");
                failB(env, "backend did not supply portable registry data"); return;
            }
        }
        g_bs.configuration.push_back({env->NewGlobalRef(replay), g_bs.session.connectionGeneration,
                                      g_bs.session.configurationGeneration});
        if (replay != packet) env->DeleteLocalRef(replay);
        schedulePump(env);
        return;
    }
    if (g_bs.session.aPhase != Phase::Play) return;
    if (ends(name, ".ClientboundRespawnPacket")) {
        clearPackets(env, g_bs.history);
        g_bs.historyComplete = false;
        g_cache.clear(env);
        // Respawn requires Login's world/player context, not a completed configuration ACK.
        if (g_bs.bChannel) {
            bool bootstrapReady = g_bs.bWorldInitialized;
            for (const auto& p : g_bs.play) {
                if (p.connection == g_bs.session.connectionGeneration &&
                    p.configuration == g_bs.session.configurationGeneration &&
                    ends(packetName(env, p.packet), ".ClientboundLoginPacket")) {
                    bootstrapReady = true;
                    break;
                }
            }
            if (!bootstrapReady) {
                failB(env, "world changed without an ordered B world bootstrap");
                return;
            }
            LogTo("BServer: retaining ordered respawn bootstrap=%s a=%d b_in=%d b_out=%d finish_ack=%d backlog=%llu",
                  g_bs.bWorldInitialized ? "written" : "queued", (int)g_bs.session.aPhase,
                  (int)g_bs.session.bInbound, (int)g_bs.session.bOutbound,
                  g_bs.session.waitingFinishAck ? 1 : 0, (unsigned long long)g_bs.play.size());
        }
    }
    if (!prefix(name, "net.minecraft.network.protocol.game.") &&
        !prefix(name, "net.minecraft.network.protocol.common.") &&
        !prefix(name, "net.minecraft.network.protocol.cookie.")) return;
    capturePlayCodecs(env);
    if (g_bs.historyComplete) {
        if (g_bs.history.size() >= kMaxPackets) {
            clearPackets(env, g_bs.history);
            g_bs.historyComplete = false;
            LogTo("BServer: full PLAY history cap reached; subsequent late joins will fail closed");
        } else g_bs.history.push_back({env->NewGlobalRef(packet), g_bs.session.connectionGeneration,
                                       g_bs.session.configurationGeneration});
    }
    if (g_bs.bChannel) {
        if (g_bs.play.size() >= kMaxPackets) { failB(env, "B PLAY backlog overflow"); return; }
        g_bs.play.push_back({env->NewGlobalRef(packet), g_bs.session.connectionGeneration,
                            g_bs.session.configurationGeneration});
        schedulePump(env);
    }
}
} // namespace

bool InstallBServer(JNIEnv* env) {
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    if (g_bs.bound) return true;
    jobject loader = GetMinecraftClassLoader(env, g_jvmti);
    if (!loader) return false;
    bool ok = cacheJavaRefs(env, loader);
    auto native = [](const char* name, const char* signature, void* address) {
        return JNINativeMethod{const_cast<char*>(name), const_cast<char*>(signature), address};
    };
    if (ok) {
        g_bs.initClass = defineNativeClass(env, loader, "io/netty/channel/ChannelInitializer", {
            native("initChannel", "(Lio/netty/channel/Channel;)V", reinterpret_cast<void*>(Native_ServerInit_initChannel))});
        g_bs.handlerClass = defineNativeClass(env, loader, "io/netty/channel/ChannelInboundHandlerAdapter", {
            native("channelActive", kContextDesc, reinterpret_cast<void*>(Native_BSide_channelActive)),
            native("channelInactive", kContextDesc, reinterpret_cast<void*>(Native_BSide_channelInactive)),
            native("channelRead", kReadDesc, reinterpret_cast<void*>(Native_BSide_channelRead)),
            native("exceptionCaught", "(Lio/netty/channel/ChannelHandlerContext;Ljava/lang/Throwable;)V", reinterpret_cast<void*>(Native_BSide_exceptionCaught))});
        g_bs.taskClass = defineNativeClass(env, loader, "java/lang/Thread", {
            native("run", "()V", reinterpret_cast<void*>(Native_Pump_run))});
        g_bs.chatTaskClass = defineNativeClass(env, loader, "java/lang/Thread", {
            native("run", "()V", reinterpret_cast<void*>(Native_Chat_run))});
        ok = g_bs.initClass && g_bs.handlerClass && g_bs.taskClass && g_bs.chatTaskClass;
    }
    if (ok) {
        g_bs.initCtor = env->GetMethodID(g_bs.initClass, "<init>", "()V");
        g_bs.handlerCtor = env->GetMethodID(g_bs.handlerClass, "<init>", "()V");
        g_bs.taskCtor = env->GetMethodID(g_bs.taskClass, "<init>", "()V");
        g_bs.chatTaskCtor = env->GetMethodID(g_bs.chatTaskClass, "<init>", "()V");
        ok = g_bs.initCtor && g_bs.handlerCtor && g_bs.taskCtor && g_bs.chatTaskCtor && bindServer(env, loader);
    }
    env->DeleteGlobalRef(loader);
    if (ok) {
        g_bs.bound = true;
        std::thread([] {
            JniAttach attach;
            if (!attach) return;
            for (;;) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
                attach.env->PushLocalFrame(16);
                schedulePump(attach.env);
                attach.env->PopLocalFrame(nullptr);
            }
        }).detach();
    }
    return ok;
}

bool BServer_IsBActive() {
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    return g_bs.session.active();
}
bool BServer_IsLoginIntention(JNIEnv* env, jobject packet) {
    if (!g_bs.intentClass || !packet || !env->IsInstanceOf(packet, g_bs.intentClass)) return false;
    jobject intent = env->CallObjectMethod(packet, g_bs.intention);
    bool login = intent && (env->IsSameObject(intent, g_bs.loginIntent) || env->IsSameObject(intent, g_bs.transferIntent));
    if (intent) env->DeleteLocalRef(intent);
    return login;
}
bool BServer_ShouldRoutePlayerPacket(const char* name) {
    if (!name) return false;
    constexpr const char* game = "net.minecraft.network.protocol.game.";
    if (std::strncmp(name, game, std::strlen(game)) != 0) return false;
    const char* leaf = name + std::strlen(game);
    static const char* const allowed[] = {
        "ServerboundCommandSuggestionPacket", "ServerboundAcceptTeleportationPacket", "ServerboundMovePlayerPacket", "ServerboundMovePlayerPacket$Pos",
        "ServerboundMovePlayerPacket$PosRot", "ServerboundMovePlayerPacket$Rot", "ServerboundMovePlayerPacket$StatusOnly",
        "ServerboundMoveVehiclePacket", "ServerboundPlayerInputPacket", "ServerboundPlayerActionPacket",
        "ServerboundPlayerCommandPacket", "ServerboundInteractPacket", "ServerboundUseItemPacket", "ServerboundUseItemOnPacket",
        "ServerboundSwingPacket", "ServerboundSetCarriedItemPacket", "ServerboundContainerClickPacket",
        "ServerboundContainerClosePacket", "ServerboundContainerButtonClickPacket", "ServerboundSetCreativeModeSlotPacket",
        "ServerboundPickItemFromBlockPacket", "ServerboundPickItemFromEntityPacket", "ServerboundPlayerAbilitiesPacket",
        "ServerboundClientCommandPacket", "ServerboundSignUpdatePacket", "ServerboundEditBookPacket",
        "ServerboundRecipeBookChangeSettingsPacket", "ServerboundRecipeBookSeenRecipePacket", "ServerboundPlaceRecipePacket",
        "ServerboundSelectTradePacket", "ServerboundSetBeaconPacket", "ServerboundRenameItemPacket",
        "ServerboundSetCommandBlockPacket", "ServerboundSetCommandMinecartPacket", "ServerboundSetStructureBlockPacket",
        "ServerboundSetJigsawBlockPacket", "ServerboundJigsawGeneratePacket", "ServerboundPaddleBoatPacket"
    };
    for (const char* candidate : allowed) if (std::strcmp(leaf, candidate) == 0) return true;
    return false;
}
void BServer_OnARead(JNIEnv* env, jobject ctx, jobject packet) {
    if (!g_bs.bound) return;
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    if (!isA(env, ctx)) return;
    env->PushLocalFrame(64);
    observeA(env, packet);
    if (env->ExceptionCheck()) { LogAndClearException(env, "BServer/A read"); failB(env, "A observation failed"); }
    env->PopLocalFrame(nullptr);
}
bool BServer_OnAWrite(JNIEnv* env, jobject ctx, jobject packet) {
    if (!g_bs.bound) return true;
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    if (BServer_IsLoginIntention(env, packet)) {
        jobject intent = env->CallObjectMethod(packet, g_bs.intention);
        const bool transfer = intent && env->IsSameObject(intent, g_bs.transferIntent);
        if (intent) env->DeleteLocalRef(intent);
        selectA(env, ctx, transfer);
    }
    if (!isA(env, ctx)) return true;
    std::string name = packetName(env, packet);
    if (ends(name, ".ServerboundLoginAcknowledgedPacket")) {
        g_bs.session.beginConfiguration();
        invalidateConfiguration(env);
    } else if (ends(name, ".ServerboundFinishConfigurationPacket")) {
        g_bs.session.aPhase = Phase::Play;
        capturePlayCodecs(env);
        schedulePump(env);
    } else if (g_bs.session.aPhase == Phase::Play) {
        capturePlayCodecs(env);
        schedulePump(env);
    }
    // A also owns chat signatures, session updates and last-seen ACKs; do not drop signed writes.
    return !(g_bs.session.active() && BServer_ShouldRoutePlayerPacket(name.c_str()));
}
jobject BServer_PrepareAWrite(JNIEnv* env, jobject ctx, jobject packet) {
    if (!g_bs.bound) return packet;
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    if (!isA(env, ctx) || !env->IsInstanceOf(packet, g_bs.serverPacksClass)) return packet;
    jobject packs = env->CallObjectMethod(packet, g_bs.knownPacks);
    jint count = packs ? env->CallIntMethod(packs, g_bs.listSize) : -1;
    if (count == 0) {
        if (packs) env->DeleteLocalRef(packs);
        return packet;
    }
    jobject empty = nullptr, wire = nullptr;
    if (count > 0 && sakura::RegistryReplay::replaceUpstreamSelection(static_cast<size_t>(count))) {
        // Keep A's immutable packet and local resource negotiation intact; only the wire opts out.
        empty = env->CallStaticObjectMethod(g_bs.listClass, g_bs.emptyList);
        if (empty) wire = env->NewObject(g_bs.serverPacksClass, g_bs.serverPacksCtor, empty);
    }
    if (packs) env->DeleteLocalRef(packs);
    if (empty) env->DeleteLocalRef(empty);
    if (!wire || env->ExceptionCheck()) {
        LogAndClearException(env, "BServer/upstream known packs");
        g_bs.historyComplete = false;
        g_bs.registries.registryEntry(false);
        failB(env, "cannot request full upstream registries");
        if (wire) env->DeleteLocalRef(wire);
        return nullptr; // Fail closed: never forward a negotiated non-empty selection.
    }
    LogTo("BServer: upstream known-packs selection %d -> 0; full registry NBT requested", (int)count);
    return wire;
}
void BServer_OnAInactive(JNIEnv* env, jobject ctx) {
    if (!g_bs.bound) return;
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    if (!isA(env, ctx)) return;
    detachB(env);
    invalidateConfiguration(env);
    RelayFilter_ClearBypass(env);
    drop(env, g_bs.aChannel); drop(env, g_bs.aConnection); drop(env, g_bs.profile);
    g_bs.session.disconnectA();
}
void BServer_SetTargetConnection(JNIEnv*, jobject) {
    // Selection is made only from an outgoing LOGIN/TRANSFER intention and its own pipeline.
}
void BServer_ForwardToB(JNIEnv* env, jobject packet) {
    std::lock_guard<std::recursive_mutex> lock(g_bs.mu);
    if (g_bs.aChannel) observeA(env, packet);
}
bool BServer_WaitForBConnected(int) { return BServer_IsBActive(); }
bool BServer_BlockAMainThreadUntilBConnected(JNIEnv*) { return false; }
bool BServer_TryCaptureLiveConnection(JNIEnv*) {
    LogTo("BServer: injection after A login is unsupported: no complete configuration/world journal");
    return false;
}
