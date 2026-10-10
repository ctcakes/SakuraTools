

#pragma once

#include "proxy.h"

#include <string>

struct RelayHandler {
    jclass    klass  = nullptr;
    jmethodID ctor   = nullptr;
    std::string internalName;
    std::string dotName;

    struct NettyRefs {
        jclass    contextCls           = nullptr;
        jmethodID pipelineMid          = nullptr;
        jmethodID fireChannelReadMid   = nullptr;
        jmethodID fireChannelInactiveMid = nullptr;
        jmethodID ctxWriteMid          = nullptr;
        jmethodID ctxWriteFlushMid     = nullptr;

        jclass    pipelineCls          = nullptr;
        jmethodID getMid          = nullptr;
        jmethodID namesMid = nullptr;
        jmethodID addBeforeMid         = nullptr;

        jclass    promiseCls           = nullptr;
        jmethodID promiseSetSuccessMid = nullptr;
    } netty;

    bool valid() const {
        return klass && ctor && netty.pipelineMid && netty.fireChannelReadMid &&
               netty.fireChannelInactiveMid && netty.ctxWriteMid && netty.getMid &&
               netty.namesMid && netty.addBeforeMid && netty.promiseSetSuccessMid;
    }
};

extern RelayHandler g_relay;

bool InstallRelayHandler(JNIEnv* env);

void RelayHandler_AttachToPipeline(JNIEnv* env, jobject ctx);

void RelayHandler_AttachToPipelineObject(JNIEnv* env, jobject pipeline);

void RelayFilter_MarkBypass(JNIEnv* env, jobject packet);

void RelayFilter_UnmarkBypass(JNIEnv* env, jobject packet);
void RelayFilter_ClearBypass(JNIEnv* env);
