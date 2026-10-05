

#pragma once

#include "proxy.h"

bool InstallBServer(JNIEnv* env);

void BServer_SetTargetConnection(JNIEnv* env, jobject connection);

bool BServer_IsBActive();

// True while B is either waiting for (AwaitConfiguration) or receiving (Play)
// A's mirrored stream -- i.e. whenever the relay should hand packets over.
bool BServer_ShouldMirror();

bool BServer_IsLoginIntention(JNIEnv* env, jobject packet);

bool BServer_WaitForBConnected(int timeoutMs);

bool BServer_BlockAMainThreadUntilBConnected(JNIEnv* env);

// aCtx is A's netty ChannelHandlerContext, needed to lift A's PLAY
// ProtocolInfo (registry access included) when B enters PLAY.
void BServer_ForwardToB(JNIEnv* env, jobject aCtx, jobject packet);

bool BServer_TryCaptureLiveConnection(JNIEnv* env);
