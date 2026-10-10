#pragma once

#include "proxy.h"

bool InstallBServer(JNIEnv* env);
void BServer_SetTargetConnection(JNIEnv* env, jobject connection);
bool BServer_IsBActive();
bool BServer_IsLoginIntention(JNIEnv* env, jobject packet);
bool BServer_WaitForBConnected(int timeoutMs);
bool BServer_BlockAMainThreadUntilBConnected(JNIEnv* env);
void BServer_ForwardToB(JNIEnv* env, jobject packet);
bool BServer_TryCaptureLiveConnection(JNIEnv* env);

void BServer_OnARead(JNIEnv* env, jobject ctx, jobject packet);
// true: allow the original A write; false: consume and complete its promise.
bool BServer_OnAWrite(JNIEnv* env, jobject ctx, jobject packet);
// Returns the borrowed original or a new local reference for this wire write only.
jobject BServer_PrepareAWrite(JNIEnv* env, jobject ctx, jobject packet);
void BServer_OnAInactive(JNIEnv* env, jobject ctx);
bool BServer_ShouldRoutePlayerPacket(const char* className);
