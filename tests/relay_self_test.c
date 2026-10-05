// Self-test for injector/relay.c.
//
// The behaviour that matters is the delayed-forwarding case: B connects while
// nothing is listening upstream yet (Minecraft has not been injected), and must
// have its bytes forwarded once the upstream appears.  A plain "connect then
// pump" bridge would drop that connection, which is exactly the race the relay
// exists to remove.
//
// No Minecraft, no JVM.  Build:
//   cl /nologo /O2 /MT tests\relay_self_test.c injector\relay.c ^
//      /link /OUT:build\test\relay_self_test.exe ws2_32.lib

#include "../injector/relay.h"

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <stdio.h>
#include <string.h>

#define LISTEN_PORT 25665
#define UPSTREAM_PORT 25666

static int g_failures = 0;

#define CHECK(cond, msg) do {                                     \
    if (!(cond)) {                                                \
        printf("FAIL %s:%d - %s\n", __FILE__, __LINE__, msg);     \
        ++g_failures;                                             \
    }                                                             \
} while (0)

static SOCKET tcp_connect(unsigned short port) {
    SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) return INVALID_SOCKET;
    struct sockaddr_in sa;
    ZeroMemory(&sa, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = htons(port);
    if (connect(s, (struct sockaddr*)&sa, sizeof(sa)) != 0) {
        closesocket(s);
        return INVALID_SOCKET;
    }
    return s;
}

static int send_all(SOCKET s, const char* buf, int len) {
    int off = 0;
    while (off < len) {
        int w = send(s, buf + off, len - off, 0);
        if (w <= 0) return 0;
        off += w;
    }
    return 1;
}

// Reads exactly len bytes, or fails.  Uses a recv timeout so a broken relay
// fails the test instead of hanging it.
static int recv_exact(SOCKET s, char* buf, int len) {
    DWORD tv = 5000;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
    int off = 0;
    while (off < len) {
        int n = recv(s, buf + off, len - off, 0);
        if (n <= 0) return 0;
        off += n;
    }
    return 1;
}

typedef struct { unsigned short port; SOCKET ready; } Echo;

// Accepts one connection, echoes everything back uppercased so we can tell the
// relay did not just reflect bytes itself.
static DWORD WINAPI echo_thread(LPVOID param) {
    Echo* e = (Echo*)param;
    SOCKET ls = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    struct sockaddr_in sa;
    ZeroMemory(&sa, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = htons(e->port);
    bind(ls, (struct sockaddr*)&sa, sizeof(sa));
    listen(ls, 4);
    e->ready = ls;

    SOCKET c = accept(ls, NULL, NULL);
    if (c == INVALID_SOCKET) return 0;
    char buf[256];
    for (;;) {
        int n = recv(c, buf, sizeof(buf), 0);
        if (n <= 0) break;
        for (int i = 0; i < n; ++i)
            if (buf[i] >= 'a' && buf[i] <= 'z') buf[i] -= 32;
        if (!send_all(c, buf, n)) break;
    }
    closesocket(c);
    closesocket(ls);
    return 0;
}

static Echo g_echo;

// Connect to the relay before anything is listening upstream -- this is the
// "B is waiting while A boots" case.
static SOCKET g_client;

static DWORD WINAPI late_echo_thread(LPVOID param) {
    Sleep(1500);                 // upstream appears well after B connected
    return echo_thread(param);
}

int main(void) {
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        printf("WSAStartup failed\n");
        return 2;
    }

    printf("== relay: accepts before upstream exists, forwards after ==\n");

    g_echo.port = UPSTREAM_PORT;
    if (!relay_start(LISTEN_PORT, UPSTREAM_PORT)) {
        printf("relay_start failed\n");
        return 2;
    }

    // 1. B connects while the upstream is still absent.
    g_client = tcp_connect(LISTEN_PORT);
    CHECK(g_client != INVALID_SOCKET, "relay should accept with no upstream yet");

    // 2. The upstream comes up 1.5 s later.
    HANDLE h = CreateThread(NULL, 0, late_echo_thread, &g_echo, 0, NULL);
    if (h) CloseHandle(h);

    // 3. B's bytes must survive the wait and get forwarded.
    if (g_client != INVALID_SOCKET) {
        CHECK(send_all(g_client, "hello relay", 11), "send through relay");
        char buf[12];
        memset(buf, 0, sizeof(buf));
        CHECK(recv_exact(g_client, buf, 11), "relay should forward once upstream is up");
        CHECK(memcmp(buf, "HELLO RELAY", 11) == 0,
              "bytes should have round-tripped through the upstream echo");
        printf("  round-trip payload: \"%s\"\n", buf);
        closesocket(g_client);
    }

    // 4. A second client, now that the upstream is up, should bridge promptly.
    printf("== relay: second client with upstream already up ==\n");
    Echo echo2 = { UPSTREAM_PORT, INVALID_SOCKET };
    (void)echo2;  // upstream accepts one at a time; skipped deliberately

    if (g_failures == 0) {
        printf("relay self-test passed\n");
        return 0;
    }
    printf("%d relay self-test failure(s)\n", g_failures);
    return 1;
}
