// TCP bridge: owns RELAY_LISTEN_PORT so B can connect before Minecraft exists,
// and forwards each accepted connection to the in-game proxy once it comes up.
//
// The delay is deliberate.  A is launched straight into a server by its
// launcher, so there is no main-menu pause to connect B during; if B had to
// wait for the in-game listener, it would have to race A's configuration phase.
// Accepting first and forwarding later removes that race entirely.

#include "relay.h"

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct Conn {
    SOCKET        client;   // B
    SOCKET        upstream; // the in-game proxy
    volatile LONG refs;     // pump threads still running
} Conn;

typedef struct PumpArg {
    Conn*  conn;
    SOCKET from;
    SOCKET to;
} PumpArg;

static SOCKET         g_listen = INVALID_SOCKET;
static unsigned short g_upstreamPort = PROXY_UPSTREAM_PORT;

static void set_nodelay(SOCKET s) {
    BOOL on = TRUE;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (const char*)&on, sizeof(on));
}

static void release(Conn* c) {
    if (InterlockedDecrement(&c->refs) != 0) return;
    if (c->client   != INVALID_SOCKET) closesocket(c->client);
    if (c->upstream != INVALID_SOCKET) closesocket(c->upstream);
    free(c);
}

static DWORD WINAPI pump_thread(LPVOID param) {
    PumpArg* a = (PumpArg*)param;
    Conn* c = a->conn;
    char buf[32768];

    for (;;) {
        int n = recv(a->from, buf, (int)sizeof(buf), 0);
        if (n <= 0) break;

        int off = 0;
        while (off < n) {
            int w = send(a->to, buf + off, n - off, 0);
            if (w <= 0) goto done;
            off += w;
        }
    }

done:
    // Tearing both halves down is what wakes the opposite pump's recv().
    shutdown(c->client, SD_BOTH);
    shutdown(c->upstream, SD_BOTH);
    free(a);
    release(c);
    return 0;
}

static SOCKET connect_upstream(unsigned short port) {
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

static DWORD WINAPI conn_thread(LPVOID param) {
    Conn* c = (Conn*)param;

    // Wait for the in-game proxy.  A modded client takes a while to boot and be
    // injected into, so this budget is generous; B just sits in "connecting".
    SOCKET up = INVALID_SOCKET;
    for (int i = 0; i < 1800 && up == INVALID_SOCKET; ++i) {
        up = connect_upstream(g_upstreamPort);
        if (up == INVALID_SOCKET) Sleep(100);
    }
    if (up == INVALID_SOCKET) {
        fprintf(stdout, "relay: proxy never came up on 127.0.0.1:%u, dropping a client\n",
                g_upstreamPort);
        fflush(stdout);
        closesocket(c->client);
        free(c);
        return 0;
    }

    c->upstream = up;
    set_nodelay(c->client);
    set_nodelay(c->upstream);
    c->refs = 2;

    PumpArg* down = (PumpArg*)malloc(sizeof(PumpArg));
    PumpArg* up_  = (PumpArg*)malloc(sizeof(PumpArg));
    if (!down || !up_) {
        free(down); free(up_);
        shutdown(c->client, SD_BOTH);
        shutdown(c->upstream, SD_BOTH);
        c->refs = 0;
        release(c);
        return 0;
    }
    down->conn = c; down->from = c->client;   down->to = c->upstream;
    up_->conn  = c; up_->from  = c->upstream; up_->to  = c->client;

    HANDLE h1 = CreateThread(NULL, 0, pump_thread, down, 0, NULL);
    HANDLE h2 = CreateThread(NULL, 0, pump_thread, up_,  0, NULL);
    if (h1) CloseHandle(h1);
    if (h2) CloseHandle(h2);

    fprintf(stdout, "relay: client connected, bridging to 127.0.0.1:%u\n",
            g_upstreamPort);
    fflush(stdout);
    return 0;
}

static DWORD WINAPI accept_thread(LPVOID param) {
    (void)param;
    for (;;) {
        SOCKET client = accept(g_listen, NULL, NULL);
        if (client == INVALID_SOCKET) return 0;

        Conn* c = (Conn*)calloc(1, sizeof(Conn));
        if (!c) { closesocket(client); continue; }
        c->client   = client;
        c->upstream = INVALID_SOCKET;
        c->refs     = 1;

        HANDLE h = CreateThread(NULL, 0, conn_thread, c, 0, NULL);
        if (h) CloseHandle(h);
        else   { closesocket(client); free(c); }
    }
}

int relay_start(unsigned short listenPort, unsigned short upstreamPort) {
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        fprintf(stderr, "relay: WSAStartup failed\n");
        return 0;
    }
    g_upstreamPort = upstreamPort;

    g_listen = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (g_listen == INVALID_SOCKET) {
        fprintf(stderr, "relay: socket() failed (%d)\n", WSAGetLastError());
        return 0;
    }

    // No SO_REUSEADDR on purpose: if the port is already taken we want a loud
    // failure rather than a second listener silently fighting the first.
    struct sockaddr_in sa;
    ZeroMemory(&sa, sizeof(sa));
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_ANY);
    sa.sin_port        = htons(listenPort);

    if (bind(g_listen, (struct sockaddr*)&sa, sizeof(sa)) != 0) {
        fprintf(stderr, "relay: bind 0.0.0.0:%u failed (%d) - is another "
                        "instance already running?\n", listenPort, WSAGetLastError());
        closesocket(g_listen);
        g_listen = INVALID_SOCKET;
        return 0;
    }
    if (listen(g_listen, SOMAXCONN) != 0) {
        fprintf(stderr, "relay: listen failed (%d)\n", WSAGetLastError());
        closesocket(g_listen);
        g_listen = INVALID_SOCKET;
        return 0;
    }

    HANDLE h = CreateThread(NULL, 0, accept_thread, NULL, 0, NULL);
    if (!h) {
        fprintf(stderr, "relay: could not start accept thread\n");
        closesocket(g_listen);
        g_listen = INVALID_SOCKET;
        return 0;
    }
    CloseHandle(h);
    return 1;
}
