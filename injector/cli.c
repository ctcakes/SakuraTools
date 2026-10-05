

#include <winsock2.h>
#include <windows.h>
#include <shellapi.h>
#include <iphlpapi.h>
#include <tlhelp32.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>

#include "relay.h"

BOOL DoInject(DWORD dwProcessId, const char* cpDllFile,
              char* outMessage, int maxLen);

// Substrings of the Minecraft window title to look for.  Several are matched at
// once because the title changes as the client boots: during NeoForge/ModLauncher
// loading it mentions "neoforge", and only later does the launcher's own name
// ("KKCraft Client @ ...") appear.  Matching the earliest one injects sooner,
// which matters because B can only be held in the relay for 30 s.
#define MAX_TITLE_NEEDLES 8
static wchar_t g_title_needles[MAX_TITLE_NEEDLES][128];
static int     g_title_needle_count = 0;
static wchar_t g_title_display[512];

// The argument as the user wrote it ("neoforge,KKCraft"), kept verbatim so the
// elevated re-launch can forward it.  g_title_display is the human-readable
// rendering and must never be passed on: the child would split it on commas and
// end up searching for the literal text "neoforge or KKCraft".
static char g_title_arg[512] = "neoforge,KKCraft";

static void set_title_needles(const char* utf8) {
    g_title_needle_count = 0;
    g_title_display[0] = 0;

    if (utf8 == NULL) return;

    strncpy(g_title_arg, utf8, sizeof(g_title_arg) - 1);
    g_title_arg[sizeof(g_title_arg) - 1] = 0;

    if (utf8[0] == 0) return;   // empty string => match any Java window

    char buf[512];
    strncpy(buf, utf8, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = 0;

    for (char* tok = strtok(buf, ","); tok && g_title_needle_count < MAX_TITLE_NEEDLES;
         tok = strtok(NULL, ",")) {
        while (*tok == ' ') ++tok;
        if (!*tok) continue;
        wchar_t* dst = g_title_needles[g_title_needle_count];
        int n = MultiByteToWideChar(CP_UTF8, 0, tok, -1, dst, 128);
        if (n <= 0) continue;
        if (g_title_display[0]) wcscat(g_title_display, L" or ");
        wcscat(g_title_display, dst);
        ++g_title_needle_count;
    }
}

static int usage(const char* argv0) {
    fprintf(stderr,
            "usage: %s <dll-path> [window-title-substrings]\n"
            "  Comma-separated title substrings, matched in order of nothing in\n"
            "  particular - any match wins.  Default \"neoforge,KKCraft\", which\n"
            "  catches the window as early as the NeoForge loading screen.\n"
            "  Pass \"\" to match any visible Java window.\n", argv0);
    return 2;
}

typedef struct ProcessChoice {
    DWORD pid;
    ULONGLONG created;
} ProcessChoice;

// Injecting means OpenProcess(PROCESS_ALL_ACCESS) + CreateRemoteThread on
// someone else's process.  If the game was started from an elevated launcher
// that fails with ERROR_ACCESS_DENIED, so we re-launch ourselves through the
// UAC prompt rather than making the user do it by hand.
static BOOL is_elevated(void) {
    HANDLE token = NULL;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token)) return FALSE;
    TOKEN_ELEVATION elevation;
    DWORD len = 0;
    BOOL ok = GetTokenInformation(token, TokenElevation, &elevation,
                                  sizeof(elevation), &len);
    CloseHandle(token);
    return ok && elevation.TokenIsElevated;
}

// needleUtf8 is the raw command-line argument, NOT the display string.
static BOOL relaunch_elevated(const char* dll, const char* needleUtf8) {
    wchar_t exe[MAX_PATH];
    if (GetModuleFileNameW(NULL, exe, MAX_PATH) == 0) return FALSE;

    wchar_t wdll[MAX_PATH];
    if (MultiByteToWideChar(CP_UTF8, 0, dll, -1, wdll, MAX_PATH) <= 0) return FALSE;

    wchar_t wneedle[512];
    if (MultiByteToWideChar(CP_UTF8, 0, needleUtf8, -1, wneedle,
                            (int)(sizeof(wneedle) / sizeof(wneedle[0]))) <= 0)
        wneedle[0] = 0;

    // Quote both arguments; a launcher directory with a space is common enough,
    // and an empty needle (match any window) must survive as an empty "" arg.
    wchar_t params[MAX_PATH * 2 + 16];
    _snwprintf(params, sizeof(params) / sizeof(params[0]), L"\"%ls\" \"%ls\"",
               wdll, wneedle);

    SHELLEXECUTEINFOW sei;
    ZeroMemory(&sei, sizeof(sei));
    sei.cbSize = sizeof(sei);
    sei.fMask = SEE_MASK_NOCLOSEPROCESS;
    sei.lpVerb = L"runas";
    sei.lpFile = exe;
    sei.lpParameters = params;
    sei.nShow = SW_SHOWNORMAL;

    if (!ShellExecuteExW(&sei)) {
        DWORD err = GetLastError();
        if (err == ERROR_CANCELLED)
            fprintf(stderr, "UAC prompt was declined.\n");
        else
            fprintf(stderr, "could not re-launch elevated (error %lu).\n", err);
        return FALSE;
    }
    if (sei.hProcess) CloseHandle(sei.hProcess);
    return TRUE;
}

static BOOL is_java_process(DWORD pid) {
    BOOL found = FALSE;
    HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
    if (snapshot == INVALID_HANDLE_VALUE) return FALSE;

    PROCESSENTRY32 entry;
    ZeroMemory(&entry, sizeof(entry));
    entry.dwSize = sizeof(entry);
    if (Process32First(snapshot, &entry)) {
        do {
            if (entry.th32ProcessID == pid) {
                found = _stricmp(entry.szExeFile, "java.exe") == 0 ||
                        _stricmp(entry.szExeFile, "javaw.exe") == 0;
                break;
            }
        } while (Process32Next(snapshot, &entry));
    }
    CloseHandle(snapshot);
    return found;
}

static ULONGLONG process_creation_time(DWORD pid) {
    ULONGLONG value = 0;
    HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
    if (!process) return 0;

    FILETIME created, exited, kernel, user;
    if (GetProcessTimes(process, &created, &exited, &kernel, &user)) {
        ULARGE_INTEGER time;
        time.LowPart = created.dwLowDateTime;
        time.HighPart = created.dwHighDateTime;
        value = time.QuadPart;
    }
    CloseHandle(process);
    return value;
}

static BOOL title_matches(const wchar_t* title) {
    if (g_title_needle_count == 0) return TRUE;   // no needles => any Java window
    for (int i = 0; i < g_title_needle_count; ++i)
        if (wcsstr(title, g_title_needles[i]) != NULL) return TRUE;
    return FALSE;
}

static BOOL CALLBACK find_mc_window(HWND window, LPARAM param) {
    ProcessChoice* choice = (ProcessChoice*)param;
    if (!IsWindowVisible(window)) return TRUE;

    wchar_t title[512];
    if (GetWindowTextW(window, title, (int)(sizeof(title) / sizeof(title[0]))) <= 0)
        return TRUE;
    if (!title_matches(title)) return TRUE;

    DWORD pid = 0;
    GetWindowThreadProcessId(window, &pid);
    if (!pid || !is_java_process(pid)) return TRUE;

    ULONGLONG created = process_creation_time(pid);
    if (!choice->pid || created > choice->created) {
        choice->pid = pid;
        choice->created = created;
    }
    return TRUE;
}

static DWORD find_mc_process(void) {
    ProcessChoice choice;
    ZeroMemory(&choice, sizeof(choice));
    EnumWindows(find_mc_window, (LPARAM)&choice);
    return choice.pid;
}

static DWORD wait_for_mc_process(void) {
    if (g_title_needle_count > 0)
        fprintf(stdout, "waiting for a visible Java window whose title contains \"%ls\"...\n",
                g_title_display);
    else
        fprintf(stdout, "waiting for any visible Java window...\n");
    fflush(stdout);
    for (;;) {
        DWORD pid = find_mc_process();
        if (pid) return pid;
        Sleep(50);
    }
}

static unsigned short network_port_to_host(DWORD value) {
    unsigned short port = (unsigned short)value;
    return (unsigned short)((port >> 8) | (port << 8));
}

static BOOL proxy_listener_ready(DWORD pid) {
    DWORD size = 0;
    DWORD rc = GetExtendedTcpTable(NULL, &size, FALSE, AF_INET,
                                   TCP_TABLE_OWNER_PID_LISTENER, 0);
    if (rc != ERROR_INSUFFICIENT_BUFFER || size == 0) return FALSE;

    PMIB_TCPTABLE_OWNER_PID table =
        (PMIB_TCPTABLE_OWNER_PID)HeapAlloc(GetProcessHeap(), 0, size);
    if (!table) return FALSE;
    rc = GetExtendedTcpTable(table, &size, FALSE, AF_INET,
                             TCP_TABLE_OWNER_PID_LISTENER, 0);
    BOOL ready = FALSE;
    if (rc == NO_ERROR) {
        for (DWORD i = 0; i < table->dwNumEntries; ++i) {
            MIB_TCPROW_OWNER_PID* row = &table->table[i];
            if (row->dwOwningPid == pid &&
                network_port_to_host(row->dwLocalPort) == PROXY_UPSTREAM_PORT) {
                ready = TRUE;
                break;
            }
        }
    }
    HeapFree(GetProcessHeap(), 0, table);
    return ready;
}

static BOOL wait_for_proxy_listener(DWORD pid) {
    fprintf(stdout, "waiting for PID %lu to listen on 127.0.0.1:%u...\n",
            pid, PROXY_UPSTREAM_PORT);
    fflush(stdout);
    for (int elapsed = 0; elapsed < 30000; elapsed += 50) {
        if (proxy_listener_ready(pid)) return TRUE;
        Sleep(50);
    }
    return FALSE;
}

int main(int argc, char** argv) {
    if (argc < 2 || argc > 3) return usage(argv[0]);

    unsigned long pid = 0;
    const char* dll = argv[1];

    // Default catches the window as early as the NeoForge loading screen, which
    // is well before the launcher renames it to "KKCraft Client @ ...".
    set_title_needles(argc == 3 ? argv[2] : "neoforge,KKCraft");

    if (GetFileAttributesA(dll) == INVALID_FILE_ATTRIBUTES) {
        fprintf(stderr, "dll not found: %s\n", dll);
        return 2;
    }

    // Elevate before binding: the elevated instance owns the relay port for its
    // whole lifetime, so if we bound it here the re-launched child could not.
    if (!is_elevated()) {
        fprintf(stdout, "not running as administrator - requesting elevation (UAC)...\n");
        fflush(stdout);
        if (relaunch_elevated(dll, g_title_arg)) {
            fprintf(stdout, "continuing in the elevated window.\n");
            return 0;
        }
        fprintf(stderr, "continuing without elevation (injection will likely fail "
                        "with access denied).\n");
    }

    // Take the port B connects to *now*, before Minecraft is even up.  The
    // launcher drops A straight into a server, so B has no window to connect
    // during; accepting it here and forwarding later is what makes the ordering
    // deterministic.
    if (!relay_start(RELAY_LISTEN_PORT, PROXY_UPSTREAM_PORT)) {
        return 3;
    }
    fprintf(stdout,
            "listening on 0.0.0.0:%u - connect the second client now if you like;\n"
            "it will wait until the in-game proxy is ready.\n", RELAY_LISTEN_PORT);
    fflush(stdout);

    pid = wait_for_mc_process();
    fprintf(stdout, "matched Java window (title contains \"%ls\"), PID %lu\n",
            g_title_needle_count > 0 ? g_title_display : L"*", pid);
    // A client that connects now sits in the relay.  It cannot be kept there
    // indefinitely -- the game's own ReadTimeoutHandler is 30 s and nothing can
    // be sent to it before the in-game proxy exists -- so this is the moment to
    // say so, rather than as soon as the relay came up.
    fprintf(stdout,
            "connect the second client to 127.0.0.1:%u NOW if you have not already "
            "(it has ~30 s before its own read timeout fires)\n", RELAY_LISTEN_PORT);
    fflush(stdout);

    char msg[1024];
    BOOL ok = DoInject((DWORD)pid, dll, msg, (int)sizeof(msg));
    fprintf(stdout, "%s\n", msg);
    if (!ok) return 1;
    // The relay already holds 25565; what we wait for now is the in-game proxy
    // on its own loopback port, which is what unblocks any client already
    // waiting in the relay.
    if (!wait_for_proxy_listener((DWORD)pid)) {
        fprintf(stderr, "injection completed, but the in-game proxy was not ready after 30 seconds\n");
        return 4;
    }
    fprintf(stdout, "in-game proxy ready - relay is forwarding\n");

    // The bridge lives in *this* process, so returning here would close 25565
    // and cut off every client the relay is holding -- which the second client
    // sees as being disconnected part-way through joining.  Stay resident until
    // the game goes away or the user stops us.
    fprintf(stdout,
            "\n"
            "  relay running:  0.0.0.0:%u  ->  127.0.0.1:%u\n"
            "  Keep this window open - closing it drops the second client.\n"
            "  Press Ctrl+C to stop.\n"
            "\n", RELAY_LISTEN_PORT, PROXY_UPSTREAM_PORT);
    fflush(stdout);

    HANDLE game = OpenProcess(SYNCHRONIZE, FALSE, (DWORD)pid);
    if (game) {
        WaitForSingleObject(game, INFINITE);
        fprintf(stdout, "game process exited - shutting down the relay.\n");
        CloseHandle(game);
    } else {
        for (;;) Sleep(1000);   // cannot watch the game; run until killed
    }
    return 0;
}
