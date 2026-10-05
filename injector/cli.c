

#include <winsock2.h>
#include <windows.h>
#include <shellapi.h>
#include <iphlpapi.h>
#include <tlhelp32.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>

BOOL DoInject(DWORD dwProcessId, const char* cpDllFile,
              char* outMessage, int maxLen);

// Substring of the Minecraft window title to look for.  Every launcher titles
// the window differently ("KKCraft Client @ ...", "布吉岛", "Minecraft 1.21.8"),
// so this is overridable on the command line rather than baked in.
static wchar_t g_title_needle[256] = L"KKCraft";

static void set_title_needle(const char* utf8) {
    int n = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, g_title_needle,
                                (int)(sizeof(g_title_needle) / sizeof(g_title_needle[0])));
    if (n <= 0) g_title_needle[0] = 0;
}

static int usage(const char* argv0) {
    fprintf(stderr,
            "usage: %s <dll-path> [window-title-substring]\n"
            "  window-title-substring defaults to \"KKCraft\";\n"
            "  pass \"\" to match any visible Java window.\n", argv0);
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

static BOOL relaunch_elevated(const char* dll, const wchar_t* needle) {
    wchar_t exe[MAX_PATH];
    if (GetModuleFileNameW(NULL, exe, MAX_PATH) == 0) return FALSE;

    wchar_t wdll[MAX_PATH];
    if (MultiByteToWideChar(CP_UTF8, 0, dll, -1, wdll, MAX_PATH) <= 0) return FALSE;

    // Quote both paths; a launcher directory with a space is common enough.
    wchar_t params[MAX_PATH * 2 + 16];
    _snwprintf(params, sizeof(params) / sizeof(params[0]), L"\"%ls\" \"%ls\"",
               wdll, needle);

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
    if (g_title_needle[0] == 0) return TRUE;
    return wcsstr(title, g_title_needle) != NULL;
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
    if (g_title_needle[0])
        fprintf(stdout, "waiting for a visible Java window whose title contains \"%ls\"...\n",
                g_title_needle);
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
                network_port_to_host(row->dwLocalPort) == 25565) {
                ready = TRUE;
                break;
            }
        }
    }
    HeapFree(GetProcessHeap(), 0, table);
    return ready;
}

static BOOL wait_for_proxy_listener(DWORD pid) {
    fprintf(stdout, "waiting for PID %lu to listen on 127.0.0.1:25565...\n", pid);
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

    if (argc == 3) set_title_needle(argv[2]);

    if (GetFileAttributesA(dll) == INVALID_FILE_ATTRIBUTES) {
        fprintf(stderr, "dll not found: %s\n", dll);
        return 2;
    }

    // Elevate up front rather than waiting for the first ERROR_ACCESS_DENIED:
    // the elevated instance then does the window-waiting too, so the user sees
    // exactly one UAC prompt.  The child is already elevated, so it will not
    // re-enter this branch.
    if (!is_elevated()) {
        fprintf(stdout, "not running as administrator - requesting elevation (UAC)...\n");
        fflush(stdout);
        if (relaunch_elevated(dll, g_title_needle)) {
            fprintf(stdout, "continuing in the elevated window.\n");
            return 0;
        }
        fprintf(stderr, "continuing without elevation (injection will likely fail "
                        "with access denied).\n");
    }

    pid = wait_for_mc_process();
    fprintf(stdout, "matched Java window (title contains \"%ls\"), PID %lu\n",
            g_title_needle[0] ? g_title_needle : L"*", pid);

    char msg[1024];
    BOOL ok = DoInject((DWORD)pid, dll, msg, (int)sizeof(msg));
    fprintf(stdout, "%s\n", msg);
    if (!ok) return 1;
    if (!wait_for_proxy_listener((DWORD)pid)) {
        fprintf(stderr, "injection completed, but the proxy listener was not ready after 30 seconds\n");
        return 4;
    }
    fprintf(stdout, "proxy listener ready\n");
    return 0;
}
