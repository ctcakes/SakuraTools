@echo off
REM ---------------------------------------------------------------------------
REM Builds the three artefacts with MSVC:
REM   build\MinecraftProxy_msvc.dll   the proxy itself (reflectively injected)
REM   build\reflective_injector.exe   reflective injector, waits for the client
REM   build\starain_inject.dll        alternate injector DLL
REM
REM Override the toolchain location with VCVARS if Visual Studio lives elsewhere.
REM ---------------------------------------------------------------------------
setlocal enabledelayedexpansion

set "ROOT=%~dp0.."
pushd "%ROOT%"

if not defined VCVARS set "VCVARS=D:\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat"
if not exist "%VCVARS%" (
    echo [!] vcvars64.bat not found at "%VCVARS%"
    echo     set VCVARS=C:\path\to\vcvars64.bat and re-run
    exit /b 1
)

call "%VCVARS%" >nul 2>&1
if errorlevel 1 ( echo [!] vcvars64.bat failed & exit /b 1 )

if not exist build mkdir build
if not exist build\inj_ mkdir build\inj_
if not exist build\si_ mkdir build\si_

set "DEFS=/DWIN_X64 /DREFLECTIVEDLLINJECTION_CUSTOM_DLLMAIN /DWIN32_LEAN_AND_MEAN /DNOMINMAX /D_CRT_SECURE_NO_WARNINGS"
set "INCS=/I native /I native\include"
set "CFLAGS=/nologo /O2 /MT /W3 /wd4996"
set "CXXFLAGS=/nologo /O2 /MT /W3 /wd4996 /EHsc /std:c++17"

echo === MinecraftProxy_msvc.dll ===
cl %CFLAGS% %CXXFLAGS% %DEFS% %INCS% ^
   /Fobuild\ /Fd:build\proxy.pdb ^
   /LD ^
   native\ReflectiveLoader.c ^
   native\loader.cpp native\env.cpp native\random_name.cpp ^
   native\classfile.cpp native\class_edit.cpp ^
   native\trampolines.cpp native\relay_handler.cpp ^
   native\connection_hook.cpp native\world_cache.cpp native\b_server.cpp ^
   /link /OUT:build\MinecraftProxy_msvc.dll ^
   psapi.lib ws2_32.lib kernel32.lib user32.lib advapi32.lib
if errorlevel 1 ( echo [!] proxy build failed & popd & exit /b 1 )

echo === reflective_injector.exe ===
cl %CFLAGS% /DWIN32_LEAN_AND_MEAN %DEFS% /I injector ^
   /Fobuild\inj_\ /Fd:build\injector.pdb ^
   injector\cli.c injector\Inject.c injector\LoadLibraryR.c injector\GetProcAddressR.c ^
   /link /OUT:build\reflective_injector.exe advapi32.lib iphlpapi.lib kernel32.lib user32.lib
if errorlevel 1 ( echo [!] injector build failed & popd & exit /b 1 )

echo === starain_inject.dll ===
cl %CFLAGS% %CXXFLAGS% %DEFS% %INCS% /I injector ^
   /Fobuild\si_\ /Fd:build\starain.pdb ^
   /LD ^
   native\jni_inject.cpp injector\Inject.c injector\LoadLibraryR.c injector\GetProcAddressR.c ^
   /link /OUT:build\starain_inject.dll kernel32.lib user32.lib
if errorlevel 1 ( echo [!] starain build failed & popd & exit /b 1 )

REM Reflective injection never runs the PE's TLS callbacks, so a statically
REM linked MSVC DLL will crash on first use of its CRT unless the TLS data
REM directory is zeroed.  See scripts/strip_tls.py.
echo === strip TLS ===
python "%~dp0strip_tls.py" build\MinecraftProxy_msvc.dll
if errorlevel 1 (
    echo     [i] skipped ^(python or pefile unavailable^) - see scripts\strip_tls.py
    echo     [!] verify the DLL has no TLS directory before injecting it
)

echo.
echo === done ===
dir /b build
popd
endlocal
