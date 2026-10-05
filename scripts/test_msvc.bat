@echo off
REM ---------------------------------------------------------------------------
REM Bytecode self-test for the class-file writer (classfile.cpp) and the
REM rewriter (class_edit.cpp).  Needs nothing from Minecraft -- the fixtures in
REM tests\java are small stubs -- but does need a JDK on PATH for javac/java.
REM
REM This is the Windows equivalent of scripts\test.sh, which hardcodes a macOS
REM zig toolchain.
REM ---------------------------------------------------------------------------
setlocal

set "ROOT=%~dp0.."
pushd "%ROOT%"

if not defined VCVARS set "VCVARS=D:\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat"
if not exist "%VCVARS%" ( echo [!] vcvars64.bat not found; set VCVARS=... & popd & exit /b 1 )
call "%VCVARS%" >nul 2>&1

if not exist build\test mkdir build\test
set "BUILD=%ROOT%\build\test"

set "CFLAGS=/nologo /O2 /MT /W3 /wd4996 /EHsc /std:c++17 /I native"

echo ==[ edit_self_test ]==
cl %CFLAGS% /Fobuild\test\ ^
   tests\edit_self_test.cpp native\classfile.cpp native\class_edit.cpp ^
   /link /OUT:build\test\edit_self_test.exe
if errorlevel 1 ( echo [!] build failed & popd & exit /b 1 )
build\test\edit_self_test.exe
if errorlevel 1 ( echo [!] edit_self_test FAILED & popd & exit /b 1 )

echo.
echo ==[ javac fixtures ]==
javac -d "%BUILD%" tests\java\Connection.java tests\java\ChannelDuplexHandler.java ^
      tests\java\GameContextContract.java tests\java\Verify.java
if errorlevel 1 ( echo [!] javac failed & popd & exit /b 1 )
if not exist "%BUILD%\net\minecraft\network\Connection.class" (
    echo [!] fixture Connection.class not produced & popd & exit /b 1
)

echo.
echo ==[ emit_samples ]==
if not exist "%BUILD%\gen" mkdir "%BUILD%\gen"
cl %CFLAGS% /Fobuild\test\ ^
   tests\emit_samples.cpp native\classfile.cpp native\class_edit.cpp ^
   /link /OUT:build\test\emit_samples.exe
if errorlevel 1 ( echo [!] build failed & popd & exit /b 1 )
build\test\emit_samples.exe "%BUILD%\gen" "%BUILD%\net\minecraft\network\Connection.class"
if errorlevel 1 ( echo [!] emit_samples failed & popd & exit /b 1 )

echo.
echo ==[ JVM verify ]==
REM PatchedConnection declares itself net.minecraft.network.Connection, but the
REM loader here defines it under its own name, so the superclass has to be
REM resolvable -- ChannelDuplexHandler.class sits in %BUILD% from javac above.
java -cp "%BUILD%;%BUILD%\gen" Verify ^
   HookBridge      "%BUILD%\gen\HookBridge.class" ^
   RelayHandler    "%BUILD%\gen\RelayHandler.class" ^
   GameContext     "%BUILD%\gen\GameContext.class" ^
   PatchedConnection "%BUILD%\gen\PatchedConnection.class"
if errorlevel 1 ( echo [!] JVM verification FAILED & popd & exit /b 1 )

echo.
echo ALL TESTS PASSED
popd
endlocal
