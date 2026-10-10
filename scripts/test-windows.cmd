@echo off
setlocal
pushd "%~dp0.."
call "D:\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat"
if errorlevel 1 goto fail
set "CMAKE=D:\Microsoft Visual Studio\18\Community\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
"%CMAKE%" -S . -B build-msvc -G Ninja -DCMAKE_MAKE_PROGRAM="D:/Microsoft Visual Studio/18/Community/Common7/IDE/CommonExtensions/Microsoft/CMake/Ninja/ninja.exe" -DCMAKE_BUILD_TYPE=Release
if errorlevel 1 goto fail
"%CMAKE%" --build build-msvc --parallel
if errorlevel 1 goto fail
"D:\Microsoft Visual Studio\18\Community\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\ctest.exe" --test-dir build-msvc --output-on-failure
if errorlevel 1 goto fail
python scripts\verify-windows.py
if errorlevel 1 goto fail
popd
exit /b 0
:fail
popd
exit /b 1
