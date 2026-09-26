@echo off
rem kafak-tools build: single-file windows GUI exe
setlocal
cd /d %~dp0

if "%1"=="test" (
    echo === running tests ===
    go test ./... -count=1
    if errorlevel 1 exit /b 1
)

echo === building bin\kafak-tools.exe ===
set CGO_ENABLED=0
if not exist bin mkdir bin
go build -ldflags "-s -w -H windowsgui" -o bin\kafak-tools.exe .\cmd\kafak-tools
if errorlevel 1 (
    echo BUILD FAILED
    exit /b 1
)
echo OK: %CD%\bin\kafak-tools.exe
endlocal
