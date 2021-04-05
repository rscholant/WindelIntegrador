@ECHO OFF

echo Waiting 4 seconds
ping 127.0.0.1 -n 5 > nul

echo Uninstall service
cmd /c "%~dp0\integrador-uninstall.bat"

echo Copying new Integrador
copy "%~dp0\Integrador-Novo.exe" "%~dp0\Integrador.exe"

echo Installing service
cmd /c "%~dp0\integrador-install.bat"

echo Deleting Novo file
del "%~dp0\Integrador-Novo.exe"


pause