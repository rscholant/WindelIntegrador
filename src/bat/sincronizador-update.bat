@ECHO OFF

echo Waiting 4 seconds
ping 127.0.0.1 -n 5 > nul

echo Uninstall service
cmd /c "%~dp0\sincronizador-uninstall.bat"

echo Copying new Sincronizador
copy "%~dp0\Sincronizador-Novo.exe" "%~dp0\Sincronizador.exe"

echo Installing service
cmd /c "%~dp0\sincronizador-install.bat"

echo Deleting Novo file
del "%~dp0\Sincronizador-Novo.exe"


pause