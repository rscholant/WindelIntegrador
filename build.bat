@ECHO OFF

mkdir .\Release
copy .\node_modules .\Release\node_modules
copy .\vendor\nssm.exe .\Release\nssm.exe 
copy .\src\bat\sincronizador-install.bat .\Release\sincronizador-install.bat
copy .\src\bat\sincronizador-uninstall.bat .\Release\sincronizador-uninstall.bat
copy .\src\bat\sincronizador-update.bat .\Release\sincronizador-update.bat
npm run pkg