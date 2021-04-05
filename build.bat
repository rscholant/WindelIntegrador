@ECHO OFF

mkdir .\Release
copy .\node_modules .\Release\node_modules
copy .\vendor\nssm.exe .\Release\nssm.exe 
copy .\src\bat\integrador-install.bat .\Release\integrador-install.bat
copy .\src\bat\integrador-uninstall.bat .\Release\integrador-uninstall.bat
copy .\src\bat\integrador-update.bat .\Release\integrador-update.bat
npm run pkg