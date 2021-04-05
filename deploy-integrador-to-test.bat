call ".\build.bat"
cd Release
tar.exe -cf ../release_integrador_teste.zip .
cd ..
copy release_integrador_teste.zip "\\server\Temp\exe_banco_teste\windel testes\Mobile"
del /f release_integrador_teste.zip