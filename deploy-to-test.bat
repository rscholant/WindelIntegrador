call ".\build.bat"
cd Release
tar.exe -cf ../release_integrador.zip .
cd ..
copy release_integrador.zip "\\server\Temp\exe_banco_teste\windel testes\Integrador"
del /f release_integrador.zip