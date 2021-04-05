VERSION=`grep "var version = " index.js | awk '{print $4}' | tr -d '";'`
cd Release
tar.exe -cf ../release.zip .
scp ../release.zip sinc@sinc.windel.com.br:~/sinc_api/public/download/Sincronizador.zip
rm -f ../release.zip
scp .\Sincronizador.exe sinc@sinc.windel.com.br:~/sinc_api/public/version/sincronizador-20.02.01.exe
sch sinc@sinc.windel.com.br cd ~/sinc_api/public/version/ && mv sincronizador-20.02.01.exe sincronizador/20.02.01.exe
