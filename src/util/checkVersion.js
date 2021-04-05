var path = require("path");
var log = require("../util/log.js");
var axios = require("axios").default;
var fs = require("fs");
const { exec } = require("child_process");

module.exports = async (dirPath, serverUrl, currentVersion)=>{

    log.writeToLog("Cheking for new version");
    try{

        var res = await axios.get(serverUrl+"/version/sincronizador");
        if(res.data.version != currentVersion){

            log.writeToLog("New Version Detected");
            var pathToNewSincronizador = path.resolve(dirPath, 'Sincronizador-Novo.exe');
            var writer = fs.createWriteStream(pathToNewSincronizador);
            writer.on("finish", ()=>{
                log.writeToLog("Execing Update script to reinstall service");
                exec("powershell -Command \"Start-Process cmd -Verb RunAs -ArgumentList '/c cd \""+dirPath+"\" && .\\sincronizador-update.bat'\"", {shell:true, cwd: dirPath}, (err, std, stderr)=>{
                    log.writeToLog(err);
                    process.exit();
                });
            });

            writer.on("error", (err)=>{
                log.writeToLog("Error downloading new sincronizador version");
                log.writeToLog(err);
            });

            var response = await axios({
                url: serverUrl+"/version/sincronizador/"+res.data.version+".exe",
                method: "GET",
                responseType: 'stream'
            });
            response.data.pipe(writer);
            
        }else{
            log.writeToLog("Version is uptodate");
        }                

    }catch(e){
        log.writeToLog(e);
    }
}
