var dirPath = "";
var fs = require("fs");

function writeToLog(log){
    if(dirPath == ""){
        dirPath = path.dirname(process.execPath);
    }
        fs.appendFileSync(dirPath+"/Sincronizador.log", (new Date()).toISOString()+": "+log+"\n");    
}

module.exports = {
    setDirPath: (newPath)=>{
        dirPath = newPath;
    },
    writeToLog: writeToLog
};