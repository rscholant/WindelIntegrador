var path = require("path");
var log = require("./src/util/log.js");

var dirPath = __dirname; //use this for dev
var enviromentDev = true;
if (dirPath.indexOf("snapshot") != -1) {
  dirPath = path.dirname(process.execPath);
  enviromentDev = false;
}
log.setDirPath(dirPath);

process.on("uncaughtException", (err) => {
  log.writeToLog(`Uncaught Exception: ${err.message} ${err.stack}`);
});
//force NON DEV environment
//dirPath = path.dirname(process.execPath);
enviromentDev = false;
enviromentIntegrador = true;
//force DEV enviroment
//dirPath = __dirname;
//enviromentDev = true;

var serverUrl = enviromentDev
  ? "http://127.0.0.1:3000"
  : enviromentIntegrador
  ? "http://18.229.142.255:3000" //INTEGRADOR
  : "http://18.229.33.13:3000"; //MOBILE

var version = "21.04.01";

if (process.argv.length <= 2) {
  require("./src/main.js")(dirPath, serverUrl, version);
} else if (process.argv[2] == "check-installation") {
  (async () => {
    var firebird = new (require("./src/common/firebird"))();
    var uri = firebird.readURIFromLocalCfg(dirPath);
    if (uri === null) {
      console.log("No Firebird found on local.cfg");
    }
    await firebird.connect(uri);
    await require("./src/util/checkInstalation")(firebird);
  })();
} else if (process.argv[2] == "initial-sync") {
  console.log("INITIAL SINC");
  (async () => {
    var firebird = new (require("./src/common/firebird"))();
    var uri = firebird.readURIFromLocalCfg(dirPath);
    if (uri === null) {
      console.log("No Firebird found on local.cfg");
    }
    await firebird.connect(uri);
    await require("./src/util/initialSync")(firebird);
  })();
}
