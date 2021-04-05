var axios = require("axios");
var dirPath = "";

var fs = require("fs");
const trataPedidos = require("./util/trataPedidos.js");

var checkVersion = require("./util/checkVersion.js");
var checkInstalation = require("./util/checkInstalation.js");
var log = require("./util/log.js");

var prepareDataForTable = require("./common/prepareDataForTable.js");

//const { v4: uuidv4 } = require('uuid');

//var isProcessing = false;
var canProcessAgain = true;
var idPedidos = [];

module.exports = async (newDirPath, serverUrl, currentVersion) => {
  dirPath = newDirPath;
  log.setDirPath(dirPath);
  let timeoutGet = 1;
  let timoutPost = 1;
  await checkInstalation(dirPath, log);

  var firebird = new (require("./common/firebird"))();
  var uri = firebird.readURIFromLocalCfg(dirPath);
  if (uri == null) {
    process.exit(1);
  }
  await firebird.connect(uri, false);

  var configResults = await firebird.query(
    `SELECT chave,valor FROM REPLIC_CONFIG `,
    [],
    ["chave", "valor"]
  );
  var config = {};

  for (var i = 0; i < configResults.length; i++) {
    config[configResults[i].chave] = configResults[i].valor;
  }
  if (!("first_sync" in config)) {
    var firebirdInitialSync = new (require("./common/firebird"))();
    var uri = firebirdInitialSync.readURIFromLocalCfg(dirPath);
    await firebirdInitialSync.connect(uri, true);
    await require("./util/initialSync")(firebirdInitialSync, log);
  }

  if (!("date_since_last_pull" in config)) {
    config.date_since_last_pull = 0;
  }

  setInterval(async () => {
    await checkVersion(dirPath, serverUrl, currentVersion);
  }, 1800000); // 30 minutos

  console.log("=======================================================");
  console.log("Pronto para instalar");

  async function run() {
    await firebird.close();
    await firebird.connect(uri, false);
    if (!canProcessAgain) {
      isProcessing = false;
      return;
    }
    isProcessing = true;

    var cache_tabela_colunas = {};

    async function prepareWithTabelaColunas(tabela) {
      if (!(tabela in cache_tabela_colunas)) {
        var colunas = await firebird.getTabelaColuna(tabela);
        var fields = [];
        for (var c = 0; c < colunas.length; c++) {
          fields.push(colunas[c].coluna.trim());
        }
        cache_tabela_colunas[tabela] = fields;
      }
      return cache_tabela_colunas[tabela];
    }

    try {
      var canPush = false;
      var configAuth = fs.readFileSync(dirPath + "/sincronizador.cfg", {
        encoding: "utf8",
      });
      if (configAuth == "") {
        throw new Exception("Auth not configured");
      }

      serverAuthToken = configAuth;

      //PULL

      //disable replication for session
      await firebird.execute(
        "select rdb$set_context('USER_SESSION', 'DONT_TRIGGER_REPLIC', 'S') from rdb$database",
        []
      );

      var result = await axios.post(serverUrl + "/modifications", {
        action: "get",
        since: isNaN(config.date_since_last_pull)
          ? "0"
          : config.date_since_last_pull,
        auth: serverAuthToken,
      });

      if (result.data.result) {
        timeoutGet =
          result.data.remaining && result.data.remaining > 0 ? 1 : 60000;
        if (result.data.data.length > 0) {
          for (var i = 0; i < result.data.data.length; i++) {
            var modification = result.data.data[i];
            modification.dados = JSON.parse(modification.dados);

            if (modification.situacao == 0 || modification.situacao == 1) {
              // inserido ou atualizado

              modification.colunas = [];
              var modificationValues = [];
              var tabelaFieldsMarkers = [];
              var hasUUIDOnDados = false;

              modification = prepareDataForTable(modification);

              for (var colkey in modification.dados) {
                if (
                  colkey.toUpperCase() == "SINC_UUID" &&
                  modification.dados[colkey] !== null
                ) {
                  hasUUIDOnDados = true;
                }
                if (
                  typeof modification.dados[colkey] !== "object" ||
                  (modification.forceDados && colkey in modification.forceDados)
                ) {
                  modification.colunas.push(colkey.toUpperCase());
                  modificationValues.push(modification.dados[colkey]);
                  tabelaFieldsMarkers.push("?");
                }
              }

              if (!hasUUIDOnDados) {
                modification.colunas.push("SINC_UUID");
                modificationValues.push(modification.uuid);
                tabelaFieldsMarkers.push("?");
              }
              await firebird.execute(
                `
                                UPDATE OR INSERT INTO ${modification.tabela}
                                (${modification.colunas.join(",")})
                                VALUES
                                (${tabelaFieldsMarkers.join(
                                  ","
                                )}) MATCHING (SINC_UUID)
                            `,
                modificationValues
              );

              if (
                modification.tabela == "MOBILE_PEDIDO" ||
                modification.tabela == "MOBILE_PEDIDO_PRODUTOS"
              ) {
                idPedidos = Object.assign(
                  idPedidos,
                  await trataPedidos.trata(firebird, modification, idPedidos)
                );
              } else {
                await firebird.execute(
                  "UPDATE OR INSERT INTO replic_data_status (UUID,Tabela,data_operacao,situacao,sincronizado) VALUES (?,?,?,?,1) MATCHING(UUID)",
                  [
                    modification.uuid,
                    modification.tabela,
                    new Date(modification.data_operacao),
                    modification.situacao,
                  ]
                );
              }
            } else if (modification.situacao == 2) {
              //deletado

              var params = [modification.uuid];
              //deleting locally

              await firebird.execute(
                `
                                DELETE FROM ${modification.tabela} WHERE SINC_UUID = ?
                            `,
                params
              );

              await firebird.execute(
                "UPDATE OR INSERT INTO replic_data_status (UUID,Tabela,data_operacao,situacao,sincronizado) VALUES (?,?,?,?,1) MATCHING(UUID)",
                [
                  modification.uuid,
                  modification.tabela,
                  new Date(modification.data_operacao),
                  modification.situacao,
                ]
              );
            }
          }
        } else {
          canPush = true;
        }
      } else {
        console.log(result.data.message);
      }

      //update date since last pull
      config.date_since_last_pull = result.data.next_since;
      await firebird.execute(
        `
                UPDATE OR INSERT INTO REPLIC_CONFIG (CHAVE, VALOR) VALUES (?,?)
            `,
        ["date_since_last_pull", config.date_since_last_pull + ""]
      );

      //reenable replication on session
      await firebird.execute(
        "select rdb$set_context('USER_SESSION', 'DONT_TRIGGER_REPLIC', null) from rdb$database",
        []
      );

      //PUSH
      if (canPush) {
        var modificationsToSendToCloud = [];
        //grabs rows to sync
        var results = await firebird.query(
          `
                select first 150 uuid, tabela, data_operacao, situacao, sincronizado
                    from replic_data_status
                    where sincronizado = 0
                ORDER BY prioridade DESC, data_operacao ASC
                `,
          [],
          ["uuid", "tabela", "data_operacao", "situacao", "sincronizado"]
        );
        var count = await firebird.query(
          `
                select count(*) as CONTADOR
                    from replic_data_status
                    where sincronizado = 0
                `,
          [],
          ["CONTADOR"]
        );

        timoutPost = count[0].CONTADOR && count[0].CONTADOR > 0 ? 1 : 60000;
        for (var key in results) {
          var toSend = results[key];
          toSend.dados = null;

          var colunas = await prepareWithTabelaColunas(toSend.tabela);
          var resultObj = await firebird.query(
            `
                            SELECT first 1 ${colunas.join(",")} FROM ${
              toSend.tabela
            } WHERE SINC_UUID = ?
                        `,
            [toSend.uuid],
            colunas
          );

          if (resultObj.length > 0) {
            toSend.dados = resultObj[0];
          }

          modificationsToSendToCloud.push(toSend);
        }

        //send modifications to cloud
        if (modificationsToSendToCloud.length > 0) {
          var result = await axios.post(serverUrl + "/modifications", {
            action: "new",
            auth: serverAuthToken,
            modifications: modificationsToSendToCloud,
          });
          if (result.data.result == true) {
            var modificationsMarkers = [];
            var modificationsUUIDs = [];
            for (var i = 0; i < modificationsToSendToCloud.length; i++) {
              modificationsMarkers.push("?");
              modificationsUUIDs.push(modificationsToSendToCloud[i].uuid);
            }
            await firebird.execute(
              `UPDATE replic_data_status SET sincronizado = 1 WHERE uuid IN (${modificationsMarkers.join(
                ","
              )})`,
              modificationsUUIDs
            );
          }
        }
      }
    } catch (e) {
      log.writeToLog(e);
    }

    isProcessing = false;

    setTimeout(
      () => {
        run();
      },
      timeoutGet <= timoutPost ? timeoutGet : timoutPost
    );
  }

  await run();
};
