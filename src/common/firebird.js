const {
  createNativeClient,
  getDefaultLibraryFilename,
} = require("node-firebird-driver-native");

var fs = require("fs");
var log = require("../util/log.js");
const { Console } = require("console");

module.exports = class Firebase {
  firebirdClient = null;
  firebirdAttachment = null;

  constructor() {}

  readURIFromLocalCfg(dirPath) {
    log.writeToLog("Reading local.cfg");
    var uri = "";
    try {
      var content = fs.readFileSync(dirPath + "/local.cfg", {
        encoding: "utf-8",
      });
      content = content.split("\r").join("");
      var lines = content.split("\n");
      for (var i = 0; i < lines.length; i++) {
        var lineArr = lines[i].split(":");
        if (lineArr.length > 1) {
          uri = lineArr.join(":").trim();
          break;
        }
      }
      return uri;
    } catch (e) {
      log.writeToLog("Erro ao ler o local.cfg");
      return null;
    }
  }

  async connect(uri, useSysDBA) {
    this.firebirdClient = createNativeClient(getDefaultLibraryFilename());
    if (useSysDBA) {
      this.firebirdAttachment = await this.firebirdClient.connect(uri, {
        username: "SYSDBA",
        password: "masterkey",
      });
    } else {
      this.firebirdAttachment = await this.firebirdClient.connect(uri, {
        username: "SINCRONIZADOR",
        password: "WINDELMOB",
      });
    }
  }

  async queryInternal(query, params) {
    try {
      var transaction = await this.firebirdAttachment.startTransaction();
      var resultSet = await this.firebirdAttachment.executeQuery(
        transaction,
        query,
        params
      );
      var results = await resultSet.fetch();
      await resultSet.close();
      return results;
    } catch (e) {
      console.log("Error on query");
      console.log(query);
      console.log(e);
    }
    await transaction.commit();
  }

  async query(query, params, mapOfFields) {
    var results = await this.queryInternal(query, params);
    var newResults = [];
    newResults = results.map((val, ind, arr) => {
      var object = {};
      for (var i = 0; i < val.length; i++) {
        object[mapOfFields[i]] = val[i];
      }
      return object;
    });
    return newResults;
  }

  async queryOne(query, params, mapOfFields) {
    var results = await this.query(query, params, mapOfFields);
    if (results.length > 0) {
      return results[0];
    } else {
      return null;
    }
  }

  async execute(query, params) {
    try {
      var transaction = await this.firebirdAttachment.startTransaction();
      await this.firebirdAttachment.execute(transaction, query, params);
    } catch (e) {
      console.log(e.stack);
      log.writeToLog("Erro ao executar query");
      log.writeToLog(query);
      log.writeToLog(params);
      log.writeToLog(e);
    }
    await transaction.commit();
  }
  async executeMultiple(query, params) {
    var transaction = await this.firebirdAttachment.startTransaction();
    var queries = query.split(";");
    if (queries.length > 1) {
      for (var i = 0; i < queries.length; i++) {
        try {
          await this.firebirdAttachment.execute(transaction, queries[i], []);
        } catch (e) {
          console.log("Error on query");
          console.log(queries[i]);
          console.log(e);
        }
      }
    } else {
      await this.firebirdAttachment.execute(transaction, query, params);
    }
    await transaction.commit();
  }

  async close() {
    //await this.firebirdAttachment.dispose();
    await this.firebirdAttachment.disconnect();
    await this.firebirdClient.dispose();
  }

  async getTabelaColuna(tabela) {
    return await this.query(
      `select f.rdb$field_name as coluna
                from rdb$relation_fields f
                join rdb$relations r on f.rdb$relation_name = r.rdb$relation_name
                and r.rdb$view_blr is null
                and (r.rdb$system_flag is null or r.rdb$system_flag = 0)
                WHERE f.rdb$relation_name = ?
                order by 1, f.rdb$field_position`,
      [tabela],
      ["coluna"]
    );
  }

  async getPKForTable(tabela) {
    return await this.query(
      `
            select      
                sg.rdb$field_name as field_name,
                rc.rdb$contraint_name as pk_name
            from
                rdb$indices ix
                left join rdb$index_segments sg on ix.rdb$index_name = sg.rdb$index_name
                left join rdb$relation_constraints rc on rc.rdb$index_name = ix.rdb$index_name
            where
                rc.rdb$constraint_type = 'PRIMARY KEY'
                and rc.rdb$relation_name = ?
        `,
      [tabela],
      ["field_name", "pk_name"]
    );
  }

  async getFKsForTable(tabela) {
    var fks = this.query(
      `
        select
            rc.rdb$index_name as index_name,
            ix.RDB$FOREIGN_KEY as pk_name,
            sg.rdb$field_name as field_name,
            mix.rdb$relation_name as pk_table
        from
            rdb$indices ix
            join rdb$indices mix on ix.RDB$FOREIGN_KEY = mix.rdb$index_name
            left join rdb$index_segments sg on ix.rdb$index_name = sg.rdb$index_name
            left join rdb$relation_constraints rc on rc.rdb$index_name = ix.rdb$index_name
        where
            rc.rdb$constraint_type = 'FOREIGN KEY'
            and rc.rdb$relation_name =  ?
        ORDER BY index_name ASC    
        `,
      [tabela],
      ["index_name", "pk_name", "field_name", "pk_table"]
    );

    var fksObjects = {};
    for (var i = 0; i < fks.length; i++) {
      var indexName = fks[i].index_name.trim();
      var pkName = fks[i].pk_name.trim();
      var pkTable = fks[i].pk_table.trim();
      var fieldName = fks[i].field_name.trim();
      if (!(indexName in fksObjects)) {
        fksObjects[indexName] = {
          fields: {},
          pk: pkName,
          pk_table: pkTable,
        };
      }
      fksObjects[indexName].fields[fieldName] = fieldName;
    }

    return fksObjects;
  }
};
