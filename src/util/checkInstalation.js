var firebirdInstance = null;

var logInstance = null;

var md5 = require("md5");
var createIntegradorUsuario = require("./createIntegradorUsuario.js");

async function updateVersionOnDb(version) {
  await firebirdInstance.execute(
    `
        UPDATE INTEGRADOR_CONFIG SET VALOR = ? WHERE CHAVE = ?
    `,
    [version + "", "version"]
  );
}

async function installSincUUIDOnTable(tabela_nome) {
  console.log("initializing table:" + tabela_nome);

  await firebirdInstance.execute(
    `EXECUTE block as
      BEGIN
      if (not exists(select 1 from RDB$RELATION_FIELDS rf where UPPER(rf.RDB$RELATION_NAME) = UPPER('${tabela_nome}') and UPPER(rf.RDB$FIELD_NAME) = 'SINC_UUID')) then
      execute statement 'ALTER TABLE ${tabela_nome} ADD SINC_UUID VARCHAR(36)';
      END`,
    []
  );

  var pkSincIdName = `IDX_SINCUUID_${tabela_nome}`;

  if (pkSincIdName.length >= 31) {
    pkSincIdName = `${
      pkSincIdName.substr(0, 27) + "_" + md5(tabela_nome).substr(0, 3)
    }`;
  }
  await firebirdInstance.execute(
    `EXECUTE block as BEGIN if (not exists(select * from rdb$indices where UPPER(rdb$index_name) = UPPER('${pkSincIdName}'))) then execute statement 'CREATE INDEX ${pkSincIdName} ON ${tabela_nome} (SINC_UUID)'; END `,
    []
  );

  await firebirdInstance.execute(
    `UPDATE ${tabela_nome} SET SINC_UUID = UUID_TO_CHAR(GEN_UUID())`,
    []
  );

  var trigger_nome_uuid = `uuid_${tabela_nome}`;
  if (trigger_nome_uuid.length >= 31) {
    var hash = md5(tabela_nome);
    trigger_nome_uuid =
      trigger_nome_uuid.substr(0, 27) + "_" + hash.substr(0, 3);
  }
  var createTriggerUUIDSql = `
        CREATE OR ALTER trigger ${trigger_nome_uuid} for ${tabela_nome}
            active before insert position 0
        AS 
        begin
            if(new.SINC_UUID is null) then
            begin
                new.SINC_UUID = UUID_TO_CHAR(GEN_UUID());
            end
        end
    `;
  await firebirdInstance.execute(createTriggerUUIDSql);
  await firebirdInstance.execute(
    `GRANT UPDATE,REFERENCES ON ${tabela_nome} TO TRIGGER ${trigger_nome_uuid}`,
    []
  );
}

async function installTriggers() {
  var tabelas = await firebirdInstance.query(
    `
    SELECT a.RDB$RELATION_NAME
        FROM RDB$RELATIONS a
    WHERE RDB$SYSTEM_FLAG = 0 AND ( RDB$RELATION_TYPE = 0 OR RDB$RELATION_TYPE IS NULL ) AND RDB$VIEW_BLR IS NULL
    ORDER BY a.RDB$RELATION_NAME
    `,
    [],
    ["nome"]
  );

  for (var key in tabelas) {
    var tabela = tabelas[key];

    if (!tabela.nome.startsWith("INTEGRADOR")) {
      tabela.nome = tabela.nome.trim();

      var tabelasPrioritarias = [
        "TITULOS",
        "MOBILE_PEDIDO_PRODUTOS",
        "MOBILE_CLIENTE_ENDERECO",
        "MOBILE_CLIENTE",
        "MOBILE_PEDIDO",
        "SUBGRUPOS_PROD",
        "GRUPOS_PROD",
        "PRODUTOS",
        "PAISES",
        "CIDADES",
        "FORMAPGTO",
        "CONDPAG",
        "OBSPESSOAS",
        "USUARIOS",
        "PESSOAS",
        "EMPRESAS",
      ];
      var tabelasExcluidas = [];
      var trigger_nome = `integrador_${tabela.nome}`;

      if (trigger_nome.length >= 31) {
        var hash = md5(tabela.nome);
        trigger_nome = trigger_nome.substr(0, 27) + "_" + hash.substr(0, 3);
      }

      var createTriggerSQL = `
                CREATE OR ALTER trigger ${trigger_nome} for ${tabela.nome}
                    ${
                      tabelasExcluidas.indexOf(tabela.nome) != -1
                        ? "INACTIVE"
                        : "ACTIVE "
                    } after insert or update or delete position 0
                AS 
                    declare variable operacao integer; 
                    declare variable isIntegradorEnabled varchar(1); 
                    declare variable prioridade integer;
                    declare variable isIntegradorSessionDisabled varchar(1); 
                begin 
                    select valor from parametros where idchave = 'integrador' into :isIntegradorEnabled; 
                    select rdb$get_context('USER_SESSION', 'DONT_TRIGGER_INTEGRADOR') from rdb$database into :isIntegradorSessionDisabled;
                    select rdb$get_context('USER_SESSION', 'INTEGRADOR_PRIORIDADE') from rdb$database into :prioridade;
                    if (:isIntegradorEnabled = 'S' AND :isIntegradorSessionDisabled is null) then 
                    begin 
                        if (inserting) then
                            :operacao = 0; 
                        else if (updating) then 
                            :operacao = 1; 
                        else if (deleting) then 
                            :operacao = 2; 
                            
                        UPDATE OR INSERT INTO 
                            INTEGRADOR_DATA_STATUS (UUID, TABELA, DATA_OPERACAO, SITUACAO, SINCRONIZADO, PRIORIDADE) 
                        VALUES ( 
                            iif(deleting, old.SINC_UUID, new.SINC_UUID),
                            '${tabela.nome}',
                            current_timestamp,
                            :operacao,
                            0,
                            ${
                              tabelasPrioritarias.indexOf(tabela.nome) != -1
                                ? tabelasPrioritarias.indexOf(tabela.nome) + 1
                                : 1
                            }
                        ) MATCHING (UUID); 
                    end 
                end
            `;

      await firebirdInstance.execute(createTriggerSQL);

      //grant access to triggers
      await firebirdInstance.execute(
        `GRANT INSERT ON INTEGRADOR_DATA_STATUS TO TRIGGER ${trigger_nome}`,
        []
      );
      await firebirdInstance.execute(
        `GRANT UPDATE,REFERENCES ON ${tabela.nome} TO TRIGGER ${trigger_nome}`,
        []
      );
      await firebirdInstance.execute(
        `GRANT SELECT ON PARAMETROS TO TRIGGER ${trigger_nome}`,
        []
      );
    }
  }
}

async function integradorInstall(version) {
  if (version < 1) {
    //instala a tabela de config
    await firebirdInstance.execute(
      `
            CREATE TABLE INTEGRADOR_CONFIG (
                CHAVE VARCHAR(500),
                VALOR VARCHAR(20000)
            ) 
        `,
      []
    );
    await firebirdInstance.execute(
      `
            INSERT INTO INTEGRADOR_CONFIG (CHAVE,VALOR) VALUES (?,?)
        `,
      ["version", "1"]
    );
    version = 1;
    await updateVersionOnDb(version);
  }

  if (version < 2) {
    await firebirdInstance.execute(`
            CREATE TABLE INTEGRADOR_DATA_STATUS (
                UUID VARCHAR(36),
                TABELA VARCHAR(62),
                DATA_OPERACAO TIMESTAMP,
                SITUACAO SMALLINT,
                SINCRONIZADO SMALLINT
            )
        `);
    await firebirdInstance.execute(`DELETE FROM INTEGRADOR_DATA_STATUS`, []);
    await firebirdInstance.execute(
      `EXECUTE block as
        BEGIN
          if (not exists(select 1 from RDB$RELATION_FIELDS rf where rf.RDB$RELATION_NAME = 'INTEGRADOR_DATA_STATUS' and rf.RDB$FIELD_NAME = 'PRIORIDADE')) then
            execute statement 'ALTER TABLE INTEGRADOR_DATA_STATUS ADD PRIORIDADE INTEGER default 0';
        END`,
      []
    );
    await firebirdInstance.execute(
      `execute block as
        begin
        if (not exists(select * from rdb$indices where rdb$index_name = 'UUID_IDX_INTEGRADOR_PK_UUID')) then
          execute statement 'CREATE INDEX UUID_IDX_INTEGRADOR_PK_UUID ON INTEGRADOR_DATA_STATUS (UUID)';
        end`,
      []
    );
    await firebirdInstance.execute(
      `ALTER TABLE INTEGRADOR_DATA_STATUS ALTER UUID SET NOT NULL`,
      []
    );
    await firebirdInstance.execute(
      `ALTER TABLE INTEGRADOR_DATA_STATUS ALTER UUID SET DEFAULT '0'`,
      []
    );
    await firebirdInstance.execute(
      `ALTER TABLE INTEGRADOR_DATA_STATUS ADD CONSTRAINT INTEGRADOR_DATA_STATUS_UUID PRIMARY KEY(UUID)`,
      []
    );
    version = 2;
    await updateVersionOnDb(version);
  }

  if (version < 3) {
    var tabelas = await firebirdInstance.query(
      `
        SELECT a.RDB$RELATION_NAME
            FROM RDB$RELATIONS a
        WHERE RDB$SYSTEM_FLAG = 0 AND ( RDB$RELATION_TYPE = 0 OR RDB$RELATION_TYPE IS NULL ) AND RDB$VIEW_BLR IS NULL
        ORDER BY a.RDB$RELATION_NAME
        `,
      [],
      ["nome"]
    );

    //disable triggers to improve performance of uuid update
    await firebirdInstance.execute(
      `alter trigger DENEGA_CANC_DOCS_BU inactive`
    );
    await firebirdInstance.execute(`alter trigger DOCS_AU_SIT_DOC inactive`);
    await firebirdInstance.execute(`alter trigger PRODUTOS1 inactive`);
    await firebirdInstance.execute(`alter trigger PRODUTOS2 inactive`);
    await firebirdInstance.execute(`alter trigger MOVDOCS1 inactive`);
    await firebirdInstance.execute(`alter trigger MOVDOCS_BU inactive`);
    await firebirdInstance.execute(`alter trigger T_CUSTOMARKUP_AU inactive`);

    for (var key in tabelas) {
      var tabela = tabelas[key];

      tabela.nome = tabela.nome.trim();

      if (!tabela.nome.startsWith("INTEGRADOR")) {
        await installSincUUIDOnTable(tabela.nome);
      }
    }

    ///reenable triggers
    await firebirdInstance.execute(`alter trigger DENEGA_CANC_DOCS_BU active`);
    await firebirdInstance.execute(`alter trigger DOCS_AU_SIT_DOC active`);
    await firebirdInstance.execute(`alter trigger PRODUTOS1 active`);
    await firebirdInstance.execute(`alter trigger PRODUTOS2 active`);
    await firebirdInstance.execute(`alter trigger MOVDOCS1 active`);
    await firebirdInstance.execute(`alter trigger MOVDOCS_BU active`);
    await firebirdInstance.execute(`alter trigger T_CUSTOMARKUP_AU active`);

    version = 3;
    await updateVersionOnDb(version);
  }

  if (version < 4) {
    await installTriggers();
    version = 4;
    await updateVersionOnDb(version);
  }

  if (version < 5) {
    await firebirdInstance.execute(
      `ALTER TABLE INTEGRADOR_CONFIG ALTER CHAVE SET NOT NULL`,
      []
    );
    await firebirdInstance.execute(
      `ALTER TABLE INTEGRADOR_CONFIG ALTER COLUMN CHAVE SET DEFAULT 0`,
      []
    );
    await firebirdInstance.execute(
      `
        ALTER TABLE INTEGRADOR_CONFIG
            ADD CONSTRAINT PK_INTEGRADOR_CONFIG
            PRIMARY KEY (CHAVE)
        `,
      []
    );
    await firebirdInstance.execute(
      `
            INSERT INTO INTEGRADOR_CONFIG (CHAVE,VALOR) VALUES (?,?)
        `,
      ["date_since_last_pull", "0"]
    );
    version = 5;
    await updateVersionOnDb(version);
  }

  if (version < 6) {
    await createIntegradorUsuario(firebirdInstance);
    version = 6;
    await updateVersionOnDb(version);
  }

  if (version < 7) {
    console.log("Adicionando OBS_NOTA no pedido MOBILE");
    await firebirdInstance.execute(
      `ALTER TABLE MOBILE_PEDIDO ADD OBS_NOTA VARCHAR(3000) DEFAULT ''`,
      []
    );
    version = 7;
    await updateVersionOnDb(version);
  }

  if (version < 8) {
    version = 8;
    await updateVersionOnDb(version);
  }

  if (version < 9) {
    await installTriggers();
    await installSincUUIDOnTable(`TITULOS`);
    version = 9;
    await updateVersionOnDb(version);
  }

  if (version < 10) {
    await firebirdInstance.execute(
      `
      DELETE FROM MOBILE_PEDIDO_PRODUTOS
        WHERE IDPEDIDO NOT IN (SELECT IDPEDIDO FROM MOBILE_PEDIDO)
    `,
      []
    );
    version = 10;
    await updateVersionOnDb(version);
  }
  if (version < 999) {
    console.log("Atualizando triggers");
    await installTriggers();
    console.log("Adicionando permissões para o usuario do integrador.");
    try {
      await firebirdInstance.execute(
        `
      EXECUTE block as
        BEGIN
          if (not exists(select 1 from sec$users where sec$user_name = 'INTEGRADOR' )) then
            execute STATEMENT 'CREATE USER INTEGRADOR PASSWORD ''WINDELMOB'' GRANT ADMIN ROLE';
        END
      `,
        []
      );
    } catch (e) {
      console.log(e);
    }
    try {
      await firebirdInstance.execute(
        `
            ALTER USER INTEGRADOR PASSWORD 'WINDELMOB' GRANT ADMIN ROLE
        `,
        []
      );
    } catch (e) {
      console.log(e);
    }
    await firebirdInstance.execute(
      `EXECUTE BLOCK
    AS
      DECLARE VARIABLE tablename VARCHAR(32);
    BEGIN
      FOR SELECT rdb$relation_name
      FROM rdb$relations
      WHERE rdb$view_blr IS NULL
      AND (rdb$system_flag IS NULL OR rdb$system_flag = 0)
      INTO :tablename DO
      BEGIN
        EXECUTE STATEMENT ('GRANT SELECT, INSERT, UPDATE, REFERENCES, DELETE ON TABLE ' || :tablename || ' TO USER INTEGRADOR WITH GRANT OPTION');
      END
    END`,
      []
    );
  }
}

module.exports = async (dirPath, log) => {
  logInstance = log;

  var firebird = new (require("../common/firebird"))();
  var uri = firebird.readURIFromLocalCfg(dirPath);
  if (uri == null) {
    process.exit(1);
  }
  await firebird.connect(uri, true);

  firebirdInstance = firebird;

  var version = 0;

  try {
    var results = await firebirdInstance.query(
      "SELECT chave,valor FROM INTEGRADOR_CONFIG WHERE chave = ?",
      ["version"],
      ["chave", "valor"]
    );
    if (results.length > 0) {
      version = results[0].valor;
    }
  } catch (e) {}

  await integradorInstall(version);
  await firebird.close();
  firebirdInstance = null;
};
