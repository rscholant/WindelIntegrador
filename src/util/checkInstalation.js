var firebirdInstance = null;

var logInstance = null;

var md5 = require("md5");
var createSincronizadorUsuario = require("./createSincronizadorUsuario.js");

async function updateVersionOnDb(version) {
  await firebirdInstance.execute(
    `
        UPDATE REPLIC_CONFIG SET VALOR = ? WHERE CHAVE = ?
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

    if (!tabela.nome.startsWith("REPLIC")) {
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
      var tabelasExcluidas = [
        "ALTERACOES",
        "ATRIBUTOS_ACESSOS",
        "DOCS",
        "DOCS_LOG",
        "DOCSADICIONAIS",
        "DOCSOBS",
        "FORMAPGTO_FRENTE_CAIXA",
        "LANCAMENTOS",
        "PAGAMENTOS",
        "MOVDOCS",
        "MOVDOCS_LOG",
        "MOVS_RELACAO",
        "CAMPOS_VISIVEIS",
        "LOG_TITULOS",
        "ABCPRODCLI1 ",
        "ABCPRODCLI10",
        "ABCPRODCLIFIN1",
        "ABCPRODCLIFIN10",
        "ACUMULADORES",
        "ALIQ_REDUCAOZ",
        "ALMOXARIFADOS",
        "ALMOXARIFADOS_K",
        "ATALHOS",
        "ATUALIZACAO",
        "ATUALIZACAOSCRIPTS",
        "AUX_COMISSOES_TIT",
        "AUX_ESTRUT_PROD",
        "AUX_GENERATOR",
        "AUX_GER_NOTAS",
        "AUX_I_GER_NOTAS",
        "AUX_INVENTARIO",
        "BALANCO",
        "CAD_CLASS_ONU",
        "CADASTROS_LOTES",
        "CADCUSTOSFIXOS",
        "CAMPOS_VISIVEIS",
        "CARTAFRETE",
        "CARTAFRETE_CTE",
        "CARTEIRAS",
        "CARTOES",
        "CARTOES_TAXAS",
        "CCREDADMINISTRADORAS",
        "CCREDLANCAMENTOS",
        "CCREDTRANSACOES",
        "CENTRO_CUSTOS",
        "CEST",
        "CFG_BLOQUETOS",
        "CFG_CARNE_CREDIARIO",
        "CFG_ETIQ_PACOTES",
        "CFG_ITENS_ETIQ_PACOTES",
        "CFGREL",
        "CFGRELCAMPOS",
        "CFGRELRECONHEC",
        "CFGRELRECONHECCAMPO",
        "CHEQUES",
        "CHEQUESTIT",
        "CLASSEDOCUMENTO",
        "CLASSES_TOXICOLOGICAS",
        "CLIENTES_VEICULOS",
        "CODBARRA1",
        "CODBARRA100",
        "COMIS_FAIXA_DESC",
        "COMIS_FAIXA_VALOR_VENDEDOR",
        "CONFIG_LMP",
        "CONFIG_ORDEM_VARGAS",
        "CONFIG_RECEITUARIO_AGRICOLA",
        "CONFIGBOLETOS",
        "CONFIGGPS",
        "CONFIGMALADIRETA",
        "CONFIGNOTAFISCAL",
        "CONFIGREGRASDOCS",
        "CONFIGURACAO_EMAIL",
        "CONHECIMENTOS",
        "CONHECIMENTOS_NOTAS",
        "CONTROLEREMESSA",
        "CONTROLEVEICULOS",
        "COTACOES",
        "CRED_PESSOAS",
        "CREDITO_INVENTARIO",
        "CSOSN_SIMPLES",
        "CST_ICMS",
        "CTE_COMPROVANTE_ENTREGA",
        "CTE_COMPROVANTE_ENTREGA_NF",
        "CTE_LOGS",
        "CTE_PRESTACAO_SERVICO_DESACORDO",
        "CTE_UFSPERCURSO",
        "CULTURA_PRODUTOS",
        "CULTURA_PRODUTOS_BACKUP",
        "CULTURAS",
        "CUSTOELETRON",
        "CUSTOMARKUP",
        "CUSTOSFIXOS",
        "DADOS_SPED",
        "DE_DOCS",
        "DEPARTAMENTO",
        "DESCRICAO_PROCESSOS",
        "DI_ADICIONAIS",
        "DI_DOCS",
        "DIAGNOSTICOS_CULTURAS",
        "DOC_ANTERIOR_CTE",
        "DOCREG",
        "DOCREG_DEP",
        "DOCREG_HIST",
        "DOCS",
        "DOCS_CONFIG",
        "DOCS_DESTINADOS",
        "DOCS_ITENSCONFIG",
        "DOCS_LOG",
        "DOCS_NOTAS",
        "DOCSADICIONAIS",
        "DOCSOBS",
        "DRE_DADOSGERAIS",
        "DRE_ITENSRELAT",
        "EIXOS_GRADE",
        "EQUIPAMENTOS",
        "EQUIPAMENTOS_OS",
        "ESPECIE_NOTAFISCAL",
        "ESTOQUEALMOXPRODUTO",
        "ESTOQUEMOVIMENTOS",
        "ESTRUTURA",
        "ESTRUTURA_COMPL",
        "ESTRUTURA_ETIQUETAS",
        "ESTRUTURA_K250",
        "ESTRUTURAREPARO",
        "ETIQUETA_TERMICA",
        "ETIQUETA_TERMICA_ITEM",
        "EVENTO_CTE",
        "EVENTO_CTE_ITEM",
        "EVENTOS_LOG",
        "EXCLUSOES",
        "EXPORTACOES",
        "FERIADOS",
        "FICHAMAN",
        "FICHAOPERACIONAL",
        "FORMAARQUIV",
        "FORMAPGTO_FRENTE_CAIXA",
        "GASTOSTONET",
        "GERADADOSK",
        "GNRE",
        "GRACOMISSOES1",
        "GRACOMISSOES100",
        "GRACOMISSOES12",
        "GRACOMISSOES18",
        "GRADE",
        "GRAFATURAMENTO1",
        "GRAFATURAMENTO100",
        "GRAFICOS",
        "GRAMENSALCOMISSOES1",
        "GRAMENSALCOMISSOES100",
        "HISTORICOS",
        "HORARIOS",
        "IBE$LOG_BLOB_FIELDS",
        "IBE$LOG_FIELDS",
        "IBE$LOG_KEYS",
        "IBE$LOG_TABLES",
        "IBPT",
        "IDENTIFICADOR",
        "INF_UNID_CARGA",
        "INF_UNID_CARGA_LACRES",
        "INMETRO_SELOS_LACRE",
        "INSCEST_ST",
        "INSPECAO",
        "ITEM_DOC_ANTERIOR_CTE",
        "ITEM_ORDEM_FABRICACAO",
        "ITEM_TABELA_FRETE",
        "ITEMS_CONHECIMENTOS",
        "ITENS_AVALIACAO",
        "ITENS_LOTES_MOVIMENTOS",
        "ITENSKITS",
        "JUSTIFICATIVA",
        "KITS",
        "LANC_CARTOES_DETALHE",
        "LANC_CARTOES_MESTRE",
        "LANCAMENTOS",
        "LIBERACOES",
        "LOCAIS",
        "LOCDOCS",
        "LOG_BANCO",
        "LOG_NATOPER",
        "LOG_PARAMETROS",
        "LOG_SQL",
        "LOG_TIT_BANCO",
        "LOG_TITULOS",
        "LOGS_DOCUMENTOS",
        "LOTES",
        "MALADIRETACLI",
        "MALADIRETACLIITENS",
        "MARGENS_LUCRO",
        "MATERIA_PRIMA_PRODUCAO",
        "MATERIAL_PROCESSOS",
        "MDFE",
        "MDFE_AVERBACAO",
        "MDFE_CARGA_PERIGOSA",
        "MDFE_CIDCARREGA",
        "MDFE_CONTRATO",
        "MDFE_DOCUMENTOS",
        "MDFE_DOCUMENTOS_POSTERIOR",
        "MDFE_EVENTOS",
        "MDFE_LOGS",
        "MDFE_MOTORISTAS",
        "MDFE_PAGTO_FRETE",
        "MDFE_PAGTO_FRETE_COMPONENTE",
        "MDFE_PAGTO_FRETE_PARCELAS",
        "MDFE_SEGURADORAS",
        "MDFE_UFPERCURSO",
        "MDFE_VALEPEDAGIO",
        "MODELO_NF",
        "MOEDAS",
        "MOV_ESTOQUE",
        "MOV_SELOS",
        "MOVDOCS",
        "MOVDOCS_LOG",
        "MOVDOCSCANCELADO",
        "MOVFICHAMAN",
        "MOVGASTOSTONET",
        "MOVS_RELACAO",
        "NCM",
        "NCM_ICMS_UF",
        "NF_PRODUTOR_REF",
        "NF02",
        "NOTAS_GUIA",
        "OBS_CONHECIMENTOS",
        "OBSFICHAMAN",
        "OPERADORA",
        "ORDEM_FABRICACAO",
        "ORGAOS_EMISSORES",
        "PAGAMENTOS",
        "PAUTAS",
        "PERDAS_ORDEM_PRODUCAO",
        "PISCOFINS",
        "PRACAS",
        "PRINCIPIOS_ATIVOS",
        "PROCESSO_ORDEM_FABRICACAO",
        "PROCESSOS",
        "PROCESSOS_INSPECOES",
        "PROCESSOS_ORDEM_PRODUCAO",
        "PROCESSOS_PRODUTOS",
        "PROFISSIONAIS",
        "PROFISSIONAIS_ARTS",
        "PROFISSIONAIS_BACKUP",
        "PROMISSORIA",
        "PROPRIEDADESRURAISCLI",
        "RAMO_ATIV",
        "RECADOS",
        "RECEITUARIO_AGRICOLA",
        "RECEITUARIO_AGRICOLA_BACKUP",
        "RECEITUARIO_AGRICOLA_BACKUP_S",
        "REDUCAOZ",
        "REGIOES",
        "RELACAO_CODBENEF",
        "RELACAO_CST",
        "RELACAO_DOCS",
        "RELACAO_DOCS_PARCIAIS",
        "RELACAO_ESTADO_CODBENEF",
        "RELACAO_ITENS_AVALIACAO",
        "REMESSA_BKB",
        "RESERVAS_ORDEM_PRODUCAO",
        "REVENDAS",
        "SCRIPTS_EXECUTADOS",
        "SELOS",
        "SERVICOFICHAMAN",
        "SETOR",
        "SITTRIB",
        "SITTRIB_PIS_COFINS",
        "SOLICITACAO_QUALIDADE",
        "SOLICITACAO_QUALIDADE_DOCS",
        "SOLICITACAO_QUALIDADE_ITENS",
        "SOLICITACAO_TREINA_PRESENCA",
        "SOLICITACAO_TREINAMENTOS",
        "SOLICITACAO_TREINAMENTOS_DIA",
        "SOLICITACAO_TREINAMENTOS_PS",
        "SOLICITACOES",
        "SUBITENS_AVALIACAO",
        "TAB_ICMS_INTERESTADUAL",
        "TABELA_FRETE",
        "TAXAS_ECF",
        "TEMPOFICHAMAN",
        "TEMPOOS",
        "TIPOS_DATA_LOTE",
        "TITULOS_DOCS",
        "TITULOS_PROMISSORIA",
        "TITULOS_REMESSA",
        "TITULOS_RETORNOS",
        "TREINAMENTOS",
        "UF_ALIQ_ICMS",
        "UN_CTE",
        "UN_SIGA",
        "VALEPED_CTE",
        "VALORES_EIXOS_GRADE",
        "VARIAVEIS_GRADE",
        "VARIEDADES",
        "VARIEDADESETOR",
        "VEIC_CTE",
        "VEICULOS",
        "VERIFICACAOFICHAMAN",
        "VIEW_BASF_PRODUCT_STOCK",
        "VLR_PREST_CTE",
        "XML_CTE",
        "XML_NFCE",
        "XML_NFE",
        "XML_NFE_ENTRADA",
        "XML_NFSE",
      ];
      var trigger_nome = `replic_${tabela.nome}`;

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
                    declare variable isReplicEnabled varchar(1); 
                    declare variable prioridade integer;
                    declare variable isReplicSessionDisabled varchar(1); 
                begin 
                    select valor from parametros where idchave = 'replicacao' into :isReplicEnabled; 
                    select rdb$get_context('USER_SESSION', 'DONT_TRIGGER_REPLIC') from rdb$database into :isReplicSessionDisabled;
                    select rdb$get_context('USER_SESSION', 'REPLIC_PRIORIDADE') from rdb$database into :prioridade;
                    if (:isReplicEnabled = 'S' AND :isReplicSessionDisabled is null) then 
                    begin 
                        if (inserting) then
                            :operacao = 0; 
                        else if (updating) then 
                            :operacao = 1; 
                        else if (deleting) then 
                            :operacao = 2; 
                            
                        UPDATE OR INSERT INTO 
                            REPLIC_DATA_STATUS (UUID, TABELA, DATA_OPERACAO, SITUACAO, SINCRONIZADO, PRIORIDADE) 
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
        `GRANT INSERT ON REPLIC_DATA_STATUS TO TRIGGER ${trigger_nome}`,
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

async function replicInstall(version) {
  if (version < 1) {
    //instala a tabela de config
    await firebirdInstance.execute(
      `
            CREATE TABLE REPLIC_CONFIG (
                CHAVE VARCHAR(500),
                VALOR VARCHAR(20000)
            ) 
        `,
      []
    );
    await firebirdInstance.execute(
      `
            INSERT INTO REPLIC_CONFIG (CHAVE,VALOR) VALUES (?,?)
        `,
      ["version", "1"]
    );
    version = 1;
    await updateVersionOnDb(version);
  }

  if (version < 2) {
    await firebirdInstance.execute(`
            CREATE TABLE REPLIC_DATA_STATUS (
                UUID VARCHAR(36),
                TABELA VARCHAR(62),
                DATA_OPERACAO TIMESTAMP,
                SITUACAO SMALLINT,
                SINCRONIZADO SMALLINT
            )
        `);
    await firebirdInstance.execute(`DELETE FROM REPLIC_DATA_STATUS`, []);
    await firebirdInstance.execute(
      `EXECUTE block as
        BEGIN
          if (not exists(select 1 from RDB$RELATION_FIELDS rf where rf.RDB$RELATION_NAME = 'REPLIC_DATA_STATUS' and rf.RDB$FIELD_NAME = 'PRIORIDADE')) then
            execute statement 'ALTER TABLE REPLIC_DATA_STATUS ADD PRIORIDADE INTEGER default 0';
        END`,
      []
    );
    await firebirdInstance.execute(
      `execute block as
        begin
        if (not exists(select * from rdb$indices where rdb$index_name = 'UUID_IDX_REPLIC_PK_UUID')) then
          execute statement 'CREATE INDEX UUID_IDX_REPLIC_PK_UUID ON REPLIC_DATA_STATUS (UUID)';
        end`,
      []
    );
    await firebirdInstance.execute(
      `ALTER TABLE REPLIC_DATA_STATUS ALTER UUID SET NOT NULL`,
      []
    );
    await firebirdInstance.execute(
      `ALTER TABLE REPLIC_DATA_STATUS ALTER UUID SET DEFAULT '0'`,
      []
    );
    await firebirdInstance.execute(
      `ALTER TABLE REPLIC_DATA_STATUS ADD CONSTRAINT REPLIC_DATA_STATUS_UUID PRIMARY KEY(UUID)`,
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

      if (!tabela.nome.startsWith("REPLIC")) {
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
      `ALTER TABLE REPLIC_CONFIG ALTER CHAVE SET NOT NULL`,
      []
    );
    await firebirdInstance.execute(
      `ALTER TABLE REPLIC_CONFIG ALTER COLUMN CHAVE SET DEFAULT 0`,
      []
    );
    await firebirdInstance.execute(
      `
        ALTER TABLE REPLIC_CONFIG
            ADD CONSTRAINT PK_REPLIC_CONFIG
            PRIMARY KEY (CHAVE)
        `,
      []
    );
    await firebirdInstance.execute(
      `
            INSERT INTO REPLIC_CONFIG (CHAVE,VALOR) VALUES (?,?)
        `,
      ["date_since_last_pull", "0"]
    );
    version = 5;
    await updateVersionOnDb(version);
  }

  if (version < 6) {
    await createSincronizadorUsuario(firebirdInstance);
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
    /*console.log(
      "Removendo pedidos e clientes feitos pelo sincronizador antigo"
    );
    await firebirdInstance.execute(`DELETE FROM MOBILE_PEDIDO_PRODUTOS`, []);
    await firebirdInstance.execute(`DELETE FROM MOBILE_PEDIDO`, []);
    await firebirdInstance.execute(`DELETE FROM MOBILE_CLIENTE_ENDERECO`, []);
    await firebirdInstance.execute(`DELETE FROM MOBILE_CLIENTE`, []);*/
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
    console.log("Adicionando permissões para o usuario do sincronizador.");
    try {
      await firebirdInstance.execute(
        `
      EXECUTE block as
        BEGIN
          if (not exists(select 1 from sec$users where sec$user_name = 'SINCRONIZADOR' )) then
            execute STATEMENT 'CREATE USER SINCRONIZADOR PASSWORD ''WINDELMOB'' GRANT ADMIN ROLE';
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
            ALTER USER SINCRONIZADOR PASSWORD 'WINDELMOB' GRANT ADMIN ROLE
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
        EXECUTE STATEMENT ('GRANT SELECT, INSERT, UPDATE, REFERENCES, DELETE ON TABLE ' || :tablename || ' TO USER SINCRONIZADOR WITH GRANT OPTION');
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
      "SELECT chave,valor FROM replic_config WHERE chave = ?",
      ["version"],
      ["chave", "valor"]
    );
    if (results.length > 0) {
      version = results[0].valor;
    }
  } catch (e) {}

  await replicInstall(version);
  await firebird.close();
  firebirdInstance = null;
};
