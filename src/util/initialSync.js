


module.exports = async (firebird, log)=>{

    var tablesDone = await firebird.queryOne(`SELECT chave,valor FROM REPLIC_CONFIG WHERE chave = ?`, ['initial_sync_tables_done'], ["chave","valor"]);

    if(tablesDone === null){
        tablesDone = ["REPLIC", "REPLIC_TABELAS", "REPLIC_CONFIG", "REPLIC_DATA_STATUS"];
    }else{
        tablesDone = JSON.parse(tablesDone.valor);
    }
    
    var tabelas = await firebird.query(`
    SELECT a.RDB$RELATION_NAME
        FROM RDB$RELATIONS a
    WHERE RDB$SYSTEM_FLAG = 0 AND ( RDB$RELATION_TYPE = 0 OR RDB$RELATION_TYPE IS NULL ) AND RDB$VIEW_BLR IS NULL
    ORDER BY a.RDB$RELATION_NAME
    `, [], ["nome"]);


    for(var key in tabelas){
        tabelas[key].nome = tabelas[key].nome.trim();
    }

    var tabelasPrioritarias = [
        "USUARIOS",
        "PESSOAS", 
        "OBSPESSOAS", 
        "CONDPAG", 
        "FORMAPGTO", 
        "EMPRESAS", 
        "CIDADES", 
        "PAISES", 
        "PRODUTOS", 
        "GRUPOS_PROD", 
        "SUBGRUPOS_PROD", 
        "MOBILE_CLIENTE", 
        "MOBILE_CLIENTE_ENDERECO", 
        "MOBILE_PEDIDO", 
        "MOBILE_PEDIDO_PRODUTOS", 
        "CONFIGURACAO_EMAIL",
        "DOCS", 
        "MOVDOCS",
        "DOCSOBS"
    ];

    tabelas.sort((a,b)=>{
        var nomeA = a.nome.trim();
        var nomeB = b.nome.trim();
        var isAPrioritario = tabelasPrioritarias.indexOf(nomeA) > -1;
        var isBPrioritario = tabelasPrioritarias.indexOf(nomeB) > -1;
        if(isAPrioritario && !isBPrioritario){
            return -1;
        }
        if(!isAPrioritario && isBPrioritario){
            return 1;
        }
        if(!isAPrioritario && !isBPrioritario){
            if(nomeA < nomeB){
                return -1;
            }
            if(nomeA > nomeB){
                return 1;
            }
        }
        if(isAPrioritario && isBPrioritario){
            if(tabelasPrioritarias.indexOf(nomeA) > tabelasPrioritarias.indexOf(nomeB)){
                return 1;
            }
            if(tabelasPrioritarias.indexOf(nomeB) > tabelasPrioritarias.indexOf(nomeA)){
                return -1;
            }
        }
        return 0;
    });

    await firebird.execute("UPDATE OR INSERT INTO parametros (IDCHAVE,VALOR) VALUES ('replicacao', 'S')");
    await firebird.execute("select rdb$set_context('USER_SESSION', 'REPLIC_PRIORIDADE', 3) from rdb$database", []);

    //disable triggers to improve performance of uuid update
    await firebird.execute(`alter trigger DENEGA_CANC_DOCS_BU inactive`);
    await firebird.execute(`alter trigger DOCS_AU_SIT_DOC inactive`);
    await firebird.execute(`alter trigger PRODUTOS1 inactive`);
    await firebird.execute(`alter trigger PRODUTOS2 inactive`);
    await firebird.execute(`alter trigger MOVDOCS1 inactive`);
    await firebird.execute(`alter trigger MOVDOCS_BU inactive`);
    await firebird.execute(`alter trigger T_CUSTOMARKUP_AU inactive`);

    for(var key in tabelas){
        var tabela = tabelas[key];
        if(tablesDone.indexOf(tabela.nome) == -1){
            console.log("Initial Sync for table:"+tabela.nome);

            if(tabelasPrioritarias.indexOf(tabela.nome) != -1){
                await firebird.execute("select rdb$set_context('USER_SESSION', 'REPLIC_PRIORIDADE', 3) from rdb$database", []);
            }else{
                await firebird.execute("select rdb$set_context('USER_SESSION', 'REPLIC_PRIORIDADE', 1) from rdb$database", []);
            }
            
            await firebird.execute(`UPDATE ${tabela.nome} SET SINC_UUID = SINC_UUID`, []);

            tablesDone.push(tabela.nome);
            await firebird.execute("UPDATE OR INSERT INTO REPLIC_CONFIG (chave,valor) VALUES (?,?)", ['initial_sync_tables_done', JSON.stringify(tablesDone)]);
        }
    }

    await firebird.execute(`alter trigger DENEGA_CANC_DOCS_BU active`);
    await firebird.execute(`alter trigger DOCS_AU_SIT_DOC active`);
    await firebird.execute(`alter trigger PRODUTOS1 active`);
    await firebird.execute(`alter trigger PRODUTOS2 active`);
    await firebird.execute(`alter trigger MOVDOCS1 active`);
    await firebird.execute(`alter trigger MOVDOCS_BU active`);
    await firebird.execute(`alter trigger T_CUSTOMARKUP_AU active`);

    await firebird.execute("UPDATE OR INSERT INTO REPLIC_CONFIG (chave,valor) VALUES (?,?)", ['first_sync', "1"]);    
    await firebird.execute("select rdb$set_context('USER_SESSION', 'REPLIC_PRIORIDADE', null) from rdb$database", []);
    
}
