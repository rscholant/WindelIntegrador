function prepareMobileCliente(data){
    var buf = Buffer.from(data.dados.OBSERVACOES, 'utf8');
    data.dados.OBSERVACOES = buf;
    data.forceDados = {};
    data.forceDados.OBSERVACOES = true;
    return data;
}

var functions = {
    MOBILE_CLIENTE: prepareMobileCliente
};

module.exports = (modification)=>{
    
    if(modification.tabela in functions){
        return functions[modification.tabela](modification);
    }

    return modification;
}