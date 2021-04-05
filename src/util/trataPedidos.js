module.exports = {
  async trata(firebird, modification, identificador) {
    var verificaImporta = await this.getParametro(
      firebird,
      "mobile_aprovacao_auto"
    );
    if (verificaImporta == "S") {
      if (modification.tabela == "MOBILE_PEDIDO") {
        const tamanhoIdentificador = identificador.length;
        var identificador = await this.trataPedido(
          firebird,
          modification,
          identificador
        );
        if (tamanhoIdentificador < identificador.length) {
          await firebird.execute(
            "UPDATE OR INSERT INTO INTEGRADOR_DATA_STATUS " +
              "(UUID,Tabela,data_operacao,situacao,sincronizado) VALUES (?,?,?,?,0) MATCHING(UUID)",
            [
              modification.uuid,
              modification.tabela,
              new Date(modification.data_operacao),
              modification.situacao,
            ]
          );

          return identificador;
        }
      } else {
        await this.trataPedidoItem(firebird, modification, identificador);
      }
    }
    await firebird.execute(
      "UPDATE OR INSERT INTO INTEGRADOR_DATA_STATUS " +
        "(UUID,Tabela,data_operacao,situacao,sincronizado) VALUES (?,?,?,?,1) MATCHING(UUID)",
      [
        modification.uuid,
        modification.tabela,
        new Date(modification.data_operacao),
        modification.situacao,
      ]
    );

    return identificador;
  },
  async trataPedido(firebird, modification) {
    var natOperPadrao = await this.getParametro(
      firebird,
      "mobile_natoper_padrao"
    );
    mod = [];
    mod.colunas = [];
    var modificationValues = [];
    var tabelaFieldsMarkers = [];
    var hasUUIDOnDados = false;
    var idEmpresa = "";
    var identificador = [];
    var idPedido = "";
    var obs = "";
    var obsNota = "";
    var serie = "";
    var mac = "";
    var idCliente = "";
    var vDesc = 0;
    var vTotal = 0;
    var vSubTotal = 0;

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
        switch (colkey.toUpperCase()) {
          case "IDPEDIDO":
            idPedido = modification.dados[colkey];
            break;
          case "IDEMPRESA":
            mod.colunas.push(colkey.toUpperCase());
            idEmpresa = modification.dados[colkey];
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "IDCLIENTE":
            mod.colunas.push("CLIFOR");
            idCliente = modification.dados[colkey];
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "IDFORMAPGTO":
            mod.colunas.push("FORMAPAG");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "IDCONDICAOPGTO":
            mod.colunas.push("CONDPAG");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "IDVENDEDOR":
            mod.colunas.push("VENDEDOR");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "DATAPEDIDO":
            mod.colunas.push("DTEMISSAO");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            mod.colunas.push("HORAEMISSAO");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "DTENTREGA":
            if (new Date(modification.dados[colkey]).getTime() !== 1) {
              mod.colunas.push("DTENTREGA");
              modificationValues.push(modification.dados[colkey]);
              tabelaFieldsMarkers.push("?");
              mod.colunas.push("HORAENTREGA");
              modificationValues.push(modification.dados[colkey]);
              tabelaFieldsMarkers.push("?");
            }
            break;
          case "VLRDESCONTO":
            vDesc = parseFloat(modification.dados[colkey]);
            break;
          case "VLRTOTAL":
            vTotal = parseFloat(modification.dados[colkey]);
            mod.colunas.push("VLRNOTA");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "VLRSUBTOTAL":
            vSubTotal = parseFloat(modification.dados[colkey]);
            mod.colunas.push("VLRPRODS");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "VLRACRESCIMO":
            mod.colunas.push("VLRACRES");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "OBS":
            obs = modification.dados[colkey];
            break;
          case "OBS_NOTA":
            obsNota = modification.dados[colkey];
            break;
          case "MAC":
            mac = modification.dados[colkey];
            break;
        }
      }
    }
    /* TIPODOC */
    mod.colunas.push("IDTIPODOC");
    modificationValues.push(6);
    tabelaFieldsMarkers.push("?");

    serie = await this.getSerieMobile(firebird, idEmpresa);
    serie = serie == "" ? "MOB" : serie;
    mod.colunas.push("SERIE");
    modificationValues.push(serie);
    tabelaFieldsMarkers.push("?");

    /* TIPO */
    mod.colunas.push("TIPO");
    modificationValues.push("S");
    tabelaFieldsMarkers.push("?");
    /* NOME CLI */

    nomecli = await firebird.query(
      "SELECT FIRST 1 NOME FROM PESSOAS WHERE IDEMPRESA = ? AND IDTIPO_PS = 1 AND IDPESSOA = ?",
      [idEmpresa, idCliente],
      ["nome"]
    );
    if (nomecli == null || nomecli.length < 1) {
      return identificador;
    }
    mod.colunas.push("NOMECLIFOR");
    modificationValues.push(nomecli[0].nome);
    tabelaFieldsMarkers.push("?");
    /* SITUAÇÃO */
    mod.colunas.push("SITUACAO");
    modificationValues.push(1);
    tabelaFieldsMarkers.push("?");
    /* USUARIO */
    mod.colunas.push("USUARIO");
    modificationValues.push(1);
    tabelaFieldsMarkers.push("?");
    /* BAIXOUESTOQUE */
    mod.colunas.push("BAIXOUESTOQUE");
    modificationValues.push(
      await this.regraEstoqueNatOper(firebird, natOperPadrao)
    );
    tabelaFieldsMarkers.push("?");
    /* NATOPER */
    mod.colunas.push("NATOPER");
    modificationValues.push(natOperPadrao);
    tabelaFieldsMarkers.push("?");
    /* DESCONTO */
    mod.colunas.push("VLRDESC");
    modificationValues.push(vSubTotal - vTotal);
    tabelaFieldsMarkers.push("?");
    /* ATENDIDO */
    mod.colunas.push("ATEND_PEDIDO");
    modificationValues.push("N");
    tabelaFieldsMarkers.push("?");

    /* GRUPO_CAIXA */
    mod.colunas.push("GRUPO_CAIXA");
    modificationValues.push(
      this.getParametro(firebird, "FOPCSISTEMA-GRUTITRECPAD")
    );
    tabelaFieldsMarkers.push("?");

    /* IDDOC */
    mod.colunas.push("IDDOC");
    var idDoc = await this.getIDDoc(firebird, idEmpresa);
    identificador[idPedido] = idDoc;
    modificationValues.push(idDoc);
    tabelaFieldsMarkers.push("?");
    /* NUMERO PEDIDO */
    mod.colunas.push("NUMERO");
    modificationValues.push(
      await this.getNumeroPedido(firebird, idEmpresa, serie)
    );
    tabelaFieldsMarkers.push("?");

    await firebird.execute(
      `
        UPDATE OR INSERT INTO DOCS
        (${mod.colunas.join(",")})
        VALUES
        (${tabelaFieldsMarkers.join(
          ","
        )}) MATCHING (IDEMPRESA, IDTIPODOC, IDDOC)`,
      modificationValues
    );

    await firebird.execute(
      `
        UPDATE OR INSERT INTO DOCSOBS
        (IDEMPRESA, IDTIPODOC, IDDOC, DESCRICAO)
        VALUES
        (?, ?, ?, ?) MATCHING (IDEMPRESA, IDTIPODOC, IDDOC)`,
      [idEmpresa, 6, idDoc, Buffer.from(obs, "utf-8")]
    );

    await firebird.execute(
      `
        UPDATE OR INSERT INTO DOCSADICIONAIS
        (IDEMPRESA, IDTIPODOC, IDDOC, DESCRICAO)
        VALUES
        (?, ?, ?, ?) MATCHING (IDEMPRESA, IDTIPODOC, IDDOC)`,
      [idEmpresa, 6, idDoc, Buffer.from(obsNota, "utf-8")]
    );

    await firebird.execute(
      `
        UPDATE MOBILE_PEDIDO
        SET SERIE = ?,
            DATAPROC = ?,
            STATUSPEDIDO = ?,
            IDDOC = ?
        WHERE IDPEDIDO = ?
            AND IDEMPRESA = ?
            AND MAC = ?`,
      [serie, new Date(), 2, idDoc, idPedido, idEmpresa, mac]
    );

    return identificador;
  },
  async trataPedidoItem(firebird, modification, identificador) {
    var natOperPadrao = await this.getParametro(
      firebird,
      "mobile_natoper_padrao"
    );
    mod = [];
    mod.colunas = [];
    var modificationValues = [];
    var tabelaFieldsMarkers = [];
    var hasUUIDOnDados = false;
    var idEmpresa = "";
    var idPedido = "";
    var idMobilePedido = "";
    var mac = "";
    var idProduto = "";
    var vUnit = 0;
    var vDesc = 0;
    var qtd = 0;
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
        switch (colkey.toUpperCase()) {
          case "IDPEDIDO":
            idMobilePedido = modification.dados[colkey];
            idPedido = "";
            for (var key in identificador) {
              if (key.toUpperCase() == modification.dados[colkey]) {
                idPedido = identificador[key];
              }
            }
            if (idPedido == "") {
              return identificador;
            }
            break;
          case "IDEMPRESAPEDIDO":
            idEmpresa = modification.dados[colkey];
            mod.colunas.push("IDEMPRESA");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "MACPEDIDO":
            mac = modification.dados[colkey];
            break;
          case "SEQUENCIA":
            mod.colunas.push("IDSEQ");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "IDPRODUTO":
            idProduto =
              "0".repeat(8 - (modification.dados[colkey] + "").length) +
              modification.dados[colkey];
            mod.colunas.push("IDPRODUTO");
            modificationValues.push(idProduto);
            tabelaFieldsMarkers.push("?");
            break;
          case "QUANTIDADE":
            qtd = parseFloat(modification.dados[colkey]);
            mod.colunas.push("QTDADE");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "VALORUNITARIO":
            vUnit = parseFloat(modification.dados[colkey]);
            mod.colunas.push("VLR_UNIT_VENDA");
            modificationValues.push(modification.dados[colkey]);
            tabelaFieldsMarkers.push("?");
            break;
          case "VALORDESCONTO":
            vDesc = parseFloat(modification.dados[colkey]);
            break;
        }
      }
    }
    /* TIPODOC */
    mod.colunas.push("IDTIPODOC");
    modificationValues.push(6);
    tabelaFieldsMarkers.push("?");

    mod.colunas.push("IDDOC");
    modificationValues.push(idPedido);
    tabelaFieldsMarkers.push("?");

    mod.colunas.push("NATOPER");
    modificationValues.push(natOperPadrao);
    tabelaFieldsMarkers.push("?");

    const retorno = await firebird.query(
      "SELECT FIRST 1 un, sittrib, descricao, est_atual, " +
        "classfiscal FROM PRODUTOS WHERE IDPRODUTO = ? AND IDEMPRESA = ?",
      [idProduto, idEmpresa],
      ["un", "sittrib", "descricao", "est_atual", "classfiscal"]
    );

    mod.colunas.push("UN");
    modificationValues.push(retorno[0].un);
    tabelaFieldsMarkers.push("?");

    mod.colunas.push("SITTRIB");
    modificationValues.push(retorno[0].sittrib);
    tabelaFieldsMarkers.push("?");

    mod.colunas.push("DESCRICAO");
    modificationValues.push(retorno[0].descricao);
    tabelaFieldsMarkers.push("?");

    mod.colunas.push("VLRUNIT");
    modificationValues.push(vUnit * qtd - vDesc);
    tabelaFieldsMarkers.push("?");

    mod.colunas.push("HORAMOV");
    modificationValues.push(new Date());
    tabelaFieldsMarkers.push("?");

    mod.colunas.push("REFERENCIA");
    modificationValues.push("");
    tabelaFieldsMarkers.push("?");
    if (this.getParametro(firebird, "FOPCSISTEMA-CHESTPEDVEN") == "S") {
      mod.colunas.push("ESTOQUE");
      modificationValues.push(
        await this.regraEstoqueNatOper(firebird, natOperPadrao)
      );
      tabelaFieldsMarkers.push("?");
    } else {
      mod.colunas.push("ESTOQUE");
      modificationValues.push("N");
      tabelaFieldsMarkers.push("?");
    }

    mod.colunas.push("CLASSFISCAL");
    modificationValues.push(retorno[0].classfiscal);
    tabelaFieldsMarkers.push("?");

    var retDescontos = await firebird.query(
      `SELECT FIRST 1 VLRDESCONTO, VLRSUBTOTAL FROM MOBILE_PEDIDO 
                WHERE IDPEDIDO = ? AND IDEMPRESA = ? AND MAC = ?`,
      [idMobilePedido, idEmpresa, mac],
      ["vlrdesconto", "vlrsubtotal"]
    );

    if (parseFloat(retDescontos[0].vlrdesconto) > 0) {
      vDesc =
        vDesc +
        (vUnit * qtd * (parseFloat(retDescontos[0].vlrdesconto) * 100)) /
          parseFloat(retDescontos[0].vlrsubtotal) /
          100;
    }

    var pDescProd = 0;
    if (vDesc > 0) {
      pDescProd = (vDesc * qtd) / (qtd * vUnit);
    }

    mod.colunas.push("PERDESC");
    modificationValues.push(pDescProd * 100);
    tabelaFieldsMarkers.push("?");

    mod.colunas.push("VALDESCONTO");
    modificationValues.push(vDesc);
    tabelaFieldsMarkers.push("?");

    await firebird.execute(
      `
        UPDATE OR INSERT INTO MOVDOCS
        (${mod.colunas.join(",")})
        VALUES
        (${tabelaFieldsMarkers.join(
          ","
        )}) MATCHING (IDEMPRESA, IDTIPODOC, IDDOC, IDSEQ)`,
      modificationValues
    );

    await firebird.execute(
      "UPDATE OR INSERT INTO INTEGRADOR_DATA_STATUS " +
        "(UUID,Tabela,data_operacao,situacao,sincronizado) VALUES (?,?,?,?,0) MATCHING(UUID)",
      [
        modification.uuid,
        modification.tabela,
        new Date(modification.data_operacao),
        modification.situacao,
      ]
    );
  },
  async getParametro(firebird, Parametro) {
    const retorno = await firebird.query(
      "SELECT FIRST 1 VALOR FROM PARAMETROS WHERE IDCHAVE = ?",
      [Parametro],
      ["valor"]
    );
    return retorno.length > 0 ? retorno[0].valor : "N";
  },
  async getIDDoc(firebird, IDEmpresa) {
    const retorno = await firebird.query(
      "select coalesce(max(iddoc),0) + 1 as Ultimo from docs where idempresa = ?",
      [IDEmpresa],
      ["ultimo"]
    );
    return retorno[0].ultimo;
  },
  async getNumeroPedido(firebird, IDEmpresa, serie) {
    const retorno = await firebird.query(
      "select coalesce(max(numero),0) + 1 as Ultimo from docs where idempresa = ? and serie = ?",
      [IDEmpresa, serie],
      ["ultimo"]
    );
    return retorno[0].ultimo;
  },
  async getSerieMobile(firebird, idEmpresa) {
    const retorno = await firebird.query(
      "select coalesce(serie_mobile, 'MOB') as serie_mobile from empresas where idempresa = ?",
      [idEmpresa],
      ["serie_mobile"]
    );
    return retorno[0].serie_mobile;
  },
  async regraEstoqueNatOper(firebird, idNatOper) {
    const retorno = await firebird.query(
      "SELECT COALESCE(N.ESTOQUE, 'N') AS ESTOQUE FROM NATOPER N WHERE N.IDNATOPER = ?",
      [idNatOper],
      ["estoque"]
    );
    if (this.getParametro(firebird, "UTILIZA_BLOCO_K") == "S") {
      return "N";
    } else {
      return retorno[0].estoque;
    }
  },
};
