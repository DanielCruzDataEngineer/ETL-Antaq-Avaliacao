SELECT
                    c.IDCarga,
                    c.IDAtracacao,
                    Origem,
                    Destino,
                    CDMercadoria,
                    `Tipo Operação da Carga`,
                    `Carga Geral Acondicionamento`,
                    ConteinerEstado,
                    `Tipo Navegação`,
                    FlagAutorizacao,
                    FlagCabotagem,
                    FlagCabotagemMovimentacao,
                    FlagConteinerTamanho,
                    FlagLongoCurso,
                    FlagMCOperacaoCarga,
                    FlagOffshore,
                    FlagTransporteViaInterioir,
                    `Percurso Transporte em vias Interiores`,
                    `Percurso Transporte Interiores`,
                    STNaturezaCarga,
                    STSH2,
                    STSH4,
                    `Natureza da Carga`,
                    Sentido,
                    TEU,
                    QTCarga,
                    VLPesoCargaBruta,
                    `Porto Atracação`,
                    SGUF,
                    DATE_FORMAT(a.`Data Início Operação`, 'MM') AS `Ano da data de início da operação da atracação`,
                    DATE_FORMAT(a.`Data Início Operação`, 'yyyy') AS `Mês da data de início da operação da atracação`,
                    (c.VLPesoCargaBruta - cc.VLPesoCargaConteinerizada) AS PesoLiquido
                        FROM Carga c
                        JOIN Carga_Conteinerizada cc ON c.IDCarga = cc.IDCarga
                        JOIN Atracacao a ON a.IDAtracacao = c.IDAtracacao