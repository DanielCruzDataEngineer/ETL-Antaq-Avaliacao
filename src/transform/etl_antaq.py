

def spark_sql_atracacao(spark):
    """
    Args:
    Função criada para gerar as tabelas requisitadas,
    Foram utilizadas as tabelas temps criadas durante o processo de extração, sendo armazenadas no Hive Metastore Temp do Spark.
    
    spark_sql_atracacao gera a tabela atracacao e utiliza a variável spark, criada no código main.py
    """
    
    
    atracacao_fato = spark.sql(""" SELECT a.IDAtracacao,
                                CDTUP,
                                IDBerco,
                                `Berço`,
                                `Porto Atracação`,
                                `Apelido Instalação Portuária`,
                                `Complexo Portuário`,
                                `Tipo da Autoridade Portuária`,
                                `Data Atracação`,
                                `Data Chegada`,
                                `Data Desatracação`,
                                `Data Início Operação`,
                                `Data Término Operação`,
                                DATE_FORMAT(a.`Data Início Operação`, 'MM') AS `Ano da data de início da operação`,
                                DATE_FORMAT(a.`Data Início Operação`, 'yyyy') AS `Mês da data de início da operação`,
                                `Tipo de Operação`,
                                `Tipo de Navegação da Atracação`,
                                `Nacionalidade do Armador`,
                                FlagMCOperacaoAtracacao,
                                Terminal,
                                `Município`,
                                UF,
                                SGUF,
                                `Região Geográfica`,
                                `Nº da Capitania`,
                                `Nº do IMO`, 
                                TEsperaAtracacao,
                                TEsperaInicioOp,
                                TOperacao,
                                TEsperaDesatracacao,
                                TAtracado,
                                TEstadia           
                        from Atracacao a INNER JOIN TemposAtracacao b ON (a.IDAtracacao = b.IDAtracacao)
                               WHERE a.`Data Atracação` IS NOT NULL""")
    
    return atracacao_fato
def spark_sql_carga(spark):
    """Args:
    Função criada para gerar as tabelas requisitadas,
    Foram utilizadas as tabelas temps criadas durante o processo de extração, sendo armazenadas no Hive Metastore Temp do Spark.
    
    spark_sql_carga gera a tabela carga e utiliza a variável spark, criada no código main.py
    """
    carga_fato = spark.sql( """
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
                    
    """)
    return carga_fato