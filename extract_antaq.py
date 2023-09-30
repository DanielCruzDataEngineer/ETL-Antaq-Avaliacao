
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from antaq_to_lake import write_to_mongo
from antaq_to_sql import write_to_sql
from pyspark.sql.functions import col, to_date, month, year
from extract_antaq_zip import extract_from_antaq
import os

# Inicializar o SparkSession
spark = SparkSession.builder.\
    appName("ANTAQDataProcessing").config("spark.mongodb.input.uri", "mongodb://localhost:27017/local") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/local") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config("spark.driver.extraClassPath", "../mssql-jdbc-6.4.0.jre8.jar")\
    .getOrCreate()

# URL base dos arquivos


# Diretório de destino para salvar os arquivos zip e os dados processados
output_dir = "antaq_data"

# Criar o diretório se não existir
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
tabelas_e_colunas = {
    "Atracacao": [
        "Atracacao",
        "TemposAtracacao"
    ],
    "Carga": [
        "Carga",
        "Carga_Conteinerizada"
    ]
}

url_db = "jdbc:sqlserver://localhost:1433;databaseName=master"
# Anos de interesse
anos = range(2023, 2019, -1)    # De 2023 a 2021 + Trazendo ano de 2020 para complementar possível histórico

for ano in anos:
    file_name_zip = str(ano) +  '.zip'
    url = f'https://web3.antaq.gov.br/ea/txt/{ano}.zip'

    extract_from_antaq(ano,output_dir)
    
            
    for tabela, file_names in tabelas_e_colunas.items():
        for file_name in file_names:
            
  

            df = spark.read.csv(f"antaq_data/{ano}{file_name}.txt", sep=";", header=True)

            if file_name == 'Atracacao':
                df = df.na.drop(subset=["Data Atracação"])
                df = df.withColumn("Data Início Operação", to_date(col("Data Início Operação"), "dd/MM/yyyy HH:mm:ss"))
                
                # Extrair o mês e o ano
                df = df.withColumn("Mês da data de início da operação", month(col("Data Início Operação")))
                df = df.withColumn("Ano da data de início da operação", year(col("Data Início Operação")))

                print(df.show(10))
            df.createOrReplaceTempView(f"{file_name}")
            # Configurar a URI de conexão com o MongoDB
            mongo_uri = f"mongodb://localhost:27017/Lake_Antaq.{file_name}_fato"

            # Salvar o DataFrame diretamente no MongoDB

                

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
                        from Atracacao a INNER JOIN TemposAtracacao b ON (a.IDAtracacao = b.IDAtracacao)""")

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
    parameters_sql = [(atracacao_fato,"atracacao_fato"),(carga_fato,"carga_fato")]
    write_to_sql(parameters_sql,url_db)
    query = """WITH AtracacoesNordesteCeara AS (
    SELECT
        YEAR([Data Atracação]) AS Ano,
        MONTH([Data Atracação]) AS Mês,
        COUNT(*) AS NumeroAtracacoes,
        AVG(CAST(REPLACE(TEsperaAtracacao, ',', '.') AS DECIMAL)) AS MediaTempoEspera,
        AVG(CAST(REPLACE(TAtracado, ',', '.') AS DECIMAL)) AS MediaTempoAtracado,
        SGUF
    FROM atracacao_fato_fato  
    WHERE
        YEAR([Data Atracação]) IN (2020, 2021, 2022, 2023)
    GROUP BY
        YEAR([Data Atracação]),
        MONTH([Data Atracação]),
        SGUF
)

SELECT * FROM (   
SELECT
    Ano,
    Mês,
    'Brasil' as Localidade,
    SUM(NumeroAtracacoes) AS NumeroAtracacoes,
    (SUM(NumeroAtracacoes) - LAG(SUM(NumeroAtracacoes), 12) OVER (ORDER BY Ano, Mês)) AS VariacaoNumeroAtracacoes,
    AVG(MediaTempoEspera) AS MediaTempoEspera,
    AVG(MediaTempoAtracado) AS MediaTempoAtracado
FROM
    AtracacoesNordesteCeara
GROUP BY Ano, Mês 

UNION ALL
    
SELECT
    Ano,
    Mês,
    'Nordeste' as Localidade,
    SUM(NumeroAtracacoes) AS NumeroAtracacoes,
    (SUM(NumeroAtracacoes) - LAG(SUM(NumeroAtracacoes), 12) OVER (ORDER BY Ano, Mês)) AS VariacaoNumeroAtracacoes,
    AVG(MediaTempoEspera) AS MediaTempoEspera,
    AVG(MediaTempoAtracado) AS MediaTempoAtracado
FROM
    AtracacoesNordesteCeara
WHERE SGUF IN ('CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA', 'MA')
GROUP BY Ano, Mês
   UNION ALL 
       SELECT
        Ano,
        Mês,
        'Ceará' as Localidade,
        NumeroAtracacoes,
        NumeroAtracacoes - LAG(NumeroAtracacoes, 12) OVER (ORDER BY Ano, Mês) AS VariacaoNumeroAtracacoes,
        MediaTempoEspera,
        MediaTempoAtracado
    FROM
        AtracacoesNordesteCeara
        WHERE SGUF = 'CE' ) as A
        WHERE A.Ano IN ('2021','2023')"""   
