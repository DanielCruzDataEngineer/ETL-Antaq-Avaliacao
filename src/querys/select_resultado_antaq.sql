WITH AtracacoesNordesteCeara AS (
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
        WHERE A.Ano IN ('2021','2023')