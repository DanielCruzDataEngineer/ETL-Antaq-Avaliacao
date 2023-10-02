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