SELECT Localidade,
NumeroAtracacoes as `Número de Atracações`,
VariacaoNumeroAtracacoes as `Variação do número de atracação em relação ao mesmo mês do ano anterior`,
MediaTempoEspera as `Tempo de espera médio`,
MediaTempoAtracado as `Tempo atracado médio`,
`Mês`,
 Ano
FROM (   
    SELECT
        Ano,
        `Mês`,
        'Brasil' as Localidade,
        SUM(NumeroAtracacoes) AS NumeroAtracacoes,
        (SUM(NumeroAtracacoes) - LAG(SUM(NumeroAtracacoes), 12) OVER (ORDER BY Ano, `Mês`)) AS VariacaoNumeroAtracacoes,
        AVG(MediaTempoEspera) AS MediaTempoEspera,
        AVG(MediaTempoAtracado) AS MediaTempoAtracado
    FROM
        AtracacoesNordesteCeara
    GROUP BY Ano, `Mês`

    UNION ALL
    
    SELECT
        Ano,
        `Mês`,
        'Nordeste' as Localidade,
        SUM(NumeroAtracacoes) AS NumeroAtracacoes,
        (SUM(NumeroAtracacoes) - LAG(SUM(NumeroAtracacoes), 12) OVER (ORDER BY Ano, `Mês`)) AS VariacaoNumeroAtracacoes,
        AVG(MediaTempoEspera) AS MediaTempoEspera,
        AVG(MediaTempoAtracado) AS MediaTempoAtracado
    FROM
        AtracacoesNordesteCeara
    WHERE SGUF IN ('CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA', 'MA')
    GROUP BY Ano, `Mês`

    UNION ALL
    
    SELECT
        Ano,
        `Mês`,
        'Ceará' as Localidade,
        NumeroAtracacoes,
        NumeroAtracacoes - LAG(NumeroAtracacoes, 12) OVER (ORDER BY Ano, `Mês`) AS VariacaoNumeroAtracacoes,
        MediaTempoEspera,
        MediaTempoAtracado
    FROM
        AtracacoesNordesteCeara
    WHERE SGUF = 'CE'
) as A
WHERE A.Ano IN (2021, 2023)