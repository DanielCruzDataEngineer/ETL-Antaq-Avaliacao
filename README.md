ETL-Antaq
==============================
Respondendo a primeira pergunta

1- Qual o seu nível de domínio nas técnicas/ferramentas listadas abaixo, onde:
• 0, 1, 2 - não tem conhecimento e experiência;
• 3, 4 ,5 - conhece a técnica e tem pouca experiência;
• 6 - domina a técnica e já desenvolveu vários projetos utilizando-a.
Tópicos de Conhecimento:
• Manipulação e tratamento de dados com Python: 6
• Manipulação e tratamento de dados com Pyspark: 6
• Desenvolvimento de data workflows em Ambiente Azure com databricks: 5
• Desenvolvimento de data workflows com Airflow: 5
• Manipulação de bases de dados NoSQL: 6
• Web crawling e web scraping para mineração de dados: 6
• Construção de APIs: REST, SOAP e Microservices:  5

Respondendo a segunda,
2 - 
Para as tabelas, minha orientação seria armazená-las em uma estrutura de banco de dados SQL no Data Lake. Isso se deve à natureza altamente estruturada e relacionada dessas tabelas, que contêm informações detalhadas sobre atracações de embarcações e movimentações de cargas em portos.

Essas tabelas envolvem dados como datas, localidades, tipos de operação, quantidades, pesos e outras métricas de desempenho, que são mais adequadas para armazenamento e consulta em um ambiente SQL. Consultas complexas, agregações e junções entre essas tabelas podem ser necessárias para análises de desempenho, tendências e relatórios detalhados.

Além disso, a estrutura de banco de dados SQL oferece a capacidade de garantir a integridade referencial entre as tabelas, o que é importante quando há relacionamentos complexos entre os dados, como no caso das atracações e suas cargas associadas.

Em resumo, para as tabelas , a escolha de um banco de dados SQL proporcionaria eficiência na consulta e na análise de dados estruturados e relacionados, atendendo aos requisitos de armazenamento e consulta no Data Lake de forma adequada.


Avaliação ETL

Organização do Projeto
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    │
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── extractors        
    │   │   └── make_dataset.py <- Código para extrair dados da Antaq
    │   │
    │   ├── loaders       <- Códigos para carregar dados no DataLake e banco de dados 
    │   │   └── antaq_to_lake.py
    │   │   └── antaq_to_sql.py
    │   │
    │   ├── transform     
    │   │   └──  etl_antaq.py    <- Código para transformação dos dados   
    │   │    
    │   │── main.py <- Código que roda todo o fluxo
    │   │
    │   └── results  
    │   |    └── consulta_sql.py <- Código que faz a consulta SQL no SQL SERVER via Spark e retorna os dados em formato de Excel.
    |   |
    |   └── querys <- Traz os códigos de consultas sqls utilizados.
    |         └── creation_atracacao_fato.txt <- Retorna a criação da tabela atracaco_fato
    |         └── creation_carga_fato.txt <- Retorna a criação da tabela carga_fato
    |         └── select_resultado_antaq.txt <- Retorna a criação da tabela com os resultados requeridos pelos economistas
    |
    │── documentation <- Traz documentação sobre colunas das tabelas, metadados. 
    |   
    |── airflow <- Traz as dags, e arquivos de deploy de Docker para Airflow. 
    |
    |
    |
    └── antaq_data <- Traz extrações do site da Antaq de 2023 até 2021, também tem um histórico de 2020, como um adicional.


--------

