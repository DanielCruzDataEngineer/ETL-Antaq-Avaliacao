# Comandos gerais
.PHONY: all install data load transform query

# Comando padrão ao executar apenas `make`
all: install data load transform query
fluxo : python src/
# Instalação de requisitos
install:
    pip install -r requirements.txt

# Extrair dados da Antaq
data:
    python src/extractors/make_dataset.py

# Carregar dados no DataLake e no banco de dados SQL
load:
    python src/loaders/antaq_to_lake.py
    python src/loaders/antaq_to_sql.py

# Transformação dos dados
transform:
    python src/transform/etl_antaq.py

# Consultar SQL Server via Spark e gerar arquivo Excel
query:
    python src/results/consulta_sql.py
