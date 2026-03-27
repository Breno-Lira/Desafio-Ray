# Decisao Arquitetural - Gold Layer em Star Schema

## Decisao
Adotar **Star Schema** como modelo principal da camada Gold.

## Modelo Proposto
Dimensoes:
- `dim_cliente`
- `dim_categoria`
- `dim_data`
- `dim_moeda`

Fatos:
- `fato_conta_receber`
- `fato_conta_pagar`
- `fato_meta_mensal`

## Justificativa: Star vs Snowflake
### 1. Consultas analiticas mais simples
No Star, as dimensoes sao desnormalizadas no nivel certo para analise e exigem menos joins. Para Power BI e SQL analitico, isso reduz complexidade e aumenta produtividade.

### 2. Melhor performance para o padrao de uso
O workload esperado e agregacao por tempo, cliente, categoria e moeda. Star tende a performar melhor nesse padrao por reduzir encadeamento de joins.

### 3. Menor custo de manutencao inicial
Para esta fase, o objetivo e entregar valor analitico rapido. Snowflake adicionaria normalizacao extra (mais tabelas e chaves), aumentando custo operacional sem ganho proporcional no cenario atual.

### 4. Dados de origem ja estao bem controlados na Silver
Como a Silver ja faz padronizacao e qualidade, a Gold pode focar em modelagem para consumo. Star aproveita essa separacao de responsabilidades com menos friccao.

### 5. Evolucao incremental
Se no futuro houver crescimento de dimensoes complexas (hierarquias profundas, dominios mestres compartilhados), partes do modelo podem evoluir para Snowflake sem quebrar a semantica da camada fato.

## Trade-offs assumidos
- Possivel redundancia em dimensoes.
- Menor rigor de normalizacao em troca de desempenho e simplicidade.


