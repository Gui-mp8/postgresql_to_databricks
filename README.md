# Extraindo dados do PostgreSQL para o Databricks

Este projeto tem como objetivo a resolucao do desafio tecnico da Vertigo tecnologia.

Este desafio consiste em extrair dados de um PostgreSQL, tarnsforma-los em parquet, analisa-los e entao disponibilizar as analises em tabelas com o formato delta.

## Tecnologias
  <table>
    <tr>
      <td>Cloud</td>
      <td>Codigo</td>
      <td>Banco de Dados</td>
      <td>Outros</td>
    </tr>
      <tr>
      <td>Azure Databricks</td>
      <td>Python/PySpark</td>
      <td>PostgreSQL</td>
      <td>DBeaver/dbdiagram.io</td>
    </tr>
  </table>

## Prerequisitos
- E necessario a criacao de uma conta na [Azure](https://azure.microsoft.com/pt-br/free/).

**OBS**: E possivel conseguir credito de graca para executar o projeto.

- E necessaria a conexao ao banco [PostgreSQL](https://uibakery.io/sql-playground) disponibilizado no desafio

**OBS**: Este banco pode ser visualizado usando o [DBeaver](https://dbeaver.io/download/).

## Executando o Desafio

### Diagrama de ER do banco disponibilizado.
Link para o [dbdiagram.io](https://dbdiagram.io/home)

![vertigo-desafio](https://github.com/Gui-mp8/postgresql_to_databricks/assets/94998733/0a62525a-7ffe-4105-9d6b-103f8227e96f)

### Criando um notebook no Databricks.

Antes de tudo e necessario a criacao da conta na Azure, apos isto, basta seguir as seguintes documentacoes:

- [Configuracao da Azure](https://learn.microsoft.com/pt-br/azure/databricks/getting-started/)
- [Criando Notebook](https://docs.databricks.com/pt/getting-started/quick-start.html)

### Execucao do codigo

O codigo para execucao esta no arquivo ``main.ipynb``

Para executa-lo no Databricks, basta copiar cada etapa e colar em algum notebook criado, ou importar o arquivo para o Databricks.

## Fluxo dos Dados

![vertigo](https://github.com/Gui-mp8/postgresql_to_databricks/assets/94998733/eac9751b-abc5-406b-930d-cb9f0339675a)
