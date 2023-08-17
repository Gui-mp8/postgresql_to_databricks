# Extraindo dados do PostgreSQL para o Databricks

Este projeto tem como objetivo a resolução do desafio técnico da Vertigo tecnologia.

Este desafio consiste em extrair dados de um PostgreSQL, transformá-los em parquet, analisá-los e então disponibilizar as análises em tabelas no formato Delta.

## Tecnologias
<table>
    <tr>
        <td>Cloud</td>
        <td>Código</td>
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

## Pré Requisitos
- É necessário a criação de uma conta na [Azure](https://azure.microsoft.com/pt-br/free/).

**OBS**É possível conseguir um período gratuito de utilização e créditos para serem utilizados.

- E necessária a conexão ao banco ``Ecommerce website database`` neste [site](https://uibakery.io/sql-playground). Ele é um banco ``PostgreSQL``.

**OBS**Este banco pode ser visualizado usando o [DBeaver](https://dbeaver.io/download/).

## Executando o Desafio

### Diagrama de ER do banco disponibilizado.
Link para criar o [dbdiagram.io](https://dbdiagram.io/home)

![vertigo-desafio](https://github.com/Gui-mp8/postgresql_to_databricks/assets/94998733/0a62525a-7ffe-4105-9d6b-103f8227e96f)

### Criando um notebook no Databricks.

Antes de tudo é necessário a criação da conta na Azure, após isto, basta seguir as seguintes documentações:

- [Configuracao da Azure](https://learn.microsoft.com/pt-br/azure/databricks/getting-started/)
- [Criando Notebook](https://docs.databricks.com/pt/getting-started/quick-start.html)

### Execução do código

O código para execução está no arquivo ``main.ipynb``

Para executá-lo no Databricks, basta copiar cada etapa e colar em algum notebook criado, ou [importar o arquivo](https://learn.microsoft.com/pt-br/azure/databricks/notebooks/notebook-export-import) para o Databricks.

## Fluxo dos Dados

![vertigo](https://github.com/Gui-mp8/postgresql_to_databricks/assets/94998733/eac9751b-abc5-406b-930d-cb9f0339675a)
