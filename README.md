# Projeto ETL - Análise de Orçamento do Estado de São Paulo (2019)

## Resultado 
#### Tabela Final
A tabelaFinal é criada no banco de dados contendo a soma do valor total de receita e despesa do Estado de São Paulo, agrupado pela fonte de recursos e tipo de despesa. Conforme o esperado.

### Dez maiores fontes de recursos
![image](https://github.com/linharesbruno/teste-engenheirodados-esfera/assets/131724502/2d7237a6-82a3-4017-b798-803606712179)

### Dez maiores fontes de despesas
![image](https://github.com/linharesbruno/teste-engenheirodados-esfera/assets/131724502/3e1cb63d-5167-4f79-a99a-8b2eb15b2088)

## Construção da Logica 
1- Comecei definindo o local onde os arquivos seriam consumidos; neste caso, foi um diretório local.

2- Carreguei os arquivos e criei as duas  tabelas .
   [TabelaDespesas]
   
   [TabelaReceitas] 
   
3- Com as tabelas criadas do banco, usei Sql para manipular e analisar os dados.

4- Para chegar a este código, realizei algumas pesquisas para compreender o melhor caminho. Como o projeto estava em um ambiente controlado, busquei uma abordagem mais simples, diferente de um cenário real.

Observação: Optei por excluir as tabelas toda vez que o processo é executado, mas poderíamos usar a operação de merge. Dessa forma, inseriríamos apenas os dados que ainda não existem na tabela.

## Estrutura do codigo

1- Carregar os arquivos e criar as duas tabelas.

2- Analisar e criar a tabela final

3- Criar a Dag.

   1.load_and_export_task = carrega os arquivos e cria as duas tabelas
   2.create_final_table_task = cria a tabela final

 
## Conclusão
Este projeto oferece uma abordagem ETL para análise do orçamento do Estado de São Paulo em 2019, proporcionando insights valiosos sobre as receitas, despesas e as principais fontes de recursos e tipos de despesa. 




