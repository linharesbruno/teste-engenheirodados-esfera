# Projeto ETL - Análise de Orçamento do Estado de São Paulo (2019)

## Resultado 
#### Tabela Final
A tabelaFinal é criada no banco de dados contendo a soma do valor total de receita e despesa do Estado de São Paulo, agrupado pela fonte de recursos e tipo de despesa.

### Dez maiores fontes de recursos
![image](https://github.com/linharesbruno/teste-engenheirodados-esfera/assets/131724502/2d7237a6-82a3-4017-b798-803606712179)

### Dez maiores fontes de despesas
![image](https://github.com/linharesbruno/teste-engenheirodados-esfera/assets/131724502/3e1cb63d-5167-4f79-a99a-8b2eb15b2088)

## Construção da Logica 
1- Comecei definindo o lugar onde os arquivos iriam ser consumidos, neste caso foi um diretorio local.

2- carreguei os arquivos e criei uma tabela para cada arquivo.
   [TabelaDespesas]
   [TabelaReceitas] 
   
3- Com as tabelas criadas do banco, usei Sql para manipular e analisar os dados.

4- Para chegar nesse codigo, realizei umas pesquisas para compreender o melhor caminho, como o projeto estava em um ambiente controlado, procurei fazer o simples, diferente de um cenario real.

## Estrutura do codigo

1- carregar os arquivos e criar as duas tabelas.

2- Analisar e criar a tabela final

3- criar a Dag
   criei duas task.
   1.load_and_export_task = carrega os arquivos e cria as duas tabelas
   2.create_final_table_task = cria a tabela final

 
## Conclusão
Este projeto oferece uma abordagem ETL para análise do orçamento do Estado de São Paulo em 2019, proporcionando insights valiosos sobre as receitas, despesas e as principais fontes de recursos e tipos de despesa. Certifique-se de adaptar as configurações conforme necessário para o seu ambiente específico.





