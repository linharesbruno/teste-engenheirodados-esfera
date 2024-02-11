# Projeto ETL - Análise de Orçamento do Estado de São Paulo (2019)

## Resultados e Análises
### Tabela Final
TabelaFinal, é criada no banco de dados contendo a soma do valor total de receita e despesa do Estado de São Paulo, agrupado pela fonte de recursos e tipo de despesa.

## Dez maiores fontes de recursos
![image](https://github.com/linharesbruno/teste-engenheirodados-esfera/assets/131724502/2d7237a6-82a3-4017-b798-803606712179)

## Dez maiores fontes de despesas
![image](https://github.com/linharesbruno/teste-engenheirodados-esfera/assets/131724502/3e1cb63d-5167-4f79-a99a-8b2eb15b2088)

## Construção Logica 
1- Comecei definindo o lugar que os arquivos iriam ser consumidos, neste caso foi um diretorio local.

2- carreguei os arquivos e criei uma tabela para cada arquivo.
   [TabelaDespesas]
   [TabelaReceitas] 
   
3- Com as tabelas criadas, usei Sql para manipular e analisar os dados.

4- Para chegar nesse codigo, realizei umas pesquisas para compreender o melhor caminho, como o projeto estava em um ambiente controlado, procurei fazer o simples, mas em um cenario real enfretaremos diversas variaveis que podem prejudicar nossa automatização, por isso é  necessario um mapeamento minucioso.


## Conclusão
Este projeto oferece uma abordagem ETL para análise do orçamento do Estado de São Paulo em 2019, proporcionando insights valiosos sobre as receitas, despesas e as principais fontes de recursos e tipos de despesa. Certifique-se de adaptar as configurações conforme necessário para o seu ambiente específico.




