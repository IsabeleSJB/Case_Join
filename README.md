# Case_Join
Case resolvido durante um processo seletivo na empresa Join Tecnologia &amp; Design
1. Desenhei o diagrama de ER do banco através da extensão dbml, adicionei o código e peguei a imagem de um renderizador. A imagem e o código estão disponíveis na pasta [Diagrama Relacional](https://github.com/IsabeleSJB/Case_Join/tree/main/Diagrama_Relacional)
2. Criei um notebook no Databricks Community que pode ser encontrado nesse [link](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6927250313212885/621646573371541/3639527847248216/latest.html).   
3. Dentro do databricks, importei a biblioteca "psycopg2" do python, para auxiliar com a conexão ao banco de dados, afim de criar uma consulta que me retornasse o nome das tabelas e passase para uma lista, da qual retirei apenas as tabelas que poderiam ser utilizadas : **'customers', 'employees',  'offices',  'orderdetails',  'orders',  'payments',  'product_lines',  'products'**

4. Logo após isso, inicei a sessão do spark para ler e trabalhar com as tabelas. Utilizando a lista das tabelas (já mencionada anteriormente), criei uma lógica com o auxílio do for, que percorresse a lista de tabelas, lesse cada uma delas com o auxílio do spark e depois as transformasse em arquivo parquet. Os arquivos estão disponíveis na pasta [Parquets](https://github.com/IsabeleSJB/Case_Join/tree/main/Parquets)

5. Para criar o merge entre a tabelas e os arquivos parquet criados, fiz uma lógica que pecorresse cada um dos dataframes lidos pelo spark para transformá-los em tabelas deltas, e com o auxílio da "delta.tables" fazer o merge entre as tabelas delta e os aqruivos parquet que contivesse a lógica de insert, update e delete. Para simplificar, criei um dicionário com os nomes das chaves primárias de cada tabela e depois fiz uma lógica que pecorresse a lista de nomes de cada tabela para fazer um merge mais automatizado das tabelas. As tabelas estão disponíveis na pasta [Delta_Tables](https://github.com/IsabeleSJB/Case_Join/tree/main/Delta_Tables)
    Para e xemplificar como seria um merge escolhendo quais campos modificar, também criei uma lógica logo abaixo apenas mudando campos específicos da tabela customers.

6. No notebook criei um código Spark que respondesse as seguintes perguntas:
    - Qual país possui a maior quantidade de itens cancelados?
    - Qual o faturamento da linha de produto mais vendido, considere como os itens
    Shipped, cujo o pedido foi realizado no ano de 2005?
    - Nome, sobrenome e e-mail dos vendedores do Japão, o local-part do e-mail
    deve estar mascarado.

    Todos os códigos para responder essas perguntas foram criados com o auxílio do spark.sql que me permitiu criar um código em sql que contou com o auxílio das cláusulas join (para conectar as tabelas) e where (para adicoonar os filtros) order by (para ordernar os resultados) e group by (para agrupar os campos necessários).
    
7. Todos os resultados foram salvos em dataframes e logo depois salvos em formato delta e estão disponíveis na pasta [Delta_Results](https://github.com/IsabeleSJB/Case_Join/tree/main/Delta_Results).
8. Os resultados foram armazenados neste github público.

