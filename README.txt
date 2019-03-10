1->Qual o objetivo do comando cache em Spark?
O comando cache é uma técnica de otimização que salva parte dos resultados do processamento ns memória (MEMORY_ONLY)
O objetivo do comando cache é reservar algum processamento na memória para que na próxima vez que for chamado
não seja necessário todo a leitura e calculo novamente.

https://stackoverflow.com/questions/28981359/why-do-we-need-to-call-cache-or-persist-on-a-rdd
https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-rdd-caching.html

2-> O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?
O processamento do spark é 100 vezes mais rápido trabalhando em memória e 10 vezes mais 
rápido trabalhando com disco.
Fora a possibilidade do spark realizar os processamentos in-memory, o motivo dessa diferença é o 
DAG de ambos, que no caso do MapReduce só possui 2 vértices, sendo 1 para a atividade do mapa e 
outro para a atividade de redução. No caso do spark é possível utilizar vários vértices e arestas
direcionadas.

https://dzone.com/articles/apache-spark-introduction-and-its-comparison-to-ma


3-> Qual é a função do SparkContext?
SparkContext é a porta de entrada para qualquer funcionalidade spark.
É usado para criar a conexão com o cluster Spark, pode ser usado para criar os RDD's, acumuladores, 
variáveis de transmissão.
 
https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/SparkContext.html


4-> Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
É um conceito usado no spark que divide os datasets nos clusters e através da memória compartilhada 
realiza os processamentos de forma muito eficiente.
É somente leitura e com isso podem ser armazenados em memória para realizar ações e transformações.

https://www.tutorialspoint.com/apache_spark/apache_spark_rdd.htm

5-> GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
reduceByKey agrupa os valores com a chave em cada partição antes de embaralhar os dados, ou seja, cada partição
retorna apenas uma ocorrência de cada chave.
 
https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html


6-> Explique o que o código Scala abaixo faz.
val textFile = sc.textFile("hdfs://...")
Le o valor de texto do HDFS e retorna como um RDD de texto

val counts = textFile.flatMap(line => line.split(""))
transforma o RDD em uma coleção de coleções depois achata para um único RDD e quebra a linha usando
separador espaço.

.map(word => (word, 1))
conta cada palavra como 1 e gera um novo RDD

.reduceByKey(_ + _)
junta os dois valores e produz um terceiro

counts.saveAsTextFile("hdfs://...")
conta quantos registros foram retornados e salva como arquivo texto no hdfs
