# Autor: Paulo Carvalho	- Cel: 11-941551222

#imports 
#import biblioteca SQLContext
from pyspark.sql import SQLContext

#import biblioteca SparkContext
from pyspark import SparkContext

#import functions pyspark.sql 
from pyspark.sql import functions as sf

#import IntegerType para conversão do valor de bytes
from pyspark.sql.types import IntegerType

#gerando dataframe de julho lendo o arquivo de log da nasa como csv e formatando a data que ficou dividida
julho95 = sqlContext.read.csv(r'C:\\semantix\\logs\\access_log_Jul95',sep=' ',timestampFormat='[dd-MMM-yyyy:hh:mm:ss -0400]').cache()

#gerando o novo dataframe de julho com a junção das partes da data e rename das colunas para o padrão do documento
julho95_2 = julho95.withColumn("DataCompleta",sf.concat("_c3","_c4")).selectExpr("_c0 as host", "_c5 as requisicao", "_c6 as codretorno", "_c7 as bytes", "DataCompleta as data")

#gerando dataframe de agosto lendo o arquivo de log da nasa como csv e formatando a data que ficou dividida
agosto95 = sqlContext.read.csv(r'C:\\semantix\\logs\\access_log_Aug95',sep=' ',timestampFormat='[dd-MMM-yyyy:hh:mm:ss -0400]').cache()

#gerando o novo dataframe de agosto com a junção das partes da data e rename das colunas para o padrão do documento
agosto95_2 = agosto95.withColumn("DataCompleta",sf.concat("_c3","_c4")).selectExpr("_c0 as host", "_c5 as requisicao", "_c6 as codretorno", "_c7 as bytes", "DataCompleta as data")

#Junção dos 
df_unido = julho95_2.unionAll(agosto95_2)

#verificação das linhas 
#julho95_2.count() 1891715
#agosto95_2.count() 1569898
#df_unido.count() 3461613

#questão 1: Número de hosts únicos.
df_unido.select("host").distinct().count()
#retorno 137979

#questão 2: O total de erros 404.
df_unido.where(df_unido.codretorno == "404").count()
#retorno 20871

#questão 3: Os 5 URLs que mais causaram erro 404.
df_unido.where(df_unido.codretorno == "404").groupBy("requisicao").count().orderBy("count",ascending=False).show(5,truncate=False)
#retorno
#+---------------------------------------------------------+-----+
#|requisicao                                               |count|
#+---------------------------------------------------------+-----+
#|GET /pub/winvn/readme.txt HTTP/1.0                       |2004 |
#|GET /pub/winvn/release.txt HTTP/1.0                      |1732 |
#|GET /shuttle/missions/STS-69/mission-STS-69.html HTTP/1.0|682  |
#|GET /shuttle/missions/sts-68/ksc-upclose.gif HTTP/1.0    |426  |
#|GET /history/apollo/a-001/a-001-patch-small.gif HTTP/1.0 |384  |
#+---------------------------------------------------------+-----+ 


#questão 4: Quantidade de erros 404 por dia.
df_unido.where(df_unido.codretorno == "404").groupBy("data").count().orderBy("count",ascending=False).show(20,truncate=False)

#retorno das top 20 linhas
#+---------------------------+-----+
#|data                       |count|
#+---------------------------+-----+
#|[28/Aug/1995:11:56:35-0400]|7    |
#|[11/Aug/1995:12:05:59-0400]|7    |
#|[11/Jul/1995:14:08:06-0400]|5    |
#|[12/Jul/1995:10:20:43-0400]|5    |
#|[12/Jul/1995:10:35:11-0400]|5    |
#|[12/Jul/1995:10:35:12-0400]|5    |
#|[28/Aug/1995:17:14:32-0400]|5    |
#|[12/Jul/1995:10:35:09-0400]|5    |
#|[12/Jul/1995:10:21:30-0400]|5    |
#|[11/Aug/1995:12:05:58-0400]|5    |
#|[12/Jul/1995:10:24:50-0400]|5    |
#|[17/Aug/1995:16:55:00-0400]|5    |
#|[28/Aug/1995:17:14:42-0400]|5    |
#|[12/Jul/1995:10:35:01-0400]|4    |
#|[20/Jul/1995:07:21:17-0400]|4    |
#|[12/Jul/1995:10:35:03-0400]|4    |
#|[28/Aug/1995:01:05:47-0400]|4    |
#|[28/Aug/1995:17:14:15-0400]|4    |
#|[04/Aug/1995:18:45:58-0400]|4    |
#|[12/Jul/1995:10:21:32-0400]|4    |
#+---------------------------+-----+



#bytes estão como string, precisamos converter
#df_unido.printSchema()
#root
# |-- host: string (nullable = true)
# |-- requisicao: string (nullable = true)
# |-- codretorno: string (nullable = true)
# |-- bytes: string (nullable = true)
# |-- data: string (nullable = true)




#questão 5: O total de bytes retornados.

#converter campo bytes em inteiro para somar
dfvalor = df_unido.withColumn("bytes", df_unido["bytes"].cast(IntegerType()))

#Conferindo o novo schema
#dfvalor.printSchema()
#root
# |-- host: string (nullable = true)
# |-- requisicao: string (nullable = true)
# |-- codretorno: string (nullable = true)
# |-- bytes: integer (nullable = true)
# |-- data: string (nullable = true)


#soma todos os valores do campo bytes
dfvalor.groupBy().sum("bytes").show()

#retorno
#+-----------+
#| sum(bytes)|
#+-----------+
#|65524319796|
#+-----------+
































