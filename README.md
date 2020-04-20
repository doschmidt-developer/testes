|Teste

1 - Qual o objetivo do comando cache em Spark?

	O comando cache, por conceito, é o armazenamento de um conjunto de dados em memória para ser reutilizados em estágios subsequentes, em âmbito padrão MEMORY_ONLY. A sua função é ajustar o nível de armazenamento de um RDD, quando liberar a memória, o Spark usará o identificador do nível de armazenamento para decidir quais partições devem ser mantidas.

2 - O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

	Basicamente, a principal diferença entre eles é abordagem no processamento dos dados, o Spark faz tudo em memória, enquanto o MapReduce precisa ler e gravar em disco. O processamento linear com grande volume de dados é uma vantagem no MapReduce, mas o Spark possui o desempenho muito mais rápido e processamento interativo com recursos avançados, como análise em tempo real, machine learning, processamento de gráficos e entre outros.

3 - Qual é a função do SparkContext ?

	O SparkContext é ponto de entrada para as funcionalidades e propriedades do Spark, que permite ao aplicativo do driver acessar o cluster por meio de um gerenciador de recursos e atuando com um client para o ambiente em execução.

4 - Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

	O RDD é o conjunto de dados imutáveis tolerante a falhas, capaz de compilar partições ausentes ou danificadas, e que também distribui os dados em vários nós do cluster. Considerado a principal estrutura de dados fundamentais do Spark, o RDD executa operações de transformação e conjunto de ações de forma paralela. 
	
5 - GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

	Apesar de que as duas funções produzem os mesmos resultados, existe uma diferença na quantidade de dados que trafegam na rede. 
	O GroupByKey trafega todos os pares de key-values do dataset pelo cluster e a mesclagem dos valores é feita depois do shuffle. Dessa forma, muitos dados são transferidos pela rede sem necessidade, o que pode causar a falta de disco. 
	Já no reduceByKey, os datasets são reduzidos e os valores de cada chave são mesclados usando uma função de redução associativa, o que funciona muito melhor em grande quantidades de dados porque o Spark sabe que pode combinar a saída com uma chave comum em cada partição antes do shuffle dos dados. 

6 - Explique o que o código Scala abaixo faz.

val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line=>line.split(" "))
	.map(word=>(word,1))
	.reduceByKey(_+_)
counts.saveNAsTextFile("hdfs://...")

	De acordo com código scala acima, podemos seguir a lógica:

	Linha 1: Criação de um RDD a partir da leitura do arquivo localizado no HDFS.
	LInha 2, 3 e 4: Combinação das transformações flatMap, map e reduceByKey para calcular as contagens por palavra no arquivo como um RDD. 
	Linha 5: O RDD com a contagem das palavras é salvo em um arquivo de texto no HDFS.

	O código possui o conceito de contar as palavras existentes no arquivo, criando uma lista de tuplas e salvando o resultado do contador.

