import org.apache.spark.sql.SparkSession
val rdd = sc.textFile("/opt/spark/smekhnev.txt")

val targetWord = "line"
val wordCount = rdd.filter(_.toLowerCase.contains(targetWord)).count()
println(s"Количество вхождений слова '$targetWord' в файле: $wordCount")

val wordsRDD = rdd.flatMap(_.split("\\s+")).filter(_.nonEmpty)
val integerRDD = wordsRDD.map(word => (word.length, 1)).reduceByKey(_+_).sortByKey()
val associativeRDD = wordsRDD.map(word => (word, 1)).reduceByKey(_+_)
integerRDD.collect().foreach(println)
associativeRDD.collect().foreach(println)

val wordLengthsRDD = wordsRDD.map(_.length)
val totalWords = wordLengthsRDD.reduce(_ + _)
