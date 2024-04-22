import org.apache.spark.sql.SparkSession
val rdd = sc.textFile("/opt/spark/smekhnyov.txt")

val targetWord = "file"
val wordCount = rdd.filter(_.toLowerCase.contains(targetWord)).count()
println(s"Количество вхождений слова '$targetWord' в файле: $wordCount")

val rdd = rdd.flatMap(_.split("\\s+")).filter(_.nonEmpty).collect

val intArrayRDD = sc.parallelize(Array(1,2,3,4,5))
val reduceRDD = intArrayRDD.reduce(_+_)
val mapRDD = intArrayRDD.map(_*2).collect

val associativeArrayRDD = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 3), ("a", 4)))
val reduceByKeyRDD = associativeArrayRDD.reduceByKey((x, y) => x + y)
val mapValuesRDD = associativeArrayRDD.mapValues(_*2).collect
