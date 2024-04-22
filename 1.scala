import org.apache.spark.sql.SparkSession
val rdd = sc.textFile("/opt/spark/smekhnev.txt")

val targetWord = "line"
val wordCount = rdd.filter(_.toLowerCase.contains(targetWord)).count()
println(s"Количество вхождений слова '$targetWord' в файле: $wordCount")

val wordsRDD = rdd.flatMap(_.split("\\s+")).filter(_.nonEmpty)

//Целочисленный массив
val intArrayRDD = sc.parallelize(Array(1,2,3,4,5))
//Применение reduce для нахождения суммы всех элементов
val reduceRDD = intArrayRDD.reduce(_+_)
//Применение map для умножения каждого элемента на 2
val mapRDD = intArrayRDD.map(_*2)

//Ассоциативный массив
val associativeArrayRDD = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 3), ("a", 4)))
//Применение reduceByKey для сложения значений по ключу
val reduceByKeyRDD = associativeArrayRDD.reduceByKey((x, y) => x + y)
//Применение mapValues для умножения каждого элемента на 2
val mapValuesRDD = associativeArrayRDD.mapValues(_*2)
