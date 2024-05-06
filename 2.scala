import org.apache.spark.sql.types._

#Создание схемы и загрузка данных из файла
val peopleSchema = StructType(Array(StructField("class", LongType, true), StructField("name", StringType, true), StructField("age", LongType, true), StructField("gender", StringType, true), StructField("subject", StringType, true), StructField("mark", LongType, true)))
val people = spark.read.option("header","true").option("sep",";").schema(peopleSchema).csv("/opt/spark/tmp.csv")
people.printSchema
people.show

#Сколько человек сдало тест?
val peopleCount = people.select("name").distinct().count()
println(s"Количество людей, сдавших тест: $peopleCount")

#Сколько человек в возрасте до 20 лет сдают тест?
val underTwenty = people.select("name", "age").filter('age < 20).distinct().count()
println(s"Количество людей до 20 лет, сдающих тест: $underTwenty")

#Сколько человек, которым исполнилось 20 лет, сдают экзамен?
val equalTwenty = people.select("name", "age").filter('age === 20).distinct().count()
println(s"Количество людей в возрасте 20 лет, сдающих тест: $equalTwenty")

#Сколько человек старше 20 сдают экзамен?
val overTwenty = people.select("name", "age").filter('age > 20).distinct().count()
println(s"Количество людей старше 20 лет, сдающих тест: $overTwenty")

#Сколько мужчин сдают экзамен?
val menCount = people.select("name", "gender").filter('gender === "man").distinct().count()
println(s"Количество парней, сдающих экзамен: $menCount")

#Сколько девушек сдают экзамен?
val womenCount = people.select("name", "gender").filter('gender === "woman").distinct().count()
println(s"Количество девушек, сдающих экзамен: $womenCount")

#Сколько человек сдают экзамен в 12 классе?
val class12 = people.select("name", "class").filter('class === 12).distinct().count()

#Сколько человек сдают экзамен в 13 классе?
val class13 = people.select("name", "class").filter('class === 13).distinct().count()
println(s"Количество учеников 13 класса, сдающих экзамен: $class13")

#Каков средний балл по языковым предметам?
val languageAvg = people.filter($"subject" === "chinese" || $"subject" === "english").agg(avg("mark").alias("average_mark")).show()

#Какова средняя оценка по математике?
val mathAvg = people.filter($"subject" === "mathematics").agg(avg("mark").alias("math_average_mark")).show()

#Какова средняя оценка по английскому языку?
val englishAvg = people.filter($"subject" === "english").agg(avg("mark").alias("eng_average_mark")).show()

#Каков средний балл одного человека?
val avgPerson = people.groupBy("name").agg(avg("mark").alias("average_mark")).show()

#Каков средний балл 12 класса?
val avg12class = people.filter('class === 12).agg(avg("mark").alias("average_mark")).show()

#Какова средняя оценка для парней в 12 классе?
val avg12classMen = people.filter('class === 12 && 'gender === "man").agg(avg("mark").alias("average_mark")).show()

#Какой средний балл у девушек из 12 класса?
val avg12classWomen = people.filter('class === 12 && 'gender === "woman").agg(avg("mark").alias("average_mark")).show()

#Предыдущие три вопроса, но для класса 13
val avg13class = people.filter('class === 13).agg(avg("mark").alias("average_mark")).show()
val avg13classMen = people.filter('class === 13 && 'gender === "man").agg(avg("mark").alias("average_mark")).show()
val avg13classWomen = people.filter('class === 13 && 'gender === "woman").agg(avg("mark").alias("average_mark")).show()

#Какова самая высокая оценка китайского языка по всей школе?
val maxChinese = people.filter('subject === "chinese").agg(max("mark").alias("max_chinese_mark")).show()

#Какой минимальный балл для 12 класса по китайскому языку?
val min12Chinese = people.filter('class === 12 && 'subject === "chinese").agg(min("mark").alias("min_12class_chinese_mark")).show()

#Какой самый высокий балл математики в 13 классе?
val max13Math = people.filter('class === 13 && 'subject === "mathematics").agg(max("mark").alias("max_13class_math_mark")).show()

#Сколько девушек в 12 классе с общим баллом более 150?
val womenCountOver150 = people.filter('class === 12 && 'gender === "woman").groupBy("name").agg(sum("mark").alias("total_mark")).filter('total_mark > 150).count()
println(s"Количество девушек в 12 классе с общим баллом больше 150: $womenCountOver150")

#Каков средний балл учащегося с общим баллом, превышающим 150 баллов, по математике превышающим или равным 70, и возрастом, превышающим или равным 20 годам?
val specialPeople = people.filter('age <= 20).groupBy("name").agg(sum("mark").alias("total_mark")).filter('total_mark > 150)
val math70 = people.filter('subject === "mathematics" && 'mark >= 70)
val joined = specialPeople.join(math70, Seq("name"), "inner")
val averageSpecial = joined.select($"name", ($"total_mark"/3).alias("average_mark"))
averageSpecial.show()

# мальчики 12 класса у кого баллы по математике выше среднего балла по математике у мальчиков 13 класса

val avgMathScoreClass13 = people.filter($"class" === 13 && $"subject" === "mathematics" && $"gender" === "man").groupBy().avg("mark").first().getDouble(0)
val result1 = people.filter($"class" === 12 && $"gender" === "man" && $"subject" === "mathematics" && $"mark" > avgMathScoreClass13).show()

# девочек 13 класса у кого баллы по англ выше среднего балла по англ всей школы

val avgEngScore = people.filter($"subject" === "english").groupBy().avg("mark").first().getDouble(0)
val result2 = people.filter($"class" === 13 && $"gender" === "woman" && $"subject" === "english" && $"mark" > avgEngScore).show()
