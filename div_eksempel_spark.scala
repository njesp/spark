import org.apache.spark.sql.functions.sum

 

val projekt_db_gb = spark.read.option("inferSchema", "true").option("header", "true").option("delimiter", ";").csv("h:/big_data/spark/projekt_db_gb.csv")

val projekt_gb = projekt_db_gb.groupBy("PROJEKT").agg(sum("GBYTES").alias("Gbytes"))

 

val projekt_kontor = spark.read.option("inferSchema", "true").option("header", "true").option("delimiter", ";").csv("h:/big_data/spark/projekt-kontor.csv").distinct()

 

val kontor_gb = projekt_gb.join(projekt_kontor, projekt_gb("PROJEKT") === projekt_kontor("PROJEKT")).select("PROJEKT_EJER_KONTOR", "Gbytes")

val kontor_gb2 = kontor_gb.groupBy("PROJEKT_EJER_KONTOR").agg(sum("Gbytes")alias("Gbytes"))

kontor_gb2.orderBy("PROJEKT_EJER_KONTOR").show(kontor_gb2.count().toInt)

 

// spark-shell --driver-class-path C:\Oracle12\Oracle12102x64\jdbc\lib\ojdbc7.jar --jars C:\Oracle12\Oracle12102x64\jdbc\lib\ojdbc7.jar

 

val x = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@//srvora12:1526/DB_PSD").option("dbtable", "(select object_name from dba_objects)").option("user", "opslag").option("password", "opslag").load()

 

// Først show laver fetch

 

x.show()

 

x.count()

 

// script-filer kan køres fra prompt med :load PATH_TO_FILE

 

val connectionProperties = new java.util.Properties()

connectionProperties.put("user", "njn")

connectionProperties.put("password", "abcd1234")

 

kontor_gb2.write.mode("overwrite").jdbc("jdbc:oracle:thin:@//srvora12:1526/DB_PSD", "XXX", connectionProperties)

 

 

// Giver en røvfuld warnings, men virker.

 

kontor_gb2.write.jdbc("jdbc:oracle:thin:@//srvora12:1526/DB_PSD", "XXX", connectionProperties)
