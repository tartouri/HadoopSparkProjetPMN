# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 10:51:21 2023

@author: itouilta
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format, year, collect_list, col, when, sum
from pyspark.sql.window import Window

# Appel Spark Session
spark = SparkSession.builder.appName('Spark').getOrCreate()
print(spark)

# Ouvrir les fichiers csv
df_country = spark.read.option("header", True).csv(
    'C:/Users/itouilta/OneDrive - Capgemini/Desktop/spark/country_classification.csv')
df_country.show()

df_services = spark.read.option("header", True).csv(
    'C:/Users/itouilta/OneDrive - Capgemini/Desktop/spark/services_classification.csv')
df_services.show()

df_goods = spark.read.option("header", True).csv(
    'C:/Users/itouilta/OneDrive - Capgemini/Desktop/spark/goods_classification.csv')
df_goods.show()

df_output = spark.read.option("header", True).csv(
    'C:/Users/itouilta/OneDrive - Capgemini/Desktop/spark/output_csv_full.csv')
df_output.show()

# Ajouter la column 'date'
df_date = df_output.withColumn('date', date_format(to_date('time_ref', 'yyyyMM'), 'dd/MM/yyyy'))
df_date.show()

# Ajouter la column 'year'
df_year = df_date.withColumn('year', year(to_date('date', 'dd/MM/yyyy')))
df_year.show()

# Ajouter la column 'nom_pays'
df_pays = df_year.join(df_country, df_year.country_code == df_country.country_code, how = "inner")
df_pays = df_pays.drop(df_country.country_code)
df_pays.show() 

# Ajouter une column détails service (filtrage sur service)
df = df_pays.join(df_services, df_pays.code == df_services.code, how='inner')
df = df.drop(df_services.code)
df.show()

# Ajouter une column détails Good (filtrage sur goods)
    #je n'ai pas reussi a faire le join, je ne savais pas quelle colonne utiliser
    #du coup, toutes les questions sur goods je n'ai pas pu les faire


# Classement des pays exportateurs (par goods et par services)
df_export_goods = df.filter((col("account") == "Exports") & (col("product_type") == "Goods"))
df_export_services = df.filter((col("account") == "Exports") & (col("product_type") == "Services"))

countries_list_export_goods = list(set(df_export_goods.select(collect_list("country_label")).first()[0]))
countries_list_export_services = list(set(df_export_services.select(collect_list("country_label")).first()[0]))

print("pays exportateurs Goods: ")
print(countries_list_export_goods)

print("pays exportateurs Services: ")
print(countries_list_export_services)

# Classement des pays importateurs (par goods et par services)
df_import_goods = df.filter((col("account") == "Imports") & (col("product_type") == "Goods"))
df_import_services = df.filter((col("account") == "Imports") & (col("product_type") == "Services"))

countries_list_import_goods = list(set(df_import_goods.select(collect_list("country_label")).first()[0]))
countries_list_import_services = list(set(df_import_services.select(collect_list("country_label")).first()[0]))

print("pays exportateurs Goods: ")
print(countries_list_import_goods)

print("pays exportateurs Services: ")
print(countries_list_import_services)


# Regroupement par goods

# Regroupement par services

# La liste des services exporté de la France
df_france_services = df.filter((col("account") == "Exports") & (col("country_label") == "France"))

liste_services = df_france_services.select("service_label").distinct().collect()
liste_services = [row.service_label for row in liste_services]

print("Services exportés de la France:")
print(liste_services)


# La liste des goods importés de la France


# Classement des services les moins demandés
services_count = df_france_services.groupBy('service_label').count()
services_count = services_count.orderBy(col('count').asc())
services_count.show()

# Classement des goods les plus demandé


# Ajouter la column 'status_import_export'
windowSpec = Window.partitionBy('country_label')
df_total = df.withColumn('total_import', sum(when(col('account') == 'Imports', col('value'))).over(windowSpec)) \
                  .withColumn('total_export', sum(when(col('account') == 'Exports', col('value'))).over(windowSpec))

df_status = df_total.withColumn('status_import_export', when(col('total_import') > col('total_export'), 'Negative').otherwise('Positive'))
df_status.show()


# Ajouter la column difference_import_export
df_difference = df_status.withColumn('difference_import_export', col('total_export') - col('total_import'))
df_difference.show()


# Ajouter la column somme_good 


# Ajouter la column somme_service
df_sum_service = df_difference.withColumn('somme_service', sum(when(col('product_type') == 'Services', col('value'))).over(windowSpec))
df_sum_service.show()


# Ajouter la column pourcentages_good 


# Ajouter la column pourcentages_services


# Regrouper les goods selon leur type (Code HS2)

# Classement des pays exportateur de pétrole


# Classement des pays importateur de viandes


# Classement des pays qui ont le plus de demandes

# Classement des pays qui ont le plus de demandes sur les services informatique
computer_services_list = ["Computer services", "Computer software", "Other computer services"]
df_filter = df_sum_service.filter(col('service_label').isin(computer_services_list))




