import urllib.request
import numpy as np
from pyspark.sql.functions import col, isnan, isnull, regexp_replace, to_date, lower, length, trim
from pyspark.sql.functions import count, sum, avg, max, min, mean, when
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Création d'une session Spark
app_name = "Application"
reseau_app = "local[*]"

spark = SparkSession\
         .builder\
         .appName(app_name)\
         .master(reseau_app)\
         .getOrCreate()
sc = spark.sparkContext

def download_file(url, filename):
    """
    Description :
    Download the url's file to a selected path.
    
    Args :
    url = str, weblink to the file
    path = str, path to file
    """
    try:
        urllib.request.urlretrieve(url, filename)
        print("File :", filename, "downloaded successfully.")
    except:
        print("Error during the download.")


# VARIABLES : 
#####################################################################

# Application :
url_csv = "<URL_TO_CSV>"
filename = "data.csv"
# download_file(url_csv, filename)


# JEU DE DONNEES :
#####################################################################

# Pour lire un jeu de donnée :
raw_data = spark.read.option("header", True)\
                    .option("inferSchema", True)\
                    .option("escape", "\"")\
                    .csv(filename)


# RENOMMER LES COLONNES + MINUSCULES :
#####################################################################
def rename_col(df):
    for col_name in df.columns:
        new_name = col_name.replace(" ", "_").lower()
        df = df.withColumnRenamed(col_name, new_name)
    return df

raw_data = rename_col(raw_data)


# REMPLACER LES VALEURS MANQUANTES PAR LA MÉDIANE
#####################################################################
def replace_missing_with_median(df):
    numeric_columns = [col_name for col_name, dtype in df.dtypes if dtype in ('int', 'double', 'float')]
    for col_name in numeric_columns:
        median = df.approxQuantile(col_name, [0.5], 0.01)[0]
        df = df.fillna({col_name: median})
    return df

raw_data = replace_missing_with_median(raw_data)


# SIGNALER LES VALEURS MANQUANTES DANS LES COLONNES STRING
#####################################################################
def report_missing_values(df):
    string_columns = [col_name for col_name, dtype in df.dtypes if dtype == 'string']
    for col_name in string_columns:
        missing_count = df.filter(isnull(col(col_name)) | (col(col_name) == '')).count()
        if missing_count > 0:
            print(f"Column {col_name} has {missing_count} missing values.")
        else:
            print(f"Column {col_name} has no missing values.")

report_missing_values(raw_data)


# CONSULTER LES VALEURS DISTINCTES DANS CHAQUE COLONNE
#####################################################################
def show_distinct_values(df):
    for col_name in df.columns:
        distinct_values = df.select(col_name).distinct().count()
        print(f"Column {col_name} has {distinct_values} distinct values.")

show_distinct_values(raw_data)


# REMPLACER UNE VALEUR MANQUANTE PAR LA MODALITE (MODE)
#####################################################################
def replace_missing_with_mode(df):
    string_columns = [col_name for col_name, dtype in df.dtype if dtype == 'string']
    for col_name in string_columns:
        mode = df.groupBy(col_name).count().orderBy('count', ascending=False).first()[0]
        df = df.fillna({col_name: mode})
    return df 

raw_data = replace_missing_with_mode(raw_data)


# REMPLACER LES CARACTÈRES SPÉCIAUX PAR DES ESPACES ET SUPPRIMER LES ESPACES DE DÉBUT ET DE FIN
######################################################
def clean_special_characters(df):
    for col_name in df.columns:
        df = df.withColumn(col_name, regexp_replace(col(col_name), "[^a-zA-Z0-9]", " "))
        df = df.withColumn(col_name, trim(col(col_name)))
    return df

raw_data = clean_special_characters(raw_data)


# CONVERTIR LES COLONNES DE DATE
######################################################
# Si la date est au format "MMMM D, YYYY" :
# raw_data = raw_data.withColumn("DATE", to_date(col("DATE"), "MMMM d, yyyy"))

# CONVERTIR LES COLONNES DE PRIX
######################################################
# $ = monnaie si présente dans le jeu de donnée (€ etc...)
# raw_data = raw_data.withColumn("price", regexp_replace(col("price"), "[$]", "").cast(DoubleType()))



# ETAPES SUPPLÉMENTAIRES
######################################################
# 1. Détecter et gérer les valeurs aberrantes (outliers)
# 2. Normaliser ou standardiser les colonnes numériques si nécessaire
# 3. Encoder les colonnes catégorielles pour les modèles ML

# STOP SPARK SESSION
######################################################
spark.stop()