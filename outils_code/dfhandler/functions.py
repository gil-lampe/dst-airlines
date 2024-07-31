import urllib.request
import numpy as np
from pyspark.sql.functions import col, isnan, isnull, \
                                  regexp_replace, trim, when
from pyspark.sql.types import BooleanType, DateType
import re


def download_file(url, filename):
    """Download the url's file from a selected path

    Args:
        url (str): weblink to the file
        filename (str): name of the file
    """
    urllib.request.urlretrieve(url, filename)
    print("File :", filename, "downloaded successfully.")

# RENOMMER LES COLONNES + MINUSCULES :
#####################################################################


def rename_col_standard(df):
    """Renames dataframe columns by standards

    Remove all non alphanumerical caracters, lower them,
    ensure there is only one space between words
    and replace spaces with underscores

    Args:
        df (pyspark.sql.DataFrame): DataFrame whose columns should be renamed

    Returns:
        pyspark.sql.DataFrame: new DataFrame with renamed columns
    """
    renamed_cols = {}
    for old_col in df.columns:
        renamed_col = old_col.lower()
        renamed_col = re.sub("[^a-z0-9]", " ", renamed_col)
        renamed_col = re.sub("[ ]{2,}", " ", renamed_col)
        renamed_col = renamed_col.strip()
        renamed_col = renamed_col.replace(" ", "_")
        renamed_cols[old_col] = renamed_col
    return df.withColumnsRenamed(renamed_cols)

#  REMPLACER LES VALEURS MANQUANTES PAR UNE STATISTIQUE AU CHOIX :
#####################################################################


def get_missing_values(df):
    """Compute the null and NaN statistics in each columns

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame

    Returns:
        :obj:`list` of :obj:`int`: Statistics
        (nb of entries, list of column names, list of nan counts,
        list of null counts)
    """
    count = df.count()
    columns = df.columns
    nan_count = []
    # we can't check for nan in a boolean type column
    # (as well as in a date type column)
    for column in columns:
        if df.schema[column].dataType == BooleanType() \
            or df.schema[column].dataType == DateType():
            nan_count.append(0)
        else:
            nan_count.append(df.where(isnan(col(column))).count())
    null_count = [df.where(isnull(col(column))).count() for column in columns]
    return [count, columns, nan_count, null_count]


def print_na_table_from_stats(stats):
    """Prints a table with, for each column, the number of null and NaN values

    Args:
        stats (:obj:`list` of :obj:`int`): Statistics 
        (nb of entries, list of column names, list of nan counts, 
        list of null counts)
    """
    count, columns, nan_count, null_count = stats
    count = str(count)
    nan_count = [str(element) for element in nan_count]
    null_count = [str(element) for element in null_count]
    max_init = np.max([len(count), 10])
    line1 = "+" + max_init*"-" + "+"
    line2 = "|" + (max_init-len(count))*" " + count + "|"
    line3 = "|" + (max_init-9)*" " + "nan count|"
    line4 = "|" + (max_init-10)*" " + "null count|"
    for i in range(len(columns)):
        max_column = np.max([len(columns[i]),
                             len(nan_count[i]),
                             len(null_count[i])])
        line1 += max_column*"-" + "+"
        line2 += (max_column - len(columns[i]))*" " + columns[i] + "|"
        line3 += (max_column - len(nan_count[i]))*" " + nan_count[i] + "|"
        line4 += (max_column - len(null_count[i]))*" " + null_count[i] + "|"
    lines = f"{line1}\n{line2}\n{line1}\n{line3}\n{line4}\n{line1}"
    print(lines)


def print_na_table(df):
    """print a table summarizing the null and NaN values of each column

    Args:
        df (pyspark.sql.DataFrame): DataFrame on which run the analysis
    """
    print_na_table_from_stats(get_missing_values(df))


# Calculer les valeurs manquantes
#####################################################################


def replace_na(df, column_names, how, nan_or_null="both"):
    """Replace null or NaN values by a statistically coherent value of choice

    Args:
        df (pyspark.sql.DataFrame): Target DataFrame
        column_names (:obj:`lsit` of :obj:`int`): List of the column to process
        how (str): Statistics to use to replace na values,
        should be "avg", "mode", "median" or "mean"
        nan_or_null (str, optional): na values to be replaced,
        should be "nan", "null" or "both". Defaults to "both".

    Raises:
        ValueError: if "how" is not in the intended list of values
        ValueError: if "nan_or_null" is not in the intended list of values
        ValueError: if "column_names" is not a list of strings

    Returns:
        pyspark.sql.DataFrame: New DataFrame with replaced na values
    """
    if how not in ('avg', 'mode', 'median', 'mean'):
        raise ValueError("'how' parameter should equal to \
                         'avg', 'mean', 'median' or 'mode'.")
    if nan_or_null not in ('nan', 'null', 'both'):
        raise ValueError("'nan_or_null' parameter should equal to \
                         'nan', 'null' or 'both'.")
    if not isinstance(column_names, list) \
            and all(isinstance(column_name, str) \
                    for column_name in column_names):
        raise ValueError("'column_names' should be a list of strings.")
    cleaned_df = df
    for column_name in column_names:
        stat_col_name = how + "(" + column_name + ")"
        stat = df.where(~isnan(col(column_name)) & ~isnull(col(column_name)))\
            .agg({column_name: how})\
            .first()[stat_col_name]
        if nan_or_null in ("nan", "both"):
            cleaned_df = cleaned_df.withColumn(
                column_name,
                when(isnan(column_name), stat).otherwise(col(column_name)))
        if nan_or_null in ("null", "both"):
            cleaned_df = cleaned_df.withColumn(
                column_name,
                when(isnull(column_name), stat).otherwise(col(column_name)))
    return cleaned_df

# IMPRIMER LES VALEURS DISTINCTES DANS CHAQUE COLONNE
#####################################################################


def print_distinct_values(df):
    """Print number of distinct values contains in each column

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame
    """
    for col_name in df.columns:
        distinct_values = df.select(col_name).distinct().count()
        print(f"Column {col_name} has {distinct_values} distinct values.")

# REMPLACER LES CARACTÈRES SPÉCIAUX PAR DES ESPACES
# ET SUPPRIMER LES ESPACES DE DÉBUT ET DE FIN
######################################################


def clean_special_characters(df, subset=[]):
    """Remove special caracters in columns containing strings

    Args:
        df (pyspark.sql.DataFrame): Traget dataframe
        subset (:obj:`list` of :obj:`str`, optional): List of the
        column names to process, empty list equals to all string columns.
        Defaults to [].

    Raises:
        ValueError: If subset is specified and at least one column
        is not a string type column, error will be raised

    Returns:
        pyspark.sql.DataFrame: Resulting DataFramen_
    """
    is_subset_specified = True

    if len(subset) == 0:
        is_subset_specified = False
        subset = df.columns

    for col_name, type in df.dtypes:
        if col_name in subset:
            if type != "string" and not is_subset_specified:
                raise ValueError("At least one provided column\
                                  is not a string type column.")
            else:
                df = df.withColumn(
                    col_name, regexp_replace(col(col_name),
                                             "[^a-zA-Z0-9]",
                                             " "))
                df = df.withColumn(col_name, trim(col(col_name)))
    return df

# CONVERTIR LES COLONNES DE DATE
######################################################
# Si la date est au format "MMMM D, YYYY" :
# raw_data = raw_data.withColumn("DATE", to_date(col("DATE"), "MMMM d, yyyy"))

# CONVERTIR LES COLONNES DE PRIX
######################################################
# $ = monnaie si présente dans le jeu de donnée (€ etc...)
# raw_data = raw_data.withColumn("price", 
#                                regexp_replace(col("price"), "[$]", "")\
#   .cast(DoubleType()))

# ETAPES SUPPLÉMENTAIRES
######################################################
# 1. Détecter et gérer les valeurs aberrantes (outliers)
# 2. Normaliser ou standardiser les colonnes numériques si nécessaire
# 3. Encoder les colonnes catégorielles pour les modèles ML
