from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

def cast_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """Faz o cast dinâmico de colunas baseado num dicionário."""
    existing_columns = df.columns
    for column, data_type in mapping.items():
        if column not in existing_columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        df = df.withColumn(column, F.col(column).cast(data_type))
    return df

def standardize_column_names(df: DataFrame) -> DataFrame:
    """Padroniza os nomes das colunas (trim, replace espaços, minúsculas)."""
    new_columns = [column.strip().replace(" ", "_").lower() for column in df.columns]
    return df.toDF(*new_columns)

def standardize_string_values(df: DataFrame) -> DataFrame:
    """Padroniza os dados internos de todas as colunas do tipo string."""
    string_columns = [col_name for col_name, dtype in df.dtypes if dtype == 'string']
    for col in string_columns:
        df = df.withColumn(col, F.trim(F.lower(F.col(col))))
    return df