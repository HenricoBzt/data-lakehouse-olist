from delta.tables import DeltaTable
from pyspark.sql.dataframe import DataFrame

def upsert_data(df_source: DataFrame, full_table_target_name: str, join_keys_condition: str, name_bucket: str, layer: str):
    """
    Realiza o MERGE. 
    df_source: O próprio DataFrame do PySpark processado.
    full_table_target_name: O nome completo no formato 'catalog.schema.tabela'.
    join_key_condition: Uma lista de strings com os nomes das primary_key.
    """

    spark = df_source.sparkSession 
    
    table_exist = spark.catalog.tableExists(full_table_target_name)

    if not table_exist:
        print(f" Table {full_table_target_name} DOES NOT exist. Initializing full load")
        print("Initializing full load...")

        writer = df_source.write.format("delta").mode("overwrite")
        
        if name_bucket and layer:
            table_only = full_table_target_name.split(".")[-1]
            target_path = f"s3://{name_bucket}/{layer}/{table_only}"
            writer = writer.option("path", target_path)
            print("Salvando no S3")
            
        writer.saveAsTable(full_table_target_name)

    else:
        print(f"Table {full_table_target_name} exists. Initializing merge")
        

        deltaTableTarget = DeltaTable.forName(spark, full_table_target_name)

        join_key_condition = [k.strip() for k in join_keys_condition.split(",")]
        join_condition = " AND ".join([f"source.{k} = target.{k}" for k in join_key_condition])

        deltaTableTarget.alias("target").merge(
            df_source.alias("source"),
            join_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        last_operation = deltaTableTarget.history(1).collect()[0]
        metrics = last_operation["operationMetrics"]
        
        rows_insert = metrics.get("numTargetRowsInserted", "0")
        rows_update = metrics.get("numTargetRowsUpdated", "0")
        
        print(" MERGE updated successfully")
        print(f" Rows inserted: {rows_insert}")
        print(f" Rows updated: {rows_update}")