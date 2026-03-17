
from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import col, row_number


def remove_duplicated_data(dataframe: DataFrame, columns: list[str]):
    try:
        window_spec = Window.partitionBy(columns).orderBy(
            col("_airbyte_extracted_at").desc()
        )

        df_dedup = (
            dataframe.withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )
        return df_dedup
    except Exception:
        raise Exception
