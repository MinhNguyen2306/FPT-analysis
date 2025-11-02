from ETL_scripts import read_mysql, process_df, category_df, sanitize_for_sqlserver
import pyspark.sql.functions as sf

def updated_data(data):
        print("------------------Retrieving data from mysql------------------")
        mysql_df = read_mysql()
        mysql_df.show(5)
        print("------------------Processing data------------------")
        df = process_df(data)
        df = df.cache() 
        df.show(5)
        
        print("------------------Categorizinng data------------------")
        category_data = category_df(df)
        
        final_df = category_data.join(mysql_df, on='job_id', how='left').drop("campaign_id").drop("group_id")

        final_df = final_df.withColumn('updated_at',sf.lit(df.agg({'ts':'max'}).take(1)[0][0]))


        final = (final_df
                .withColumnRenamed('date','dates')
                .withColumnRenamed('hour','hours')
                .withColumnRenamed('qualified','qualified_application')
                .withColumnRenamed('disqualified','disqualified_application')
                .withColumnRenamed('click','clicks')
                .withColumn('sources',sf.lit('Cassandra'))
                )

        final = final.withColumn('sources',sf.lit('Cassandra'))
        final = sanitize_for_sqlserver(final)

        return final       