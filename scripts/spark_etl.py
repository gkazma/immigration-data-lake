from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as F
from pyspark.sql.functions import col, create_map, lit, expr, to_date
from itertools import chain

def etl():
    # Read all tables
    immigration_staging = spark.read.options(header='true', inferSchema='true').csv("/input/i94_apr16_sub.csv")
    country_code_mapping_staging = spark.read.options(header='true', inferSchema='true').csv("/input/code_to_country_mapping.csv")
    demographics_staging = spark.read.options(header='true', inferSchema='true', delimiter=';').csv("/input/us-cities-demographics.csv")
    weather_staging = spark.read.options(header='true', inferSchema='true').csv("/input/GlobalLandTemperaturesByState.csv")

    transportation_mapping = {1: 'air', 2: 'sea', 3: 'land', 4: 'not reported'}
    transportation_mapping_expr = create_map([lit(x) for x in chain(*transportation_mapping.items())])

    state_mapping = {'AL':'ALABAMA',
                        'AK':'ALASKA',
                        'AZ':'ARIZONA',
                        'AR':'ARKANSAS',
                        'CA':'CALIFORNIA',
                        'CO':'COLORADO',
                        'CT':'CONNECTICUT',
                        'DE':'DELAWARE',
                        'DC':'DIST. OF COLUMBIA',
                        'FL':'FLORIDA',
                        'GA':'GEORGIA',
                        'GU':'GUAM',
                        'HI':'HAWAII',
                        'ID':'IDAHO',
                        'IL':'ILLINOIS',
                        'IN':'INDIANA',
                        'IA':'IOWA',
                        'KS':'KANSAS',
                        'KY':'KENTUCKY',
                        'LA':'LOUISIANA',
                        'ME':'MAINE',
                        'MD':'MARYLAND',
                        'MA':'MASSACHUSETTS',
                        'MI':'MICHIGAN',
                        'MN':'MINNESOTA',
                        'MS':'MISSISSIPPI',
                        'MO':'MISSOURI',
                        'MT':'MONTANA',
                        'NC':'N. CAROLINA',
                        'ND':'N. DAKOTA',
                        'NE':'NEBRASKA',
                        'NV':'NEVADA',
                        'NH':'NEW HAMPSHIRE',
                        'NJ':'NEW JERSEY',
                        'NM':'NEW MEXICO',
                        'NY':'NEW YORK',
                        'OH':'OHIO',
                        'OK':'OKLAHOMA',
                        'OR':'OREGON',
                        'PA':'PENNSYLVANIA',
                        'PR':'PUERTO RICO',
                        'RI':'RHODE ISLAND',
                        'SC':'S. CAROLINA',
                        'SD':'S. DAKOTA',
                        'TN':'TENNESSEE',
                        'TX':'TEXAS',
                        'UT':'UTAH',
                        'VT':'VERMONT',
                        'VI':'VIRGIN ISLANDS',
                        'VA':'VIRGINIA',
                        'WV':'W. VIRGINIA',
                        'WA':'WASHINGTON',
                        'WI':'WISCONSON',
                        'WY':'WYOMING' ,
                        '99':'All Other Codes'}
    state_mapping = dict((k, v.title()) for k, v in state_mapping.items())
    state_mapping_expr = create_map([lit(x) for x in chain(*state_mapping.items())])

    visa_mapping = {1: 'business', 2: 'pleasure', 3: 'student'}
    visa_mapping_expr = create_map([lit(x) for x in chain(*visa_mapping.items())])

    immigration_fact = immigration_staging.select(col("cicid").alias("id").cast("int"),
                        col("I94YR").alias("year").cast("int"),
                        col("I94MON").alias("month").cast("int"),
                        col("I94PORT").alias("port_of_entry"),
                        col("ARRDATE").alias("arrival_date").cast("int"),
                        col("I94MODE").alias("mode_of_transportation").cast("int"),
                        col("I94ADDR").alias("state_of_arrival_code"),
                        col("DEPDATE").alias("departure_date").cast("int"),
                        col("I94VISA").alias("visa_type").cast("int"),) \
    .withColumn("mode_of_transportation", transportation_mapping_expr[col("mode_of_transportation")]) \
    .withColumn("state_of_arrival", state_mapping_expr[col("state_of_arrival_code")]) \
    .withColumn("sas_date", to_date(lit("01/01/1960"), "MM/dd/yyyy")) \
    .withColumn("arrival_date", expr("date_add(sas_date, arrival_date)")) \
    .withColumn("departure_date", expr("date_add(sas_date, departure_date)")) \
    .withColumn("visa_type", visa_mapping_expr[col("visa_type")]) \
    .drop('sas_date')

    person_dim = immigration_staging.join(country_code_mapping_staging, 
                         immigration_staging["I94CIT"] == country_code_mapping_staging["code"], 
                         "left").withColumnRenamed("country", "country_of_origin").drop('code')
    person_dim = person_dim.join(country_code_mapping_staging, 
                            person_dim["I94RES"] == country_code_mapping_staging["code"], 
                            "left").withColumnRenamed("country", "country_of_residence").drop('code')
    person_dim = person_dim.select(col("cicid").alias("id").cast("int"),
                                                col("GENDER").alias("gender"),
                                                col("I94BIR").alias("age").cast("int"),
                                                col("country_of_origin").alias("country_of_origin"),
                                                col("country_of_residence").alias("country_of_residence"),
                                                col("BIRYEAR").alias("year_of_birth").cast("int"))

    # Demographics dimension
    demographics_dim = demographics_staging.withColumn("id", monotonically_increasing_id()) \
                    .select(col("id"),
                            col("State").alias("state"),
                            col("Median Age").alias("median_age"),
                            col("Female Population").alias("female_population"),
                            col("Number of Veterans").alias("number_of_veterans"),
                            col("Foreign-born").alias("foreign_born"),
                            col("Average Household Size").alias("average_household_size"),
                            col("State Code").alias("state_code"),
                            col("Race").alias("race"),
                            col("Count").alias("count"))

    weather_dim = weather_staging.withColumn("id", monotonically_increasing_id()) \
                              .select(col("id"),
                                     col("dt").alias("date"),
                                     col("AverageTemperature").alias("average_temperature"),
                                     col("AverageTemperatureUncertainty").alias("average_temperature_uncertainty"),
                                     col("State").alias("state"),
                                     col("Country").alias("country"))                                 

    # Data Quality Check
    if immigration_fact.count() < 1:
        raise ValueError('Immigration Fact Table is Empty')

    if person_dim.count() < 1:
        raise ValueError('Person Dimension Table is Empty')

    if demographics_dim.count() < 1:
        raise ValueError('Demographics Dimension Table is Empty')

    if weather_dim.count() < 1:
        raise ValueError('Weather Dimension Table is Empty')

    immigration_fact.write.mode("overwrite").parquet("/output/immigration_fact")
    person_dim.write.mode("overwrite").parquet("/output/person_dim")
    demographics_dim.write.mode("overwrite").parquet("/output/demographics_dim")
    weather_dim.write.mode("overwrite").parquet("/output/weather_dim")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Immigration Data Lake").getOrCreate()
    etl()
