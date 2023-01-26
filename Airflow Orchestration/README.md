The purpose of this project was to build a ETL data pipeline that extract API from Alodocter.com website, and then store it in PostgreSQL database.

The table initially was created using PostgresOperator. Then we checked if the API is available or not using HttpSensor. We extracted the JSON file, turned it into a Python dictionary, and using the PythonOperator, we processed the data in Pandas dataframe. We ended up converting the dataframe into csv file and storing it in PostgreSQL database using PostgresHook.
