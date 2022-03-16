# airflow-assignment

Given Tasks:
1) Create the first task to use endpoint /Current Weather Data for at least 10 states of India and fill up the csv file with details of
.
2) Create a second task to create a postgres table “Weather” that would have columns
same as the csv file.
3) Create a third task that should fill the columns with the table while reading the data from
the csv file.
4) Schedule the DAG in AirFlow to run every day at 6:00 am and update the daily weather
detail in csv as well as the table.

API Service provider for weather data: https://rapidapi.com/community/api/open-weather-map/