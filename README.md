# Homework for the Topic: "Apache Airflow"

Your task today is to implement a DAG using the operators you have learned to perform a specific set of practical tasks.

### [Solution (Screenshots of program execution explained)](./Solution_explained.md)

### Step-by-Step Instructions:

1. Create a table.

```
👉🏻 Use IF NOT EXISTS with the following fields:
id (auto-increment, primary key),
medal_type,
count,
created_at.
```

2. Randomly select one of the three values: `['Bronze', 'Silver', 'Gold']`.
3. Based on the selected value, trigger one of three tasks (branching).
4. Description of the three tasks:
   1. The task counts the number of records in the table `olympic_dataset.athlete_event_results` where the `medal` field contains "Bronze" and inserts this value into the table created in step 1, along with the medal type and the record creation time.
   2. The task counts the number of records in the table `olympic_dataset.athlete_event_results` where the `medal` field contains "Silver" and inserts this value into the table created in step 1, along with the medal type and the record creation time.
   3. The task counts the number of records in the table `olympic_dataset.athlete_event_results` where the `medal` field contains "Gold" and inserts this value into the table created in step 1, along with the medal type and the record creation time.
   4. Introduce a delay before proceeding to the next task.
      👉🏻 Use a `PythonOperator` with the function `time.sleep(n)` after one of the three tasks above has successfully completed.
   5. Use a sensor to check whether the most recent record in the table created in step 1 is no older than 30 seconds (compared to the current time).
      The idea is to ensure that a record was actually inserted into the table.
      👉🏻 By using the delay from step 5, you can create a 35-second delay to ensure that the sensor fails if the delay exceeds 30 seconds.
