import duckdb
import dlt

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

pipeline = dlt.pipeline(pipeline_name="generators_data",
						destination='duckdb',
						dataset_name='generators')

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_1,
                    table_name="persons",
                    write_disposition="replace")

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_2,
                    table_name="persons",
                    write_disposition="append")

# Question 3: Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people.
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
sum_of_ages = conn.sql(f"SELECT SUM(Age) FROM {pipeline.dataset_name}.persons")
display(sum_of_ages)

