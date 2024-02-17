import duckdb
import dlt


def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


pipeline = dlt.pipeline(pipeline_name="generators_data",
						destination='duckdb',
						dataset_name='generators')

info = pipeline.run(people_1,
                    table_name="persons_merged",
                    write_disposition="replace",
                    primary_key="ID")

info = pipeline.run(people_2,
                    table_name="persons_merged",
                    write_disposition="merge",
                    primary_key="ID")

# Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above.
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
sum_of_ages = conn.sql(f"SELECT SUM(Age) FROM {pipeline.dataset_name}.persons_merged")
display(sum_of_ages)
