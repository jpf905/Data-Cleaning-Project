import duckdb

con = duckdb.connect("data/warehouse/chaos_to_clean.duckdb")

# example queries
print(con.execute("SHOW TABLES").fetchdf())
print(con.execute("SELECT * FROM clean_data LIMIT 5").fetchdf())

con.close()

