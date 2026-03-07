import duckdb

#connection to database
con = duckdb.connect("fedex.db")

#load raw data
con.execute("""
CREATE OR REPLACE TABLE sale_report AS
SELECT * FROM read_csv_auto('data/Fashionable Sale Report.csv')
""")

#Data check
con.execute("SELECT * FROM sale_report LIMIT 5").fetchall()

con.execute("DESCRIBE sale_report").fetchall()

