import duckdb


# q = """
#     select 
#     * 
#     from 
#     '/data/user_purchase.csv'
#     limit 5
# """
# con = duckdb.connect()
# con.sql(q).show()
# duckdb.sql(q).write_csv("/data/behaviour_metrics.csv")
# con.close()

q = """
    INSERT INTO test 
    select invoice_number, invoice_date
    from 
    '/data/user_purchase.csv'
    limit 5
"""
con = duckdb.connect("/data/my_database.duckdb")
con.sql("CREATE TABLE test (invoice_number varchar, invoice_date timestamp)")
con.sql(q)
con.table("test").show()
con.close()