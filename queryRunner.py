# Note: the module name is psycopg, not psycopg3
import psycopg

connections = ["dbname=tcph user=drewbuck", "dbname=stack user=drewbuck", "dbname=imdbload user=drewbuck"]

def get_explain_results(query, connection):
    conn = psycopg.connect(connection)
    cur = conn.cursor()
    with open(query, "r") as file:
        sql = file.read()
    cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql)
    plan_json = cur.fetchall()[0][0]  # First row, first column (JSON data)
    cur.close()
    conn.close()
    return plan_json

def extract_nodes(plan, results):
    """Recursively traverse the plan tree to collect node info."""
    node = {
        "Node Type": plan["Node Type"],
        "Relation Name": plan.get("Relation Name"),
        "Plan Rows": plan.get("Plan Rows"),
        "Actual Rows": plan.get("Actual Rows"),
        # "Query": plan
    }
    results.append(node)
    for subplan in plan.get("Plans", []):
        extract_nodes(subplan, results)
    return results

queryName = "./TCPH-Queries/2.sql"
JOBQueryName = "./JOB-queries/1a.sql"
plan_json = get_explain_results(JOBQueryName, connections[2])
plan = plan_json[0]["Plan"]

nodes = extract_nodes(plan, [])
for n in nodes:
    print("************************************\n")
    print(n)

# Connect to an existing database
# with psycopg.connect("dbname=tcph user=drewbuck") as conn:

    # Open a cursor to perform database operations
    # with conn.cursor() as cur:

        # Execute a command: this creates a new table
        # cur.execute("""
        # EXPLAIN ANALYZE
        #     SELECT *
        #     FROM customer
        #     """)

        # print(cur.fetchall())
        # with open("./TCPH-Queries/2.sql", "r") as file:
        #     sql = file.read()
        # cur.execute(sql)
        # print(cur.fetchall())
        # will print (1, 100, "abc'def")

        # You can use `cur.executemany()` to perform an operation in batch
        # cur.executemany(
        #     "INSERT INTO test (num) values (%s)",
        #     [(33,), (66,), (99,)])

        # You can use `cur.fetchmany()`, `cur.fetchall()` to return a list
        # of several records, or even iterate on the cursor
        # cur.execute("SELECT id, num FROM test order by num")
        # for record in cur:
        #     print(record)

        # Make the changes to the database persistent
        # conn.commit()