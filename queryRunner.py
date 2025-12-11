import os
import psycopg
import pandas as pd

connections = ["dbname=tcph user=drewbuck", "dbname=stack user=drewbuck", "dbname=imdbload user=drewbuck"]

def get_explain_results(query, connection):
    conn = psycopg.connect(connection)
    cur = conn.cursor()
    with open(query, "r") as file:
        sql = file.read()
    # cur.execute("SET enable_nestloop = off")
    cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql)
    plan_json = cur.fetchall()[0][0]
    cur.close()
    conn.close()
    return plan_json




# need to go through jsons and grab relevant data
def extract_nodes_bottom_up(plan, results, queryName, join_counter=None):
    # keep track of join index bottm up
    if join_counter is None:
        join_counter = {"count": 0}

    node_type = plan["Node Type"]

    # Determine whether this node is a real join operator
    def is_join_node(p):
        t = p["Node Type"]
        if "Join" in t:                 
            return True
        # Need to account for nested loops
        if t == "Nested Loop" and "Join Type" in p:
            return True
        return False

    # Recurse into child plans first to find join index
    for child in plan.get("Plans", []):
        extract_nodes_bottom_up(child, results, queryName, join_counter)

    outer_rel = None
    inner_rel = None

    outer_est = None
    inner_est = None

    outer_act = None
    inner_act = None

    # Now get children cardinalities
    for child in plan.get("Plans", []):
        relation = child.get("Relation Name")
        parent_rel = child.get("Parent Relationship")
        est = child.get("Plan Rows")
        act = child.get("Actual Rows")
        # I think the actual names will not show up a lot because they might not be actual tables just parts of joins
        if parent_rel == "Outer":
            outer_rel = relation
            outer_est = est
            outer_act = act

        elif parent_rel == "Inner":
            inner_rel = relation
            inner_est = est
            inner_act = act

    # Assign join index only for real joins
    join_index = None
    if is_join_node(plan):
        join_counter["count"] += 1
        join_index = join_counter["count"]

    # Collect node info
    node = {
        "Node Type": node_type,
        "Relation Name": plan.get("Relation Name"),
        "Plan Rows": plan.get("Plan Rows"),
        "Actual Rows": plan.get("Actual Rows"),
        "Join Index": join_index,
        "Query Name": queryName,
        "Startup Cost": plan.get("Startup Cost"),
        "Total Cost": plan.get("Total Cost"),
        "Actual Startup Time": plan.get("Actual Startup Time"),
        "Actual Total Time": plan.get("Actual Total Time"),
        "outer_rel":outer_rel,
        "inner_rel":inner_rel,
        "outer_est":outer_est,
        "inner_est":inner_est,
        "outer_act":outer_act,
        "inner_act":inner_act
    }
    results.append(node)

    return results


def save_nodes_to_csv(nodes, filename="plan_nodes.csv"):
    df = pd.DataFrame(nodes)
    write_header = not os.path.exists(filename)
    df.to_csv(filename, mode="a", index=False, header=write_header)


# queryName = "./TCPH-Queries/2.sql"
# JOBQueryName = "./JOB-queries"
# plan_json = get_explain_results(JOBQueryName, connections[2])
# plan = plan_json[0]["Plan"]


directory = "./TCPH-Queries"
JOBQueryName = "./JOB-queries"
stackQueries = "./Stack-queries/q3"
tcphQueries = "./TCPH-Queries/extras"
tcph15 = "./TCPH-Queries/extras/15"

for filename in os.listdir(tcph15):
    if filename.endswith(".sql"):
        filepath = os.path.join(tcph15, filename)
        print("Running:", filepath)

        plan_json = get_explain_results(filepath, connections[0])
        print('\n********* HERE IS THE PLAN **********\n')
        # print(plan_json)
        print('\n********* END **********\n')
        plan = plan_json[0]["Plan"]

        nodes = extract_nodes_bottom_up(plan, [], filepath)
        save_nodes_to_csv(nodes, "query15tcph.csv")
        print("DONE with %s", filename)

# nodes = extract_nodes(plan, [], JOBQueryName)
# save_nodes_to_csv(nodes)
# for n in nodes:
#     print("************************************\n")
#     print(n)

