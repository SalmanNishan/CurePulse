from sqlalchemy import create_engine

conn_string = 'postgres://curepulseadmin:Saluteryjanisar0!#@172.16.101.152/curepulse_superset_metadata'
db = create_engine(conn_string)
conn = db.connect()

rows = conn.execute("""
        SELECT t1.user_id, t2.username, t3.name
        FROM "ab_user_role" as t1
        JOIN "ab_user" as t2
        ON t1.user_id = t2.id
        JOIN "ab_role" as t3
        ON t1.role_id = t3.id
        ORDER BY t3.name
    """).fetchall()

results_dict = {}

def create_dict_from_tuples(lst):
    result_dict = {}
    for item in lst:
        key = item[2]
        value = item[1]
        if key in result_dict:
            result_dict[key].append(value)
        else:
            result_dict[key] = [value]
    return result_dict

# Example usage:
my_dict = create_dict_from_tuples(rows)

with open("agent_names.txt", "w") as outfile:
    outfile.write("\n".join(my_dict["CS_agents"]))

with open("sales_agent_names.txt", "w") as outfile:
    outfile.write("\n".join(my_dict["Sales_agents"]))

with open("indian_agent_names.txt", "w") as outfile:
    outfile.write("\n".join(my_dict["India_agents"]))
