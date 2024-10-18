import awswrangler as wr
from query import get_query_by_name


def pa_join_tr():
    #  Get data
    # print("Fetching tr Data...")
    query = get_query_by_name("pa_join_tr")

    pa_join_tr = wr.athena.read_sql_query(sql=query, database="rd_dl_pa_hudi", ctas_approach=False)

    return pa_join_tr
