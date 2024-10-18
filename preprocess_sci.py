import awswrangler as wr
from query import get_query_by_name


def pa_join_sci():
    #  Get data
    # print("Fetching sciforma Data...")
    query = get_query_by_name("pa_join_sci")

    pa_join_schi = wr.athena.read_sql_query(sql=query, database="rd_dl_pa_hudi", ctas_approach=False)

    return pa_join_schi
