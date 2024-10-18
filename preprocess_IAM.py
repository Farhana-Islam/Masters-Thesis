import awswrangler as wr
from query import get_query_by_name


def pa_join_IAM():
    #  Get data
    # print("Fetching IAM Data...")
    query = get_query_by_name("pa_join_IAM")

    pa_join_IAM = wr.athena.read_sql_query(sql=query, database="rd_dl_pa_hudi", ctas_approach=False)

    #
    # print("Identifyig office hours....")
    # df_location = pa_join_IAM.loc[pa_join_IAM['city'].notnull()]
    # df_location.loc[:,"submit_time_gmt"] = pd.to_datetime(df_location["submit_time_gmt"]).dt.floor("30T")
    # df_location = df_location.loc[df_location["submit_time_gmt"] > datetime.today() - timedelta(7)]
    # df_location = df_location.sort_values("submit_time_gmt")

    # df_cities = pa_join_IAM.drop_duplicates("city").reset_index(drop=True)[["city", "country"]]
    # geolocator = Nominatim(user_agent="none")
    # obj = TimezoneFinder()
    # geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    # df_cities["location"] = df_cities["city"].apply(geocode)
    # df_cities["point"] = df_cities["location"].apply(lambda loc: tuple(loc.point) if loc else None)
    # df_cities["lat"], df_cities["lon"], df_cities["other"] = zip(*df_cities.point)
    # df_cities["timezone"] = df_cities.apply(lambda city: obj.timezone_at(lng=city["lon"], lat=city["lat"]), axis=1)
    # df_geo_location = pd.merge(df_location, df_cities, on=["city", "country"], how="left")

    # df_geo_location = pd.merge(
    #     pa_join_IAM, df_cities, on=["city", "country"], how="left"
    # )
    # df_geo_location["local_time"] = df_geo_location.apply(
    #     lambda event: event["submit_time_gmt"]
    #     .replace(tzinfo=tz.gettz("GMT"))
    #     .astimezone(tz.gettz(event["timezone"])),
    #     axis=1,
    # )

    # df_geo_location["office_hours"] = df_geo_location.apply(
    #     (
    #         lambda event: "Early_Morning" if (event["local_time"].hour > 8 and event["local_time"].hour <= 10) and (event["local_time"].day_of_week in (0, 1, 2, 3, 4))
    #                     else "Late_Morning" if (event["local_time"].hour > 10 and event["local_time"].hour <= 12) and (event["local_time"].day_of_week in (0, 1, 2, 3, 4))
    #                     else "Early_Afternoon" if (event["local_time"].hour > 12 and event["local_time"].hour <= 15) and (event["local_time"].day_of_week in (0, 1, 2, 3, 4))
    #                     else "Late_Afternoon" if (event["local_time"].hour > 15 and event["local_time"].hour <= 18) and (event["local_time"].day_of_week in (0, 1, 2, 3))
    #                     else "Just_before_weekend" if (event["local_time"].hour > 15 and event["local_time"].hour <= 21) and (event["local_time"].day_of_week ==4)
    #                     else "Out_of_Office_Hour"
    #     ),
    #     axis=1,
    # )

    # df_geo_location.drop(["location", "point","lat","lon","other"],axis =1, inplace =True)
    # print(pa_join_IAM)

    return pa_join_IAM
