"""Print the headline gold results and cross check them against silver.

Run from the repository root after `make run`:
    uv run python scripts/check_results.py            # Sydney spot check
    uv run python scripts/check_results.py per        # Perth spot check

Read only: it never changes the warehouse.

Part A shows the answers the project was built for.
Part B recomputes key numbers straight from silver with separate SQL (not via the intermediate
models) and compares them with gold. If the two disagree, a transformation is wrong.
"""

from __future__ import annotations

import sys

import duckdb

LOC = sys.argv[1] if len(sys.argv) > 1 else "syd"
con = duckdb.connect("data/warehouse.duckdb", read_only=True)


def show(title: str, sql: str, params: list | None = None) -> None:
    print(f"\n=== {title}")
    con.sql(sql, params=params).show(max_width=200)


# ------------------------------------------------------------------ Part A: the answers

show("A1. Pipeline health (failed_last_7_days should be 0, min_pct_present close to 100)",
     "select source, last_ingested_at_utc, data_until_utc, files_stored, failed_last_7_days, "
     "round(min_pct_present, 1) as min_pct_present from gold.gold_pipeline_health order by source")

show(f"A2. 7 day forecast for {LOC} (local dates)", """
    select model, local_date, lead_day, round(temperature_min, 1) as tmin, round(temperature_max, 1) as tmax,
           round(expected_tmax_error, 2) as typical_tmax_err, round(precipitation_sum, 1) as rain_mm,
           weather_description
    from gold.gold_forecast_7day where location_id = ? order by local_date, model""", [LOC])

show("A3. Accuracy by lead day: daily max temperature, all locations", """
    select lead_day,
           round(max(mae) filter (where model = 'ecmwf_ifs025'), 2)               as ecmwf_mae,
           round(max(mae) filter (where model = 'gfs_global'), 2)                 as gfs_mae,
           round(100 * max(share_within_2) filter (where model = 'ecmwf_ifs025')) as ecmwf_within_2c_pct,
           round(100 * max(share_within_2) filter (where model = 'gfs_global'))   as gfs_within_2c_pct,
           max(n_days)                                                            as days_scored
    from gold.gold_daily_accuracy
    where metric = 'temperature_max' and location_id = 'ALL' and season = 'all'
    group by lead_day order by lead_day""")

show("A4. Model comparison by city: daily max temperature MAE at lead day 1 (bias > 0 means too warm)", """
    select location_id,
           round(max(mae)  filter (where model = 'ecmwf_ifs025'), 2) as ecmwf_mae,
           round(max(mae)  filter (where model = 'gfs_global'), 2)   as gfs_mae,
           round(max(bias) filter (where model = 'ecmwf_ifs025'), 2) as ecmwf_bias,
           round(max(bias) filter (where model = 'gfs_global'), 2)   as gfs_bias
    from gold.gold_daily_accuracy
    where metric = 'temperature_max' and lead_day = 1 and season = 'all'
    group by location_id order by location_id""")

show("A5. Rain day skill (rain day = 1 mm or more), all locations", """
    select model, lead_day, n_days, round(hit_rate, 2) as hit_rate, round(false_alarm_ratio, 2) as false_alarm_ratio,
           round(critical_success_index, 2) as csi, round(frequency_bias, 2) as frequency_bias
    from gold.gold_rain_skill where location_id = 'ALL' order by model, lead_day""")

# ------------------------------------------------------------------ Part B: independent checks

print("\n" + "=" * 70 + "\nPart B: recompute from silver and compare with gold\n" + "=" * 70)

# B1: hourly temperature MAE per model and lead day, paired, straight from silver.
b1 = con.sql("""
    with paired as (
        -- an hour counts only if every model has a value and the actual exists
        select f.location_id, f.valid_time_utc, f.lead_day
        from silver.silver_forecast_by_lead f
        join silver.silver_actuals a using (location_id, valid_time_utc)
        where f.temperature_2m is not null and a.temperature_2m is not null
        group by all
        having count(distinct f.model) = (select count(distinct model) from silver.silver_forecast_by_lead)
    ),
    recomputed as (
        select f.model, f.lead_day, count(*) as n_hours, avg(abs(f.temperature_2m - a.temperature_2m)) as mae
        from silver.silver_forecast_by_lead f
        join silver.silver_actuals a using (location_id, valid_time_utc)
        join paired p using (location_id, valid_time_utc, lead_day)
        group by all
    )
    select r.model, r.lead_day, r.n_hours, g.n_hours as gold_n_hours,
           round(r.mae, 4) as recomputed_mae, round(g.mae, 4) as gold_mae,
           abs(r.mae - g.mae) < 1e-9 and r.n_hours = g.n_hours as match
    from recomputed r
    join gold.gold_accuracy_by_lead g
      on g.model = r.model and g.lead_day = r.lead_day and g.variable = 'temperature_2m' and g.location_id = 'ALL'
    order by r.model, r.lead_day""")
print("\n=== B1. Hourly temperature MAE, recomputed from silver vs gold")
b1.show(max_width=200)
b1_ok = all(row[-1] for row in b1.fetchall())

# B2: one real day by hand. Take the most recent complete local day for LOC with a lead 1 ECMWF forecast.
b2 = con.sql("""
    with loc as (select timezone from silver.dim_location where location_id = $loc),
    hours as (
        -- same UTC to local conversion as the pipeline's to_local macro (independent of the session time zone)
        select timezone(l.timezone, timezone('UTC', f.valid_time_utc))::date as local_date,
               f.temperature_2m as fc, a.temperature_2m as ac
        from silver.silver_forecast_by_lead f
        join silver.silver_actuals a using (location_id, valid_time_utc)
        cross join loc l
        where f.location_id = $loc and f.model = 'ecmwf_ifs025' and f.lead_day = 1
    ),
    days as (
        select local_date, count(*) as n_hours, max(fc) as fc_tmax, max(ac) as ac_tmax
        from hours group by local_date
        having count(*) >= 23 and count(fc) = count(*) and count(ac) = count(*)
    )
    select d.local_date, d.n_hours,
           round(d.fc_tmax, 2) as recomputed_fc_tmax, round(i.fc_tmax, 2) as gold_fc_tmax,
           round(d.ac_tmax, 2) as recomputed_actual_tmax, round(i.ac_tmax, 2) as gold_actual_tmax,
           abs(d.fc_tmax - i.fc_tmax) < 1e-9 and abs(d.ac_tmax - i.ac_tmax) < 1e-9 as match
    from days d
    join intermediate.int_daily_errors i
      on i.location_id = $loc and i.model = 'ecmwf_ifs025' and i.lead_day = 1 and i.local_date = d.local_date
    order by d.local_date desc limit 1""", params={"loc": LOC})
print(f"\n=== B2. One day by hand: {LOC}, ECMWF, lead day 1, daily max temperature")
b2.show(max_width=200)
b2_rows = b2.fetchall()
b2_ok = bool(b2_rows) and all(row[-1] for row in b2_rows)

# B3: sanity. Error should generally grow with lead day; RMSE can never be below MAE.
b3 = con.sql("""
    select model,
           round(max(mae) filter (where lead_day = 1), 2) as day1_mae,
           round(max(mae) filter (where lead_day = 7), 2) as day7_mae,
           max(mae) filter (where lead_day = 7) > max(mae) filter (where lead_day = 1) as grows_with_lead,
           bool_and(rmse >= mae) as rmse_not_below_mae
    from gold.gold_accuracy_by_lead
    where variable = 'temperature_2m' and location_id = 'ALL' group by model order by model""")
print("\n=== B3. Sanity: error grows with lead day, RMSE >= MAE")
b3.show(max_width=200)
b3_ok = all(row[-1] and row[-2] for row in b3.fetchall())

print("\nRESULT")
print(f"  B1 gold hourly MAE matches silver recomputation : {'PASS' if b1_ok else 'FAIL'}")
print(f"  B2 one day by hand matches intermediate          : {'PASS' if b2_ok else 'FAIL'}")
print(f"  B3 sanity checks                                 : {'PASS' if b3_ok else 'FAIL'}")
sys.exit(0 if (b1_ok and b2_ok and b3_ok) else 1)
