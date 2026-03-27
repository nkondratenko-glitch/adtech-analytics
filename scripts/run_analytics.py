import csv
import json
import os
from pathlib import Path

import mysql.connector

OUTPUT_DIR = Path("reports")
OUTPUT_DIR.mkdir(exist_ok=True)

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DATABASE", "adtech"),
}

START_DATE = os.getenv("ANALYTICS_START_DATE", "2024-10-01")
END_DATE = os.getenv("ANALYTICS_END_DATE", "2024-10-31")

QUERIES = {
    "q1_top_campaign_ctr": """
        SELECT
            c.campaign_id,
            c.campaign_name,
            COUNT(i.impression_id) AS impressions,
            COUNT(cl.click_id) AS clicks,
            ROUND(100.0 * COUNT(cl.click_id) / NULLIF(COUNT(i.impression_id), 0), 2) AS ctr_pct
        FROM campaigns c
        JOIN impressions i
            ON i.campaign_id = c.campaign_id
           AND i.impression_time >= %(start_date)s
           AND i.impression_time <  %(end_date)s
        LEFT JOIN clicks cl
            ON cl.impression_id = i.impression_id
           AND cl.click_time >= %(start_date)s
           AND cl.click_time <  %(end_date)s
        GROUP BY c.campaign_id, c.campaign_name
        HAVING COUNT(i.impression_id) > 0
        ORDER BY ctr_pct DESC, clicks DESC, impressions DESC
        LIMIT 5
    """,
    "q2_top_advertisers_by_spend": """
        SELECT
            a.advertiser_id,
            a.advertiser_name,
            ROUND(SUM(i.impression_cost), 2) AS total_impression_spend,
            COUNT(i.impression_id) AS impressions,
            COUNT(cl.click_id) AS clicks,
            ROUND(100.0 * COUNT(cl.click_id) / NULLIF(COUNT(i.impression_id), 0), 2) AS ctr_pct
        FROM advertisers a
        JOIN campaigns c ON c.advertiser_id = a.advertiser_id
        JOIN impressions i
            ON i.campaign_id = c.campaign_id
           AND i.impression_time >= %(start_date)s
           AND i.impression_time <  %(end_date)s
        LEFT JOIN clicks cl
            ON cl.impression_id = i.impression_id
           AND cl.click_time >= %(start_date)s
           AND cl.click_time <  %(end_date)s
        GROUP BY a.advertiser_id, a.advertiser_name
        ORDER BY total_impression_spend DESC, clicks DESC
    """,
    "q3_campaign_cost_efficiency": """
        SELECT
            c.campaign_id,
            c.campaign_name,
            COUNT(i.impression_id) AS impressions,
            COUNT(cl.click_id) AS clicks,
            ROUND(COALESCE(SUM(i.impression_cost), 0), 2) AS impression_spend,
            ROUND(COALESCE(SUM(cl.cpc_amount), 0), 2) AS click_spend,
            ROUND(COALESCE(SUM(cl.cpc_amount), 0) / NULLIF(COUNT(cl.click_id), 0), 4) AS avg_cpc,
            ROUND(1000.0 * COALESCE(SUM(i.impression_cost), 0) / NULLIF(COUNT(i.impression_id), 0), 4) AS cpm
        FROM campaigns c
        LEFT JOIN impressions i
            ON i.campaign_id = c.campaign_id
           AND i.impression_time >= %(start_date)s
           AND i.impression_time <  %(end_date)s
        LEFT JOIN clicks cl
            ON cl.impression_id = i.impression_id
           AND cl.click_time >= %(start_date)s
           AND cl.click_time <  %(end_date)s
        GROUP BY c.campaign_id, c.campaign_name
        ORDER BY c.campaign_id
    """,
    "q4_top_locations_by_click_revenue": """
        SELECT
            l.location_id,
            l.location_name,
            ROUND(SUM(cl.cpc_amount), 2) AS total_click_revenue,
            COUNT(cl.click_id) AS total_clicks
        FROM clicks cl
        JOIN impressions i ON i.impression_id = cl.impression_id
        JOIN users u ON u.user_id = i.user_id
        JOIN locations l ON l.location_id = u.location_id
        WHERE cl.click_time >= %(start_date)s
          AND cl.click_time <  %(end_date)s
        GROUP BY l.location_id, l.location_name
        ORDER BY total_click_revenue DESC, total_clicks DESC
        LIMIT 10
    """,
    "q5_top_users_by_clicks": """
        SELECT
            u.user_id,
            COUNT(cl.click_id) AS total_clicks,
            ROUND(SUM(cl.cpc_amount), 2) AS generated_click_revenue
        FROM users u
        JOIN impressions i ON i.user_id = u.user_id
        JOIN clicks cl ON cl.impression_id = i.impression_id
        WHERE cl.click_time >= %(start_date)s
          AND cl.click_time <  %(end_date)s
        GROUP BY u.user_id
        ORDER BY total_clicks DESC, generated_click_revenue DESC
        LIMIT 10
    """,
    "q6_campaigns_near_budget_exhaustion": """
        SELECT
            c.campaign_id,
            c.campaign_name,
            c.budget,
            c.remaining_budget,
            ROUND(c.budget - c.remaining_budget, 2) AS spent_from_budget_column,
            ROUND(100.0 * (c.budget - c.remaining_budget) / NULLIF(c.budget, 0), 2) AS budget_used_pct,
            ROUND(COALESCE(SUM(i.impression_cost), 0), 2) AS realized_impression_spend,
            ROUND(COALESCE(SUM(cl.cpc_amount), 0), 2) AS realized_click_spend
        FROM campaigns c
        LEFT JOIN impressions i
            ON i.campaign_id = c.campaign_id
           AND i.impression_time >= %(start_date)s
           AND i.impression_time <  %(end_date)s
        LEFT JOIN clicks cl
            ON cl.impression_id = i.impression_id
           AND cl.click_time >= %(start_date)s
           AND cl.click_time <  %(end_date)s
        GROUP BY c.campaign_id, c.campaign_name, c.budget, c.remaining_budget
        HAVING budget_used_pct > 80
        ORDER BY budget_used_pct DESC, remaining_budget ASC
    """,
    "q7_device_ctr_optional": """
        SELECT
            i.device_type,
            COUNT(i.impression_id) AS impressions,
            COUNT(cl.click_id) AS clicks,
            ROUND(100.0 * COUNT(cl.click_id) / NULLIF(COUNT(i.impression_id), 0), 2) AS ctr_pct
        FROM impressions i
        LEFT JOIN clicks cl
            ON cl.impression_id = i.impression_id
           AND cl.click_time >= %(start_date)s
           AND cl.click_time <  %(end_date)s
        WHERE i.impression_time >= %(start_date)s
          AND i.impression_time <  %(end_date)s
        GROUP BY i.device_type
        ORDER BY ctr_pct DESC
    """,
}

def write_csv(path: Path, columns, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    params = {
        "start_date": START_DATE,
        "end_date": END_DATE,
    }

    report_json = {
        "time_window": {
            "start_date": START_DATE,
            "end_date_exclusive": END_DATE,
        },
        "queries": {},
    }

    for name, query in QUERIES.items():
        try:
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

            write_csv(OUTPUT_DIR / f"{name}.csv", columns, rows)

            report_json["queries"][name] = {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "status": "ok",
            }

            print(f"{name}: {len(rows)} rows exported")
        except mysql.connector.Error as exc:
            report_json["queries"][name] = {
                "status": "error",
                "error": str(exc),
            }
            print(f"{name}: skipped -> {exc}")

    with (OUTPUT_DIR / "analytics_report.json").open("w", encoding="utf-8") as f:
        json.dump(report_json, f, indent=2, ensure_ascii=False, default=str)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
