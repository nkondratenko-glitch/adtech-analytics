## Kondratenko | HW 2 | Data Engineering: Ad Campaign Analytics

This homework extends the normalized MySQL schema from Homework 1 with analytical SQL queries for evaluating campaign performance, advertiser spending, cost efficiency, regional revenue, user engagement, and budget consumption.

### Added files
- `sql/04_indexes.sql` – secondary indexes added to improve analytical query performance
- `sql/05_analytics_queries.sql` – SQL statements that answer all required business questions
- `scripts/run_analytics.py` – Python script that connects to MySQL, runs the queries, and exports results to CSV and JSON
- `reports/` – generated output files from the Python script
- `docs/hw2_queries_demo.png` – screenshot demonstrating query execution and results

### Shared 30-day analysis window
All analytical queries use the same 30-day reporting window:
- Start date: `2024-10-01`
- End date (exclusive): `2024-10-31`

This corresponds to the inclusive period from `2024-10-01` through `2024-10-30`.

### Performance considerations
To reduce full-table scans and improve joins/grouping on large event tables, additional indexes were introduced on:
- `impressions(impression_time, campaign_id)`
- `impressions(campaign_id, user_id)`
- `clicks(click_time, impression_id)`
- `campaigns(advertiser_id)`
- `users(location_id)`

These indexes support the most common access patterns in the required reports: filtering by time range, joining impressions to clicks, aggregating by campaign, aggregating by advertiser, and resolving user location for regional analysis.

### Business questions covered
1. Top 5 campaigns by CTR over the selected 30-day period
2. Highest-spending advertisers based on impression spend
3. CPC and CPM for each campaign
4. Top-performing locations by click-generated revenue
5. Top 10 most engaged users by number of ad clicks
6. Campaigns that have consumed more than 80% of their total budget
7. Device-type CTR comparison

### Important note about Question 7
The original Homework 1 schema does not include a `device_type` attribute in `impressions`, so the device comparison query is included as an optional extension. To run it, add:

```sql
ALTER TABLE impressions
ADD COLUMN device_type ENUM('mobile', 'desktop', 'tablet') NOT NULL DEFAULT 'desktop';
```

After that, the optional device-performance query in:
- `sql/05_analytics_queries.sql`
- `scripts/run_analytics.py`

can be executed normally.

### Python report generation
Install dependency:

```bash
pip install mysql-connector-python
```

Run the analytics exporter:

```bash
python scripts/run_analytics.py
```

Optional environment variables:
- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`
- `ANALYTICS_START_DATE`
- `ANALYTICS_END_DATE`

The script creates:
- CSV output per query in `reports/`
- Combined JSON summary in `reports/analytics_report.json`

### Example execution order
If Homework 1 database setup is already available, the analytical workflow is:

```bash
mysql -uroot -proot adtech < sql/04_indexes.sql
mysql -uroot -proot adtech < sql/05_analytics_queries.sql
python scripts/run_analytics.py
```

### Deliverables included for Homework 2
- SQL queries answering all required business questions
- Python automation script for report generation
- Reused Docker/MySQL setup from Homework 1
- Query execution screenshots stored in `docs/`
