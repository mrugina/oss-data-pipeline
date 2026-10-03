# Local Aviation Data Pipeline

This project sets up a local data pipeline to fetch flight information from [AviationStack](https://aviationstack.com/), process it, and visualize it.

## Architecture

*   **Orchestration**: Apache Airflow
*   **Data Ingestion**: Airbyte
*   **Streaming Buffer**: Apache Kafka
*   **Transformation**: dbt
*   **Warehouse**: PostgreSQL (serving as both Airflow metadata DB, Superset metadata DB, and Data Warehouse)
*   **Visualization**: Apache Superset

## Prerequisites

*   Docker & Docker Compose
*   `curl` and `git`
*   An API Key from [AviationStack](https://aviationstack.com/) (Free tier available — 100 API calls/month)

## Project Structure

*   `docker-compose.yaml`: Runs the core infrastructure (Airflow, Kafka, Postgres, Superset).
*   `deploy.sh`: Automated script to deploy everything, including fetching Airbyte.
*   `dbt_project/`: dbt project for transformations.
*   `dags/`: Airflow DAGs.

## Accessing the Tools

Once deployed, you can access the tools at the following URLs:

*   **Airflow**: [http://localhost:8080](http://localhost:8080) (User: `airflow`, Pass: `airflow`)
*   **Airbyte**: [http://localhost:8000](http://localhost:8000) (User: `airbyte`, Pass: `password`)
*   **Superset**: [http://localhost:8088](http://localhost:8088) (User: `admin`, Pass: `admin`)
*   **Postgres**: `localhost:5432` (User: `postgres`, Pass: `postgres`)

## How to Deploy

1.  Make the deploy script executable:
    ```bash
    chmod +x deploy.sh
    ```

2.  Run the deployment script:
    ```bash
    ./deploy.sh
    ```
    *Note: This script will clone the official Airbyte repository and start it, then start the local `docker-compose.yaml` for the rest of the stack.*

## Configuration Steps (Post-Deployment)

### 1. Airbyte — Create the AviationStack connector and streams

Go to http://localhost:8000 (User: `airbyte`, Pass: `password`)

#### Create the connector

1.  Click on **Connector Builder** (pencil & ruler icon in the left sidebar).
2.  **Global Configuration**:
    *   **Connector Name**: `AviationStack`
    *   **API Base URL**: `http://api.aviationstack.com/v1/`
3.  **Authentication**:
    *   **Type**: `API Key`
    *   **Key name**: `access_key`
    *   **Inject into**: `Query Parameter`
    *   **Test value**: Paste your actual API Key here to test.

#### Add streams

Click the `+` button to add each stream. For each stream, set the **Name**, **URL Path** (`flights` for all three), and add a **Query Parameter** (the `key=value` filter).

> **Note**: The option is called **"Query Parameter"** in the Airbyte UI — this is the correct one. It appends `?key=value` to the API URL.

| Stream name | URL Path | Query Parameter Key | Query Parameter Value | What it does | API calls per sync |
|---|---|---|---|---|---|
| `departures_otp` | `flights` | `dep_iata` | `OTP` | Flights **departing from** Bucharest | 1 |
| `arrivals_otp` | `flights` | `arr_iata` | `OTP` | Flights **arriving at** Bucharest | 1 |
| `active_flights` | `flights` | `flight_status` | `active` | All flights currently **in the air** (count only) | 1 |

*Each sync of all 3 streams = 3 API calls. With 100 calls/month you can refresh ~33 times.*

> **Tip**: You can replace `OTP` with any airport IATA code (e.g. `JFK`, `LHR`, `CDG`). You can also add streams for specific airlines using `airline_iata` as the key (e.g. `RO` for Tarom, `W6` for Wizz Air).

#### Publish & connect

1.  Click the **Publish** button (bottom right) to save the connector.
2.  **Set up Source**:
    *   Go to **Sources** (left sidebar) → **+ New Source**.
    *   Search for and select the **AviationStack** connector you just published.
    *   Paste your **AviationStack API Key** in the `access_key` field.
    *   Click **Set up source**.
3.  **Set up Destination** (Kafka):
    *   Go to **Destinations** (left sidebar) → **+ New Destination**.
    *   Search for and select **Kafka**.
    *   **Bootstrap Servers**: `host.docker.internal:9092` (Mac/Windows) or your host IP (Linux).
    *   **Topic Pattern**: `flight_data`
    *   **Protocol**: `PLAINTEXT`
    *   Leave all other settings at their defaults.
    *   Click **Set up destination**.
4.  **Create the connection and set it to Manual**:
    *   Go to **Connections** (left sidebar) → **+ New Connection**.
    *   Pick your **AviationStack** source and your **Kafka** destination.
    *   On the connection configuration screen, find **Replication frequency** (also called **Schedule type**).
    *   Open the dropdown and select **Manual** — this ensures syncs only happen when you explicitly click "Sync now".
    *   Under **Activate the streams you want to sync**, make sure all three streams (`departures_otp`, `arrivals_otp`, `active_flights`) are toggled **on**.
    *   Click **Set up connection**.
5.  **Run the first sync**:
    *   You'll land on the connection page. Click the **Sync now** button to trigger the first sync.
    *   Wait for the status to show **Succeeded** (usually takes 10–30 seconds).

##### How to change an existing connection to Manual

If you already have a connection and it's set to a schedule (e.g. every 24 hours):

1.  Go to **Connections** (left sidebar) → click your connection.
2.  Click the **Replication** tab at the top.
3.  Find **Replication frequency** / **Schedule type**.
4.  Change it to **Manual**.
5.  Click **Save changes** (bottom right).

#### Clean up old data (optional)

If you previously synced with the old unfiltered `flights` stream, you can remove that stale data.

Run this in Superset SQL Lab or any Postgres client:
```sql
DELETE FROM raw_data.flights WHERE data->>'_airbyte_stream' = 'flights';
```

> **If SQL Lab gives you a "Database does not allow data manipulation" error**:
> Go to **Settings** → **Database Connections** → click the **pencil icon** on `Warehouse` → expand **Advanced** → open the **Security** tab → check **Allow DML** → **Save**. Then re-run the DELETE.
>
> You can also run it from the terminal instead:
> ```bash
> docker compose exec postgres psql -U postgres -c "DELETE FROM raw_data.flights WHERE data->>'_airbyte_stream' = 'flights';"
> ```

### 2. Airflow — Enable the dbt DAG

1.  Go to http://localhost:8080 (User: `airflow`, Pass: `airflow`)
2.  Enable the `aviation_dbt_pipeline` DAG. It is set to **manual trigger only** — it will never run on its own.
3.  To run it: click the **▶ (Play)** button on the DAG row, then **Trigger DAG**.

### 3. Run dbt (first time)

The Airflow DAG triggers dbt when you click play, but you can also run it directly:
```bash
docker compose run --rm dbt run --profiles-dir .
```
You should see two models created:
*   `analytics.flights_metrics` — one row per flight (for charts)
*   `analytics.active_flights_total` — latest total count of flights in the air (for the big number)

*Note: This requires data in `raw_data.flights`. Make sure the Airbyte connection has synced at least once and the consumer is running.*

### 4. Superset — Connect and build the dashboard

Go to http://localhost:8088 (User: `admin`, Pass: `admin`)

#### Add the database connection

1.  Go to **Settings** (top right) → **Database Connections** → **+ Database**.
2.  Select **PostgreSQL**.
3.  Enter:
    *   **Host**: `postgres` *(IMPORTANT: Do NOT use `localhost`)*
    *   **Port**: `5432`
    *   **Database Name**: `postgres`
    *   **Username**: `postgres`
    *   **Password**: `postgres`
    *   **Display Name**: `Warehouse`
4.  Click **Connect**.
5.  *If the connection fails, click "Connect with SQLAlchemy URI" and paste:*
    `postgresql+psycopg2://postgres:postgres@postgres:5432/postgres`

#### Verify data in SQL Lab

1.  Hover over **SQL** in the top menu → click **SQL Lab**.
2.  Set **Database** to `Warehouse`, **Schema** to `analytics`.
3.  Run: `SELECT * FROM analytics.flights_metrics LIMIT 10;` — should return flight rows.
4.  Run: `SELECT * FROM analytics.active_flights_total;` — should return one row with the count.

#### Create Dataset 1 — `flights_metrics`

1.  Click the **+** (top right) → **Dataset**.
2.  **Database**: `Warehouse` · **Schema**: `analytics` · **Table**: `flights_metrics`
3.  Click **Create Dataset and Create Chart**.

#### Create Chart 1 — "Departures from OTP" (Bar Chart)

1.  Choose **Bar Chart**. Click **Create New Chart**.
2.  Configure:
    *   **X-Axis**: `airline`
    *   **Metrics**: click **+ Add metric** → **SIMPLE** tab → Column: `airline`, Aggregate: `COUNT` → **Save**
    *   **Filters**: click **+ Add filter**:
        *   Column: `direction`
        *   Operator: `equals`
        *   Value: `departure`
    *   **Sort By**: same metric, descending
    *   **Row Limit**: `20`
3.  Click **Create Chart** to preview.
4.  Click **Save** → name it `Departures from OTP` → select **Add to new dashboard** → name it `Aviation Dashboard` → **Save**.

#### Create Chart 2 — "Arrivals at OTP" (Bar Chart)

1.  Click the **+** (top right) → **Chart**.
2.  Pick the existing **`flights_metrics`** dataset → choose **Bar Chart** → **Create New Chart**.
3.  Configure (same as above but with a different filter):
    *   **X-Axis**: `airline`
    *   **Metrics**: click **+ Add metric** → **SIMPLE** tab → Column: `airline`, Aggregate: `COUNT` → **Save**
    *   **Filters**: click **+ Add filter**:
        *   Column: `direction`
        *   Operator: `equals`
        *   Value: `arrival`
    *   **Sort By**: same metric, descending
    *   **Row Limit**: `20`
4.  Click **Create Chart** to preview.
5.  Click **Save** → name it `Arrivals at OTP` → select **Add to existing dashboard** → pick `Aviation Dashboard` → **Save**.

#### Create Dataset 2 — `active_flights_total`

1.  Click the **+** (top right) → **Dataset**.
2.  **Database**: `Warehouse` · **Schema**: `analytics` · **Table**: `active_flights_total`
3.  Click **Create Dataset and Create Chart**.

#### Create Chart 3 — "Flights in the Air" (Big Number)

This chart displays a single large number showing the total count of flights worldwide.

1.  On the **Choose a visualization type** screen, scroll down or search for **Big Number** and select it. Click **Create New Chart**.
2.  You'll land in the chart editor. In the left panel, configure:
    *   **Metric**:
        1.  Click **+ Add metric**.
        2.  Switch to the **SIMPLE** tab.
        3.  **Column**: select `flights_in_the_air` from the dropdown.
        4.  **Aggregate**: select `MAX`.
        5.  Click **Save**.
    *   **Subheader**: type `flights worldwide right now` (this appears as smaller text below the number).
    *   You can leave **Temporal Column** and other fields empty.
3.  Click **Create Chart** (or **Update Chart**) to preview — you should see a large number (e.g. ~490,000).
4.  Click **Save** (top right):
    *   **Chart name**: `Flights in the Air`
    *   Select **Add to existing dashboard** → pick `Aviation Dashboard`
    *   Click **Save & go to dashboard**.

#### Arrange the dashboard

1.  Go to **Dashboards** → click **Aviation Dashboard**.
2.  Click the **pencil icon** (top right) to edit.
3.  Drag and resize the charts — e.g. put the big number at the top, the two bar charts side by side below.
4.  Click **Save**.

## Data Freshness & On-Demand Refresh

Everything is set to **manual trigger only** to protect your 100 free API calls/month.

### Check when data was last refreshed

```sql
SELECT MAX(created_at) AS last_refreshed FROM raw_data.flights;
```

### Refresh workflow (on-demand, costs 3 API calls)

1.  **Sync Airbyte**: go to http://localhost:8000 → **Connections** → click your connection → **Sync now**.
    *   ⚠️ Make sure the connection's **Replication frequency** is set to **Manual**.
2.  **Wait for the consumer** — it picks up new Kafka messages automatically and writes them to `raw_data.flights`.
3.  **Run dbt** (rebuilds the analytics views):
    *   **Option A** — from Airflow: go to http://localhost:8080, find `aviation_dbt_pipeline`, click **▶ → Trigger DAG**.
    *   **Option B** — from terminal:
        ```bash
        docker compose run --rm dbt run --profiles-dir .
        ```
4.  **Refresh Superset** — open the dashboard and click the **↻** refresh icon, or re-run the queries in SQL Lab.

## Troubleshooting

### "Unable to connect to database" in Superset
If you see a "Database Creation Error" or cannot connect:

1.  **Use the SQLAlchemy URI**: Instead of the form fields, click **"Connect with SQLAlchemy URI"** and use:
    ```
    postgresql+psycopg2://postgres:postgres@postgres:5432/postgres
    ```
2.  **Verify Connectivity**: Run this command in your terminal to check if Superset can reach Postgres:
    ```bash
    docker compose exec superset python3 -c "import psycopg2; print('Connection successful');"
    ```
    *If this fails with `ModuleNotFoundError`, the driver is missing. Rebuild the Superset image with `docker compose build superset`.*
    *If it hangs or errors, the network link is broken.*

### "Could not load database driver: PostgresEngineSpec" in Superset
The `psycopg2` driver is not installed in Superset's Python environment. Fix:
```bash
docker compose build --no-cache superset && docker compose up -d superset
```

### Airflow logs: "Request URL is missing an 'http://' or 'https://' protocol"
This is fixed by the `AIRFLOW__WEBSERVER__BASE_URL` environment variable in `docker-compose.yaml`. If you still see it, restart Airflow:
```bash
docker compose restart airflow-webserver airflow-scheduler
```

### Airbyte sync fails: "The stream flights no longer exists in the configuration"
This happens when you changed the streams in the Connector Builder (e.g. replaced `flights` with `departures_otp`, `arrivals_otp`, `active_flights`) but the existing connection still references the old stream.

1.  Go to **Connections** → click your connection → **Replication** tab.
2.  Click **Refresh schema** (or the **↻** icon next to the streams table). If you see a banner about schema changes, click **Review schema changes**.
3.  The old `flights` stream will be removed. The new streams will appear.
4.  Toggle **all new streams on**.
5.  Click **Save changes** → **Sync now**.

### Airbyte cannot find "run-ab-platform.sh"
Ensure you are using the correct version of Airbyte. The `deploy.sh` script attempts to checkout `v0.50.33`. If issues persist, try deleting the `airbyte` folder and running `./deploy.sh`
