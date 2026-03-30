# 🚀 Azure Multi-Source Retail Data Pipeline

> An end-to-end Azure Data Engineering project that ingests data from multiple sources (Azure SQL DB + REST API), transforms it through a Medallion Architecture (Bronze → Silver → Gold), and visualizes insights in Power BI.

---

## 📐 Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│  SOURCE LAYER           INGEST         STORAGE          CONSUME    │
│                                                                    │
│  Azure SQL DB ──┐                   ┌─ Bronze ─┐                  │
│  (transactions) │                   │  (Parquet)│                  │
│  Azure SQL DB ──┼──► Azure Data ───►│  Silver  ─┼──► Power BI     │
│  (stores)       │    Factory (ADF)  │  (Delta)  │    Dashboard     │
│  Azure SQL DB ──┘                   └─ Gold ────┘                  │
│  (products)        ▲                   (Delta)                     │
│  GitHub JSON API ──┘    ADLS Gen2              ▲                   │
│  (customers)            (Medallion)     Databricks + PySpark       │
└────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Technology Stack

| Layer | Service | Purpose |
|---|---|---|
| Source | Azure SQL Database | Transactions, Products, Stores tables |
| Source | GitHub REST API (JSON) | Customer data |
| Ingestion | Azure Data Factory (ADF) | Copy pipelines to Bronze |
| Storage | Azure Data Lake Gen2 (ADLS) | Bronze / Silver / Gold layers |
| Transform | Azure Databricks + PySpark | Cleaning, joins, aggregations |
| Format | Delta Lake | ACID compliance, time travel |
| Visualize | Power BI Desktop | Business dashboards & KPIs |

---

## 📂 Repository Structure

```
azure-data-engineer---multi-source/
├── SCRIPT_SQL.txt          # SQL to create & seed tables in Azure SQL DB
├── retail_project/         # Databricks notebooks (PySpark transformation code)
├── customers.json          # Customer data hosted on GitHub (API source)
└── app.py                  # All optimized PySpark commands (standalone)
```

---

## 📋 Prerequisites

- [ ] Azure Subscription (free trial works)
- [ ] Azure SQL Database
- [ ] Azure Data Lake Storage Gen2 (Hierarchical Namespace enabled)
- [ ] Azure Data Factory V2
- [ ] Azure Databricks (Community Edition or Azure-hosted)
- [ ] Power BI Desktop

---

## ⚙️ Setup Instructions

### Step 1 — Azure SQL Database

1. Create a Resource Group (e.g., `qwertyu`) in Azure Portal
2. Create a SQL Server (`myserver-pod`, region: West US 2, SQL auth: `chulya` / `Allen@786`)
3. Create a SQL Database (`mydata`) — choose Basic compute (2 GB), Development workload
4. Set connectivity to **Public endpoint**, enable Microsoft Defender (free trial)
5. Open **Query Editor** → paste `SCRIPT_SQL.txt` from this repo → Run
6. Verify tables:

```sql
SELECT * FROM transactions;
SELECT * FROM products;
SELECT * FROM stores;
```

---

### Step 2 — Azure Data Lake Storage Gen2

1. Create a Storage Account (`mystg0poc`, region: West US 2)
2. In the **Advanced** tab → enable **Hierarchical namespace** (converts to ADLS Gen2)
3. Create container: `retailer`
4. Inside `retailer`, create folders:
   ```
   Bronze/
     ├── transaction/
     ├── store/
     ├── product/
     └── customer/
   Silver/
   Gold/
   ```

---

### Step 3 — Azure Data Factory Pipelines

1. Create Data Factory (`mydatafac36543`, West US 2, V2) → **Launch Studio**
2. Author → New Pipeline → drag **Copy Data** activity onto canvas
3. For each SQL table (transactions, stores, products):
   - **Source**: Azure SQL Database linked service (`myserver-pod` / `mydata` / SQL auth)
   - **Sink**: ADLS Gen2 linked service → Parquet format → path: `retailer/Bronze/<table>/`
   - Add SQL Server firewall rule to allow ADF's IP
4. For customer JSON (API source):
   - **Source**: HTTP linked service → Base URL: `https://raw.githubusercontent.com` → JSON format
   - Relative URL: `mantu0tech/Azure-data-engineer-pipeline/refs/heads/main/customers`
   - **Sink**: ADLS Gen2 → `retailer/Bronze/customer/`
5. Click **Debug** → verify all files appear in ADLS

---

### Step 4 — Azure Databricks Transformation

1. Create Azure Databricks workspace → Launch
2. Create **Access Connector** for Azure Databricks (`democon`) in Azure Portal
3. In ADLS (`mystg0poc`) → Access Control (IAM) → assign **Storage Blob Data Contributor** to `democon` (Managed Identity)
4. In Databricks → Catalog → **Create a credential** (Azure Managed Identity, paste Access Connector ID)
5. Create **External Location**: URL `abfss://retailer@mystg0poc.blob.core.windows.net/` → grant ALL PRIVILEGES
6. Create a **Notebook** → attach to compute → run commands from `app.py`

---

### Step 5 — Power BI Dashboard

1. Download [Power BI Desktop](https://www.microsoft.com/en-us/download/details.aspx?id=58494)
2. In Databricks → run Gold query → download CSV from results
3. In Power BI → **Get Data** → Files → Text/CSV → load the downloaded file
4. Build visuals in Report view:
   - KPI cards: Total Sales, Quantity Sold, Transactions
   - Line chart: Sales by Day
   - Bar chart: Sales by Product
   - Pie chart: Sales by Store / Category

---

## 🔑 Key Commands (Databricks Notebook)

```python
# Verify external location
%sql SHOW EXTERNAL LOCATIONS;

# List Bronze folder
dbutils.fs.ls("abfss://retailer@mystg0poc.dfs.core.windows.net/Bronze")

# Query Silver table
%sql SELECT * FROM retail_silver_cleaned;

# Query Gold table
%sql SELECT * FROM retail_gold_sales_summary;
```

---

## 🗃️ Data Model

### Source Tables (Bronze)

| Table | Source | Key Columns |
|---|---|---|
| `transactions` | Azure SQL DB | transaction_id, customer_id, product_id, store_id, quantity, transaction_date |
| `products` | Azure SQL DB | product_id, product_name, category, price |
| `stores` | Azure SQL DB | store_id, store_name, location |
| `customers` | GitHub JSON API | customer fields |

### Transformed Tables

| Table | Layer | Format | Description |
|---|---|---|---|
| `retail_silver_cleaned` | Silver | Delta | Joined transactions + products + stores + total_amount |
| `retail_gold_sales_summary` | Gold | Delta | Aggregated KPIs by date, product, store |

### Gold Table Schema

```
transaction_date         date
product_id               integer
product_name             string
category                 string
store_id                 integer
store_name               string
location                 string
total_quantity_sold      long
total_sales_amount       double
number_of_transactions   long
average_transaction_value double
```

---

## 📊 Dashboard Results

| KPI | Value |
|---|---|
| Total Sales Amount | **134.57K** |
| Total Quantity Sold | **101** |
| Total Transactions | **30** |
| Top Store (by Sales) | **Tech World Outlet** |
| Top Category | **Electronics** |

---

## 🔗 References

- **GitHub Repo**: (https://github.com/mantu0tech/Azure-data-engineer-pipeline.git)
- **Customer API**: https://raw.githubusercontent.com/mantu0tech/Azure-data-engineer-pipeline/refs/heads/main/customers.json
- **Power BI Download**: https://www.microsoft.com/en-us/download/details.aspx?id=58494

---

## 👤 Author

Built as a hands-on Azure Data Engineering portfolio project demonstrating multi-source ingestion, Medallion Architecture, and end-to-end pipeline orchestration.

`#Azure` `#DataEngineering` `#Databricks` `#PySpark` `#DeltaLake` `#ADF` `#PowerBI`
