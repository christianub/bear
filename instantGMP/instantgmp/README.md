# 📦 instantGMP ETL Pipeline

This project provides an ETL (Extract, Transform, Load) pipeline for interfacing with the instantGMP inventory management platform. The pipeline extracts data from various API endpoints, validates the data using Pydantic models, and loads it into a Snowflake database for analysis and reporting.

---

## 📁 Project Structure

```
instantgmp/
│
├── api/ # Contains one file per API endpoint
│   ├── inventory.py
│   ├── materials_planned.py
│   ├── materials.py
│   ├── pending_receipts.py
│   ├── requisitions.py
│   └── specifications.py
│
├── models/ # Contains Pydantic models for each API response
│   └── models.py
│
├── config.py # Pipeline configuration (API confgs, flow_params, etc.)
├── deploy.py # Main script to run the ETL pipeline
└── README.md # Project documentation
```

---

## 🚀 How It Works

### 1. Extract
Each script in the `api/` folder is responsible for calling one of the instantGMP API endpoints and retrieving data.

### 2. Transform
Data returned from the API is validated and transformed using Pydantic models defined in the `models/` directory. This ensures consistency and correctness before loading.

### 3. Load
The validated data is then loaded into a Snowflake data warehouse for downstream use.

---

## ⚙️ Configuration

All configuration values (API confgs, flow_params, etc.) are managed in `config.py`.
