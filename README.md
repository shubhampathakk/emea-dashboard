# EMEA Delivery & Staffing 360

An executive staffing, delivery intelligence, and resource allocation dashboard for Google Cloud Consulting (PSO) EMEA.

The platform provides delivery leads, resource managers, and practice heads with real-time visibility into engineer allocations, customer portfolio health, roll-off timelines, bench capacity, and billable utilization.

---

## 🌟 Key Features

- **Executive KPI Suite**:
  - **EMEA Team**: Complete delivery roster count with trendline indicators.
  - **Global Customers**: Active enterprise accounts and portfolio rollups.
  - **100% Fully Staffed**: Dedicated delivery resource counts and ratios.
  - **Bench & Available Capacity**: Immediate unallocated capacity ready for deployment.
  - **Allocated Hrs/Wk**: Real-time weekly billable hours with an animated radial utilization ring gauge.
  - Interactive **3D tilt card elevation** with cosmic dark header styling and luminous light data canvas.
- **Timeframe Partition Filtering**:
  - Filter delivery data across **30D**, **Last 90D**, **180D**, **YTD**, and **+90D Runway**, or select arbitrary custom date ranges.
- **Live BigQuery Integration**:
  - Direct connection to upstream Cloud BI production views (`concord-prod.service_cloudbi.scheduled_vs_actual_utilization`).
  - Supports user OAuth bearer tokens (via Google Sign-In) with automatic fallback to Application Default Credentials (ADC) / Service Account.
- **Unified EMEA Delivery Organization**:
  - Multi-stop gradient utilization meter (`linear-gradient(90deg, #2f80ed, #00cea8, #915EFF, #ec008c)`).
  - Hub breakdown across EMEA Field and GSD delivery pools.
- **Customer Portfolio Explorer**:
  - Ranked account workloads, billable weekly hours, and delivery staffing depth.
- **Runway & Roll-off Timeline**:
  - Current-week and upcoming roll-off spotlight with contract end-date tracking.
- **Direct Support & Issue Reporting**:
  - Built-in issue reporting modal pre-populating system diagnostics and routing directly to **`shubhampathakk@google.com`** via default mail client or 1-click Gmail Web compose.

---

## 🛠 Tech Stack

- **Backend**: Python 3.11, [FastAPI](https://fastapi.tiangolo.com/), [Uvicorn](https://www.uvicorn.org/), `google-cloud-bigquery`
- **Frontend**: HTML5, Vanilla JavaScript (ES6+), [Tailwind CSS](https://tailwindcss.com/), [GSAP](https://greensock.com/gsap/)
- **Data Warehouse**: Google Cloud BigQuery
- **Deployment**: Google Cloud Run, Cloud Build, Docker, Identity-Aware Proxy (IAP)

---

## 📁 Repository Structure

```text
.
├── Dockerfile                    # Container definition for Cloud Run
├── deploy.sh                     # Automated deployment script
├── .gitignore                    # Excludes venvs, node_modules, caches
├── README.md                     # Documentation and deployment guide
├── backend/
│   ├── main.py                   # FastAPI application entrypoint & static mounting
│   ├── requirements.txt          # Python dependencies
│   ├── src/
│   │   ├── core/                 # Authentication & configuration
│   │   ├── routers/              # API endpoints (/api/data, /api/upload, etc.)
│   │   └── services/             # BigQuery queries, caching, data transformation
│   └── data/                     # Baseline / offline fallback delivery JSON datasets
└── frontend/
    └── index.html                # Single-page executive dashboard interface
```

---

## 🚀 Local Development Setup

### Prerequisites

- Python 3.11+
- Google Cloud SDK (`gcloud` CLI)
- Access to the GCP project hosting BigQuery (`concord-prod`)

### 1. Clone the Repository

```bash
git clone https://github.com/shubhampathakk/emea-dashboard.git
cd emea-dashboard
```

### 2. Set Up Virtual Environment

```bash
python3 -m venv backend/venv
source backend/venv/bin/activate
pip install -r backend/requirements.txt
```

### 3. Authenticate with Google Cloud

```bash
gcloud auth application-default login
```

### 4. Run the Application

```bash
cd backend
export GCP_PROJECT_ID="concord-prod"
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

Open your browser and navigate to `http://localhost:8080`.

---

## 🚢 Deployment Guide (Google Cloud Run)

The application is containerized and deployed as a serverless service on **Google Cloud Run**.

### Configuration

- **Target GCP Project**: `pso-gdc-japac-wedevelop-df`
- **Region**: `us-central1`
- **Service Name**: `emea-dashboard`
- **BigQuery Source Project**: `concord-prod`

---

### Option A: Using the Automated Deployment Script

Run `deploy.sh` from the repository root:

```bash
chmod +x deploy.sh
./deploy.sh
```

---

### Option B: Manual Deployment via `gcloud`

Run the following command from the repository root:

```bash
gcloud run deploy emea-dashboard \
  --source . \
  --project pso-gdc-japac-wedevelop-df \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars="GCP_PROJECT_ID=concord-prod"
```

#### What this command does:
1. Packages the codebase and uploads it to **Cloud Build**.
2. Builds the Docker container using `Dockerfile` (installing dependencies from `backend/requirements.txt` and packaging `frontend/`).
3. Deploys the container to **Cloud Run** under service `emea-dashboard`.
4. Sets the environment variable `GCP_PROJECT_ID=concord-prod` for BigQuery query routing.
5. Emits the live service URL upon completion (e.g., `https://emea-dashboard-673796126218.us-central1.run.app`).

---

## 🔒 Security & Access Control

- **Identity-Aware Proxy (IAP)**: When deployed on corporate GCP infrastructure, Google Cloud IAP can protect the Cloud Run service URL to restrict access exclusively to authorized `@google.com` corporate accounts.
- **OAuth Token Pass-Through**: The frontend includes Google OAuth capabilities (`firebase-auth`) to pass user-scoped tokens directly to BigQuery for authenticated data queries.

---

## 📬 Support & Feedback

For questions, issues, or feature requests, contact:

- **Shubham Pathak**: [`shubhampathakk@google.com`](mailto:shubhampathakk@google.com)
