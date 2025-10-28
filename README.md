# 🧠 Calendly Marketing Data Pipeline

## 🚀 Overview
This project builds an end-to-end **data engineering pipeline** for marketing and Calendly data, designed to analyze lead generation and marketing performance across campaigns.  
The pipeline ingests **Calendly webhooks** and **marketing spend data**, processes them through a **medallion architecture** (Bronze → Silver → Gold), and visualizes key business metrics in a **Streamlit dashboard**.

---

## 🏗️ Architecture
The pipeline is designed using **AWS native services** for scalability, modularity, and automation.

### Data Flow
1. **Ingestion (Bronze Layer)**  
   - Calendly Webhooks → Captured and stored as raw JSON in S3 (`/bronze/calendly/webhooks/`)  
   - Marketing Spend Data → Pulled daily from a public S3 source (`dea-data-bucket/calendly_spend_data/`)  
   - All raw data stored in the **Bronze S3 bucket** (SSE-S3 encrypted)

2. **ETL & Transformations (Silver + Gold Layers)**  
   - AWS Glue Jobs (PySpark) handle transformations and cleaning.  
   - **Bronze → Silver**: Normalizes nested Calendly JSON and marketing spend files  
   - **Silver → Gold**: Joins Calendly and marketing data to compute metrics such as:
     - Daily Bookings by Source  
     - Cost per Booking (CPB)  
     - Bookings Trend Over Time  
     - Meeting Load per Employee  
   - Transformed data stored as **Delta tables** in S3 for versioning and query optimization.

3. **Analytics & Visualization (Streamlit App)**  
   - Interactive Streamlit dashboard built using data from the Gold layer  
   - Visualizations include:
     - 📅 Daily Calls by Source  
     - 💸 Cost per Booking (CPB) by Channel  
     - 📈 Booking Trends Over Time  
     - 🔥 Channel Attribution Leaderboard  
     - 🕓 Booking Volume by Time Slot / Day of Week  
     - 👥 Meeting Load per Employee  

---

## 🧩 Tech Stack

**Storage:** Amazon S3  
**ETL / Orchestration:** AWS Glue, AWS Lambda  
**Data Catalog / Querying:** AWS Glue Catalog, Amazon Athena  
**Monitoring:** AWS CloudWatch  
**Visualization:** Streamlit  
**CI/CD:** GitHub Actions  

---

## 🧱 Medallion Architecture

      +-----------------------+
      |     Streamlit App     |
      |   (Visualization)     |
      +----------▲------------+
                 │
       +---------+----------+
       |      GOLD Layer     |
       |  Aggregated tables  |
       +---------▲-----------+
                 │
       +---------+----------+
       |     SILVER Layer    |
       |  Cleaned & joined   |
       | normalized data     |
       +---------▲-----------+
                 │
       +---------+----------+
       |     BRONZE Layer    |
       |  Raw JSON from APIs |
       | (Calendly + Spend)  |
       +---------------------+

---

## 🖼️ Pipeline Diagram

Below is a placeholder where you can upload your finalized **architecture diagram** (from Draw.io, Miro, or Lucidchart).

```markdown
![Pipeline Architecture Diagram](docs/pipeline_architecture.png)


📊 Business Metrics

1. Daily Calls by Source
Count of bookings per day, per source
→ Visualized with a line chart

2. Cost per Booking (CPB)
Spend ÷ Bookings per channel
→ Visualized with bar chart and KPI tiles

3. Bookings Trend Over Time
Tracks booking activity and trends
→ Visualized with line or area chart

4. Channel Attribution
Rank channels by volume and cost per booking
→ Visualized with leaderboard and heatmap

5. Booking Volume by Time Slot / Day of Week
Analyzes booking behavior by hour and weekday
→ Visualized with heatmap and histogram

6. Meeting Load per Employee
Calculates average meetings per week per employee
→ Visualized with bar and line charts

Repository Structure
calendly-marketing-pipeline/
│
├── app/                    # Streamlit app & dashboard
├── docs/                   # Documentation & diagrams
├── infrastructure/          # IaC (e.g., IAM roles, bucket setup)
├── jobs/                   # Glue job scripts
├── lambda_src/             # Lambda webhook handlers
├── schemas/                # JSON schema definitions
├── scripts/                # Local ETL orchestration utilities
├── tests/                  # Validation & QA scripts
│
├── deploy_glue_jobs.py     # CI/CD deployment script
├── requirements.txt         # Python dependencies
├── README.md                # Project overview
└── test_connections.py      # AWS connection test

How to Run Locally
# 1. Clone the repository
git clone https://github.com/elupovit/calendly-marketing-pipeline.git
cd calendly-marketing-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the Streamlit dashboard
streamlit run app/dashboard.py

📈 Example Visuals

The Streamlit dashboard includes:

Cost per Booking (CPB) by Channel

Daily Bookings Trend

Channel Attribution Leaderboard

Booking Heatmap (time vs day)

🔒 Security

All data stored with SSE-S3 encryption

AWS IAM roles follow least privilege principle

Credentials managed securely via AWS Secrets Manager

🧾 Deliverables

AWS pipeline setup (Lambda + Glue + S3 + Athena)

Bronze/Silver/Gold Delta tables

Streamlit dashboard visualizing KPIs

CI/CD automation via GitHub Actions

👨‍💻 Author

Eitan Lupovitch
Data Engineer | Analytics Consultant
📧 elup94@gmail.com

📂 GitHub: elupovit
