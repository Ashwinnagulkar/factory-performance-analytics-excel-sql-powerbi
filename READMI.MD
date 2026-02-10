# 📊 Workforce Performance Intelligence Dashboard

🚀 **End-to-End Data Analytics Project | Excel → SQL → Power BI**

This project is a comprehensive **Workforce Productivity Analytics Solution** designed to monitor, analyze, and optimize factory workforce performance. It transforms raw operational data into **actionable business intelligence** that supports management decisions in productivity improvement, workforce stability, and leadership effectiveness.

---

## 🧠 Problem Statement

Factory management lacked clear visibility into productivity trends, workforce performance gaps, supervisor impact, and employee attrition causes. Additionally, inconsistencies in event data recording made performance evaluation difficult.

The goal of this project was to build a structured analytics system that identifies operational inefficiencies and supports data-driven workforce improvement decisions.

---

## 🧠 Project Objective

To identify:
- Productivity trends
- Workforce performance gaps
- Leadership effectiveness
- Training needs
- Retention risk factors

And convert them into **strategic improvement actions** using data.

---

## 🔄 Data Analytics Workflow

This project follows a **complete industry-style data pipeline**:

### 🟢 Step 1 — Data Cleaning & Preparation (Excel)
- Cleaned raw factory logs  
- Handled missing values  
- Standardized categorical fields  
- Used **Pivot Tables** for initial pattern analysis  
- Verified consistency before database import  

### 🔵 Step 2 — Data Modeling (SQL)
Data was imported into SQL and structured into a **Star Schema**:

**Fact Table**
- `factory_fact_worker` → Productivity & behavior records

**Dimension Tables**
- `factory_dim_emp` → Employee details  
- `factory_dim_sup` → Supervisor details  
- `dim_date` → Dedicated date dimension table for time-based analysis ✅  

This structure improved:
✔ Query performance  
✔ Analytical clarity  
✔ BI tool efficiency  

### 🟣 Step 3 — Data Visualization (Power BI)
The modeled data was imported into Power BI to build an **interactive 3-page intelligence dashboard**.

---

# 📊 KPI Metrics

- **Total Employees**
- **Average Actual Efficiency**
- **Average Recorded Efficiency**
- **Efficiency Gap (Recorded vs Actual Efficiency)**

---

# 📈 Chart Requirements Implemented

- KPI Cards  
- Monthly Efficiency Trend Chart  
- Weekly Performance Chart  
- Gender Distribution Chart  
- Age Group Performance Chart  
- Team Performance Treemap Charts  
- Top 5 & Bottom 5 Employees  
- Top 5 & Bottom 5 Supervisors  
- Efficiency Variance Comparison  

---

# 🛠 Dashboard Pages & Business Insights

---

## 1️⃣ Executive Overview Dashboard  
**Focus:** Operational Productivity Trends  

### Key Insights
- Mid-year productivity dip (May–July)
- Monday–Wednesday performance lag
- Saturday peak efficiency (~80%)
- Workforce Gender Split: **Male 47.74% | Female 52.26%**

### Business Impact
📈 Proposed seasonal recovery strategy to boost lean-month productivity by **10%**

---

## 2️⃣ Workforce Analytics Center  
**Focus:** Workforce Stability & Demographic Performance  

### Key Insights
- **Under-25 employees show lower efficiency because many are new hires**
- Employee resignation major reason: **Low Commitment**
- **192,000+ events marked as “Not Checked”** indicating data quality gaps
- Under-25 group performance gap (56%) vs senior employees (74%)
- Team 3 identified as high-risk performance zone

### Business Impact
👥 Suggested leadership coaching, onboarding improvement, and employee engagement programs to reduce exits and improve team morale.

---

## 3️⃣ Leadership Performance Dashboard  
**Focus:** Individual Benchmarking & Training Needs  

### Key Insights
- Top Performer: **Harold Johansen (1.40 Efficiency)**
- Training Priority: **Emily Castellano (0.16 Efficiency)**
- **Top & Bottom Employees identified for performance benchmarking**
- Identified mentor candidates among high-performing supervisors

### Business Impact
🎯 Created mentorship framework to reduce efficiency variance and uplift bottom performers.

---

# 📈 Technical Stack

| Tool | Purpose |
|------|--------|
| **Excel** | Data cleaning & exploratory analysis |
| **SQL** | Data modeling (Fact & Dimension tables) |
| **Power BI** | Dashboard & visual intelligence |
| **DAX** | Performance metrics calculation |

---

# 💡 Strategic Recommendations

✔ 8-week training program for Bottom 5 workers  
✔ Standardize behavior logging (reduce "None" entries)  
✔ Use top performers as internal mentors  
✔ Focus retention strategies on high-risk teams  
✔ Improve early-week productivity planning  

---
# 📁 Project Folder Structure

```
factory/
│
├── data/
│   ├── factory.csv
│   └── factory.xlsx
│
├── sql/
│   └── factory.sql
│
├── dashboard/
│   └── factory.pbix
│
└── images/
    ├── Executive Overview Dashboard.png
    ├── Workforce Analytics Center.png
    └── Leadership Performance Dashboard.png
```
--- 
# 🎯 Skills Demonstrated

- Data Cleaning  
- Data Modeling (Star Schema)  
- SQL Data Structuring  
- DAX Calculations  
- KPI Design  
- Business Insight Generation  
- Dashboard Storytelling  
- Decision Support Analytics  

---


 
# 📸 Dashboard Screenshots

## Executive Overview Dashboard
![Executive Overview](images/Executive Overview Dashboard.png)

## Workforce Analytics Center
![Workforce Analytics](images/Workforce Analytics Center.png)

## Leadership Performance Dashboard
![Leadership Performance](images/Leadership Performance Dashboard.png)

---

## 👨‍💻 Author

**Ashwin Nagulkar**  
Data Analysis | EXCEL | SQL | Power BI Enthusiast


---

## ⭐ Project Outcome

This dashboard transforms factory data into a **management decision system**, enabling:

📈 Productivity optimization  
👥 Workforce stability planning  
🎯 Leadership performance tracking  

---
