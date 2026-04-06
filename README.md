# 📊 Spare Parts Sales Analysis | SQL Project

## 🔍 Project Overview
This project analyzes multi-branch spare parts and service sales data to evaluate revenue performance, identify business trends, and uncover key drivers of revenue.

The goal is to transform raw ERP sales data into meaningful business insights and actionable recommendations that support better decision-making.

---

## 💼 Business Problem
The business operates across multiple branches but lacks clear visibility into:

- Which branches drive the most revenue  
- Whether revenue is growing or stagnating  
- The contribution of services vs spare parts  
- Opportunities to increase revenue and efficiency  

This analysis aims to bridge that gap using data-driven insights.

---

## 🗂️ Dataset Description
The dataset contains transactional sales data from multiple branches, including:

- Branch information  
- Invoice-level transactions  
- Item details (spare parts & services)  
- Revenue and pricing data  
- Dates and customer information  

The raw data required cleaning due to:
- Missing values  
- Inconsistent formats  
- Presence of non-transactional summary rows  

---

## 🧹 Data Cleaning & Preparation
To ensure reliable analysis:

- Combined multiple branch datasets into a unified structure  
- Converted date and numeric fields into proper formats  
- Handled missing branch values (labeled as *Counter Sale*)  
- Removed invalid and system-generated summary rows (e.g., “Total”)  
- Created a derived category:
  - **Service**
  - **Spare Parts**

This resulted in a clean, analysis-ready dataset.

---

## 📌 Key Findings

1. **Branch Performance**
   - Palayam generates the highest revenue (~₹7.6M), followed by Muttathara (~₹5.1M).
   - Counter sales contribute a smaller but meaningful share (~₹1.7M).

2. **Revenue Trend**
   - Revenue remains relatively stable across months, indicating consistent demand but limited growth.

3. **Revenue Distribution**
   - Spare parts contribute the majority of revenue (~75%), while services contribute ~25%.

4. **Business Model**
   - The business operates on a high-volume, mid-value model with a large number of invoices and moderate average transaction value.

---

## 🧠 Key Insights

### 1. Branch Performance & Risk
Revenue is unevenly distributed across branches, with strong dependence on top-performing locations.  
This creates operational risk if performance declines in key branches.

---

### 2. Revenue Stability & Growth Limitation
Stable monthly revenue indicates consistent demand but also suggests a lack of growth strategy or expansion efforts.

---

### 3. Revenue Opportunity (Service Segment)
Services generate lower revenue despite similar transaction volumes, highlighting an opportunity to improve pricing, bundling, or upselling strategies.

---

### 4. Monetization Opportunity
The current high-volume model indicates strong customer flow, but revenue can be increased by improving average transaction value.

---

## 🚀 Business Recommendations

### 1. Optimize Branch Performance
- Replicate successful strategies from top-performing branches (Palayam)  
- Implement branch-level performance tracking and targets  

---

### 2. Drive Revenue Growth
- Launch targeted marketing campaigns during slower periods  
- Use historical sales data to plan seasonal promotions  

---

### 3. Increase Service Revenue
- Bundle services with spare parts  
- Introduce premium service packages  
- Promote preventive maintenance services  

---

### 4. Improve Revenue Per Transaction
- Implement upselling and cross-selling strategies  
- Review pricing for high-value invoices  
- Focus on increasing average order value  

---

## ✅ Conclusion
This analysis highlights key revenue drivers, operational risks, and growth opportunities within the business.

By improving service monetization, optimizing branch performance, and increasing transaction value, the business can significantly enhance overall revenue and profitability.

“This project demonstrates my ability to clean raw business data, identify hidden issues, and deliver actionable insights.”
---

## 🛠️ Tools Used
- SQL (MySQL)
- Data Cleaning & Transformation
- Business Analysis

---

## 📬 Author
HAFSA R ~
Aspiring Data Analyst focused on solving real-world business problems using data.

