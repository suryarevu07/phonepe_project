# 📱 PhonePe Pulse Data Visualization

## 🧾 Overview

This is a Streamlit-based interactive web application that visualizes PhonePe transaction and user data across India. It uses **MySQL** as the backend to store preprocessed data and **GeoJSON** maps to show regional insights with charts, rankings, and metrics.

---

## 🚀 Features

- 📍 **Interactive Maps**: Choropleth maps to visualize transactions and user data across states and districts.
- 📊 **Metrics**: Total transactions, payment value, average transaction value, registered users, app opens.
- 🔍 **Filters**: Filter by Transaction/User data, Year, Quarter, State.
- 🏆 **Top 10 Rankings**: Shows top states, districts, and pincodes.
- 💡 **Dark Mode UI**: Modern, clean, and responsive layout.
- 🗺️ **Explore Special Data Points** (placeholder for custom insights).

---

## 📦 Tech Stack

- **Frontend**: Streamlit, Plotly, Tailwind CSS (via custom CSS)
- **Backend**: MySQL
- **Programming Language**: Python 3.8+
- **Data Format**: CSV & GeoJSON

---

## 📁 Folder Structure

phonepe_app/
│
├── app.py # Streamlit app entry point
├── database.py # (if separated DB logic)
├── queries.py # SQL queries
├── helper.py # Utility functions
├── requirements.txt # Required Python packages
└── README.md # Project documentation  



---

## 🗃️ Database Setup

1. Install MySQL and create a database:

```sql
CREATE DATABASE phonepe_new;
### Import the required CSV files as tables into phonepe_new. Tables needed:

aggregated_transaction

map_transaction

map_user

top_transaction

top_user

streamlit
pandas
mysql-connector-python
streamlit-option-menu
plotly-express
requests
 
