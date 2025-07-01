import streamlit as st
import pandas as pd
import mysql.connector
from streamlit_option_menu import option_menu
import plotly.express as px
import requests
from functools import lru_cache
import json
import os

# Set Streamlit-configuration
st.set_page_config(layout="wide", page_title="PhonePe Pulse Data Visualization")

#custom CSS
st.markdown("""
    <style>
    body {
        background-color: #1a0d3d;
        color: white;
    }
    .stApp {
        background-color: #1a0d3d;
        color: white;
    }
    .stSelectbox, .stButton, .stMarkdown, .stText {
        color: white !important;
        background-color: #2b1a5e !important;
        border-radius: 10px;
    }
    .stSelectbox div[data-baseweb="select"] > div {
        background-color: #2b1a5e;
        color: white;
    }
    h1, h2, h3, h4, h5, h6 {
        color: #e6e6e6 !important;
    }
    .css-1d391kg, .css-1v0mbdj, .css-1v3fvcr {
        color: white !important;
    }
    .css-1v0mbdj a {
        color: #e6e6e6 !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #2b1a5e;
        color: white;
        border-radius: 10px;
        padding: 5px 15px;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background-color: #f5c518;
        color: black;
    }
    .stSpinner > div {
        border-color: #f5c518 !important;
        border-right-color: transparent !important;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_data
def load_all_data():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="phonepe_new"
        )
        queries = {
            "aggregated_transaction": "SELECT * FROM aggregated_transaction",
            "map_transaction": "SELECT * FROM map_transaction",
            "map_user": "SELECT * FROM map_user",
            "top_transaction": "SELECT * FROM top_transaction",
            "top_user": "SELECT * FROM top_user"
        }
        data = {}
        for table, query in queries.items():
            df = pd.read_sql(query, conn)
            data[table] = df
        conn.close()
        return data
    except Exception as e:
        st.error(f"Error fetching data from database: {e}")
        st.stop()

#Cache GeoJSON data locally to avoid repeated network requests
@st.cache_data
def load_state_geojson():
    local_file = "india_states.geojson"
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
#local file exists
    if os.path.exists(local_file):
        try:
            with open(local_file, "r") as f:
                return json.load(f)
        except Exception as e:
            st.warning(f"Failed to load local state GeoJSON: {e}. Attempting to fetch from URL.")
    
#URL if local file doesn't exist or fails
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
#Save to local file for future use
        with open(local_file, "w") as f:
            json.dump(data, f)
        return data
    except Exception as e:
        st.error(f"Failed to load state GeoJSON: {e}. Please provide a local GeoJSON file.")
        return None

@st.cache_data
def load_district_geojson():
    local_file = "india_district.geojson"
    url = "https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson"
    

    if os.path.exists(local_file):
        try:
            with open(local_file, "r") as f:
                return json.load(f)
        except Exception as e:
            st.warning(f"Failed to load local district GeoJSON: {e}. Attempting to fetch from URL.")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
   
        with open(local_file, "w") as f:
            json.dump(data, f)
        return data
    except Exception as e:
        st.error(f"Failed to load district GeoJSON: {e}. Please provide a local GeoJSON file.")
        return None

# Pre-aggregate-faster access
@st.cache_data
def pre_aggregate_data(data):
    Aggre_transaction = data["aggregated_transaction"]
    Map_transaction = data["map_transaction"]
    Map_user = data["map_user"]
    Top_transaction = data["top_transaction"]
    Top_user = data["top_user"]

#Statemapping_standardize names
    state_mapping = {
        "Andaman & Nicobar": "Andaman and Nicobar Islands",
        "Dadra and Nagar Haveli and Daman and Diu": "Dadra and Nagar Haveli",
        "Jammu & Kashmir": "Jammu and Kashmir",
        "Delhi": "NCT of Delhi",
        "India": "All India"
    }
#apply mapping
    for df in [Aggre_transaction, Map_transaction, Map_user, Top_transaction, Top_user]:
        df["States"] = df["States"].str.strip().str.title()
        df["States"] = df["States"].replace(state_mapping)

  
    agg_transaction_dict = {}
    map_transaction_state_dict = {}
    map_transaction_district_dict = {}
    top_transaction_dict = {}


    map_user_state_dict = {}
    map_user_district_dict = {}
    top_user_dict = {}

    years = sorted(Map_transaction["Years"].unique())
    quarters = sorted(Map_transaction["Quarter"].unique())
    states = sorted(Map_transaction["States"].unique())


    agg_txn_grouped = Aggre_transaction.groupby(["Years", "Quarter", "States", "Transaction_type"])["Transaction_count"].sum().reset_index()
    categories_by_year_quarter = {}
    for year in years:
        for quarter in quarters:
            agg_txn_subset = agg_txn_grouped[(agg_txn_grouped["Years"] == year) & (agg_txn_grouped["Quarter"] == quarter) & (agg_txn_grouped["States"] == "All India")]
            categories_by_year_quarter[(year, quarter)] = agg_txn_subset.groupby("Transaction_type")["Transaction_count"].sum().to_dict()


    map_txn_grouped = Map_transaction.groupby(["Years", "Quarter", "States", "Districts"])[["Transaction_count", "Transaction_amount"]].sum().reset_index()
    map_usr_grouped = Map_user.groupby(["Years", "Quarter", "States", "Districts"])[["RegisteredUser", "AppOpens"]].sum().reset_index()
    top_txn_grouped = Top_transaction.groupby(["Years", "Quarter", "States", "Pincodes"])["Transaction_count"].sum().reset_index()
    top_usr_grouped = Top_user.groupby(["Years", "Quarter", "States", "Pincodes"])["RegisteredUser"].sum().reset_index()

    for year in years:
        for quarter in quarters:
            map_txn = map_txn_grouped[(map_txn_grouped["Years"] == year) & (map_txn_grouped["Quarter"] == quarter)]
            map_usr = map_usr_grouped[(map_usr_grouped["Years"] == year) & (map_usr_grouped["Quarter"] == quarter)]
            top_txn = top_txn_grouped[(top_txn_grouped["Years"] == year) & (top_txn_grouped["Quarter"] == quarter)]
            top_usr = top_usr_grouped[(top_usr_grouped["Years"] == year) & (top_usr_grouped["Quarter"] == quarter)]

          
            categories_data = categories_by_year_quarter.get((year, quarter), {})

            for state in states:
                key = (year, quarter, state)
                agg_transaction_dict[key] = categories_data

                map_txn_state = map_txn[map_txn["States"] == state]
                map_transaction_state_dict[key] = {
                    "Transaction_count": map_txn_state["Transaction_count"].sum(),
                    "Transaction_amount": map_txn_state["Transaction_amount"].sum()
                }
                map_transaction_district_dict[key] = map_txn_state[["Districts", "Transaction_count", "Transaction_amount"]]

       
                map_usr_state = map_usr[map_usr["States"] == state]
                map_user_state_dict[key] = {
                    "RegisteredUser": map_usr_state["RegisteredUser"].sum(),
                    "AppOpens": map_usr_state["AppOpens"].sum()
                }
                map_user_district_dict[key] = map_usr_state[["Districts", "RegisteredUser", "AppOpens"]]

             
                top_txn_state = top_txn[top_txn["States"] == state]
                top_transaction_dict[key] = top_txn_state[["Pincodes", "Transaction_count"]]

            
                top_usr_state = top_usr[top_usr["States"] == state]
                top_user_dict[key] = top_usr_state[["Pincodes", "RegisteredUser"]]

            key = (year, quarter, "All India")
            agg_transaction_dict[key] = categories_data
            map_transaction_state_dict[key] = map_txn.groupby("States")[["Transaction_count", "Transaction_amount"]].sum().reset_index()
            map_transaction_district_dict[key] = map_txn[["Districts", "Transaction_count", "Transaction_amount"]]
            map_user_state_dict[key] = map_usr.groupby("States")[["RegisteredUser", "AppOpens"]].sum().reset_index()
            map_user_district_dict[key] = map_usr[["Districts", "RegisteredUser", "AppOpens"]]
            top_transaction_dict[key] = top_txn[["Pincodes", "Transaction_count"]]
            top_user_dict[key] = top_usr[["Pincodes", "RegisteredUser"]]

    return {
        "agg_transaction_dict": agg_transaction_dict,
        "map_transaction_state_dict": map_transaction_state_dict,
        "map_transaction_district_dict": map_transaction_district_dict,
        "map_user_state_dict": map_user_state_dict,
        "map_user_district_dict": map_user_district_dict,
        "top_transaction_dict": top_transaction_dict,
        "top_user_dict": top_user_dict,
        "years": years,
        "quarters": quarters,
        "states": states
    }

#GeoJSON for each state
@st.cache_data
def pre_filter_district_geojson(district_geojson, states):
    if district_geojson is None:
        return None
    filtered_geojson = {}
    for state in states:
        if state == "All India":
            filtered_geojson[state] = district_geojson
            continue
        filtered_features = [
            feature for feature in district_geojson["features"]
            if feature["properties"]["NAME_1"] == state
        ]
        filtered_geojson[state] = {
            "type": "FeatureCollection",
            "features": filtered_features
        }
    return filtered_geojson


all_data = load_all_data()
pre_agg_data = pre_aggregate_data(all_data)


agg_transaction_dict = pre_agg_data["agg_transaction_dict"]
map_transaction_state_dict = pre_agg_data["map_transaction_state_dict"]
map_transaction_district_dict = pre_agg_data["map_transaction_district_dict"]
map_user_state_dict = pre_agg_data["map_user_state_dict"]
map_user_district_dict = pre_agg_data["map_user_district_dict"]
top_transaction_dict = pre_agg_data["top_transaction_dict"]
top_user_dict = pre_agg_data["top_user_dict"]
years = pre_agg_data["years"]
quarters = pre_agg_data["quarters"]
states = pre_agg_data["states"]
states.insert(0, "All India")


state_geojson = load_state_geojson()
district_geojson = load_district_geojson()
filtered_district_geojson = pre_filter_district_geojson(district_geojson, states)

if state_geojson is None or filtered_district_geojson is None:
    st.error("Cannot proceed without GeoJSON data. Please check the URLs or provide local GeoJSON files.")
    st.stop()

with st.container():
    col1, col2, col3 = st.columns([1, 3, 1])
    with col1:
        st.image("https://www.phonepe.com/pulse/static/pulse-logo-white.png", width=150)
    with col2:
        selected = option_menu(
            menu_title=None,
            options=["EXPLORE DATA"],
            icons=["bar-chart"],
            menu_icon="cast",
            default_index=0,
            orientation="horizontal",
            styles={
                "container": {"background-color": "#1a0d3d", "color": "white"},
                "nav-link": {"color": "white", "--hover-color": "#e6e6e6"},
                "nav-link-selected": {"background-color": "#f5c518", "color": "black"}
            }
        )
    with col3:
        st.write("") 


with st.sidebar:
    st.header("Filters")
    category = st.selectbox("Category", ["Transactions", "Users"])
    selected_state = st.selectbox("Region", states)
    selected_year = st.selectbox("Year", years)
    selected_quarter = st.selectbox("Quarter", quarters)


st.header(category)

with st.spinner("Loading map..."):
    if category == "Transactions":
        key = (selected_year, selected_quarter, selected_state)
        if selected_state == "All India":
            df = map_transaction_state_dict[key]
            
            total_transactions = df["Transaction_count"].sum()
            total_amount = df["Transaction_amount"].sum()
            avg_transaction = total_amount / total_transactions if total_transactions > 0 else 0

            fig_map = px.choropleth(
                df,
                geojson=state_geojson,
                locations="States",
                featureidkey="properties.ST_NM",
                color="Transaction_count",
                color_continuous_scale="Reds",
                hover_name="States",
                hover_data={"Transaction_count": ":,", "Transaction_amount": ":,"},
                title=f"Transaction Count by State (Q{selected_quarter} {selected_year})",
                height=600
            )
            fig_map.update_geos(visible=False, fitbounds="locations")
            fig_map.update_layout(
                paper_bgcolor="#1a0d3d",
                plot_bgcolor="#1a0d3d",
                font=dict(color="white"),
                margin=dict(l=0, r=0, t=50, b=0)
            )
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            df = map_transaction_district_dict[key]
            
            total_transactions = df["Transaction_count"].sum()
            total_amount = df["Transaction_amount"].sum()
            avg_transaction = total_amount / total_transactions if total_transactions > 0 else 0

            df["Districts"] = df["Districts"].str.title()
            fig_map = px.choropleth(
                df,
                geojson=filtered_district_geojson[selected_state],
                locations="Districts",
                featureidkey="properties.NAME_2",
                color="Transaction_count",
                color_continuous_scale="Reds",
                hover_name="Districts",
                hover_data={"Transaction_count": ":,", "Transaction_amount": ":,"},
                title=f"Transaction Count in {selected_state} (Q{selected_quarter} {selected_year})",
                height=600
            )
            fig_map.update_geos(visible=False, fitbounds="locations")
            fig_map.update_layout(
                paper_bgcolor="#1a0d3d",
                plot_bgcolor="#1a0d3d",
                font=dict(color="white"),
                margin=dict(l=0, r=0, t=50, b=0)
            )
            st.plotly_chart(fig_map, use_container_width=True)

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.subheader("All PhonePe transactions (UPI + Cards + Wallets)")
            st.markdown(f"<h1 style='color: #e6e6e6;'>{total_transactions:,}</h1>", unsafe_allow_html=True)
        with col2:
            st.subheader("Total payment value")
            st.markdown(f"<h3 style='color: #e6e6e6;'>₹{total_amount/10000000:,.0f} Cr</h3>", unsafe_allow_html=True)
        with col3:
            st.subheader("Avg. transaction value")
            st.markdown(f"<h3 style='color: #e6e6e6;'>₹{avg_transaction:,.0f}</h3>", unsafe_allow_html=True)

        
        st.subheader("Categories")
        categories_data = agg_transaction_dict[key]
        
        if not categories_data:
            st.warning(f"No transaction data available for Year {selected_year}, Quarter {selected_quarter}, State {selected_state}.")
        else:
            categories_dict = {
                "Merchant payments": 0,
                "Peer-to-peer payments": 0,
                "Recharge & bill payments": 0,
                "Financial Services": 0,
                "Others": 0
            }
            for transaction_type, count in categories_data.items():
                transaction_type_lower = transaction_type.lower()
                if "merchant" in transaction_type_lower:
                    categories_dict["Merchant payments"] += count
                elif "p2p" in transaction_type_lower or "peer-to-peer" in transaction_type_lower:
                    categories_dict["Peer-to-peer payments"] += count
                elif "recharge" in transaction_type_lower or "bill" in transaction_type_lower:
                    categories_dict["Recharge & bill payments"] += count
                elif "financial" in transaction_type_lower:
                    categories_dict["Financial Services"] += count
                else:
                    categories_dict["Others"] += count

            for key, value in categories_dict.items():
                st.markdown(f"{key}: {value:,}")

    else: 
        key = (selected_year, selected_quarter, selected_state)
        if selected_state == "All India":
            df = map_user_state_dict[key]
            
            total_users = df["RegisteredUser"].sum()
            total_app_opens = df["AppOpens"].sum()

            fig_map = px.choropleth(
                df,
                geojson=state_geojson,
                locations="States",
                featureidkey="properties.ST_NM",
                color="RegisteredUser",
                color_continuous_scale="Reds",
                hover_name="States",
                hover_data={"RegisteredUser": ":,", "AppOpens": ":,"},
                title=f"Registered Users by State (Q{selected_quarter} {selected_year})",
                height=600
            )
            fig_map.update_geos(visible=False, fitbounds="locations")
            fig_map.update_layout(
                paper_bgcolor="#1a0d3d",
                plot_bgcolor="#1a0d3d",
                font=dict(color="white"),
                margin=dict(l=0, r=0, t=50, b=0)
            )
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            df = map_user_district_dict[key]
            
            total_users = df["RegisteredUser"].sum()
            total_app_opens = df["AppOpens"].sum()

            df["Districts"] = df["Districts"].str.title()
            fig_map = px.choropleth(
                df,
                geojson=filtered_district_geojson[selected_state],
                locations="Districts",
                featureidkey="properties.NAME_2",
                color="RegisteredUser",
                color_continuous_scale="Reds",
                hover_name="Districts",
                hover_data={"RegisteredUser": ":,", "AppOpens": ":,"},
                title=f"Registered Users in {selected_state} (Q{selected_quarter} {selected_year})",
                height=600
            )
            fig_map.update_geos(visible=False, fitbounds="locations")
            fig_map.update_layout(
                paper_bgcolor="#1a0d3d",
                plot_bgcolor="#1a0d3d",
                font=dict(color="white"),
                margin=dict(l=0, r=0, t=50, b=0)
            )
            st.plotly_chart(fig_map, use_container_width=True)

     
        col1, col2 = st.columns([1, 1])
        with col1:
            st.subheader(f"Registered PhonePe users till Q{selected_quarter} {selected_year}")
            st.markdown(f"<h1 style='color: #e6e6e6;'>{total_users:,}</h1>", unsafe_allow_html=True)
        with col2:
            st.subheader(f"PhonePe app opens in Q{selected_quarter} {selected_year}")
            st.markdown(f"<h1 style='color: #e6e6e6;'>{total_app_opens:,}</h1>", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["States", "Districts", "Postal Codes"])

if category == "Transactions":
    with tab1:
        st.subheader("Top 10 States")
        key = (selected_year, selected_quarter, "All India")
        df = map_transaction_state_dict[key]
        top_states = df.sort_values("Transaction_count", ascending=False).head(10)
        for i, row in top_states.iterrows():
            st.markdown(f"{i+1}. {row['States']}: {row['Transaction_count']/10000000:,.2f}Cr")

    with tab2:
        st.subheader("Top 10 Districts")
        key = (selected_year, selected_quarter, selected_state)
        df = map_transaction_district_dict[key]
        top_districts = df.sort_values("Transaction_count", ascending=False).head(10)
        for i, row in top_districts.iterrows():
            st.markdown(f"{i+1}. {row['Districts']}: {row['Transaction_count']/100000:,.2f}L")

    with tab3:
        st.subheader("Top 10 Postal Codes")
        key = (selected_year, selected_quarter, selected_state)
        df = top_transaction_dict[key]
        top_pincodes = df.sort_values("Transaction_count", ascending=False).head(10)
        for i, row in top_pincodes.iterrows():
            if pd.notna(row['Pincodes']):
                st.markdown(f"{i+1}. {row['Pincodes']}: {row['Transaction_count']/100000:,.2f}L")
else: 
    with tab1:
        st.subheader("Top 10 States")
        key = (selected_year, selected_quarter, "All India")
        df = map_user_state_dict[key]
        top_states = df.sort_values("RegisteredUser", ascending=False).head(10)
        for i, row in top_states.iterrows():
            st.markdown(f"{i+1}. {row['States']}: {row['RegisteredUser']/10000000:,.2f}Cr")

    with tab2:
        st.subheader("Top 10 Districts")
        key = (selected_year, selected_quarter, selected_state)
        df = map_user_district_dict[key]
        top_districts = df.sort_values("RegisteredUser", ascending=False).head(10)
        for i, row in top_districts.iterrows():
            st.markdown(f"{i+1}. {row['Districts']}: {row['RegisteredUser']/100000:,.2f}L")

    with tab3:
        st.subheader("Top 10 Postal Codes")
        key = (selected_year, selected_quarter, selected_state)
        df = top_user_dict[key]
        top_pincodes = df.sort_values("RegisteredUser", ascending=False).head(10)
        for i, row in top_pincodes.iterrows():
            if pd.notna(row['Pincodes']):
                st.markdown(f"{i+1}. {row['Pincodes']}: {row['RegisteredUser']/100000:,.2f}L")


st.markdown("""
    <div style='text-align: center; margin-top: 20px;'>
        <button style='background-color: transparent; color: #00ddeb; border: 2px solid #00ddeb; padding: 10px 20px; border-radius: 20px;'>
            EXPLORE SPECIAL DATA POINTS
        </button>
    </div>
""", unsafe_allow_html=True)
