# Paths
base_path = r"D:\pulse-master (1)new\pulse-master\data"

# Aggregated Insurance
path7 = os.path.join(base_path, "aggregated/insurance/country/india/state/")
agg_insur_list = os.listdir(path7)
columns7 = {"States": [], "Years": [], "Quarter": [], "Insurance_type": [], "Insurance_count": [], "Insurance_amount": []}

for state in agg_insur_list:
    cur_states = os.path.join(path7, state, "")
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = os.path.join(cur_states, year, "")
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = os.path.join(cur_years, file)
            with open(cur_files, "r") as data:
                A = json.load(data)

            for i in A["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns7["Insurance_type"].append(name)
                columns7["Insurance_count"].append(count)
                columns7["Insurance_amount"].append(amount)
                columns7["States"].append(state)
                columns7["Years"].append(year)
                columns7["Quarter"].append(int(file.strip(".json")))

aggre_insurance = pd.DataFrame(columns7)
aggre_insurance["States"] = aggre_insurance["States"].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
aggre_insurance["States"] = aggre_insurance["States"].str.replace("-", " ")
aggre_insurance["States"] = aggre_insurance["States"].str.title()
aggre_insurance['States'] = aggre_insurance['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Aggregated Transaction
path1 = os.path.join(base_path, "aggregated/transaction/country/india/state/")
agg_tran_list = os.listdir(path1)
columns1 = {"States": [], "Years": [], "Quarter": [], "Transaction_type": [], "Transaction_count": [], "Transaction_amount": []}

for state in agg_tran_list:
    cur_states = os.path.join(path1, state, "")
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = os.path.join(cur_states, year, "")
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = os.path.join(cur_years, file)
            with open(cur_files, "r") as data:
                B = json.load(data)

            for i in B["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns1["Transaction_type"].append(name)
                columns1["Transaction_count"].append(count)
                columns1["Transaction_amount"].append(amount)
                columns1["States"].append(state)
                columns1["Years"].append(year)
                columns1["Quarter"].append(int(file.strip(".json")))

aggre_transaction = pd.DataFrame(columns1)
aggre_transaction["States"] = aggre_transaction["States"].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
aggre_transaction["States"] = aggre_transaction["States"].str.replace("-", " ")
aggre_transaction["States"] = aggre_transaction["States"].str.title()
aggre_transaction['States'] = aggre_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Aggregated User
path2 = os.path.join(base_path, "aggregated/user/country/india/state/")
agg_user_list = os.listdir(path2)
columns2 = {"States": [], "Years": [], "Quarter": [], "Brands": [], "Transaction_count": [], "Percentage": []}

for state in agg_user_list:
    cur_states = os.path.join(path2, state, "")
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = os.path.join(cur_states, year, "")
        agg_file_list = os.listdir(cur_years)
        
        for file in agg_file_list:
            cur_files = os.path.join(cur_years, file)
            with open(cur_files, "r") as data:
                C = json.load(data)

            try:
                for i in C["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    columns2["Brands"].append(brand)
                    columns2["Transaction_count"].append(count)
                    columns2["Percentage"].append(percentage)
                    columns2["States"].append(state)
                    columns2["Years"].append(year)
                    columns2["Quarter"].append(int(file.strip(".json")))
            except:
                pass

aggre_user = pd.DataFrame(columns2)
aggre_user["States"] = aggre_user["States"].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
aggre_user["States"] = aggre_user["States"].str.replace("-", " ")
aggre_user["States"] = aggre_user["States"].str.title()
aggre_user['States'] = aggre_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


# Map Insurance
path8 = os.path.join(base_path, "map/insurance/hover/country/india/state/")
map_insur_list = os.listdir(path8)
columns8 = {"States": [], "Years": [], "Quarter": [], "Districts": [], "Transaction_count": [], "Transaction_amount": []}

for state in map_insur_list:
    cur_states = os.path.join(path8, state, "")
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = os.path.join(cur_states, year, "")
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = os.path.join(cur_years, file)
            with open(cur_files, "r") as data:
                D = json.load(data)

            for i in D["data"]["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns8["Districts"].append(name)
                columns8["Transaction_count"].append(count)
                columns8["Transaction_amount"].append(amount)
                columns8["States"].append(state)
                columns8["Years"].append(year)
                columns8["Quarter"].append(int(file.strip(".json")))

map_insurance = pd.DataFrame(columns8)
map_insurance["States"] = map_insurance["States"].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
map_insurance["States"] = map_insurance["States"].str.replace("-", " ")
map_insurance["States"] = map_insurance["States"].str.title()
map_insurance['States'] = map_insurance['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Map Transaction
path3 = os.path.join(base_path, "map/transaction/hover/country/india/state/")
map_tran_list = os.listdir(path3)
columns3 = {"States": [], "Years": [], "Quarter": [], "District": [], "Transaction_count": [], "Transaction_amount": []}

for state in map_tran_list:
    cur_states = os.path.join(path3, state, "")
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = os.path.join(cur_states, year, "")
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = os.path.join(cur_years, file)
            with open(cur_files, "r") as data:
                E = json.load(data)

            for i in E['data']["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns3["District"].append(name)
                columns3["Transaction_count"].append(count)
                columns3["Transaction_amount"].append(amount)
                columns3["States"].append(state)
                columns3["Years"].append(year)
                columns3["Quarter"].append(int(file.strip(".json")))

map_transaction = pd.DataFrame(columns3)
map_transaction["States"] = map_transaction["States"].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
map_transaction["States"] = map_transaction["States"].str.replace("-", " ")
map_transaction["States"] = map_transaction["States"].str.title()
map_transaction['States'] = map_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Map User
path4 = os.path.join(base_path, "map/user/hover/country/india/state/")
map_user_list = os.listdir(path4)
columns4 = {"States": [], "Years": [], "Quarter": [], "Districts": [], "RegisteredUser": [], "AppOpens": []}

for state in map_user_list:
    cur_states = os.path.join(path4, state, "")
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = os.path.join(cur_states, year, "")
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = os.path.join(cur_years, file)
            with open(cur_files, "r") as data:
                F = json.load(data)

            for i in F["data"]["hoverData"].items():
                district = i[0]
                registeredusers = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                columns4["Districts"].append(district)
                columns4["RegisteredUser"].append(registeredusers)
                columns4["AppOpens"].append(appopens)
                columns4["States"].append(state)
                columns4["Years"].append(year)
                columns4["Quarter"].append(int(file.strip(".json")))

map_user = pd.DataFrame(columns4)
map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
map_user["States"] = map_user["States"].str.replace("-", " ")
map_user["States"] = map_user["States"].str.title()
map_user['States'] = map_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Top Insurance
path9 = os.path.join(base_path, "top/insurance/country/india/state/")
top_insur_list = os.listdir(path9)
columns9 = {"States": [], "Years": [], "Quarter": [], "Pincodes": [], "Transaction_count": [], "Transaction_amount": []}

for state in top_insur_list:
    cur_states = os.path.join(path9, state, "")
    top_year_list = os.listdir(cur_states)

    for year in top_year_list:
        cur_years = os.path.join(cur_states, year, "")
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = os.path.join(cur_years, file)
            with open(cur_files, "r") as data:
                G = json.load(data)

            for i in G["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns9["Pincodes"].append(entityName)
                columns9["Transaction_count"].append(count)
                columns9["Transaction_amount"].append(amount)
                columns9["States"].append(state)
                columns9["Years"].append(year)
                columns9["Quarter"].append(int(file.strip(".json")))

top_insur = pd.DataFrame(columns9)
top_insur["States"] = top_insur["States"].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
top_insur["States"] = top_insur["States"].str.replace("-", " ")
top_insur["States"] = top_insur["States"].str.title()
top_insur['States'] = top_insur['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Top Transaction
path5 = os.path.join(base_path, "top/transaction/country/india/state/")
top_tran_list = os.listdir(path5)
columns5 = {"States": [], "Years": [], "Quarter": [], "Pincodes": [], "Transaction_count": [], "Transaction_amount": []}

for state in top_tran_list:
    cur_states = os.path.join(path5, state, "")
    top_year_list = os.listdir(cur_states)
    
    for year in top_year_list:
        cur_years = os.path.join(cur_states, year, "")
        top_file_list = os.listdir(cur_years)
        
        for file in top_file_list:
            cur_files = os.path.join(cur_years, file)
            with open(cur_files, "r") as data:
                H = json.load(data)

            for i in H["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns5["Pincodes"].append(entityName)
                columns5["Transaction_count"].append(count)
                columns5["Transaction_amount"].append(amount)
                columns5["States"].append(state)
                columns5["Years"].append(year)
                columns5["Quarter"].append(int(file.strip(".json")))

top_transaction = pd.DataFrame(columns5)
top_transaction["States"] = top_transaction["States"].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
top_transaction["States"] = top_transaction["States"].str.replace("-", " ")
top_transaction["States"] = top_transaction["States"].str.title()
top_transaction['States'] = top_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Top User
path6 = os.path.join(base_path, "top/user/country/india/state/")
top_user_list = os.listdir(path6)
columns6 = {"States": [], "Years": [], "Quarter": [], "Pincodes": [], "RegisteredUser": []}

for state in top_user_list:
    cur_states = os.path.join(path6, state, "")
    top_year_list = os.listdir(cur_states)

    for year in top_year_list:
        cur_years = os.path.join(cur_states, year, "")
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = os.path.join(cur_years, file)
            with open(cur_files, "r") as data:
                I = json.load(data)

            for i in I["data"]["pincodes"]:
                name = i["name"]
                registeredusers = i["registeredUsers"]
                columns6["Pincodes"].append(name)
                columns6["RegisteredUser"].append(registeredusers)
                columns6["States"].append(state)
                columns6["Years"].append(year)
                columns6["Quarter"].append(int(file.strip(".json")))

top_user = pd.DataFrame(columns6)
top_user["States"] = top_user["States"].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
top_user["States"] = top_user["States"].str.replace("-", " ")
top_user["States"] = top_user["States"].str.title()
top_user['States'] = top_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu") 

mydb = mysql.connector.connect(
    host="localhost",        
    user="root",            
    password="root",      
    database="businesscard",                 
)

cursor = mydb.cursor()
print("✅ MySQL connection successful!")

# Aggregated Insurance Table
create_query7 = '''CREATE TABLE IF NOT EXISTS aggregated_insurance (
    States varchar(50), Years int, Quarter int, Insurance_type varchar(50),
    Insurance_count bigint, Insurance_amount bigint
)'''
cursor.execute(create_query7)
mydb.commit()

for _, row in aggre_insurance.iterrows():
    insert_query7 = '''INSERT INTO aggregated_insurance (States, Years, Quarter, Insurance_type, Insurance_count, Insurance_amount)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (row["States"], row["Years"], row["Quarter"], row["Insurance_type"], row["Insurance_count"], row["Insurance_amount"])
    cursor.execute(insert_query7, values)
    mydb.commit()

# Aggregated Transaction Table
create_query1 = '''CREATE TABLE IF NOT EXISTS aggregated_transaction (
    States varchar(50), Years int, Quarter int, Transaction_type varchar(50),
    Transaction_count bigint, Transaction_amount bigint
)'''
cursor.execute(create_query1)
mydb.commit()

for _, row in aggre_transaction.iterrows():
    insert_query1 = '''INSERT INTO aggregated_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (row["States"], row["Years"], row["Quarter"], row["Transaction_type"], row["Transaction_count"], row["Transaction_amount"])
    cursor.execute(insert_query1, values)
    mydb.commit()

# Aggregated User Table
create_query2 = '''CREATE TABLE IF NOT EXISTS aggregated_user (
    States varchar(50), Years int, Quarter int, Brands varchar(50),
    Transaction_count bigint, Percentage float
)'''
cursor.execute(create_query2)
mydb.commit()

for _, row in aggre_user.iterrows():
    insert_query2 = '''INSERT INTO aggregated_user (States, Years, Quarter, Brands, Transaction_count, Percentage)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (row["States"], row["Years"], row["Quarter"], row["Brands"], row["Transaction_count"], row["Percentage"])
    cursor.execute(insert_query2, values)
    mydb.commit()

# Map Insurance Table
create_query8 = '''CREATE TABLE IF NOT EXISTS map_insurance (
    States varchar(50), Years int, Quarter int, District varchar(50),
    Transaction_count bigint, Transaction_amount float
)'''
cursor.execute(create_query8)
mydb.commit()

for _, row in map_insurance.iterrows():
    insert_query8 = '''INSERT INTO map_insurance (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (row['States'], row['Years'], row['Quarter'], row['Districts'], row['Transaction_count'], row['Transaction_amount'])
    cursor.execute(insert_query8, values)
    mydb.commit()

# Map Transaction Table
create_query3 = '''CREATE TABLE IF NOT EXISTS map_transaction (
    States varchar(50), Years int, Quarter int, District varchar(50),
    Transaction_count bigint, Transaction_amount float
)'''
cursor.execute(create_query3)
mydb.commit()

for _, row in map_transaction.iterrows():
    insert_query3 = '''INSERT INTO map_transaction (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (row['States'], row['Years'], row['Quarter'], row['District'], row['Transaction_count'], row['Transaction_amount'])
    cursor.execute(insert_query3, values)
    mydb.commit()

# Map User Table
create_query4 = '''CREATE TABLE IF NOT EXISTS map_user (
    States varchar(50), Years int, Quarter int, Districts varchar(50),
    RegisteredUser bigint, AppOpens bigint
)'''
cursor.execute(create_query4)
mydb.commit()

for _, row in map_user.iterrows():
    insert_query4 = '''INSERT INTO map_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (row["States"], row["Years"], row["Quarter"], row["Districts"], row["RegisteredUser"], row["AppOpens"])
    cursor.execute(insert_query4, values)
    mydb.commit()

# Top Insurance Table
create_query9 = '''CREATE TABLE IF NOT EXISTS top_insurance (
    States varchar(50), Years int, Quarter int, Pincodes int,
    Transaction_count bigint, Transaction_amount bigint
)'''
cursor.execute(create_query9)
mydb.commit()

for _, row in top_insur.iterrows():
    insert_query9 = '''INSERT INTO top_insurance (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (row["States"], row["Years"], row["Quarter"], row["Pincodes"], row["Transaction_count"], row["Transaction_amount"])
    cursor.execute(insert_query9, values)
    mydb.commit()

# Top Transaction Table
create_query5 = '''CREATE TABLE IF NOT EXISTS top_transaction (
    States varchar(50), Years int, Quarter int, Pincodes int,
    Transaction_count bigint, Transaction_amount bigint
)'''
cursor.execute(create_query5)
mydb.commit()

for _, row in top_transaction.iterrows():
    insert_query5 = '''INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (row["States"], row["Years"], row["Quarter"], row["Pincodes"], row["Transaction_count"], row["Transaction_amount"])
    cursor.execute(insert_query5, values)
    mydb.commit()

# Top User Table
create_query6 = '''CREATE TABLE IF NOT EXISTS top_user (
    States varchar(50), Years int, Quarter int, Pincodes int,
    RegisteredUser bigint
)'''
cursor.execute(create_query6)
mydb.commit()

for _, row in top_user.iterrows():
    insert_query6 = '''INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser)
                      VALUES (%s, %s, %s, %s, %s)'''
    values = (row["States"], row["Years"], row["Quarter"], row["Pincodes"], row["RegisteredUser"])
    cursor.execute(insert_query6, values)
    mydb.commit() 
