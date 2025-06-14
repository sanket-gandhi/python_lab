import streamlit as st
import snowflake.snowpark as snowpark

session = snowpark.Session.builder.getOrCreate()

st.title("🔍 Snowflake Table & Column Explorer")

search_type = st.radio("Search by", ["Table Name", "Column Name"])
search_input = st.text_input("Enter name to search...", "")

enable_fuzzy = st.checkbox("Enable fuzzy search")

if search_input:
    pattern = f"%{search_input}%"
    
    if search_type == "Table Name":
        df = session.sql(f"""
            SELECT database_name, schema_name, table_name, COUNT(column_name) AS column_count
            FROM central_metadata.table_and_column_lookup
            WHERE LOWER(table_name) LIKE LOWER('{pattern}')
            GROUP BY database_name, schema_name, table_name
        """).to_pandas()
        
        st.dataframe(df)
        
        # Optional: If same table exists in more than one place, allow comparison
        table_names = df["table_name"].unique()
        if len(table_names) == 1 and df.shape[0] > 1:
            st.markdown("### 🧪 Compare Table Across Schemas")
            table_name = table_names[0]
            selected_pairs = st.multiselect("Select two entries to compare:", df[["database_name", "schema_name"]].apply(lambda row: f"{row[0]}.{row[1]}", axis=1).tolist())
            if len(selected_pairs) == 2:
                db1, schema1 = selected_pairs[0].split(".")
                db2, schema2 = selected_pairs[1].split(".")
                
                cols1 = session.sql(f"""
                    SELECT column_name FROM central_metadata.table_and_column_lookup
                    WHERE table_name = '{table_name}' AND database_name = '{db1}' AND schema_name = '{schema1}'
                """).to_pandas()["COLUMN_NAME"].tolist()
                
                cols2 = session.sql(f"""
                    SELECT column_name FROM central_metadata.table_and_column_lookup
                    WHERE table_name = '{table_name}' AND database_name = '{db2}' AND schema_name = '{schema2}'
                """).to_pandas()["COLUMN_NAME"].tolist()
                
                st.write("✅ Common Columns", list(set(cols1) & set(cols2)))
                st.write(f"❌ In {db1}.{schema1} only", list(set(cols1) - set(cols2)))
                st.write(f"❌ In {db2}.{schema2} only", list(set(cols2) - set(cols1)))

    else:  # Column search
        df = session.sql(f"""
            SELECT database_name, schema_name, table_name, column_name
            FROM central_metadata.table_and_column_lookup
            WHERE LOWER(column_name) LIKE LOWER('{pattern}')
        """).to_pandas()
        
        st.dataframe(df)
