import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from langchain_google_genai import GoogleGenerativeAI
import re
import os
import json
from dotenv import load_dotenv

# تحميل المتغيرات من ملف .env
load_dotenv()

# ------------------- CONFIG -------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
DB_URL = os.getenv("DB_URL")

st.set_page_config(page_title="SQL Chatbot", page_icon=":bar_chart:", layout="wide")
st.title("Chat with Postgres DB :bar_chart:")

# ------------------- DATABASE -------------------
@st.cache_resource
def get_db_engine():
    return create_engine(DB_URL)

def get_schema():
    engine = get_db_engine()
    inspector_query = text("""
        SELECT table_name, column_name 
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
    """)
    
    schema_string = ""
    try:
        with engine.connect() as conn:
            result = conn.execute(inspector_query)
            current_table = None
            for row in result:
                table_name, column_name = row
                if table_name != current_table:
                    if current_table is not None:
                        schema_string += "\n"
                    schema_string += f"Table: {table_name}\n"
                    current_table = table_name
                schema_string += f"  - {column_name}\n"
    except Exception as e:
        st.error(f"Error fetching schema: {e}")
        return ""
    return schema_string

# ------------------- LLM INITIALIZATION -------------------
@st.cache_resource
def get_llm():
    return GoogleGenerativeAI(model="models/gemini-2.5-flash", api_key=GOOGLE_API_KEY)

llm = get_llm()

# ------------------- HELPERS -------------------
def clean_sql(sql_text: str) -> str:
    """
    Clean LLM output before executing as SQL.
    """
    sql_text = re.sub(r"```sql", "", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r"```", "", sql_text)
    return sql_text.strip()

def get_few_shot_examples(file_path="fewshots.json"):
    """
    Reads examples from JSON and formats them for the prompt.
    """
    try:
        if not os.path.exists(file_path):
            return ""
        with open(file_path, "r", encoding="utf-8") as f:
            examples = json.load(f)
        
        formatted_examples = "\n### EXAMPLES OF QUESTIONS AND SQL QUERIES:\n"
        for ex in examples:
            # استخدام .get لضمان عدم حدوث خطأ إذا اختلف المسمى قليلاً
            question = ex.get("naturalQuestion")
            sql = ex.get("sqlQuery")
            if question and sql:
                formatted_examples += f"Question: {question}\n"
                formatted_examples += f"SQL: {sql}\n\n"
        return formatted_examples
    except Exception as e:
        st.error(f"Error loading few-shot file: {e}")
        return ""

# ------------------- SQL GENERATION -------------------
def generate_sql_query(user_question, schema):
    # تحميل الأمثلة
    few_shots = get_few_shot_examples("fewshots.json")

    prompt = f"""
You are an expert PostgreSql Data Analyst.

Here is the database schema:
{schema}

CRITICAL RULES:
1. The tables were created via pandas and are Case-Sensitive.
2. ALWAYS surround ALL table names and column names with double quotes (e.g., "Customer", "CustomerId").
3. If joining tables with identical column names, ALWAYS use Aliases.
4. Return ONLY the SQL query without any Markdown formatting or comments.
5. POSTGRES TYPE CASTING (ESSENTIAL):
   - For dates: "ColumnName"::timestamp
   - For math: "ColumnName"::numeric

{few_shots}

### NOW YOUR TURN:
Your task is to write a SQL query that answers the following question:
Question: {user_question}
SQL:
"""
    try:
        response = llm.invoke(prompt)
        return clean_sql(response)
    except Exception as e:
        st.error(f"Error generating SQL query: {e}")
        return ""

# ------------------- NATURAL LANGUAGE RESPONSE -------------------
def get_natural_language_response(question, data_df):
    try:
        limited_data = data_df.head(15).to_markdown(index=False)
    except:
        limited_data = data_df.head(15).to_string(index=False)

    prompt = f"""
You are a professional Data Analyst.
User Question: {question}

Below is a table containing the SQL query results (showing up to 15 rows):
{limited_data}

Instructions:
1. Answer the user's question directly and clearly based on the data above.
2. If the data contains numbers, include them in your answer.
3. Keep your tone professional and concise.
"""
    try:
        response = llm.invoke(prompt)
        return response
    except Exception as e:
        st.error(f"Error generating natural language response: {e}")
        return "Error generating response."

# ------------------- STREAMLIT APP -------------------
if __name__ == "__main__":
    schema = get_schema()
    if not schema:
        st.stop()

    user_question = st.text_input("Ask a question about the database:")
    if st.button("Get Answer") and user_question:
        sql_query = generate_sql_query(user_question, schema)
        st.code(sql_query, language="sql")
        
        clean_query_check = sql_query.lower().strip()
        allowed_starts = ("select", "with")

        if not clean_query_check.startswith(allowed_starts):
            st.warning("LLM did not generate a valid SELECT or WITH query.")
            result_df = pd.DataFrame()
        else:
            try:
                engine = get_db_engine()
                with engine.connect() as conn:
                    result_df = pd.read_sql(text(sql_query), conn)
                st.dataframe(result_df)
            except Exception as e:
                st.error(f"Error executing SQL: {e}")
                result_df = pd.DataFrame()

        if not result_df.empty:
            answer = get_natural_language_response(user_question, result_df)
            st.markdown(f"**Answer:** {answer}")