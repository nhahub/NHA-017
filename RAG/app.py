import os
import json
import requests
from flask import Flask, request, jsonify, render_template
import faiss
from flask_cors import CORS
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer
import ollama
import psycopg2
import traceback


# --- DEBUG UTILITY ---
def debug(*args):
    print("\n" + "="*80)
    print("[ DEBUG ]", *args)
    print("="*80 + "\n")

# --- Configuration ---
OLLAMA_MODEL = "llama3"
OLLAMA_API_URL = "http://localhost:11434/api/generate"

# PostgreSQL config
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "real_state_dwh")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
POSTGRES_TABLE = "real_estate_one_big_table" # Define the table name clearly

# ----- Load FAISS Index and Metadata -----
try:
    debug("Loading FAISS index and metadata...")
    index = faiss.read_index("RAG/faiss_index.idx")
    with open("RAG/faiss_metadata.pkl", "rb") as f:
        metadata = pickle.load(f)
    debug("FAISS index loaded successfully.")
except Exception as e:
    debug("Error loading FAISS or metadata:", e)
    index = None
    metadata = []
# ----- Initialize Embedding Model -----
debug("Loading embedding model...")
model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")
debug("Embedding model loaded.")

# ----- Initialize Flask -----
app = Flask(__name__, template_folder="template")
CORS(app)

# ----- Helper: Query FAISS -----
def query_faiss(prompt, top_n=5):
    debug("Querying FAISS with:", prompt)
    if not index:
        debug("FAISS index not found.")
        return []
    emb = model.encode([prompt], convert_to_numpy=True).astype("float32")
    D, I = index.search(emb, top_n)
    results = []
    for i, dist in zip(I[0], D[0]):
        if i < len(metadata):
            item = metadata[i].copy()
            item['similarity'] = round(max(0, 1 - dist) * 100, 2)
            results.append(item)
    debug("FAISS results:", results)
    return results

# ----- Helper: Run SQL Query -----
def run_sql_query(sql_query):
    debug("Executing SQL Query:", sql_query)

    if not sql_query.lower().strip().startswith("select"):
        return {"error": "Only SELECT queries are allowed."}

    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute(sql_query)

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        conn.close()

        debug("SQL execution successful.")
        debug("Returned rows:", rows[:10])

        return {"columns": columns, "rows": rows[:10], "full_count": len(rows)}

    except Exception as e:
        debug("SQL ERROR:", e)
        debug("Traceback:", traceback.format_exc())
        return {"error": str(e)}

# ----- System Instruction for LLM - First Call (Intent and SQL Generation) -----
SYSTEM_INSTRUCTION_SQL_INTENT = f"""
You are an expert real estate data analyst and Python programmer. Your task is to understand the user's intent and, if necessary, generate a SQL query.

DATABASE DETAILS:
- Table Name: {POSTGRES_TABLE}

COLUMNS AND DATA TYPES:
- listing_id        SERIAL PRIMARY KEY
- title             TEXT
- link              TEXT
- price             DOUBLE PRECISION
- location          TEXT
- area              DOUBLE PRECISION
- bedrooms          INTEGER
- bathrooms         INTEGER
- latitude          DOUBLE PRECISION
- longitude         DOUBLE PRECISION
- property_type     TEXT
- source            TEXT
- price_per_sqm     DOUBLE PRECISION
- Jacuzzi           INTEGER
- Garden            INTEGER
- Balcony           INTEGER
- Pool              INTEGER
- Parking           INTEGER
- Gym               INTEGER
- Maids_Quarters    INTEGER
- Spa               INTEGER
- description       TEXT

DATA TYPE SUMMARY:
- Numeric columns: price, area, bedrooms, bathrooms, latitude, longitude,
                   price_per_sqm, Jacuzzi, Garden, Balcony, Pool, Parking,
                   Gym, Maids_Quarters, Spa
- Text columns: title, link, location, property_type, source, description

YOUR TASK:
1. **Determine Intent**
   - If the user is asking for *analytics* (averages, counts, comparisons over the full database), classify as **ANALYTICAL**.
   - If the user wants specific property suggestions (filters, preferences), classify as **RECOMMENDATION**.
   - If the user is chatting, classify as **CONVERSATIONAL**.

2. **If intent = RECOMMENDATION**
   - Leave `"sql_query": ""`
   - The system will use RAG retrieval.

3. **If intent = ANALYTICAL**
   - Generate one valid, safe PostgreSQL SELECT query.
   - The query must reference the table '{POSTGRES_TABLE}'.
   - Only SELECT queries are allowed.

4. **If intent = CONVERSATIONAL**
   - No SQL. Provide acknowledgment only.

OUTPUT FORMAT (STRICT):
You MUST return a valid JSON object with these three keys only:

{{
  "intent": "RECOMMENDATION" | "ANALYTICAL" | "CONVERSATIONAL",
  "sql_query": "SELECT ...", 
  "raw_response": "Acknowledged."
}}

The JSON must contain no extra text before or after.
"""


# ----- System Instruction for LLM - Second Call (Analysis and Summary) -----
SYSTEM_INSTRUCTION_ANALYSIS = """
You are a senior real estate advisor in Egypt. You have just executed an SQL query on the property database.

**CRITICAL INSTRUCTION: Your final response MUST be a single, valid JSON object with only two main keys: 'recommendations' and 'raw_response'.**

1.  **'recommendations' (list):** MUST be EMPTY: [].
2.  **'raw_response' (string):** Use the provided SQL Results to write a clear, concise, and professional summary/answer to the original user request. Do not include the raw columns and rows in the final output. State your analytical finding clearly.
"""

@app.route("/")
def home():
    print("DEBUG >>> Serving index.html")
    return render_template("index.html")


@app.route("/recommend", methods=["POST"])
def recommend():
    print("DEBUG >>> /recommend endpoint hit")

    data = request.json
    print("DEBUG >>> Incoming request JSON:", data)

    user_prompt = data.get("prompt")
    print("DEBUG >>> User Prompt:", user_prompt)

    if not user_prompt:
        print("DEBUG >>> ERROR: Missing prompt")
        return jsonify({"error": "Missing prompt"}), 400

    # Step 1: FAISS
    results = query_faiss(user_prompt, top_n=5)
    print("DEBUG >>> RAG retrieved:", len(results), "items")

    joined_text = "\n\n".join([str(r) for r in results])
    # 2. First LLM Call: Intent and SQL Generation
    intent_prompt = f"""
    {SYSTEM_INSTRUCTION_SQL_INTENT}
    
    --- RAG CONTEXT (Top 5 properties for basic reference) ---
    {joined_text}
    -------------------
    
    User Request: {user_prompt}
    """
    print("DEBUG >>> Sending FIRST LLM REQUEST (Intent Detection)")
    try:
        intent_response = ollama.generate(
            model=OLLAMA_MODEL,
            prompt=intent_prompt,
            options={"temperature": 0.0},
            format="json"
        )
        print("DEBUG >>> Raw LLM Intent Response:", intent_response)

        raw_intent_response = intent_response.get("response", "")
        print("DEBUG >>> Extracted JSON:", raw_intent_response)

        intent_data = json.loads(raw_intent_response)

        intent = intent_data.get("intent")
        sql_query = intent_data.get("sql_query")

        print(f"DEBUG >>> Intent: {intent}")
        print(f"DEBUG >>> SQL Query: {sql_query}")

        # ANALYTICAL
        if intent == "ANALYTICAL" and sql_query:
            sql_result = run_sql_query(sql_query)
            print("DEBUG >>> SQL Result:", sql_result)

            if "error" in sql_result:
                print("DEBUG >>> SQL execution failed.")
                return jsonify({
                    "recommendations": [],
                    "raw_response": f"Database Query Failed: {sql_result['error']}",
                    "sql_query": sql_query
                })
            # Second LLM Call: Summarize SQL Results
            analysis_prompt = f"""
            {SYSTEM_INSTRUCTION_ANALYSIS}

            Original User Request: {user_prompt}
            SQL Query Executed: {sql_query}
            
            SQL Results Summary (Columns: {sql_result['columns']}):
            {sql_result['rows']}
            """
            print("DEBUG >>> Sending SECOND LLM REQUEST (Summary)")
            summary_response = ollama.generate(
                model=OLLAMA_MODEL,
                prompt=analysis_prompt,
                options={"temperature": 0.3},
                format="json"
            )
            raw_summary_response = summary_response.get("response", "")
            summary_data = json.loads(raw_summary_response)
            
            # Return the final analytical summary
            return jsonify({
                "recommendations": summary_data.get("recommendations", []),
                "raw_response": summary_data.get("raw_response", "Analysis complete but summary failed to parse."),
                "sql_query": sql_query,
                "sql_result": sql_result 
            })

        # --- B. Recommendation/Conversational Intent Path (RAG Context) ---
        else:
            # RAG/Recommendation Prompt - Uses the original SYSTEM_INSTRUCTION_SQL_INTENT keys for output.
            recommendation_prompt = f"""
            You are a senior real estate advisor in Egypt, specializing in data analysis and property recommendation.
            Your goal is to be conversational, analytical, and helpful.
            
            **CRITICAL INSTRUCTION: Your final response MUST be a single, valid JSON object with two main keys: 'recommendations' and 'raw_response'.**
            
            ### INTENT GUIDANCE:
            -   If the request is a RECOMMENDATION, provide 3-5 properties from the RAG CONTEXT. Analyze and include the best value buy.
            -   If the intent is CONVERSATIONAL, leave 'recommendations' EMPTY and answer in 'raw_response'.
            
            --- RAG CONTEXT (Top {len(results)} matching properties) ---
            {joined_text}
            -------------------
            
            User Request: {user_prompt}
            """

            llm_output = ollama.generate(
            model=OLLAMA_MODEL,
            prompt=recommendation_prompt,
            options={"temperature": 0.3},
            format="json"
        )

        print("DEBUG >>> Recommendation LLM Output:", llm_output)

        raw_llm_response = llm_output.get("response", "")
        final_data = json.loads(raw_llm_response)

        return jsonify({
            "recommendations": final_data.get("recommendations", []),
            "raw_response": final_data.get("raw_response", ""),
            "sql_query": None,
            "sql_result": None
        })
    except json.JSONDecodeError:
        print("DEBUG >>> JSON PARSE ERROR. RAW:", raw_intent_response)
        return jsonify({
            "recommendations": [],
            "raw_response": "JSON decode error.\nRAW OUTPUT:\n" + raw_intent_response
        }), 500
    except Exception as e:
        print("DEBUG >>> UNEXPECTED ERROR:", e)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    print("DEBUG >>> Starting Flask server...")
    app.run(host="0.0.0.0", port=5000, debug=True)