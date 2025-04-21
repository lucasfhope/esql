from flask import Flask, request, jsonify, send_file
import pandas as pd
import io

app = Flask(__name__)

@app.route("/query", methods=["POST"])
def run_query():
    try:
        esql_query = request.form.get("query")
        file = request.files.get("file")
        rounding

        if not file or not esql_query:
            return jsonify({"error": "Both 'query' and 'file' are required"}), 400

        filename = file.filename.lower()

        # Determine file type
        if filename.endswith(".csv"):
            df = pd.read_csv(file)
            filetype = "csv"
        elif filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(file)
            filetype = "excel"
        else:
            return jsonify({"error": "Unsupported file type"}), 400

        # Run query (replace this with your ESQL logic)
        result_df = df.esql.query(esql_query)

        # Convert result_df back to the original format
        buffer = io.BytesIO() if filetype == "excel" else io.StringIO()

        if filetype == "csv":
            result_df.to_csv(buffer, index=False)
            buffer.seek(0)
            return send_file(
                buffer,
                mimetype="text/csv",
                as_attachment=True,
                download_name="query_result.csv"
            )
        else:  # Excel
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                result_df.to_excel(writer, index=False)
            buffer.seek(0)
            return send_file(
                buffer,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name="query_result.xlsx"
            )

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)