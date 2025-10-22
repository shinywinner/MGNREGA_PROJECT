from flask import Flask, render_template, jsonify, request
import psycopg2
from db import get_db_connection
import os

app = Flask(__name__)
app.secret_key = os.getenv("$1f7a!K9p@3xZq#rL8vW0t&nB4jH2dQ")

# ---------------- States ----------------
@app.route('/get_states')
def get_states():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM states ORDER BY name")
    states = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify(states)

# ---------------- Districts ----------------
@app.route('/get_districts')
def get_districts():
    state_id = request.args.get('state_id')
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM districts WHERE state_id=%s ORDER BY name", (state_id,))
    districts = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify(districts)

# ---------------- Metrics ----------------
@app.route('/metrics')
def metrics():
    district_id = request.args.get('district_id')
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT mt.label_en, dmm.month, dmm.metric_value
        FROM district_monthly_metrics dmm
        JOIN metric_types mt ON dmm.metric_type_id = mt.id
        WHERE dmm.district_id=%s
        ORDER BY dmm.month
    """, (district_id,))
    data = {}
    for label, month, value in cur.fetchall():
        if label not in data:
            data[label] = []
        data[label].append({"month": month, "value": value})
    cur.close()
    conn.close()
    return jsonify(data)

# ---------------- Auto-detect District ----------------
@app.route('/get_location')
def get_location():
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    if lat is None or lon is None:
        return jsonify({}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, state_id, name, latitude, longitude FROM districts WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    best = None
    best_dist = None
    for d_id, s_id, name, d_lat, d_lon in cur.fetchall():
        dx = (d_lat - lat)
        dy = (d_lon - lon)
        dist = dx*dx + dy*dy
        if best_dist is None or dist < best_dist:
            best_dist = dist
            best = {"district_id": d_id, "state_id": s_id, "district_name": name}
    cur.close()
    conn.close()
    return jsonify(best or {})

# ---------------- Home ----------------
@app.route('/')
def home():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)

