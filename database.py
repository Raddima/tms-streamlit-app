import sqlite3
import streamlit as st
from datetime import datetime, time as dt_time

# --- Утиліта для міграцій ---
def _add_column_if_not_exists(cursor, table_name, column_name, column_type):
    """Додає колонку до таблиці, якщо вона ще не існує."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row['name'] for row in cursor.fetchall()]
    if column_name not in columns:
        try:
            cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}')
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e):
                 st.warning(f"Не вдалося додати колонку {column_name} до {table_name}: {e}")

@st.cache_resource
def get_db_connection():
    """Створює та повертає з'єднання з базою даних SQLite."""
    conn = sqlite3.connect('logistics_data.db', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON") # Вмикаємо підтримку зовнішніх ключів
    return conn

def run_migrations(conn):
    """Перевіряє структуру всіх таблиць і додає відсутні колонки."""
    cursor = conn.cursor()
    _add_column_if_not_exists(cursor, 'vehicles', 'fuel_consumption', 'REAL NOT NULL DEFAULT 10.0')
    _add_column_if_not_exists(cursor, 'runs', 'total_distance', 'REAL')
    _add_column_if_not_exists(cursor, 'runs', 'total_fuel_spent', 'REAL')
    _add_column_if_not_exists(cursor, 'vehicle_routes', 'fuel_spent', 'REAL')
    _add_column_if_not_exists(cursor, 'run_requests', 'request_type', 'TEXT NOT NULL DEFAULT \'Доставка\'')
    conn.commit()

def init_db():
    """Ініціалізує базу даних, створює всі необхідні таблиці та запускає міграції."""
    conn = get_db_connection()
    conn.execute('CREATE TABLE IF NOT EXISTS locations (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, address TEXT NOT NULL UNIQUE)')
    conn.execute('CREATE TABLE IF NOT EXISTS vehicles (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, capacity INTEGER NOT NULL, fuel_consumption REAL NOT NULL DEFAULT 10.0)')
    conn.execute('CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY AUTOINCREMENT, run_date DATE NOT NULL, status TEXT NOT NULL DEFAULT \'Заплановано\', total_distance REAL, total_fuel_spent REAL)')
    # Зберігаємо ON DELETE CASCADE про всяк випадок, але покладаємось на явне видалення
    conn.execute('CREATE TABLE IF NOT EXISTS run_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, name TEXT NOT NULL, address TEXT NOT NULL, weight INTEGER NOT NULL, time_from TEXT NOT NULL, time_to TEXT NOT NULL, request_type TEXT NOT NULL DEFAULT \'Доставка\', FOREIGN KEY (run_id) REFERENCES runs (id) ON DELETE CASCADE)')
    conn.execute('CREATE TABLE IF NOT EXISTS vehicle_routes (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, vehicle_name TEXT NOT NULL, vehicle_capacity INTEGER NOT NULL, route_text TEXT NOT NULL, distance REAL, load REAL, fuel_spent REAL, FOREIGN KEY (run_id) REFERENCES runs (id) ON DELETE CASCADE)')
    conn.commit()
    run_migrations(conn)

# --- Функції для локацій ---
def get_saved_locations(): return get_db_connection().execute('SELECT name, address FROM locations ORDER BY name').fetchall()
def add_location_to_db(name, address):
    try: get_db_connection().execute('INSERT INTO locations (name, address) VALUES (?, ?)', (name, address)).connection.commit()
    except sqlite3.IntegrityError: pass

# --- Функції для автомобілів ---
def get_saved_vehicles():
    rows = get_db_connection().execute('SELECT id, name, capacity, fuel_consumption FROM vehicles ORDER BY name').fetchall()
    return [dict(row) for row in rows]
def add_vehicle_to_db(name, capacity, fuel_consumption):
    try: get_db_connection().execute('INSERT INTO vehicles (name, capacity, fuel_consumption) VALUES (?, ?, ?)', (name, capacity, fuel_consumption)).connection.commit()
    except sqlite3.IntegrityError: st.warning(f"Автомобіль '{name}' вже існує.")
def delete_vehicle_from_db(vehicle_id): get_db_connection().execute('DELETE FROM vehicles WHERE id = ?', (vehicle_id,)).connection.commit()

# --- Функції для рейсів ---
def create_run(run_date):
    conn = get_db_connection()
    cursor = conn.cursor()
    run_date_str = run_date.isoformat()
    cursor.execute('INSERT INTO runs (run_date) VALUES (?)', (run_date_str,))
    conn.commit()
    return cursor.lastrowid

def save_requests_for_run(run_id, requests):
    conn = get_db_connection()
    for req in requests:
        time_from_str = req['time_from'].strftime('%H:%M') if isinstance(req['time_from'], dt_time) else str(req['time_from'])
        time_to_str = req['time_to'].strftime('%H:%M') if isinstance(req['time_to'], dt_time) else str(req['time_to'])
        conn.execute('INSERT INTO run_requests (run_id, name, address, weight, time_from, time_to, request_type) VALUES (?, ?, ?, ?, ?, ?, ?)',
                     (run_id, req['name'], req['address'], req['weight'], time_from_str, time_to_str, req['type']))
    conn.commit()

def save_routes_for_run(run_id, routes_data):
    conn, total_fuel = get_db_connection(), 0
    for route in routes_data:
        fuel_spent = route.get('fuel_spent', 0) or 0
        conn.execute('INSERT INTO vehicle_routes (run_id, vehicle_name, vehicle_capacity, route_text, distance, load, fuel_spent) VALUES (?, ?, ?, ?, ?, ?, ?)',
                     (run_id, route['vehicle_name'], route['vehicle_capacity'], route['route_text'], route.get('distance_km', 0), route.get('load', 0), fuel_spent))
        total_fuel += fuel_spent
    conn.commit(); return total_fuel

def update_run_totals(run_id, total_distance, total_fuel):
    total_distance = total_distance or 0
    total_fuel = total_fuel or 0
    get_db_connection().execute('UPDATE runs SET total_distance = ?, total_fuel_spent = ? WHERE id = ?',
                                (total_distance, total_fuel, run_id)).connection.commit()

def get_all_runs():
    return get_db_connection().execute('SELECT id, run_date, status, total_distance, total_fuel_spent FROM runs ORDER BY run_date DESC, id DESC').fetchall()

def get_run_details(run_id):
    conn = get_db_connection()
    requests = conn.execute('SELECT * FROM run_requests WHERE run_id = ?', (run_id,)).fetchall()
    routes = conn.execute('SELECT * FROM vehicle_routes WHERE run_id = ?', (run_id,)).fetchall()
    return [dict(r) for r in requests], [dict(r) for r in routes]

def update_run_status(run_id, new_status):
    get_db_connection().execute('UPDATE runs SET status = ? WHERE id = ?', (new_status, run_id)).connection.commit()

# ==== ОНОВЛЕНА ФУНКЦІЯ ВИДАЛЕННЯ ====
def delete_run(run_id):
    """Повністю видаляє рейс, явно видаляючи пов'язані записи."""
    conn = get_db_connection()
    try:
        # Починаємо транзакцію
        conn.execute('BEGIN TRANSACTION')
        # 1. Видаляємо пов'язані маршрути
        conn.execute('DELETE FROM vehicle_routes WHERE run_id = ?', (run_id,))
        # 2. Видаляємо пов'язані заявки
        conn.execute('DELETE FROM run_requests WHERE run_id = ?', (run_id,))
        # 3. Видаляємо сам рейс
        conn.execute('DELETE FROM runs WHERE id = ?', (run_id,))
        # Завершуємо транзакцію
        conn.commit()
    except Exception as e:
        conn.rollback() # Відкочуємо зміни у разі помилки
        st.error(f"Помилка при видаленні рейсу №{run_id}: {e}")

def get_assigned_vehicles_for_date(run_date):
    run_date_str = run_date.isoformat()
    runs_on_date = get_db_connection().execute("SELECT id FROM runs WHERE run_date = ? AND status IN ('Заплановано', 'В дорозі')", (run_date_str,)).fetchall()
    if not runs_on_date: return []
    run_ids = [r['id'] for r in runs_on_date]
    placeholders = ','.join('?' for _ in run_ids)
    assigned_vehicles = get_db_connection().execute(f'SELECT DISTINCT vehicle_name FROM vehicle_routes WHERE run_id IN ({placeholders})', run_ids).fetchall()
    return [v['vehicle_name'] for v in assigned_vehicles]

def get_fuel_report(start_date, end_date):
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    return get_db_connection().execute("""
        SELECT
            vr.vehicle_name,
            SUM(CASE WHEN vr.fuel_spent IS NOT NULL THEN vr.fuel_spent ELSE 0 END) as total_fuel,
            SUM(CASE WHEN vr.distance IS NOT NULL THEN vr.distance ELSE 0 END) as total_distance
        FROM vehicle_routes vr
        JOIN runs r ON vr.run_id = r.id
        WHERE r.run_date BETWEEN ? AND ? AND r.status = 'Завершено'
        GROUP BY vr.vehicle_name
        ORDER BY total_fuel DESC
    """, (start_date_str, end_date_str)).fetchall()
