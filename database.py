import sqlite3
import streamlit as st


@st.cache_resource
def get_db_connection():
    """Створює та повертає з'єднання з базою даних SQLite."""
    conn = sqlite3.connect('logistics_data.db', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def run_migrations(conn):
    """Перевіряє структуру всіх таблиць і додає відсутні колонки."""
    cursor = conn.cursor()
    all_tables = [row['name'] for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

    if 'vehicles' in all_tables:
        cursor.execute("PRAGMA table_info(vehicles)")
        vehicle_columns = [row['name'] for row in cursor.fetchall()]
        if 'fuel_consumption' not in vehicle_columns:
            try:
                cursor.execute('ALTER TABLE vehicles ADD COLUMN fuel_consumption REAL NOT NULL DEFAULT 10.0')
            except sqlite3.OperationalError:
                pass

    if 'runs' in all_tables:
        cursor.execute("PRAGMA table_info(runs)")
        run_columns = [row['name'] for row in cursor.fetchall()]
        if 'total_distance' not in run_columns:
            try:
                cursor.execute('ALTER TABLE runs ADD COLUMN total_distance REAL')
            except sqlite3.OperationalError:
                pass
        if 'total_fuel_spent' not in run_columns:
            try:
                cursor.execute('ALTER TABLE runs ADD COLUMN total_fuel_spent REAL')
            except sqlite3.OperationalError:
                pass

    if 'vehicle_routes' in all_tables:
        cursor.execute("PRAGMA table_info(vehicle_routes)")
        if 'fuel_spent' not in [row['name'] for row in cursor.fetchall()]:
            try:
                cursor.execute('ALTER TABLE vehicle_routes ADD COLUMN fuel_spent REAL')
            except sqlite3.OperationalError:
                pass

    if 'run_requests' in all_tables:
        cursor.execute("PRAGMA table_info(run_requests)")
        if 'request_type' not in [row['name'] for row in cursor.fetchall()]:
            try:
                cursor.execute('ALTER TABLE run_requests ADD COLUMN request_type TEXT NOT NULL DEFAULT \'Доставка\'')
            except sqlite3.OperationalError:
                pass

    conn.commit()


def init_db():
    """Ініціалізує базу даних, створює всі необхідні таблиці та запускає міграції."""
    conn = get_db_connection()
    conn.execute(
        'CREATE TABLE IF NOT EXISTS locations (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, address TEXT NOT NULL UNIQUE)')
    conn.execute(
        'CREATE TABLE IF NOT EXISTS vehicles (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, capacity INTEGER NOT NULL, fuel_consumption REAL NOT NULL DEFAULT 10.0)')
    conn.execute(
        'CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY AUTOINCREMENT, run_date DATE NOT NULL, status TEXT NOT NULL DEFAULT \'Заплановано\', total_distance REAL, total_fuel_spent REAL)')
    conn.execute(
        'CREATE TABLE IF NOT EXISTS run_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, name TEXT NOT NULL, address TEXT NOT NULL, weight INTEGER NOT NULL, time_from TEXT NOT NULL, time_to TEXT NOT NULL, request_type TEXT NOT NULL DEFAULT \'Доставка\', FOREIGN KEY (run_id) REFERENCES runs (id))')
    conn.execute(
        'CREATE TABLE IF NOT EXISTS vehicle_routes (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, vehicle_name TEXT NOT NULL, vehicle_capacity INTEGER NOT NULL, route_text TEXT NOT NULL, distance REAL, load REAL, fuel_spent REAL, FOREIGN KEY (run_id) REFERENCES runs (id))')
    conn.commit()
    run_migrations(conn)


# --- Функції для локацій ---
def get_saved_locations(): return get_db_connection().execute(
    'SELECT name, address FROM locations ORDER BY name').fetchall()


def add_location_to_db(name, address):
    try:
        get_db_connection().execute('INSERT INTO locations (name, address) VALUES (?, ?)',
                                    (name, address)).connection.commit()
    except sqlite3.IntegrityError:
        pass


# --- Функції для автомобілів ---
def get_saved_vehicles():
    rows = get_db_connection().execute(
        'SELECT id, name, capacity, fuel_consumption FROM vehicles ORDER BY name').fetchall()
    return [dict(row) for row in rows]


def add_vehicle_to_db(name, capacity, fuel_consumption):
    try:
        get_db_connection().execute('INSERT INTO vehicles (name, capacity, fuel_consumption) VALUES (?, ?, ?)',
                                    (name, capacity, fuel_consumption)).connection.commit()
    except sqlite3.IntegrityError:
        st.warning(f"Автомобіль '{name}' вже існує.")


def delete_vehicle_from_db(vehicle_id): get_db_connection().execute('DELETE FROM vehicles WHERE id = ?',
                                                                    (vehicle_id,)).connection.commit()


# --- Функції для рейсів ---
def create_run(run_date):
    cursor = get_db_connection().execute('INSERT INTO runs (run_date) VALUES (?)', (run_date,))
    get_db_connection().commit();
    return cursor.lastrowid


def save_requests_for_run(run_id, requests):
    conn = get_db_connection()
    for req in requests:
        conn.execute(
            'INSERT INTO run_requests (run_id, name, address, weight, time_from, time_to, request_type) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (run_id, req['name'], req['address'], req['weight'], req['time_from'].strftime('%H:%M'),
             req['time_to'].strftime('%H:%M'), req['type']))
    conn.commit()


def save_routes_for_run(run_id, routes_data):
    conn, total_fuel = get_db_connection(), 0
    for route in routes_data:
        conn.execute(
            'INSERT INTO vehicle_routes (run_id, vehicle_name, vehicle_capacity, route_text, distance, load, fuel_spent) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (run_id, route['vehicle_name'], route['vehicle_capacity'], route['route_text'], route['distance_km'],
             route['load'], route['fuel_spent']))
        total_fuel += route['fuel_spent']
    conn.commit();
    return total_fuel


def update_run_totals(run_id, total_distance, total_fuel): get_db_connection().execute(
    'UPDATE runs SET total_distance = ?, total_fuel_spent = ? WHERE id = ?',
    (total_distance, total_fuel, run_id)).connection.commit()


def get_all_runs(): return get_db_connection().execute(
    'SELECT id, run_date, status, total_distance, total_fuel_spent FROM runs ORDER BY run_date DESC, id DESC').fetchall()


def get_run_details(run_id):
    conn = get_db_connection()
    requests = conn.execute('SELECT * FROM run_requests WHERE run_id = ?', (run_id,)).fetchall()
    routes = conn.execute('SELECT * FROM vehicle_routes WHERE run_id = ?', (run_id,)).fetchall()
    return [dict(r) for r in requests], [dict(r) for r in routes]


def update_run_status(run_id, new_status):
    get_db_connection().execute('UPDATE runs SET status = ? WHERE id = ?', (new_status, run_id)).connection.commit()


def get_assigned_vehicles_for_date(run_date):
    runs_on_date = get_db_connection().execute(
        "SELECT id FROM runs WHERE run_date = ? AND status IN ('Заплановано', 'В дорозі')", (run_date,)).fetchall()
    if not runs_on_date: return []
    run_ids, placeholders = [r['id'] for r in runs_on_date], ','.join('?' for _ in runs_on_date)
    assigned_vehicles = get_db_connection().execute(
        f'SELECT DISTINCT vehicle_name FROM vehicle_routes WHERE run_id IN ({placeholders})', run_ids).fetchall()
    return [v['vehicle_name'] for v in assigned_vehicles]


def get_fuel_report(start_date, end_date):
    return get_db_connection().execute(
        "SELECT vr.vehicle_name, SUM(vr.fuel_spent) as total_fuel, SUM(vr.distance) as total_distance FROM vehicle_routes vr JOIN runs r ON vr.run_id = r.id WHERE r.run_date BETWEEN ? AND ? AND r.status = 'Завершено' GROUP BY vr.vehicle_name ORDER BY total_fuel DESC",
        (start_date, end_date)).fetchall()


# ==== НОВА ФУНКЦІЯ ====
def delete_run(run_id):
    """Повністю видаляє рейс та всі пов'язані з ним дані."""
    conn = get_db_connection()
    # Використовуємо транзакцію для надійності
    with conn:
        conn.execute('DELETE FROM vehicle_routes WHERE run_id = ?', (run_id,))
        conn.execute('DELETE FROM run_requests WHERE run_id = ?', (run_id,))
        conn.execute('DELETE FROM runs WHERE id = ?', (run_id,))
