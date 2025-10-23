# Блок 1: Імпорт бібліотек
import streamlit as st
import pandas as pd
import googlemaps
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import folium
from streamlit_folium import st_folium
import time
import polyline
import database as db
from datetime import time as dt_time, datetime, timedelta
from io import BytesIO


# Блок 2: Функції для розрахунків та роботи з API (зміни тільки в get_solution_routes)
def get_api_data(api_key, locations_df, use_traffic):
    """Отримує дані з Google Maps API, враховуючи опцію трафіку."""
    try:
        gmaps = googlemaps.Client(key=api_key)
    except Exception as e:
        st.error(f"Помилка ініціалізації клієнта Google Maps: {e}"); return None, None, None, None

    with st.spinner("Отримання координат..."):
        coords, addresses = [], locations_df['address'].tolist()
        for address in addresses:
            try:
                geocode_result = gmaps.geocode(address)
                if not geocode_result: st.error(
                    f"Не вдалося знайти координати: {address}"); return None, None, None, None
                location = geocode_result[0]['geometry']['location']
                coords.append((location['lat'], location['lng']));
                time.sleep(0.05)
            except Exception as e:
                st.error(f"Помилка геокодування '{address}': {e}"); return None, None, None, None
    locations_df['lat'], locations_df['lon'] = [c[0] for c in coords], [c[1] for c in coords]

    with st.spinner("Розрахунок матриць відстаней та часу..."):
        try:
            departure_time = 'now' if use_traffic else None
            matrix_result = gmaps.distance_matrix(origins=coords, destinations=coords, mode="driving",
                                                  departure_time=departure_time)
        except Exception as e:
            st.error(f"Помилка отримання матриці: {e}"); return None, None, None, None

    distance_matrix, duration_matrix = [], []
    for row in matrix_result['rows']:
        dist_row, dur_row = [], []
        for element in row['elements']:
            dist_row.append(element['distance']['value'] if element['status'] == 'OK' else 9999999)
            duration_key = 'duration_in_traffic' if 'duration_in_traffic' in element else 'duration'
            dur_row.append(element[duration_key]['value'] if element['status'] == 'OK' else 9999999)
        distance_matrix.append(dist_row);
        duration_matrix.append(dur_row)

    return gmaps, locations_df, distance_matrix, duration_matrix


def create_data_model(locations_df, vehicles_df, distance_matrix, duration_matrix, service_time_seconds,
                      depot_working_hours):
    """Створює словник з даними для розв'язувача OR-Tools."""
    demands = [row['weight'] for _, row in locations_df.iterrows()]
    demands[0] = 0

    data = {
        'distance_matrix': distance_matrix, 'duration_matrix': duration_matrix,
        'demands': demands, 'vehicle_capacities': vehicles_df['capacity'].tolist(),
        'num_vehicles': len(vehicles_df), 'depot': 0,
        'location_names': locations_df['name'].tolist(),
        'vehicle_names': vehicles_df['name'].tolist(),
        'vehicle_fuel_consumptions': vehicles_df['fuel_consumption'].tolist(),
        'service_time': service_time_seconds
    }
    time_windows = []
    for _, row in locations_df.iterrows():
        start, end = (t.hour * 3600 + t.minute * 60 for t in (row['time_from'], row['time_to']))
        time_windows.append((start, end))
    depot_start, depot_end = (t.hour * 3600 + t.minute * 60 for t in depot_working_hours)
    time_windows[0] = (depot_start, depot_end)
    data['time_windows'] = time_windows
    return data


def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    processed_data = output.getvalue()
    return processed_data


# ==== ОНОВЛЕНА ФУНКЦІЯ ДЛЯ ПРАВИЛЬНОГО ПІДРАХУНКУ ВІДСТАНІ ====
def get_solution_routes(data, manager, routing, solution, time_dimension):
    routes_output, routes_data = "", []
    total_dist_actual, active_vehicles = 0, 0

    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        if routing.IsEnd(solution.Value(routing.NextVar(index))): continue

        active_vehicles += 1
        route_distance, route_load, route_nodes_info = 0, 0, []

        while not routing.IsEnd(index):
            node_idx, prev_idx = manager.IndexToNode(index), index
            route_load += data['demands'][node_idx]
            arrival_seconds = solution.Min(time_dimension.CumulVar(index))
            arrival_time = str(timedelta(seconds=int(arrival_seconds)))
            route_nodes_info.append(f"{data['location_names'][node_idx]} (приб. о {arrival_time})")
            index = solution.Value(routing.NextVar(index))
            # Підсумовуємо реальну відстань з матриці, а не "вартість" з оптимізатора
            route_distance += data['distance_matrix'][manager.IndexToNode(prev_idx)][manager.IndexToNode(index)]

        # Додаємо останній сегмент до депо
        last_node_idx = manager.IndexToNode(routing.Start(vehicle_id))
        index_end = solution.Value(routing.NextVar(routing.Start(vehicle_id)))
        while not routing.IsEnd(index_end):
            prev_idx = index_end
            index_end = solution.Value(routing.NextVar(index_end))
        route_distance += data['distance_matrix'][manager.IndexToNode(prev_idx)][last_node_idx]

        total_dist_actual += route_distance
        arrival_seconds = solution.Min(time_dimension.CumulVar(index))
        arrival_time = str(timedelta(seconds=int(arrival_seconds)))
        route_nodes_info.append(f"{data['location_names'][0]} (пов. о {arrival_time})")

        distance_km = route_distance / 1000.0
        fuel_spent = (distance_km / 100.0) * data['vehicle_fuel_consumptions'][vehicle_id]

        route_text = ' -> '.join(route_nodes_info)
        routes_output += f"🚐 **{data['vehicle_names'][vehicle_id]}** ({data['vehicle_capacities'][vehicle_id]}кг):\n"
        routes_output += f"   - Маршрут: **{route_text}**\n"
        routes_output += f"   - Відстань: **{distance_km:.2f} км**\n"
        routes_output += f"   - Завантаження: **{route_load} кг**\n"
        routes_output += f"   - Паливо: **{fuel_spent:.2f} л**\n\n"

        routes_data.append({'vehicle_name': data['vehicle_names'][vehicle_id],
                            'vehicle_capacity': data['vehicle_capacities'][vehicle_id], 'route_text': route_text,
                            'distance_km': distance_km, 'load': route_load, 'fuel_spent': fuel_spent})

    final_report = f"🎯 **Задіяно автомобілів: {active_vehicles}**\n"
    final_report += f"🛣️ **Загальний пробіг: {total_dist_actual / 1000:.2f} км**\n\n" + routes_output

    return final_report, routes_data, total_dist_actual / 1000


def create_solution_map(gmaps_client, locations_df, data, manager, routing, solution):
    depot_coords = (locations_df.iloc[0]['lat'], locations_df.iloc[0]['lon'])
    route_map = folium.Map(location=depot_coords, zoom_start=12, tiles="cartodbpositron")

    marker_group = folium.FeatureGroup(name="Всі точки").add_to(route_map)
    for _, location in locations_df.iterrows():
        is_depot = location['id'] == 0
        marker_color = 'red' if is_depot else ('green' if location['type'] == 'Доставка' else 'orange')
        marker_icon = 'home' if is_depot else ('arrow-down' if location['type'] == 'Доставка' else 'arrow-up')

        folium.Marker(
            location=(location['lat'], location['lon']),
            popup=f"<b>{location['name']}</b><br>Вага: {location['weight']}кг<br>Тип: {location['type']}",
            tooltip=location['name'],
            icon=folium.Icon(color=marker_color, icon=marker_icon, prefix='fa')
        ).add_to(marker_group)

    colors = ['blue', 'purple', 'darkred', 'cadetblue', 'darkgreen', 'pink']

    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        if routing.IsEnd(solution.Value(routing.NextVar(index))): continue

        vehicle_name = data["vehicle_names"][vehicle_id]
        vehicle_layer = folium.FeatureGroup(name=f"Маршрут: {vehicle_name}", show=True).add_to(route_map)

        waypoints = [locations_df.iloc[manager.IndexToNode(routing.Start(vehicle_id))]['address']]
        while not routing.IsEnd(index):
            index = solution.Value(routing.NextVar(index))
            waypoints.append(locations_df.iloc[manager.IndexToNode(index)]['address'])

        if len(waypoints) > 1:
            try:
                directions_result = gmaps_client.directions(origin=waypoints[0], destination=waypoints[-1],
                                                            waypoints=waypoints[1:-1], mode="driving")
                points = polyline.decode(directions_result[0]['overview_polyline']['points'])
                folium.PolyLine(locations=points, color=colors[vehicle_id % len(colors)], weight=5, opacity=0.8,
                                popup=f'Маршрут {vehicle_name}').add_to(vehicle_layer)
            except Exception as e:
                st.warning(f"Не вдалося побудувати детальний маршрут для авто {vehicle_name}: {e}")

    folium.LayerControl().add_to(route_map)

    return route_map


# Блок 3: Код веб-додатку Streamlit
st.set_page_config(page_title="TMS Pro", layout="wide", initial_sidebar_state="auto")
st.title("Система Управління Транспортом (TMS Pro)")
db.init_db()

if 'requests' not in st.session_state: st.session_state.requests = []
if 'active_tab' not in st.session_state: st.session_state.active_tab = "🗓️ **Планування Рейсу**"
if st.session_state.get('edit_run_id'):
    run_id_to_edit = st.session_state.edit_run_id
    requests, routes = db.get_run_details(run_id_to_edit)
    st.session_state.requests = []
    for i, req in enumerate(requests):
        st.session_state.requests.append(
            {"id": i + 1, "name": req['name'], "address": req['address'], "type": req['request_type'],
             "weight": req['weight'], "time_from": dt_time.fromisoformat(
                '0' + req['time_from'] if len(req['time_from']) < 5 else req['time_from']),
             "time_to": dt_time.fromisoformat('0' + req['time_to'] if len(req['time_to']) < 5 else req['time_to'])})
    st.session_state.vehicles_to_edit = [r['vehicle_name'] for r in routes]
    db.delete_run(run_id_to_edit)
    del st.session_state.edit_run_id
    st.session_state.active_tab = "🗓️ **Планування Рейсу**"
    st.toast(f"Завантажено дані рейсу №{run_id_to_edit} для редагування.", icon="✏️")
    st.rerun()

st.sidebar.header("⚙️ Глобальні налаштування")
api_key = st.sidebar.text_input("🔑 Google Maps API ключ", type="password")
st.sidebar.markdown("---")
st.sidebar.subheader("Параметри оптимізації")
service_time_minutes = st.sidebar.number_input("Час на обслуговування (хв)", min_value=0, value=20)
working_hours = st.sidebar.slider("Робочі години автопарку", value=(dt_time(8, 0), dt_time(18, 0)),
                                  step=timedelta(minutes=30))
st.sidebar.subheader("Економічні параметри")
base_vehicle_cost = st.sidebar.number_input("Базова вартість залучення авто (в км)", min_value=0, value=20, step=5,
                                            help="Фіксований 'штраф' за використання будь-якого авто.")
capacity_cost_coefficient = st.sidebar.number_input("Коефіцієнт вартості від вантажопідйомності", min_value=0.0,
                                                    value=2.0, step=0.1, format="%.1f",
                                                    help="Додатковий 'штраф' за кожні 1000 кг вантажопідйомності. Наприклад, 2.0 означає +2 км вартості за кожну тонну.")

tab1, tab2, tab3, tab4 = st.tabs(
    ["🗓️ **Планування Рейсу**", "📊 **Історія Рейсів**", "🚛 **Статус Автопарку**", "⛽ **Звіт по Паливу**"])

with tab1:
    st.header("Планування нового рейсу")
    c_date, c_traffic = st.columns(2)
    selected_date = c_date.date_input("Оберіть дату", datetime.now())
    depot_address = st.text_input("Адреса депо:", "м. Київ, вул. Пирогівський шлях, 135")
    use_traffic = st.toggle("Враховувати поточний трафік", value=True)
    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Заявки на доставку та забір")
        if st.button("🧪 Заповнити тестовими даними"):
            st.session_state.requests = [
                {'id': 1, 'name': 'Епіцентр К', 'address': 'м. Київ, вул. Полярна, 20Д', 'type': 'Доставка',
                 'weight': 1200, 'time_from': dt_time(9, 0), 'time_to': dt_time(12, 0)},
                {'id': 2, 'name': 'Склад \'Розетка\'', 'address': 'м. Київ, проспект Степана Бандери, 34В',
                 'type': 'Доставка', 'weight': 850, 'time_from': dt_time(10, 0), 'time_to': dt_time(14, 0)},
                {'id': 3, 'name': 'ТРЦ \'Ocean Plaza\'', 'address': 'м. Київ, вул. Антоновича, 176', 'type': 'Доставка',
                 'weight': 500, 'time_from': dt_time(11, 0), 'time_to': dt_time(17, 0)},
                {'id': 4, 'name': 'Кафе \'Urban Space\'', 'address': 'м. Київ, вул. Бориса Грінченка, 9',
                 'type': 'Доставка', 'weight': 150, 'time_from': dt_time(9, 0), 'time_to': dt_time(11, 0)},
                {'id': 5, 'name': 'Повернення (Arena City)', 'address': 'м. Київ, вул. Велика Васильківська, 1-3/2',
                 'type': 'Забір', 'weight': 300, 'time_from': dt_time(14, 0), 'time_to': dt_time(16, 0)},
                {'id': 6, 'name': 'Нова Пошта №1', 'address': 'м. Київ, Столичне шосе, 103', 'type': 'Доставка',
                 'weight': 700, 'time_from': dt_time(13, 0), 'time_to': dt_time(18, 0)}]
            st.rerun()

        with st.form("request_form", clear_on_submit=True):
            req_name = st.text_input("Назва клієнта")
            req_address = st.text_input("Адреса")
            req_type = st.radio("Тип заявки:", ('Доставка', 'Забір'), horizontal=True)
            c1f, c2f = st.columns(2)
            req_weight = c1f.number_input("Вага (кг)", min_value=1, step=1)
            req_time_window = c2f.slider("Часове вікно", value=(dt_time(9, 0), dt_time(17, 0)))
            if st.form_submit_button("➕ Додати заявку"):
                new_id = max([req['id'] for req in st.session_state.requests] + [0]) + 1
                st.session_state.requests.append(
                    {"id": new_id, "name": req_name, "address": req_address, "type": req_type, "weight": req_weight,
                     "time_from": req_time_window[0], "time_to": req_time_window[1]})
                st.rerun()
        if st.session_state.requests:
            st.write("Поточний список заявок:");
            st.session_state.requests = st.data_editor(pd.DataFrame(st.session_state.requests), num_rows="dynamic",
                                                       use_container_width=True).to_dict('records')

    with col2:
        st.subheader("Автомобілі на рейсі")
        all_vehicles, assigned_vehicles = db.get_saved_vehicles(), db.get_assigned_vehicles_for_date(selected_date)
        available_vehicles = [v for v in all_vehicles if v['name'] not in assigned_vehicles]
        default_selection = []
        if 'vehicles_to_edit' in st.session_state:
            default_selection = [v['name'] for v in all_vehicles if v['name'] in st.session_state.vehicles_to_edit]
            vehicles_in_edit_run = [v for v in all_vehicles if v['name'] in st.session_state.vehicles_to_edit]
            available_vehicles.extend(vehicles_in_edit_run)
            available_vehicles = [dict(t) for t in {tuple(d.items()) for d in available_vehicles}]
            del st.session_state.vehicles_to_edit
        if not available_vehicles: st.warning(f"На {selected_date.strftime('%d.%m.%Y')} немає вільних авто.")
        vehicle_options = {f"{v['name']} ({v['capacity']}кг, {v['fuel_consumption']:.1f}л/100км)": v for v in
                           available_vehicles}
        selected_vehicles_keys = st.multiselect("Виберіть вільні авто:", options=vehicle_options.keys(),
                                                default=[k for k, v in vehicle_options.items() if
                                                         v['name'] in default_selection])
        vehicles_for_run = [vehicle_options[key] for key in selected_vehicles_keys]

    st.markdown("---")

    if st.button("🚀 Розрахувати та зберегти рейс", type="primary", use_container_width=True):
        if not api_key:
            st.warning("Введіть API ключ в бічній панелі.")
        elif not st.session_state.requests:
            st.warning("Додайте хоча б одну заявку.")
        elif not vehicles_for_run:
            st.warning("Виберіть хоча б один автомобіль.")
        else:
            depot_df = pd.DataFrame([{"id": 0, "name": "Депо", "address": depot_address, "type": "Депо", "weight": 0,
                                      "time_from": working_hours[0], "time_to": working_hours[1]}])
            locations_df = pd.concat([depot_df, pd.DataFrame(st.session_state.requests)], ignore_index=True)

            gmaps, locs_upd, dist_matrix, dur_matrix = get_api_data(api_key, locations_df, use_traffic)
            if locs_upd is not None:
                with st.spinner('Пошук оптимальних маршрутів...'):
                    vehicles_df = pd.DataFrame(vehicles_for_run)
                    data = create_data_model(locs_upd, vehicles_df, dist_matrix, dur_matrix, service_time_minutes * 60,
                                             working_hours)
                    manager = pywrapcp.RoutingIndexManager(len(data['distance_matrix']), data['num_vehicles'],
                                                           data['depot'])
                    routing = pywrapcp.RoutingModel(manager)

                    for i, vehicle in enumerate(vehicles_for_run):
                        cost = int(
                            (base_vehicle_cost + (vehicle['capacity'] / 1000.0) * capacity_cost_coefficient) * 1000)
                        routing.SetFixedCostOfVehicle(cost, i)

                    dist_cb = routing.RegisterTransitCallback(
                        lambda f, t: data['distance_matrix'][manager.IndexToNode(f)][manager.IndexToNode(t)])
                    routing.SetArcCostEvaluatorOfAllVehicles(dist_cb)


                    def demand_callback(from_index):
                        node = manager.IndexToNode(from_index)
                        return abs(data['demands'][node])


                    demand_cb_idx = routing.RegisterUnaryTransitCallback(demand_callback)
                    routing.AddDimensionWithVehicleCapacity(demand_cb_idx, 0, data['vehicle_capacities'], True,
                                                            'Capacity')

                    time_cb = routing.RegisterTransitCallback(lambda f, t: int(
                        data['duration_matrix'][manager.IndexToNode(f)][manager.IndexToNode(t)] + data['service_time']))
                    routing.AddDimension(time_cb, 3600, 24 * 3600, False, "Time")
                    time_dim = routing.GetDimensionOrDie("Time")
                    for loc_idx, time_win in enumerate(data['time_windows']):
                        if loc_idx != 0: time_dim.CumulVar(manager.NodeToIndex(loc_idx)).SetRange(int(time_win[0]),
                                                                                                  int(time_win[1]))
                    for i in range(data['num_vehicles']): time_dim.CumulVar(routing.Start(i)).SetRange(
                        int(data['time_windows'][0][0]), int(data['time_windows'][0][1]))

                    params = pywrapcp.DefaultRoutingSearchParameters()
                    params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
                    params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
                    params.time_limit.FromSeconds(20)
                    solution = routing.SolveWithParameters(params)

                if solution:
                    st.success("Рейс успішно розраховано та збережено!")
                    run_id = db.create_run(selected_date)
                    db.save_requests_for_run(run_id, st.session_state.requests)
                    solution_text, routes_data, total_dist = get_solution_routes(data, manager, routing, solution,
                                                                                 time_dim)
                    st.markdown(solution_text)
                    st.subheader("Карта маршрутів:")
                    st_folium(create_solution_map(gmaps, locs_upd, data, manager, routing, solution), width='100%',
                              height=500, returned_objects=[])
                    total_fuel = db.save_routes_for_run(run_id, routes_data)
                    db.update_run_totals(run_id, total_dist, total_fuel)
                    st.session_state.requests = []
                    if st.button("Перейти до історії рейсів"): st.rerun()
                else:
                    st.error(
                        "Не вдалося знайти рішення. Спробуйте змінити економічні параметри, додати більше авто або збільшити часові вікна.")

# Вкладки "Історія", "Статус" та "Звіт по паливу"
with tab2:
    st.header("Архів та статуси рейсів")
    all_runs = db.get_all_runs()
    if not all_runs:
        st.info("Історія рейсів порожня.")
    else:
        df_runs = pd.DataFrame([dict(r) for r in all_runs])
        st.download_button("📥 Експортувати в Excel", to_excel(df_runs), file_name="runs_history.xlsx")
        for run in all_runs:
            with st.expander(
                    f"**Рейс №{run['id']}** від **{datetime.strptime(run['run_date'], '%Y-%m-%d').strftime('%d.%m.%Y')}** | Статус: **{run['status']}**"):
                requests, routes = db.get_run_details(run['id'])
                st.subheader("Заявки:");
                st.dataframe(
                    pd.DataFrame(requests, columns=['name', 'address', 'type', 'weight', 'time_from', 'time_to']),
                    use_container_width=True)
                st.subheader("Маршрути:")
                for route in routes: st.markdown(
                    f"**{route['vehicle_name']}**: {route['route_text']} (Паливо: {route['fuel_spent']:.2f} л)")
                st.markdown("---")
                if run['status'] == 'Заплановано':
                    c1, c2, c3 = st.columns(3)
                    if c1.button("▶️ Розпочати", key=f"start_{run['id']}", type="primary"): db.update_run_status(
                        run['id'], "В дорозі"); st.rerun()
                    if c2.button("✏️ Редагувати", key=f"edit_{run['id']}"):
                        st.session_state.edit_run_id = run['id']
                        st.rerun()
                    if c3.button("❌ Видалити", key=f"delete_{run['id']}"):
                        db.delete_run(run['id'])
                        st.toast(f"Рейс №{run['id']} видалено.", icon="🗑️")
                        st.rerun()
                elif run['status'] == 'В дорозі':
                    if st.button("✅ Завершити", key=f"end_{run['id']}", type="primary"): db.update_run_status(run['id'],
                                                                                                              "Завершено"); st.rerun()

with tab3:
    st.header("Огляд зайнятості автопарку")
    status_date = st.date_input("Оберіть дату", datetime.now(), key="status_date")
    all_vehicles, assigned_vehicles = db.get_saved_vehicles(), db.get_assigned_vehicles_for_date(status_date)
    st.subheader(f"Статус на {status_date.strftime('%d.%m.%Y')}:")
    for vehicle in all_vehicles:
        status_text = "Зайнятий" if vehicle['name'] in assigned_vehicles else "Вільний"
        status_icon = "🔴" if vehicle['name'] in assigned_vehicles else "🟢"
        st.markdown(f"{status_icon} **{vehicle['name']}** - **{status_text}**")

    with st.expander("⚙️ Керування загальним автопарком"):
        with st.form("vehicle_form_manage", clear_on_submit=True):
            veh_name = st.text_input("Назва/номер")
            c1, c2 = st.columns(2)
            veh_capacity = c1.number_input("Вантажопідйомність (кг)", min_value=1, value=1000)
            veh_fuel = c2.number_input("Витрата (л/100км)", min_value=1.0, value=10.0, step=0.1, format="%.1f")
            if st.form_submit_button("➕ Додати до автопарку"):
                if veh_name and veh_capacity > 0: db.add_vehicle_to_db(veh_name, veh_capacity, veh_fuel); st.rerun()
        st.subheader("Наявний автопарк")
        for vehicle in all_vehicles:
            c1, c2 = st.columns([4, 1])
            c1.write(f"**{vehicle['name']}** ({vehicle['capacity']}кг, {vehicle['fuel_consumption']:.1f}л/100км)")
            if c2.button("❌", key=f"del_main_{vehicle['id']}", help="Видалити"): db.delete_vehicle_from_db(
                vehicle['id']); st.rerun()

with tab4:
    st.header("Звіт по витратах палива")
    st.write("Аналіз проводиться тільки для рейсів зі статусом 'Завершено'.")
    today = datetime.now()
    c1, c2 = st.columns(2)
    start_date, end_date = c1.date_input("Початкова дата", today - timedelta(days=30)), c2.date_input("Кінцева дата",
                                                                                                      today)
    if st.button("📈 Сформувати звіт"):
        report_data = db.get_fuel_report(start_date, end_date)
        if not report_data:
            st.info("За обраний період немає даних по завершених рейсах.")
        else:
            report_df = pd.DataFrame(report_data, columns=['vehicle_name', 'total_fuel', 'total_distance'])
            st.download_button("📥 Експортувати в Excel", to_excel(report_df),
                               file_name=f"fuel_report_{start_date}_to_{end_date}.xlsx")
            c1, c2 = st.columns(2)
            c1.metric("Загальні витрати палива", f"{report_df['total_fuel'].sum():.2f} л")
            c2.metric("Загальна пройдена відстань", f"{report_df['total_distance'].sum():.2f} км")
            st.subheader("Деталізація по автомобілях:")
            st.dataframe(report_df.style.format({'total_fuel': '{:.2f} л', 'total_distance': '{:.2f} км'}),
                         use_container_width=True)
            st.subheader("Візуалізація витрат палива:")
            st.bar_chart(report_df.set_index('vehicle_name')[['total_fuel']])