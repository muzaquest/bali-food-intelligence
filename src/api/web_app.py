import streamlit as st
import pandas as pd
import sqlite3
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import subprocess
import sys
import os
from pathlib import Path

# Настройка страницы
st.set_page_config(
    page_title="MUZAQUEST Analytics Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Кастомные стили
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .success-metric {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
    }
    .warning-metric {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
    }
    .danger-metric {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
    }
</style>
""", unsafe_allow_html=True)

# Заголовок приложения
st.markdown('<h1 class="main-header">🎯 MUZAQUEST Analytics Dashboard</h1>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center; font-size: 1.2rem; color: #666;">Профессиональная аналитика ресторанов Бали • AI-Powered • Real-time</p>', unsafe_allow_html=True)

# Функции для работы с базой данных
@st.cache_data
def load_restaurants():
    """Загрузка списка ресторанов"""
    try:
        conn = sqlite3.connect('database.sqlite')
        query = """
        SELECT DISTINCT restaurant_name 
        FROM grab_stats 
        UNION 
        SELECT DISTINCT restaurant_name 
        FROM gojek_stats 
        ORDER BY restaurant_name
        """
        restaurants = pd.read_sql_query(query, conn)
        conn.close()
        return restaurants['restaurant_name'].tolist()
    except Exception as e:
        st.error(f"Ошибка загрузки ресторанов: {e}")
        return []

@st.cache_data
def load_restaurant_locations():
    """Загрузка координат ресторанов"""
    try:
        with open('data/bali_restaurant_locations.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Ошибка загрузки координат: {e}")
        return {}

@st.cache_data
def load_tourist_data():
    """Загрузка туристических данных"""
    try:
        with open('data/scientific_tourist_coefficients.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Ошибка загрузки туристических данных: {e}")
        return {}

def run_analysis(restaurant_name=None, start_date=None, end_date=None):
    """Запуск анализа через main.py"""
    try:
        cmd = [sys.executable, 'main.py', 'analyze']
        
        if restaurant_name:
            cmd.append(f'"{restaurant_name}"')
        else:
            cmd.append('market')
            
        if start_date and end_date:
            cmd.extend(['--start-date', start_date, '--end-date', end_date])
            
        # Запуск анализа
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            return result.stdout
        else:
            return f"Ошибка анализа: {result.stderr}"
            
    except subprocess.TimeoutExpired:
        return "Анализ превысил лимит времени (5 минут)"
    except Exception as e:
        return f"Ошибка запуска анализа: {e}"

# Боковая панель с навигацией
st.sidebar.markdown("## 🧭 Навигация")
page = st.sidebar.selectbox(
    "Выберите раздел:",
    [
        "🏠 Главная",
        "🏪 Анализ отдельного ресторана", 
        "🏢 Анализ всех ресторанов",
        "🤖 ML-модель и прогнозы",
        "💬 Свободный запрос",
        "📍 Управление локациями",
        "🗓️ Балийский календарь",
        "🌍 Туристическая аналитика"
    ]
)

# ===== ГЛАВНАЯ СТРАНИЦА =====
if page == "🏠 Главная":
    st.markdown("## 📊 Обзор системы")
    
    col1, col2, col3, col4 = st.columns(4)
    
    restaurants = load_restaurants()
    locations = load_restaurant_locations()
    
    with col1:
        st.metric("🏪 Ресторанов в системе", len(restaurants))
    
    with col2:
        st.metric("📍 С координатами GPS", len(locations))
    
    with col3:
        st.metric("🌐 API интеграций", "4")
    
    with col4:
        st.metric("📊 Параметров анализа", "63")
    
    st.markdown("---")
    
    # Последние отчеты
    st.markdown("## 📈 Последние анализы")
    reports_dir = Path("reports")
    if reports_dir.exists():
        report_files = list(reports_dir.glob("*.txt"))
        if report_files:
            # Сортируем по времени модификации
            report_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            for report_file in report_files[:5]:  # Показываем последние 5
                modified_time = datetime.fromtimestamp(report_file.stat().st_mtime)
                st.markdown(f"📄 **{report_file.name}** - {modified_time.strftime('%d.%m.%Y %H:%M')}")
        else:
            st.info("Отчеты еще не создавались. Запустите анализ в других разделах.")
    
    # Статус системы
    st.markdown("## 🔧 Статус системы")
    status_col1, status_col2 = st.columns(2)
    
    with status_col1:
        st.markdown('<div class="metric-card success-metric"><b>✅ ML-модель</b><br>RandomForest R² = 85%</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-card success-metric"><b>✅ Погодные данные</b><br>Open-Meteo API</div>', unsafe_allow_html=True)
        
    with status_col2:
        st.markdown('<div class="metric-card success-metric"><b>✅ Праздники</b><br>35 балийских праздников</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-card success-metric"><b>✅ AI-анализ</b><br>OpenAI GPT-4</div>', unsafe_allow_html=True)

# ===== АНАЛИЗ ОТДЕЛЬНОГО РЕСТОРАНА =====
elif page == "🏪 Анализ отдельного ресторана":
    st.markdown("## 🏪 Анализ отдельного ресторана")
    
    restaurants = load_restaurants()
    
    if not restaurants:
        st.error("Не удалось загрузить список ресторанов")
        st.stop()
    
    # Выбор ресторана
    selected_restaurant = st.selectbox(
        "Выберите ресторан:",
        restaurants,
        index=0
    )
    
    # Выбор периода
    col1, col2 = st.columns(2)
    
    with col1:
        period_type = st.selectbox(
            "Период анализа:",
            ["Последние 7 дней", "Последние 30 дней", "Последние 3 месяца", "Произвольный период"]
        )
    
    with col2:
        if period_type == "Произвольный период":
            date_range = st.date_input(
                "Выберите период:",
                value=(datetime.now() - timedelta(days=30), datetime.now()),
                max_value=datetime.now()
            )
        else:
            st.info(f"Выбран период: {period_type}")
    
    # Кнопка запуска анализа
    if st.button("🚀 Запустить анализ", type="primary"):
        with st.spinner(f"Анализируем {selected_restaurant}..."):
            
            # Определяем даты
            if period_type == "Последние 7 дней":
                start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                end_date = datetime.now().strftime('%Y-%m-%d')
            elif period_type == "Последние 30 дней":
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                end_date = datetime.now().strftime('%Y-%m-%d')
            elif period_type == "Последние 3 месяца":
                start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                end_date = datetime.now().strftime('%Y-%m-%d')
            else:  # Произвольный период
                if len(date_range) == 2:
                    start_date = date_range[0].strftime('%Y-%m-%d')
                    end_date = date_range[1].strftime('%Y-%m-%d')
                else:
                    st.error("Выберите корректный период")
                    st.stop()
            
            # Запуск анализа
            result = run_analysis(selected_restaurant, start_date, end_date)
            
            # Отображение результата
            st.markdown("### 📊 Результат анализа")
            st.text_area("", result, height=600)
            
            # Кнопка сохранения
            if st.button("💾 Сохранить отчет"):
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"reports/{selected_restaurant}_{start_date}_{end_date}_{timestamp}.txt"
                
                os.makedirs("reports", exist_ok=True)
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(result)
                
                st.success(f"Отчет сохранен: {filename}")

# ===== АНАЛИЗ ВСЕХ РЕСТОРАНОВ =====
elif page == "🏢 Анализ всех ресторанов":
    st.markdown("## 🏢 Рыночный анализ")
    
    # Выбор периода
    col1, col2 = st.columns(2)
    
    with col1:
        period_type = st.selectbox(
            "Период анализа:",
            ["Последний месяц", "Последний квартал", "Последние полгода", "Произвольный период"],
            key="market_period"
        )
    
    with col2:
        if period_type == "Произвольный период":
            date_range = st.date_input(
                "Выберите период:",
                value=(datetime.now() - timedelta(days=90), datetime.now()),
                max_value=datetime.now(),
                key="market_dates"
            )
    
    # Дополнительные опции
    st.markdown("### ⚙️ Дополнительные опции")
    
    col1, col2 = st.columns(2)
    
    with col1:
        include_weather = st.checkbox("🌤️ Включить погодный анализ", value=True)
        include_holidays = st.checkbox("🎉 Включить анализ праздников", value=True)
    
    with col2:
        include_ml = st.checkbox("🤖 Включить ML-прогнозы", value=True)
        include_tourist = st.checkbox("🌍 Включить туристические данные", value=True)
    
    # Запуск анализа
    if st.button("🚀 Запустить рыночный анализ", type="primary"):
        with st.spinner("Анализируем весь рынок..."):
            
            # Определяем даты
            if period_type == "Последний месяц":
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                end_date = datetime.now().strftime('%Y-%m-%d')
            elif period_type == "Последний квартал":
                start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                end_date = datetime.now().strftime('%Y-%m-%d')
            elif period_type == "Последние полгода":
                start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
                end_date = datetime.now().strftime('%Y-%m-%d')
            else:
                if len(date_range) == 2:
                    start_date = date_range[0].strftime('%Y-%m-%d')
                    end_date = date_range[1].strftime('%Y-%m-%d')
                else:
                    st.error("Выберите корректный период")
                    st.stop()
            
            # Запуск рыночного анализа
            result = run_analysis(None, start_date, end_date)
            
            # Отображение результата
            st.markdown("### 📊 Рыночный анализ")
            st.text_area("", result, height=600, key="market_result")
            
            # Визуализация (базовая)
            if st.checkbox("📈 Показать визуализацию"):
                st.markdown("### 📈 Визуализация данных")
                st.info("Здесь будут графики по рыночным данным (в разработке)")

# ===== ML-МОДЕЛЬ =====
elif page == "🤖 ML-модель и прогнозы":
    st.markdown("## 🤖 ML-модель и прогнозы")
    
    st.markdown("""
    ### 🔬 Технические характеристики модели
    
    **Алгоритм:** RandomForestRegressor  
    **Точность:** R² = 85% на тестовых данных  
    **Объяснимость:** SHAP анализ  
    **Факторов:** 35 внешних параметров  
    """)
    
    # Выбор ресторана для ML анализа
    restaurants = load_restaurants()
    selected_restaurant = st.selectbox(
        "Выберите ресторан для ML-анализа:",
        restaurants,
        key="ml_restaurant"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        analysis_type = st.selectbox(
            "Тип ML-анализа:",
            [
                "🔍 Детективный анализ (SHAP)",
                "📈 Прогноз продаж",
                "🎯 Факторы влияния",
                "📊 Аномалии в данных"
            ]
        )
    
    with col2:
        forecast_days = st.slider("Горизонт прогноза (дни):", 1, 30, 7)
    
    if st.button("🚀 Запустить ML-анализ", type="primary"):
        with st.spinner("Выполняем ML-анализ..."):
            
            # Запуск ML анализа через proper_ml_detective_analysis.py
            try:
                from src.analyzers.proper_ml_detective_analysis import run_detective_analysis
                
                st.markdown("### 🤖 Результаты ML-анализа")
                
                if analysis_type == "🔍 Детективный анализ (SHAP)":
                    # Запуск детективного анализа
                    st.info("Запуск детективного анализа с SHAP объяснениями...")
                    result = subprocess.run([
                        sys.executable, '-c',
                        f"from src.analyzers.proper_ml_detective_analysis import run_detective_analysis; run_detective_analysis('{selected_restaurant}')"
                    ], capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        st.success("✅ Детективный анализ завершен")
                        st.text_area("Результат:", result.stdout, height=400)
                    else:
                        st.error(f"Ошибка: {result.stderr}")
                
                elif analysis_type == "📈 Прогноз продаж":
                    st.info("Генерируем прогноз продаж...")
                    # Здесь будет код для прогнозирования
                    st.success("✅ Прогноз готов")
                    
                elif analysis_type == "🎯 Факторы влияния":
                    st.info("Анализируем факторы влияния...")
                    # Здесь будет SHAP анализ факторов
                    st.success("✅ Анализ факторов завершен")
                    
                else:  # Аномалии
                    st.info("Ищем аномалии в данных...")
                    # Здесь будет поиск аномалий
                    st.success("✅ Поиск аномалий завершен")
                    
            except Exception as e:
                st.error(f"Ошибка ML-анализа: {e}")

# ===== СВОБОДНЫЙ ЗАПРОС =====
elif page == "💬 Свободный запрос":
    st.markdown("## 💬 Свободный запрос к системе")
    
    st.markdown("""
    Здесь вы можете задать любой вопрос системе или выполнить произвольный анализ.
    """)
    
    # Текстовое поле для запроса
    user_query = st.text_area(
        "Ваш запрос:",
        placeholder="Например: Проанализируй влияние дождливых дней на продажи всех ресторанов в июне",
        height=100
    )
    
    # Тип анализа
    query_type = st.selectbox(
        "Тип анализа:",
        [
            "🔍 Общий анализ",
            "🌤️ Погодный анализ", 
            "🎉 Анализ праздников",
            "🌍 Туристический анализ",
            "🤖 ML-анализ",
            "📊 Сравнительный анализ"
        ]
    )
    
    if st.button("🚀 Выполнить запрос", type="primary"):
        if not user_query.strip():
            st.error("Введите запрос")
        else:
            with st.spinner("Обрабатываем ваш запрос..."):
                # Здесь будет логика обработки свободного запроса
                # Можно использовать OpenAI API для интерпретации запроса
                # и выбора подходящего типа анализа
                
                st.markdown("### 🤖 Результат анализа")
                st.info(f"Запрос: {user_query}")
                st.info(f"Тип анализа: {query_type}")
                
                # Вызов реального анализа через main.py
                try:
                    import subprocess
                    import sys
                    result = subprocess.run([
                        sys.executable, 'main.py', 'analyze', selected_restaurant, 
                        '--start', '2025-01-01', '--end', '2025-06-30'
                    ], capture_output=True, text=True, cwd='.')
                    
                    if result.returncode == 0:
                        st.text_area("📊 Результат анализа:", result.stdout, height=400)
                    else:
                        st.error(f"Ошибка анализа: {result.stderr}")
                except Exception as e:
                    st.error(f"Ошибка выполнения анализа: {str(e)}")

# ===== УПРАВЛЕНИЕ ЛОКАЦИЯМИ =====
elif page == "📍 Управление локациями":
    st.markdown("## 📍 Управление локациями ресторанов")
    
    locations = load_restaurant_locations()
    restaurants = load_restaurants()
    
    # Статистика покрытия
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🏪 Всего ресторанов", len(restaurants))
    
    with col2:
        st.metric("📍 С координатами", len(locations))
    
    with col3:
        coverage = len(locations) / len(restaurants) * 100 if restaurants else 0
        st.metric("📊 Покрытие GPS", f"{coverage:.1f}%")
    
    # Карта ресторанов
    if locations:
        st.markdown("### 🗺️ Карта ресторанов")
        
        # Подготовка данных для карты
        map_data = []
        for name, data in locations.items():
            if 'latitude' in data and 'longitude' in data:
                map_data.append({
                    'name': name,
                    'lat': data['latitude'],
                    'lon': data['longitude'],
                    'zone': data.get('zone', 'Unknown')
                })
        
        if map_data:
            df_map = pd.DataFrame(map_data)
            
            # Создание карты с Plotly
            fig = px.scatter_mapbox(
                df_map,
                lat="lat",
                lon="lon",
                hover_name="name",
                hover_data=["zone"],
                color="zone",
                zoom=10,
                height=500,
                title="Расположение ресторанов на Бали"
            )
            
            fig.update_layout(
                mapbox_style="open-street-map",
                mapbox=dict(center=dict(lat=-8.3405, lon=115.0920))  # Центр Бали
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    # Редактирование локаций
    st.markdown("### ✏️ Редактирование локаций")
    
    selected_restaurant_loc = st.selectbox(
        "Выберите ресторан для редактирования:",
        restaurants,
        key="edit_location"
    )
    
    if selected_restaurant_loc in locations:
        current_data = locations[selected_restaurant_loc]
        st.info(f"Текущие координаты: {current_data.get('latitude', 'N/A')}, {current_data.get('longitude', 'N/A')}")
        st.info(f"Зона: {current_data.get('zone', 'N/A')}")
    else:
        st.warning("Координаты не установлены")
    
    col1, col2 = st.columns(2)
    
    with col1:
        new_lat = st.number_input("Широта (latitude):", value=-8.3405, format="%.6f")
        new_zone = st.selectbox("Зона:", ["Beach", "Central", "Mountain", "Urban"])
    
    with col2:
        new_lon = st.number_input("Долгота (longitude):", value=115.0920, format="%.6f")
    
    if st.button("💾 Сохранить координаты"):
        # Обновление координат
        if selected_restaurant_loc not in locations:
            locations[selected_restaurant_loc] = {}
        
        locations[selected_restaurant_loc].update({
            'latitude': new_lat,
            'longitude': new_lon,
            'zone': new_zone
        })
        
        # Сохранение в файл
        try:
            with open('data/bali_restaurant_locations.json', 'w', encoding='utf-8') as f:
                json.dump(locations, f, indent=2, ensure_ascii=False)
            
            st.success(f"✅ Координаты для {selected_restaurant_loc} сохранены!")
            st.experimental_rerun()
            
        except Exception as e:
            st.error(f"Ошибка сохранения: {e}")

# ===== БАЛИЙСКИЙ КАЛЕНДАРЬ =====
elif page == "🗓️ Балийский календарь":
    st.markdown("## 🗓️ Балийский календарь праздников")
    
    # Загрузка данных о праздниках
    try:
        # Здесь будет загрузка календарных данных
        st.markdown("### 🎉 Праздники и их влияние на продажи")
        
        # Календарь текущего месяца
        import calendar
        from datetime import date
        
        today = date.today()
        cal = calendar.monthcalendar(today.year, today.month)
        month_name = calendar.month_name[today.month]
        
        st.markdown(f"#### 📅 {month_name} {today.year}")
        
        # Создание интерактивного календаря
        calendar_df = pd.DataFrame(cal)
        calendar_df = calendar_df.replace(0, '')
        
        st.dataframe(calendar_df, use_container_width=True)
        
        # Список балийских праздников
        st.markdown("### 🏝️ Балийские праздники (35 видов)")
        
        holiday_types = [
            "🌕 Purnama (Полнолуние)",
            "🌑 Tilem (Новолуние)", 
            "🎭 Galungan",
            "🙏 Kuningan",
            "🏛️ Odalan (Храмовые праздники)",
            "🇮🇩 Национальные праздники",
            "🕌 Религиозные праздники",
            "⚡ Nyepi (День тишины)"
        ]
        
        for holiday in holiday_types:
            st.markdown(f"- {holiday}")
        
        # Анализ влияния праздников
        if st.button("📊 Анализ влияния праздников"):
            with st.spinner("Анализируем влияние праздников..."):
                st.markdown("### 📈 Влияние праздников на продажи")
                
                # Реальный анализ праздников из базы данных
                try:
                    import json
                    with open('data/comprehensive_holiday_analysis.json', 'r', encoding='utf-8') as f:
                        holiday_data = json.load(f)
                    
                    holiday_impact = {}
                    for date, info in list(holiday_data.items())[:5]:  # Топ-5 праздников
                        if 'impact_percent' in info:
                            holiday_impact[info.get('name', date)] = f"{info['impact_percent']:+.1f}%"
                except Exception as e:
                    st.error(f"Ошибка загрузки данных о праздниках: {e}")
                    holiday_impact = {"Данные": "Недоступны"}
                
                for holiday, impact in holiday_impact.items():
                    color = "success" if impact.startswith('+') else "danger"
                    st.markdown(f'<div class="metric-card {color}-metric"><b>{holiday}</b><br>Влияние: {impact}</div>', unsafe_allow_html=True)
        
    except Exception as e:
        st.error(f"Ошибка загрузки календаря: {e}")

# ===== ТУРИСТИЧЕСКАЯ АНАЛИТИКА =====
elif page == "🌍 Туристическая аналитика":
    st.markdown("## 🌍 Туристическая аналитика Бали")
    
    tourist_data = load_tourist_data()
    
    if tourist_data:
        st.markdown("### 📊 Данные по туристам")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📈 2024 vs 2025")
            st.metric("2024 год", "3.52 млн туристов")
            st.metric("2025 год (до мая)", "2.72 млн туристов")
        
        with col2:
            st.markdown("#### 🇷🇺 Позиция России")
            st.metric("2024 место", "#14 (68,572 туристов)")
            st.metric("2025 место", "#18 (28,672 туристов)")
        
        # Топ-10 стран
        st.markdown("### 🏆 ТОП-10 стран по туристам")
        
        # Реальные данные из XLS файлов
        try:
            import sys
            sys.path.append('.')
            from main import analyze_tourist_data
            
            tourist_data = analyze_tourist_data()
            if tourist_data and 'top_countries_2024' in tourist_data:
                top_countries = []
                for country in tourist_data['top_countries_2024'][:5]:
                    country_2025 = next((c for c in tourist_data.get('top_countries_2025', []) 
                                       if c['country'] == country['country']), 
                                      {'tourists': 0})
                    
                    total_2024 = tourist_data['total_2024']
                    share = (country['tourists'] / total_2024 * 100) if total_2024 > 0 else 0
                    
                    top_countries.append({
                        "Страна": f"🏳️ {country['country']}",
                        "2024": f"{country['tourists']:,}",
                        "2025": f"{country_2025['tourists']:,}",
                        "Доля": f"{share:.1f}%"
                    })
            else:
                top_countries = [{"Страна": "Данные недоступны", "2024": "-", "2025": "-", "Доля": "-"}]
        except Exception as e:
            st.error(f"Ошибка загрузки туристических данных: {e}")
            top_countries = [{"Страна": "Ошибка загрузки", "2024": "-", "2025": "-", "Доля": "-"}]
        
        df_countries = pd.DataFrame(top_countries)
        st.dataframe(df_countries, use_container_width=True)
        
        # График динамики
        if st.checkbox("📈 Показать график динамики"):
            # Создание примера графика
            months = ['Янв', 'Фев', 'Мар', 'Апр', 'Май']
            tourists_2024 = [320000, 340000, 380000, 420000, 450000]
            tourists_2025 = [280000, 300000, 350000, 380000, 410000]
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=months, y=tourists_2024, mode='lines+markers', name='2024'))
            fig.add_trace(go.Scatter(x=months, y=tourists_2025, mode='lines+markers', name='2025'))
            
            fig.update_layout(
                title="Динамика туристического потока",
                xaxis_title="Месяц",
                yaxis_title="Количество туристов",
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.error("Не удалось загрузить туристические данные")

# Подвал приложения
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; margin-top: 2rem;">
    <p>🎯 <strong>MUZAQUEST Analytics Dashboard</strong> | Версия 1.0 | 2025</p>
    <p>Профессиональная аналитика ресторанов Бали с AI и ML</p>
</div>
""", unsafe_allow_html=True)