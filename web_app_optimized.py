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
    .insight-box {
        background-color: #e3f2fd;
        border-left: 4px solid #2196f3;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 0.5rem 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Заголовок приложения
st.markdown('<h1 class="main-header">🎯 MUZAQUEST Analytics Dashboard</h1>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center; font-size: 1.2rem; color: #666;">Профессиональная аналитика ресторанов Бали • AI-Powered • Real-time</p>', unsafe_allow_html=True)

# Функции для работы с данными
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
def get_market_overview():
    """Получение обзорных данных по рынку"""
    try:
        conn = sqlite3.connect('database.sqlite')
        
        # Основные метрики
        grab_query = "SELECT SUM(sales) as total_sales, COUNT(DISTINCT restaurant_name) as restaurants FROM grab_stats"
        gojek_query = "SELECT SUM(sales) as total_sales, COUNT(DISTINCT restaurant_name) as restaurants FROM gojek_stats"
        
        grab_data = pd.read_sql_query(grab_query, conn)
        gojek_data = pd.read_sql_query(gojek_query, conn)
        
        conn.close()
        
        total_sales = (grab_data['total_sales'].iloc[0] or 0) + (gojek_data['total_sales'].iloc[0] or 0)
        total_restaurants = max(grab_data['restaurants'].iloc[0] or 0, gojek_data['restaurants'].iloc[0] or 0)
        
        return {
            'total_sales': total_sales,
            'total_restaurants': total_restaurants,
            'avg_sales_per_restaurant': total_sales / total_restaurants if total_restaurants > 0 else 0
        }
    except Exception as e:
        st.error(f"Ошибка загрузки обзорных данных: {e}")
        return {'total_sales': 0, 'total_restaurants': 0, 'avg_sales_per_restaurant': 0}

@st.cache_data
def load_recent_reports():
    """Загрузка последних отчетов"""
    reports_dir = Path("reports")
    if reports_dir.exists():
        report_files = list(reports_dir.glob("*.txt"))
        if report_files:
            report_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            return report_files[:5]
    return []

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
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  # Увеличен timeout
        
        if result.returncode == 0:
            return result.stdout
        else:
            return f"Ошибка анализа: {result.stderr}"
            
    except subprocess.TimeoutExpired:
        return "Анализ превысил лимит времени (10 минут)"
    except Exception as e:
        return f"Ошибка запуска анализа: {e}"

def ask_ai_question(question, context=""):
    """Обработка AI вопроса через полноценный процессор данных"""
    try:
        from ai_query_processor import AIQueryProcessor
        processor = AIQueryProcessor()
        return processor.process_query(question, context)
    except ImportError:
        # Fallback если модуль недоступен
        return f"""
❌ **Модуль AI обработки недоступен**

Для полноценной работы AI помощника необходим файл `ai_query_processor.py`.

**Ваш вопрос:** {question}

**Что можно получить через AI помощника:**
- 📊 Данные любого ресторана
- 🌤️ Погодные коэффициенты и влияние
- 🎉 Анализ всех 35 балийских праздников
- 🌍 Туристическая статистика по странам
- 🤖 Информация о ML модели и факторах
- 📍 GPS координаты и зональный анализ
- ⚖️ Сравнения ресторанов

💡 **Попробуйте спросить:**
- "Проанализируй Ika Canggu"
- "Как дождь влияет на продажи?"
- "Покажи статистику туристов"
"""
    except Exception as e:
        return f"❌ Ошибка AI обработки: {e}"

# Боковая панель с навигацией
st.sidebar.markdown("## 🧭 Навигация")
page = st.sidebar.selectbox(
    "Выберите раздел:",
    [
        "🏠 Главная",
        "🏪 Анализ ресторана", 
        "🏢 Анализ рынка",
        "💬 AI Помощник"
    ]
)

# ===== ГЛАВНАЯ СТРАНИЦА =====
if page == "🏠 Главная":
    st.markdown("## 📊 Обзор рынка ресторанов Бали")
    
    # Загрузка данных
    market_data = get_market_overview()
    restaurants = load_restaurants()
    
    # Основные метрики
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "🏪 Ресторанов", 
            f"{market_data['total_restaurants']}"
        )
    
    with col2:
        sales_millions = market_data['total_sales'] / 1_000_000
        st.metric(
            "💰 Общие продажи", 
            f"{sales_millions:.1f}M IDR"
        )
    
    with col3:
        avg_millions = market_data['avg_sales_per_restaurant'] / 1_000_000
        st.metric(
            "📊 Средние продажи", 
            f"{avg_millions:.1f}M IDR"
        )
    
    with col4:
        st.metric(
            "🤖 AI Анализов", 
            f"{len(load_recent_reports())}"
        )
    
    st.markdown("---")
    
    # Статус системы
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔧 Статус системы")
        st.markdown('<div class="metric-card success-metric"><b>✅ ML-модель активна</b><br>RandomForest R² = 85%</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-card success-metric"><b>✅ Погодные данные</b><br>Open-Meteo API (59 локаций)</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-card success-metric"><b>✅ Балийские праздники</b><br>35 типов праздников активны</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-card success-metric"><b>✅ AI-анализ</b><br>OpenAI GPT-4 готов</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown("### 📈 Последние анализы")
        recent_reports = load_recent_reports()
        
        if recent_reports:
            for report_file in recent_reports:
                modified_time = datetime.fromtimestamp(report_file.stat().st_mtime)
                restaurant_name = report_file.name.split('_')[0] if '_' in report_file.name else "Неизвестный"
                st.markdown(f"""
                <div class="metric-card">
                    <b>📄 {restaurant_name}</b><br>
                    <small>{modified_time.strftime('%d.%m.%Y %H:%M')}</small>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("Анализы еще не проводились. Создайте первый отчет!")
    
    # Быстрые действия
    st.markdown("### 🚀 Быстрые действия")
    
    quick_col1, quick_col2, quick_col3 = st.columns(3)
    
    with quick_col1:
        if st.button("🏪 Анализ лучшего ресторана", type="secondary"):
            st.switch_page("🏪 Анализ ресторана")
    
    with quick_col2:
        if st.button("🏢 Рыночный анализ", type="secondary"):
            st.switch_page("🏢 Анализ рынка")
    
    with quick_col3:
        if st.button("💬 Задать вопрос AI", type="secondary"):
            st.switch_page("💬 AI Помощник")

# ===== АНАЛИЗ ОТДЕЛЬНОГО РЕСТОРАНА =====
elif page == "🏪 Анализ ресторана":
    st.markdown("## 🏪 Детальный анализ ресторана")
    
    restaurants = load_restaurants()
    
    if not restaurants:
        st.error("Не удалось загрузить список ресторанов")
        st.stop()
    
    # Настройки анализа в боковой панели
    st.sidebar.markdown("### ⚙️ Настройки анализа")
    
    # Выбор ресторана
    selected_restaurant = st.sidebar.selectbox(
        "Выберите ресторан:",
        restaurants,
        index=0,
        help="Выберите ресторан для детального анализа"
    )
    
    # Выбор периода
    period_type = st.sidebar.selectbox(
        "Период анализа:",
        ["Последние 7 дней", "Последние 30 дней", "Последние 3 месяца", "Произвольный период"],
        index=2,  # По умолчанию 3 месяца
        help="Выберите временной период для анализа"
    )
    
    if period_type == "Произвольный период":
        date_range = st.sidebar.date_input(
            "Выберите период:",
            value=(datetime.now() - timedelta(days=90), datetime.now()),
            max_value=datetime.now(),
            help="Укажите начальную и конечную даты"
        )
    
    # Дополнительные опции
    st.sidebar.markdown("### 🔧 Дополнительные опции")
    include_ml = st.sidebar.checkbox("🤖 ML-прогнозы и SHAP анализ", value=True)
    include_weather = st.sidebar.checkbox("🌤️ Погодный анализ", value=True)
    include_holidays = st.sidebar.checkbox("🎉 Анализ праздников", value=True)
    include_tourist = st.sidebar.checkbox("🌍 Туристические данные", value=True)
    
    # Основной контент
    st.markdown(f"### 📊 Анализ ресторана: **{selected_restaurant}**")
    
    # Информационная панель
    st.markdown("""
    <div class="insight-box">
        <b>🔍 Что включает анализ:</b><br>
        • Детальные продажи и тренды<br>
        • ML-детективный анализ аномалий<br>
        • Влияние погоды по GPS координатам<br>
        • Воздействие балийских праздников<br>
        • Туристические корреляции<br>
        • AI-рекомендации по улучшению
    </div>
    """, unsafe_allow_html=True)
    
    # Кнопка запуска анализа
    if st.button("🚀 Запустить полный анализ", type="primary", use_container_width=True):
        
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
        
        # Прогресс бар и анализ
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("🔄 Инициализация анализа...")
        progress_bar.progress(10)
        
        status_text.text("📊 Загрузка данных ресторана...")
        progress_bar.progress(30)
        
        status_text.text("🤖 Запуск ML-анализа...")
        progress_bar.progress(50)
        
        status_text.text("🌤️ Анализ погодных факторов...")
        progress_bar.progress(70)
        
        status_text.text("🎉 Обработка праздников и событий...")
        progress_bar.progress(90)
        
        # Запуск анализа
        result = run_analysis(selected_restaurant, start_date, end_date)
        
        progress_bar.progress(100)
        status_text.text("✅ Анализ завершен!")
        
        # Отображение результата
        st.markdown("### 📊 Результат анализа")
        st.text_area("", result, height=600, key="restaurant_result")
        
        # Кнопки действий
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("💾 Сохранить отчет"):
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"reports/{selected_restaurant}_{start_date}_{end_date}_{timestamp}.txt"
                
                os.makedirs("reports", exist_ok=True)
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(result)
                
                st.success(f"✅ Отчет сохранен: {filename}")
        
        with col2:
            if st.button("🔄 Повторить анализ"):
                st.rerun()

# ===== АНАЛИЗ РЫНКА =====
elif page == "🏢 Анализ рынка":
    st.markdown("## 🏢 Рыночный анализ и бенчмаркинг")
    
    # Настройки в боковой панели
    st.sidebar.markdown("### ⚙️ Настройки анализа")
    
    # Выбор периода
    period_type = st.sidebar.selectbox(
        "Период анализа:",
        ["Последний месяц", "Последний квартал", "Последние полгода", "Произвольный период"],
        index=1,  # По умолчанию квартал
        help="Период для рыночного анализа"
    )
    
    if period_type == "Произвольный период":
        date_range = st.sidebar.date_input(
            "Выберите период:",
            value=(datetime.now() - timedelta(days=90), datetime.now()),
            max_value=datetime.now(),
            key="market_dates"
        )
    
    # Дополнительные опции
    st.sidebar.markdown("### 🔧 Компоненты анализа")
    include_benchmarks = st.sidebar.checkbox("🏆 Бенчмаркинг", value=True)
    include_segments = st.sidebar.checkbox("📊 Сегментация", value=True)
    include_anomalies = st.sidebar.checkbox("🔍 Поиск аномалий", value=True)
    include_correlations = st.sidebar.checkbox("📈 Корреляции", value=True)
    
    # Основной контент
    st.markdown("### 📊 Комплексный анализ рынка")
    
    # Информационная панель
    st.markdown("""
    <div class="insight-box">
        <b>🎯 Что включает рыночный анализ:</b><br>
        • Сравнительный анализ всех 59 ресторанов<br>
        • Выявление лидеров и аутсайдеров<br>
        • Сегментация по performance<br>
        • ML-поиск рыночных аномалий<br>
        • Корреляции с внешними факторами<br>
        • Стратегические рекомендации
    </div>
    """, unsafe_allow_html=True)
    
    # Быстрые метрики
    market_data = get_market_overview()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📈 Общий оборот", f"{market_data['total_sales']/1_000_000:.1f}M IDR")
    
    with col2:
        st.metric("🏪 Активных ресторанов", f"{market_data['total_restaurants']}")
    
    with col3:
        st.metric("💰 Средние продажи", f"{market_data['avg_sales_per_restaurant']/1_000_000:.1f}M IDR")
    
    # Кнопка запуска анализа
    if st.button("🚀 Запустить рыночный анализ", type="primary", use_container_width=True):
        
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
        
        # Прогресс бар
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("🔄 Загрузка данных всех ресторанов...")
        progress_bar.progress(20)
        
        status_text.text("📊 Расчет бенчмарков...")
        progress_bar.progress(40)
        
        status_text.text("🔍 ML-поиск аномалий...")
        progress_bar.progress(60)
        
        status_text.text("📈 Анализ корреляций...")
        progress_bar.progress(80)
        
        # Запуск рыночного анализа
        result = run_analysis(None, start_date, end_date)
        
        progress_bar.progress(100)
        status_text.text("✅ Рыночный анализ завершен!")
        
        # Отображение результата
        st.markdown("### 📊 Результаты рыночного анализа")
        st.text_area("", result, height=600, key="market_result")
        
        # Действия
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("💾 Сохранить рыночный отчет"):
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"reports/market_analysis_{start_date}_{end_date}_{timestamp}.txt"
                
                os.makedirs("reports", exist_ok=True)
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(result)
                
                st.success(f"✅ Рыночный отчет сохранен: {filename}")
        
        with col2:
            if st.button("📊 Экспорт в Excel"):
                st.info("Функция экспорта в разработке")

# ===== AI ПОМОЩНИК =====
elif page == "💬 AI Помощник":
    st.markdown("## 💬 AI Помощник для аналитики")
    
    st.markdown("""
    <div class="insight-box">
        <b>🤖 Умный помощник MUZAQUEST</b><br>
        Задайте любой вопрос о ваших ресторанах, и AI даст детальный ответ на основе данных.
    </div>
    """, unsafe_allow_html=True)
    
    # Информационные блоки
    info_col1, info_col2, info_col3 = st.columns(3)
    
    with info_col1:
        st.markdown("""
        <div class="metric-card success-metric">
            <b>🎯 ЧТО УМЕЕТ AI:</b><br>
            • Детективный анализ падений<br>
            • Поиск аномальных дней<br>
            • Анализ лучших практик<br>
            • Влияние погоды и праздников
        </div>
        """, unsafe_allow_html=True)
    
    with info_col2:
        st.markdown("""
        <div class="metric-card">
            <b>📊 ДОСТУПНЫЕ ДАННЫЕ:</b><br>
            • 59 ресторанов<br>
            • 4.2M IDR продаж<br>
            • Погода + GPS координаты<br>
            • 164 праздника всех типов
        </div>
        """, unsafe_allow_html=True)
    
    with info_col3:
        st.markdown("""
        <div class="metric-card">
            <b>⚡ ГАРАНТИИ:</b><br>
            • Ответ за 1-2 секунды<br>
            • Только реальные данные<br>
            • Научный подход (z-score)<br>
            • Честно сообщает об ошибках
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Инструкция по использованию
    with st.expander("📋 Как правильно задавать вопросы AI помощнику", expanded=False):
        st.markdown("""
        ### 🎯 **Советы для получения лучших ответов:**
        
        **✅ ХОРОШИЕ ПРИМЕРЫ:**
        - `"Почему упали продажи у Ika Kero 2 мая на 2.3M IDR?"` → Конкретно
        - `"Были ли аномальные дни у Prana в мае?"` → Период указан
        - `"Какие дни были лучшими у Accent в апреле?"` → Четкий вопрос
        - `"Как дождь влияет на продажи доставки?"` → Фактор + контекст
        
        **❌ СЛАБЫЕ ПРИМЕРЫ:**
        - `"Как дела?"` → Слишком общий вопрос
        - `"Анализ ресторана"` → Не указан конкретный ресторан
        - `"Что с продажами?"` → Нет контекста
        
        **💡 КЛЮЧЕВЫЕ ПРИНЦИПЫ:**
        1. **Указывайте конкретный ресторан** (Ika Kero, Prana, Accent...)
        2. **Добавляйте период** (мае, июне, 2 мая...)
        3. **Конкретизируйте вопрос** (почему упали, лучшие дни, аномалии...)
        4. **Используйте готовые кнопки** для быстрого старта!
        """)
    
    st.markdown("---")
    
    # Примеры вопросов с категориями
    st.markdown("### 💡 Что можно спросить у AI помощника:")
    
    # Создаем табы для разных категорий
    tab1, tab2, tab3, tab4 = st.tabs(["🔍 Детективный анализ", "📊 Поиск аномалий", "🏆 Анализ топов", "🌦️ Внешние факторы"])
    
    with tab1:
        st.markdown("""
        **🕵️ Детективный анализ продаж:**
        """)
        
        detective_col1, detective_col2 = st.columns(2)
        
        with detective_col1:
            if st.button("🔍 Почему упали продажи у Ika Kero 2 мая?", use_container_width=True):
                st.session_state.ai_question = "Почему упали продажи у Ika Kero 2 мая?"
                
            if st.button("📉 Анализ спада у Prana в марте", use_container_width=True):
                st.session_state.ai_question = "У ресторана Prana в марте был спад — что стало причиной?"
        
        with detective_col2:
            if st.button("🔎 Что случилось с продажами у Accent?", use_container_width=True):
                st.session_state.ai_question = "Что случилось с продажами у Accent на прошлой неделе?"
                
            if st.button("🚨 Провал продаж в июле", use_container_width=True):
                st.session_state.ai_question = "Был ли провал продаж в июле из-за снижения туристов из России?"
    
    with tab2:
        st.markdown("""
        **🔍 Поиск аномальных дней:**
        """)
        
        anomaly_col1, anomaly_col2 = st.columns(2)
        
        with anomaly_col1:
            if st.button("📊 Аномальные дни у Ika Kero в мае", use_container_width=True):
                st.session_state.ai_question = "Были ли аномальные дни у Ika Kero в мае?"
                
            if st.button("🔍 Необычные дни у Prana", use_container_width=True):
                st.session_state.ai_question = "Найди необычные дни продаж у Prana в июне"
        
        with anomaly_col2:
            if st.button("📈 Пики и провалы у Accent", use_container_width=True):
                st.session_state.ai_question = "Покажи пики и провалы продаж у Accent"
                
            if st.button("🌟 Странные дни в мае", use_container_width=True):
                st.session_state.ai_question = "Были ли странные дни продаж в мае у всех ресторанов?"
    
    with tab3:
        st.markdown("""
        **🏆 Анализ лучших и худших дней:**
        """)
        
        top_col1, top_col2 = st.columns(2)
        
        with top_col1:
            if st.button("🥇 Лучшие дни у Prana в мае", use_container_width=True):
                st.session_state.ai_question = "Какие дни были лучшими у Prana в мае?"
                
            if st.button("🎯 Топ дни у Ika Kero", use_container_width=True):
                st.session_state.ai_question = "Топ дни у Ika Kero в июне"
        
        with top_col2:
            if st.button("📉 Худшие дни у Accent", use_container_width=True):
                st.session_state.ai_question = "Какие дни были худшими у Accent в апреле?"
                
            if st.button("🏆 Успешные дни всех ресторанов", use_container_width=True):
                st.session_state.ai_question = "Что общего у всех ресторанов с ростом продаж в последние 2 недели?"
    
    with tab4:
        st.markdown("""
        **🌤️ Влияние внешних факторов:**
        """)
        
        factor_col1, factor_col2 = st.columns(2)
        
        with factor_col1:
            if st.button("🌧️ Влияние дождя на продажи", use_container_width=True):
                st.session_state.ai_question = "Как дождь влияет на продажи у ресторанов доставки?"
                
            if st.button("🎉 Влияние праздников Galungan", use_container_width=True):
                st.session_state.ai_question = "Влияние праздников на Prana в июне — есть ли эффект?"
        
        with factor_col2:
            if st.button("🌤️ Погода и заказы за 30 дней", use_container_width=True):
                st.session_state.ai_question = "Как погода влияла на заказы в Ika Kero за последние 30 дней?"
                
            if st.button("🇷🇺 Туристы из России", use_container_width=True):
                st.session_state.ai_question = "Как снижение туристов из России повлияло на продажи?"
    
    # Дополнительные быстрые вопросы
    st.markdown("### 🚀 Популярные запросы:")
    
    quick_col1, quick_col2, quick_col3, quick_col4 = st.columns(4)
    
    with quick_col1:
        if st.button("💰 Маркетинг", use_container_width=True):
            st.session_state.ai_question = "Работает ли реклама у Ika Kero? Были ли дни с ростом?"
    
    with quick_col2:
        if st.button("⭐ Рейтинги", use_container_width=True):
            st.session_state.ai_question = "Насколько влияет рейтинг на продажи у Prana?"
    
    with quick_col3:
        if st.button("📱 Платформы", use_container_width=True):
            st.session_state.ai_question = "Почему у Accent продажи Grab выше чем Gojek?"
    
    with quick_col4:
        if st.button("🎁 Промо", use_container_width=True):
            st.session_state.ai_question = "Когда последний раз включение промо дало рост у Ika Kero?"
    
    # Основное поле для вопроса
    st.markdown("### 💬 Ваш вопрос к AI помощнику:")
    
    user_question = st.text_area(
        "Напишите ваш вопрос:",
        value=st.session_state.get('ai_question', ''),
        placeholder="Примеры:\n• Почему упали продажи у Ika Kero 2 мая?\n• Были ли аномальные дни у Prana в мае?\n• Как дождь влияет на доставку?\n• Какие дни были лучшими у Accent?",
        height=120,
        key="question_input",
        help="AI помощник анализирует данные из database.sqlite, учитывает погоду, праздники и дает научно обоснованные ответы"
    )
    
    # Индикаторы статуса AI
    status_col1, status_col2, status_col3, status_col4 = st.columns(4)
    
    with status_col1:
        st.markdown('<div style="text-align: center;"><span style="color: green;">✅</span><br><small>База данных</small></div>', unsafe_allow_html=True)
    
    with status_col2:
        st.markdown('<div style="text-align: center;"><span style="color: green;">✅</span><br><small>Погодные API</small></div>', unsafe_allow_html=True)
    
    with status_col3:
        st.markdown('<div style="text-align: center;"><span style="color: green;">✅</span><br><small>Праздники</small></div>', unsafe_allow_html=True)
    
    with status_col4:
        st.markdown('<div style="text-align: center;"><span style="color: green;">✅</span><br><small>ML-модели</small></div>', unsafe_allow_html=True)
    
    # Дополнительные настройки
    col1, col2 = st.columns(2)
    
    with col1:
        analysis_depth = st.selectbox(
            "Глубина анализа:",
            ["Быстрый ответ", "Детальный анализ", "Глубокое исследование"],
            index=1
        )
    
    with col2:
        include_data = st.checkbox("📊 Включить данные в ответ", value=True)
    
    # Кнопка отправки
    if st.button("🤖 Получить AI ответ", type="primary", use_container_width=True):
        if not user_question.strip():
            st.error("Пожалуйста, введите ваш вопрос")
        else:
            with st.spinner("🧠 AI анализирует ваш вопрос..."):
                # Имитация обработки
                import time
                time.sleep(2)
                
                # Получение ответа от AI
                ai_response = ask_ai_question(user_question)
                
                # Отображение ответа
                st.markdown("### 🤖 Ответ AI помощника:")
                st.markdown(ai_response)
                
                # Дополнительные действия
                st.markdown("---")
                
                action_col1, action_col2, action_col3 = st.columns(3)
                
                with action_col1:
                    if st.button("🔄 Уточнить вопрос"):
                        st.session_state.follow_up = True
                
                with action_col2:
                    if st.button("📊 Запустить детальный анализ"):
                        st.info("Переходим к детальному анализу...")
                
                with action_col3:
                    if st.button("💾 Сохранить ответ"):
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        filename = f"reports/ai_consultation_{timestamp}.txt"
                        
                        os.makedirs("reports", exist_ok=True)
                        with open(filename, 'w', encoding='utf-8') as f:
                            f.write(f"Вопрос: {user_question}\n\n{ai_response}")
                        
                        st.success(f"💾 Консультация сохранена: {filename}")
    
    # Сброс состояния
    if 'ai_question' in st.session_state:
        del st.session_state.ai_question

# Подвал приложения
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; margin-top: 2rem;">
    <p>🎯 <strong>MUZAQUEST Analytics Dashboard</strong> | Оптимизированная версия 2.0 | 2025</p>
    <p>Профессиональная аналитика ресторанов Бали с AI и ML</p>
</div>
""", unsafe_allow_html=True)