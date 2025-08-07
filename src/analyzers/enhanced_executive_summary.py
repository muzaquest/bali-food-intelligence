"""
Модуль для создания улучшенного исполнительного резюме
Включает fake orders, четкую структуру и правильные расчеты
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Tuple, Optional

# Импорт fake orders filter если доступен
try:
    from ..utils.fake_orders_filter import FakeOrdersFilter
    FAKE_ORDERS_AVAILABLE = True
except ImportError:
    FAKE_ORDERS_AVAILABLE = False


class EnhancedExecutiveSummary:
    """Создает улучшенное исполнительное резюме с fake orders и четкой структурой"""
    
    def __init__(self):
        """Инициализация"""
        if FAKE_ORDERS_AVAILABLE:
            self.fake_orders_filter = FakeOrdersFilter()
        else:
            self.fake_orders_filter = None
    
    def generate_summary(self, restaurant_name: str, start_date: str, end_date: str) -> List[str]:
        """
        Генерирует улучшенное исполнительное резюме
        
        Args:
            restaurant_name: Название ресторана
            start_date: Начальная дата (YYYY-MM-DD)
            end_date: Конечная дата (YYYY-MM-DD)
            
        Returns:
            List[str]: Строки отчета
        """
        results = []
        
        # Заголовок
        results.append("📊 1. ИСПОЛНИТЕЛЬНОЕ РЕЗЮМЕ")
        results.append("=" * 50)
        results.append("")
        
        # Получаем данные из базы
        data = self._get_summary_data(restaurant_name, start_date, end_date)
        if not data:
            results.append("❌ Нет данных для анализа")
            return results
        
        # Получаем fake orders данные
        fake_data = self._get_fake_orders_data(restaurant_name, start_date, end_date)
        
        # 1. Общая выручка
        results.extend(self._format_revenue_section(data, fake_data))
        results.append("")
        
        # 2. Заказы и их структура
        results.extend(self._format_orders_section(data, fake_data))
        results.append("")
        
        # 3. Средний чек и эффективность
        results.extend(self._format_efficiency_section(data, fake_data))
        results.append("")
        
        # 4. Качество обслуживания
        results.extend(self._format_quality_section(data))
        results.append("")
        
        # 5. Маркетинг и ROAS
        results.extend(self._format_marketing_section(data))
        results.append("")
        
        # 6. Ключевые выводы
        results.extend(self._format_key_insights(data, fake_data))
        
        return results
    
    def _get_summary_data(self, restaurant_name: str, start_date: str, end_date: str) -> Optional[Dict]:
        """Получает сводные данные из базы"""
        try:
            with sqlite3.connect('database.sqlite') as conn:
                # Получаем ID ресторана
                restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
                restaurant_df = pd.read_sql_query(restaurant_query, conn)
                if restaurant_df.empty:
                    return None
                
                restaurant_id = restaurant_df.iloc[0]['id']
                
                # Получаем данные Grab
                grab_query = f"""
                SELECT 
                    SUM(COALESCE(sales, 0)) as grab_sales,
                    SUM(COALESCE(orders, 0)) as grab_orders,
                    SUM(COALESCE(cancelled_orders, 0)) as grab_cancelled,
                    AVG(COALESCE(rating, 0)) as grab_rating,
                    SUM(COALESCE(ads_spend, 0)) as grab_ads_spend,
                    SUM(COALESCE(ads_sales, 0)) as grab_ads_sales,
                    COUNT(*) as grab_days
                FROM grab_stats 
                WHERE restaurant_id = {restaurant_id} 
                AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                """
                
                # Получаем данные Gojek
                gojek_query = f"""
                SELECT 
                    SUM(COALESCE(sales, 0)) as gojek_sales,
                    SUM(COALESCE(orders, 0)) as gojek_orders,
                    SUM(COALESCE(cancelled_orders, 0)) as gojek_cancelled,
                    SUM(COALESCE(lost_orders, 0)) as gojek_lost,
                    AVG(COALESCE(rating, 0)) as gojek_rating,
                    SUM(COALESCE(ads_spend, 0)) as gojek_ads_spend,
                    SUM(COALESCE(ads_sales, 0)) as gojek_ads_sales,
                    COUNT(*) as gojek_days
                FROM gojek_stats 
                WHERE restaurant_id = {restaurant_id} 
                AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                """
                
                grab_df = pd.read_sql_query(grab_query, conn)
                gojek_df = pd.read_sql_query(gojek_query, conn)
                
                if grab_df.empty and gojek_df.empty:
                    return None
                
                # Объединяем данные
                grab_data = grab_df.iloc[0].to_dict() if not grab_df.empty else {}
                gojek_data = gojek_df.iloc[0].to_dict() if not gojek_df.empty else {}
                
                # Заполняем нулями если нет данных
                for key in ['grab_sales', 'grab_orders', 'grab_cancelled', 'grab_rating', 'grab_ads_spend', 'grab_ads_sales']:
                    if key not in grab_data:
                        grab_data[key] = 0
                
                for key in ['gojek_sales', 'gojek_orders', 'gojek_cancelled', 'gojek_lost', 'gojek_rating', 'gojek_ads_spend', 'gojek_ads_sales']:
                    if key not in gojek_data:
                        gojek_data[key] = 0
                
                # Объединяем все данные
                data = {**grab_data, **gojek_data}
                
                # Рассчитываем общие показатели
                data['total_sales'] = data['grab_sales'] + data['gojek_sales']
                data['total_orders'] = data['grab_orders'] + data['gojek_orders']
                data['total_cancelled'] = data['grab_cancelled'] + data['gojek_cancelled']
                data['total_lost'] = data.get('gojek_lost', 0)
                data['total_ads_spend'] = data['grab_ads_spend'] + data['gojek_ads_spend']
                data['total_ads_sales'] = data['grab_ads_sales'] + data['gojek_ads_sales']
                
                return data
                
        except Exception as e:
            print(f"Ошибка получения данных: {e}")
            return None
    
    def _get_fake_orders_data(self, restaurant_name: str, start_date: str, end_date: str) -> Dict:
        """Получает данные о fake orders за период"""
        if not self.fake_orders_filter:
            return {
                'total_fake_orders': 0,
                'total_fake_amount': 0,
                'grab_fake_orders': 0,
                'grab_fake_amount': 0,
                'gojek_fake_orders': 0,
                'gojek_fake_amount': 0
            }
        
        try:
            # Получаем все fake orders за период
            fake_summary = self.fake_orders_filter.get_fake_orders_summary(
                restaurant_name, start_date, end_date
            )
            
            return {
                'total_fake_orders': fake_summary.get('total_fake_orders', 0),
                'total_fake_amount': fake_summary.get('total_fake_amount', 0),
                'grab_fake_orders': fake_summary.get('grab_fake_orders', 0),
                'grab_fake_amount': fake_summary.get('grab_fake_amount', 0),
                'gojek_fake_orders': fake_summary.get('gojek_fake_orders', 0),
                'gojek_fake_amount': fake_summary.get('gojek_fake_amount', 0)
            }
            
        except Exception as e:
            print(f"Ошибка получения fake orders: {e}")
            return {
                'total_fake_orders': 0,
                'total_fake_amount': 0,
                'grab_fake_orders': 0,
                'grab_fake_amount': 0,
                'gojek_fake_orders': 0,
                'gojek_fake_amount': 0
            }
    
    def _format_revenue_section(self, data: Dict, fake_data: Dict) -> List[str]:
        """Форматирует секцию выручки"""
        results = []
        
        gross_sales = data['total_sales']
        fake_amount = fake_data['total_fake_amount']
        net_sales = gross_sales - fake_amount
        
        results.append("💰 ВЫРУЧКА")
        results.append("─" * 25)
        results.append(f"📈 Валовая выручка:     {gross_sales:>12,.0f} IDR")
        
        if fake_amount > 0:
            results.append(f"🚨 Fake orders исключено: -{fake_amount:>10,.0f} IDR")
            results.append("   " + "─" * 35)
            results.append(f"✅ Чистая выручка:     {net_sales:>12,.0f} IDR")
        else:
            results.append(f"✅ Чистая выручка:     {net_sales:>12,.0f} IDR")
        
        results.append("")
        results.append("📊 Распределение по платформам:")
        
        grab_net = data['grab_sales'] - fake_data['grab_fake_amount']
        gojek_net = data['gojek_sales'] - fake_data['gojek_fake_amount']
        
        grab_pct = (grab_net / net_sales * 100) if net_sales > 0 else 0
        gojek_pct = (gojek_net / net_sales * 100) if net_sales > 0 else 0
        
        results.append(f"   📱 GRAB:  {grab_net:>12,.0f} IDR ({grab_pct:>5.1f}%)")
        results.append(f"   🛵 GOJEK: {gojek_net:>12,.0f} IDR ({gojek_pct:>5.1f}%)")
        
        return results
    
    def _format_orders_section(self, data: Dict, fake_data: Dict) -> List[str]:
        """Форматирует секцию заказов"""
        results = []
        
        # Исходные данные
        grab_total = data['grab_orders']
        gojek_total = data['gojek_orders']
        total_gross = grab_total + gojek_total
        
        # Отмененные и потерянные
        grab_cancelled = data['grab_cancelled']
        gojek_cancelled = data['gojek_cancelled']
        gojek_lost = data['total_lost']
        
        # Fake orders
        grab_fake = fake_data['grab_fake_orders']
        gojek_fake = fake_data['gojek_fake_orders']
        total_fake = grab_fake + gojek_fake
        
        # Успешные заказы
        grab_successful = grab_total - grab_cancelled - grab_fake
        gojek_successful = gojek_total - gojek_cancelled - gojek_lost - gojek_fake
        total_successful = grab_successful + gojek_successful
        
        results.append("📦 ЗАКАЗЫ")
        results.append("─" * 25)
        results.append(f"📊 Общие заказы:        {total_gross:>8,.0f}")
        results.append("")
        results.append("   📱 GRAB:")
        results.append(f"      • Всего:          {grab_total:>8,.0f}")
        results.append(f"      • Отменено:       {grab_cancelled:>8,.0f}")
        if grab_fake > 0:
            results.append(f"      • Fake orders:    {grab_fake:>8,.0f}")
        results.append(f"      • Успешно:        {grab_successful:>8,.0f}")
        results.append("")
        results.append("   🛵 GOJEK:")
        results.append(f"      • Всего:          {gojek_total:>8,.0f}")
        results.append(f"      • Отменено:       {gojek_cancelled:>8,.0f}")
        results.append(f"      • Потеряно:       {gojek_lost:>8,.0f}")
        if gojek_fake > 0:
            results.append(f"      • Fake orders:    {gojek_fake:>8,.0f}")
        results.append(f"      • Успешно:        {gojek_successful:>8,.0f}")
        
        if total_fake > 0:
            results.append("")
            results.append("🚨 FAKE ORDERS ИСКЛЮЧЕНЫ:")
            results.append(f"   📊 Всего fake:       {total_fake:>8,.0f} заказов")
            results.append(f"   💰 Сумма fake:       {fake_data['total_fake_amount']:>8,.0f} IDR")
        
        results.append("")
        results.append("─" * 35)
        results.append(f"✅ ИТОГО успешных:     {total_successful:>8,.0f} заказов")
        
        return results
    
    def _format_efficiency_section(self, data: Dict, fake_data: Dict) -> List[str]:
        """Форматирует секцию эффективности"""
        results = []
        
        # Чистые данные
        net_sales = data['total_sales'] - fake_data['total_fake_amount']
        net_orders = data['total_orders'] - fake_data['total_fake_orders']
        
        grab_net_sales = data['grab_sales'] - fake_data['grab_fake_amount']
        grab_net_orders = data['grab_orders'] - fake_data['grab_fake_orders']
        
        gojek_net_sales = data['gojek_sales'] - fake_data['gojek_fake_amount']
        gojek_net_orders = data['gojek_orders'] - fake_data['gojek_fake_orders']
        
        # Средние чеки
        avg_check = net_sales / net_orders if net_orders > 0 else 0
        grab_avg_check = grab_net_sales / grab_net_orders if grab_net_orders > 0 else 0
        gojek_avg_check = gojek_net_sales / gojek_net_orders if gojek_net_orders > 0 else 0
        
        results.append("💵 ЭФФЕКТИВНОСТЬ")
        results.append("─" * 25)
        results.append(f"💎 Средний чек:         {avg_check:>8,.0f} IDR")
        results.append("")
        results.append("   📊 По платформам:")
        results.append(f"      📱 GRAB:          {grab_avg_check:>8,.0f} IDR")
        results.append(f"      🛵 GOJEK:         {gojek_avg_check:>8,.0f} IDR")
        
        # Дневная статистика (примерно)
        # Предполагаем что данные за месяц (30 дней)
        days_count = 30  # Можно улучшить, подсчитав реальные дни
        daily_avg = net_sales / days_count
        
        results.append("")
        results.append(f"📅 Дневная выручка:     {daily_avg:>8,.0f} IDR (средняя)")
        
        return results
    
    def _format_quality_section(self, data: Dict) -> List[str]:
        """Форматирует секцию качества"""
        results = []
        
        grab_rating = data.get('grab_rating', 0)
        gojek_rating = data.get('gojek_rating', 0)
        
        # Взвешенный рейтинг по заказам
        grab_orders = data['grab_orders']
        gojek_orders = data['gojek_orders']
        total_orders = grab_orders + gojek_orders
        
        if total_orders > 0:
            weighted_rating = (grab_rating * grab_orders + gojek_rating * gojek_orders) / total_orders
        else:
            weighted_rating = 0
        
        results.append("⭐ КАЧЕСТВО ОБСЛУЖИВАНИЯ")
        results.append("─" * 25)
        results.append(f"🏆 Общий рейтинг:       {weighted_rating:>8.2f}/5.0")
        results.append("")
        results.append("   📊 По платформам:")
        results.append(f"      📱 GRAB:          {grab_rating:>8.2f}/5.0")
        results.append(f"      🛵 GOJEK:         {gojek_rating:>8.2f}/5.0")
        
        return results
    
    def _format_marketing_section(self, data: Dict) -> List[str]:
        """Форматирует секцию маркетинга"""
        results = []
        
        total_spend = data['total_ads_spend']
        total_ads_sales = data['total_ads_sales']
        
        grab_spend = data['grab_ads_spend']
        grab_ads_sales = data['grab_ads_sales']
        
        gojek_spend = data['gojek_ads_spend']
        gojek_ads_sales = data['gojek_ads_sales']
        
        # ROAS расчеты
        total_roas = total_ads_sales / total_spend if total_spend > 0 else 0
        grab_roas = grab_ads_sales / grab_spend if grab_spend > 0 else 0
        gojek_roas = gojek_ads_sales / gojek_spend if gojek_spend > 0 else 0
        
        results.append("💸 МАРКЕТИНГ И РЕКЛАМА")
        results.append("─" * 25)
        results.append(f"💰 Рекламный бюджет:    {total_spend:>8,.0f} IDR")
        results.append(f"📈 Продажи от рекламы:  {total_ads_sales:>8,.0f} IDR")
        results.append(f"🎯 ROAS общий:          {total_roas:>8.1f}x")
        results.append("")
        results.append("   📊 По платформам:")
        results.append(f"      📱 GRAB:")
        results.append(f"         • Бюджет:      {grab_spend:>8,.0f} IDR")
        results.append(f"         • Продажи:     {grab_ads_sales:>8,.0f} IDR")
        results.append(f"         • ROAS:        {grab_roas:>8.1f}x")
        results.append("")
        results.append(f"      🛵 GOJEK:")
        results.append(f"         • Бюджет:      {gojek_spend:>8,.0f} IDR")
        results.append(f"         • Продажи:     {gojek_ads_sales:>8,.0f} IDR")
        results.append(f"         • ROAS:        {gojek_roas:>8.1f}x")
        
        return results
    
    def _format_key_insights(self, data: Dict, fake_data: Dict) -> List[str]:
        """Форматирует ключевые выводы"""
        results = []
        
        results.append("💡 КЛЮЧЕВЫЕ ВЫВОДЫ")
        results.append("─" * 25)
        
        # Анализ fake orders
        if fake_data['total_fake_orders'] > 0:
            fake_pct = (fake_data['total_fake_orders'] / data['total_orders']) * 100
            results.append(f"🚨 Обнаружено {fake_data['total_fake_orders']} fake orders ({fake_pct:.1f}% от общего)")
            results.append(f"   💸 Исключена сумма: {fake_data['total_fake_amount']:,.0f} IDR")
        
        # Анализ платформ
        net_grab = data['grab_sales'] - fake_data['grab_fake_amount']
        net_gojek = data['gojek_sales'] - fake_data['gojek_fake_amount']
        net_total = net_grab + net_gojek
        
        if net_grab > net_gojek:
            results.append(f"📱 GRAB - основная платформа ({net_grab/net_total*100:.1f}% выручки)")
        else:
            results.append(f"🛵 GOJEK - основная платформа ({net_gojek/net_total*100:.1f}% выручки)")
        
        # Анализ ROAS
        total_roas = data['total_ads_sales'] / data['total_ads_spend'] if data['total_ads_spend'] > 0 else 0
        if total_roas > 5:
            results.append(f"🎯 Отличная эффективность рекламы (ROAS {total_roas:.1f}x)")
        elif total_roas > 3:
            results.append(f"✅ Хорошая эффективность рекламы (ROAS {total_roas:.1f}x)")
        elif total_roas > 1:
            results.append(f"⚠️ Умеренная эффективность рекламы (ROAS {total_roas:.1f}x)")
        else:
            results.append(f"🚨 Низкая эффективность рекламы (ROAS {total_roas:.1f}x)")
        
        # Анализ качества
        grab_rating = data.get('grab_rating', 0)
        gojek_rating = data.get('gojek_rating', 0)
        avg_rating = (grab_rating + gojek_rating) / 2 if grab_rating > 0 and gojek_rating > 0 else max(grab_rating, gojek_rating)
        
        if avg_rating >= 4.5:
            results.append(f"⭐ Высокое качество обслуживания ({avg_rating:.2f}/5.0)")
        elif avg_rating >= 4.0:
            results.append(f"✅ Хорошее качество обслуживания ({avg_rating:.2f}/5.0)")
        else:
            results.append(f"⚠️ Требуется улучшение качества ({avg_rating:.2f}/5.0)")
        
        return results