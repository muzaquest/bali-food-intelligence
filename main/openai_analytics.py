#!/usr/bin/env python3
"""
🤖 ИНТЕГРАЦИЯ С OPENAI ДЛЯ УГЛУБЛЕННОГО АНАЛИЗА
Использует GPT для генерации инсайтов и рекомендаций на основе данных ресторанов
"""

import os
import json
import pandas as pd
from typing import Dict, List, Any, Optional

try:
    import openai
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

class OpenAIAnalytics:
    """Интеграция с OpenAI для углубленного анализа ресторанов"""
    
    def __init__(self):
        self.api_key = os.getenv('OPENAI_API_KEY')
        self.model = "gpt-4"  # Можно изменить на gpt-3.5-turbo для экономии
        
        if self.api_key and HAS_OPENAI:
            openai.api_key = self.api_key
            self.enabled = True
        else:
            self.enabled = False
    
    def generate_business_insights(self, restaurant_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Генерирует бизнес-инсайты с помощью OpenAI на основе данных ресторана
        """
        
        if not self.enabled:
            return self._get_fallback_insights(restaurant_data)
        
        try:
            # Подготавливаем данные для анализа
            prompt = self._prepare_analysis_prompt(restaurant_data)
            
            # Запрос к OpenAI
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {
                        "role": "system", 
                        "content": """Ты - эксперт по аналитике ресторанного бизнеса на Бали. 
                        Анализируй данные и давай конкретные, действенные рекомендации на русском языке.
                        Учитывай специфику Бали: туризм, погоду, местные праздники."""
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=1500,
                temperature=0.3
            )
            
            # Парсим ответ
            insights = self._parse_openai_response(response.choices[0].message.content)
            
            return insights
            
        except Exception as e:
            print(f"⚠️ Ошибка OpenAI API: {e}")
            return self._get_fallback_insights(restaurant_data)
    
    def analyze_competitive_positioning(self, restaurant_name: str, market_data: pd.DataFrame) -> str:
        """
        Анализирует конкурентное позиционирование ресторана с помощью AI
        """
        
        if not self.enabled:
            return self._get_fallback_positioning(restaurant_name, market_data)
        
        try:
            # Готовим данные о рынке
            market_summary = self._prepare_market_summary(restaurant_name, market_data)
            
            prompt = f"""
            Проанализируй конкурентное позиционирование ресторана {restaurant_name} на рынке Бали.
            
            Данные о рынке:
            {market_summary}
            
            Дай конкретные рекомендации по:
            1. Позиционированию относительно конкурентов
            2. Уникальным преимуществам
            3. Зонам роста
            4. Стратегии обгона лидеров
            """
            
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "Ты - стратегический консультант по ресторанному бизнесу"
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                max_tokens=800,
                temperature=0.4
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            print(f"⚠️ Ошибка анализа позиционирования: {e}")
            return self._get_fallback_positioning(restaurant_name, market_data)
    
    def generate_promotional_strategies(self, restaurant_data: Dict[str, Any]) -> List[str]:
        """
        Генерирует персонализированные рекламные стратегии
        """
        
        if not self.enabled:
            return self._get_fallback_strategies(restaurant_data)
        
        try:
            prompt = f"""
            На основе данных ресторана создай 5 конкретных рекламных стратегий:
            
            Текущие показатели:
            - Продажи: {restaurant_data.get('total_sales', 0):,} IDR
            - ROAS: {restaurant_data.get('roas', 0):.1f}
            - Рейтинг: {restaurant_data.get('rating', 0):.1f}/5.0
            - Время доставки: {restaurant_data.get('delivery_time', 0):.1f} мин
            - Лучший день недели: {restaurant_data.get('best_day', 'N/A')}
            
            Каждая стратегия должна содержать:
            1. Название кампании
            2. Целевую аудиторию  
            3. Конкретные действия
            4. Ожидаемый результат
            """
            
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "Ты - эксперт по digital-маркетингу ресторанов"
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=1000,
                temperature=0.5
            )
            
            # Парсим стратегии из ответа
            strategies = self._parse_strategies(response.choices[0].message.content)
            
            return strategies
            
        except Exception as e:
            print(f"⚠️ Ошибка генерации стратегий: {e}")
            return self._get_fallback_strategies(restaurant_data)
    
    def predict_future_trends(self, historical_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Предсказывает будущие тренды с помощью AI анализа
        """
        
        if not self.enabled:
            return self._get_fallback_predictions()
        
        try:
            # Анализируем исторические паттерны
            trends_summary = self._analyze_historical_patterns(historical_data)
            
            prompt = f"""
            На основе исторических данных предскажи тренды на следующие 3 месяца:
            
            Исторические паттерны:
            {trends_summary}
            
            Предскажи:
            1. Ожидаемый рост/падение продаж (%)
            2. Лучшие/худшие периоды
            3. Влияние сезонности
            4. Рекомендации по подготовке
            """
            
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "Ты - аналитик-прогнозист ресторанного бизнеса"
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=800,
                temperature=0.2
            )
            
            predictions = self._parse_predictions(response.choices[0].message.content)
            
            return predictions
            
        except Exception as e:
            print(f"⚠️ Ошибка предсказания трендов: {e}")
            return self._get_fallback_predictions()
    
    def _prepare_analysis_prompt(self, data: Dict[str, Any]) -> str:
        """Подготавливает промпт для анализа"""
        
        return f"""
        Проанализируй данные ресторана и дай конкретные бизнес-инсайты:
        
        ПОКАЗАТЕЛИ:
        - Общие продажи: {data.get('total_sales', 0):,} IDR
        - Средние дневные продажи: {data.get('daily_avg', 0):,} IDR  
        - Количество заказов: {data.get('total_orders', 0):,}
        - Рейтинг: {data.get('avg_rating', 0):.1f}/5.0
        - Время доставки: {data.get('avg_delivery_time', 0):.1f} мин
        - ROAS: {data.get('roas', 0):.1f}
        
        АНОМАЛИИ:
        {data.get('anomalies_summary', 'Нет значительных аномалий')}
        
        КОНКУРЕНТЫ:
        {data.get('market_position', 'Данные о рынке недоступны')}
        
        Дай 3 ключевых инсайта и 3 конкретных рекомендации.
        """
    
    def _parse_openai_response(self, response_text: str) -> Dict[str, str]:
        """Парсит ответ от OpenAI в структурированный формат"""
        
        try:
            # Попробуем найти секции в ответе
            insights = {}
            
            if "ИНСАЙТЫ" in response_text or "инсайты" in response_text:
                insights_section = response_text.split("ИНСАЙТЫ")[1] if "ИНСАЙТЫ" in response_text else response_text
                insights['insights'] = insights_section.split("РЕКОМЕНДАЦИИ")[0] if "РЕКОМЕНДАЦИИ" in insights_section else insights_section[:500]
            
            if "РЕКОМЕНДАЦИИ" in response_text or "рекомендации" in response_text:
                recommendations_section = response_text.split("РЕКОМЕНДАЦИИ")[1] if "РЕКОМЕНДАЦИИ" in response_text else ""
                insights['recommendations'] = recommendations_section[:500]
            
            if not insights:
                insights['analysis'] = response_text[:1000]
            
            return insights
            
        except Exception:
            return {'analysis': response_text[:1000]}
    
    def _get_fallback_insights(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Fallback инсайты без OpenAI"""
        
        insights = []
        recommendations = []
        
        # Анализ ROAS
        roas = data.get('roas', 0)
        if roas > 15:
            insights.append(f"🚀 Отличный ROAS {roas:.1f} - реклама очень эффективна")
            recommendations.append("Масштабировать успешные рекламные кампании")
        elif roas < 5:
            insights.append(f"⚠️ Низкий ROAS {roas:.1f} - реклама неэффективна")
            recommendations.append("Пересмотреть рекламную стратегию и таргетинг")
        
        # Анализ рейтинга
        rating = data.get('avg_rating', 0)
        if rating > 4.7:
            insights.append(f"⭐ Высокий рейтинг {rating:.1f} - отличное качество")
            recommendations.append("Использовать высокий рейтинг в маркетинге")
        elif rating < 4.0:
            insights.append(f"📉 Низкий рейтинг {rating:.1f} - проблемы с качеством")
            recommendations.append("Срочно улучшить качество еды и сервиса")
        
        # Анализ времени доставки
        delivery_time = data.get('avg_delivery_time', 0)
        if delivery_time > 40:
            insights.append(f"🐌 Медленная доставка {delivery_time:.1f}мин")
            recommendations.append("Оптимизировать логистику доставки")
        
        return {
            'insights': '\n'.join(insights),
            'recommendations': '\n'.join(recommendations)
        }
    
    def _get_fallback_positioning(self, name: str, market_data: pd.DataFrame) -> str:
        """Fallback анализ позиционирования"""
        
        if market_data.empty:
            return f"Для анализа позиционирования {name} требуются данные о рынке"
        
        restaurant_data = market_data[market_data['restaurant_name'] == name]
        if restaurant_data.empty:
            return f"Ресторан {name} не найден в данных рынка"
        
        position = restaurant_data.index[0] + 1
        total = len(market_data)
        
        return f"""
        Позиция {name} на рынке: #{position} из {total}
        
        РЕКОМЕНДАЦИИ:
        • Изучить стратегии топ-3 ресторанов
        • Улучшить слабые стороны относительно лидеров
        • Найти уникальную нишу для дифференциации
        """
    
    def _get_fallback_strategies(self, data: Dict[str, Any]) -> List[str]:
        """Fallback рекламные стратегии"""
        
        strategies = [
            "🎯 Таргетированная реклама на туристов в соцсетях",
            "🍔 Промо-акции в низкосезонные периоды", 
            "⭐ Программа лояльности для постоянных клиентов",
            "📱 Push-уведомления с персональными предложениями",
            "🌟 Партнерство с отелями и гестхаусами"
        ]
        
        return strategies
    
    def _get_fallback_predictions(self) -> Dict[str, Any]:
        """Fallback предсказания"""
        
        return {
            'growth_forecast': "Прогноз требует больше исторических данных",
            'seasonal_trends': "Анализ сезонности доступен с OpenAI API",
            'recommendations': [
                "Собрать больше исторических данных",
                "Отслеживать сезонные паттерны",
                "Мониторить конкурентов"
            ]
        }
    
    def _prepare_market_summary(self, restaurant_name: str, market_data: pd.DataFrame) -> str:
        """Подготавливает сводку по рынку"""
        
        if market_data.empty:
            return "Данные о рынке недоступны"
        
        # Топ-5 ресторанов
        top_5 = market_data.head(5)[['restaurant_name', 'total_sales', 'avg_rating']].to_string()
        
        # Позиция текущего ресторана
        restaurant_row = market_data[market_data['restaurant_name'] == restaurant_name]
        position = restaurant_row.index[0] + 1 if not restaurant_row.empty else "Не найден"
        
        return f"""
        ТОП-5 РЕСТОРАНОВ:
        {top_5}
        
        ПОЗИЦИЯ {restaurant_name}: #{position}
        """
    
    def _analyze_historical_patterns(self, data: pd.DataFrame) -> str:
        """Анализирует исторические паттерны"""
        
        if data.empty:
            return "Исторические данные недоступны"
        
        # Основная статистика
        avg_sales = data['total_sales'].mean()
        growth_trend = "растущий" if data['total_sales'].iloc[-1] > data['total_sales'].iloc[0] else "падающий"
        
        return f"""
        Средние продажи: {avg_sales:,.0f} IDR
        Общий тренд: {growth_trend}
        Периодов данных: {len(data)}
        """
    
    def _parse_strategies(self, response_text: str) -> List[str]:
        """Парсит стратегии из ответа AI"""
        
        # Простой парсинг по строкам
        lines = response_text.split('\n')
        strategies = []
        
        for line in lines:
            if any(marker in line for marker in ['1.', '2.', '3.', '4.', '5.', '-', '•']):
                strategies.append(line.strip())
        
        return strategies[:5] if strategies else self._get_fallback_strategies({})
    
    def _parse_predictions(self, response_text: str) -> Dict[str, Any]:
        """Парсит предсказания из ответа AI"""
        
        return {
            'forecast_summary': response_text[:500],
            'confidence': 'medium',
            'recommendations': ['Мониторить тренды', 'Адаптировать стратегию']
        }