#!/usr/bin/env python3
"""
AI Explainer API Integration для генерации понятных объяснений и рекомендаций
"""

import openai
import json
from datetime import datetime
from typing import Dict, List, Optional

class AIExplainer:
    """Сервис для генерации объяснений с помощью OpenAI API"""
    
    def __init__(self, api_key: str, model: str = "gpt-4"):
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model
        
        # Шаблоны промптов для разных типов анализа
        self.prompts = {
            'sales_analysis': """
Ты - эксперт по анализу продаж ресторанов доставки на Бали. 
Проанализируй данные и дай понятное объяснение с рекомендациями для менеджера ресторана.

ДАННЫЕ:
- Ресторан: {restaurant_name} ({region})
- Дата: {date}
- Фактические продажи: {actual_sales:,.0f} IDR
- Прогноз модели: {predicted_sales:,.0f} IDR
- Отклонение: {difference_percent:+.1f}%

ФАКТОРЫ:
- Погода: {weather_summary}
- Влияние погоды: {weather_impact:+.1f}%
- Праздники: {holidays}
- Влияние праздников: {holiday_impact:+.1f}%
- Заказов: {orders}
- Средний чек: {avg_order_value:,.0f} IDR
- Реклама: {ads_status}
- Рейтинг: {rating}
- Время доставки: {delivery_time} мин

ИНСТРУКЦИЯ:
1. Объясни причины отклонения от прогноза простым языком
2. Укажи главные факторы влияния
3. Дай 3-5 конкретных рекомендаций для менеджера
4. Используй эмодзи для наглядности
5. Пиши на русском языке

Формат ответа:
📊 АНАЛИЗ ПРОДАЖ
[Объяснение причин]

🎯 ГЛАВНЫЕ ФАКТОРЫ
[Список факторов]

💡 РЕКОМЕНДАЦИИ
[Конкретные действия]
""",
            
            'weekly_summary': """
Ты - аналитик продаж ресторанов. Создай еженедельный отчет с анализом трендов и прогнозом.

ДАННЫЕ ЗА НЕДЕЛЮ:
{weekly_data}

ИНСТРУКЦИЯ:
1. Проанализируй тренды продаж
2. Выдели лучшие и худшие дни
3. Объясни причины колебаний
4. Дай прогноз на следующую неделю
5. Предложи стратегические рекомендации

Формат ответа:
📈 ЕЖЕНЕДЕЛЬНЫЙ АНАЛИЗ
[Общий тренд]

📊 ДЕТАЛЬНЫЙ АНАЛИЗ
[По дням недели]

🔮 ПРОГНОЗ НА СЛЕДУЮЩУЮ НЕДЕЛЮ
[Ожидания]

🎯 СТРАТЕГИЧЕСКИЕ РЕКОМЕНДАЦИИ
[Долгосрочные действия]
""",
            
            'emergency_alert': """
Ты - система мониторинга продаж. Создай экстренное уведомление для менеджера.

КРИТИЧЕСКАЯ СИТУАЦИЯ:
- Ресторан: {restaurant_name}
- Отклонение: {difference_percent:+.1f}%
- Причина: {main_reason}
- Время: {timestamp}

ИНСТРУКЦИЯ:
1. Кратко объясни проблему
2. Укажи срочность (1-5)
3. Дай немедленные действия
4. Предложи план восстановления

Формат ответа:
🚨 ЭКСТРЕННОЕ УВЕДОМЛЕНИЕ
[Краткое описание]

⚡ НЕМЕДЛЕННЫЕ ДЕЙСТВИЯ
[Что делать сейчас]

🔧 ПЛАН ВОССТАНОВЛЕНИЯ
[Шаги для решения]
"""
        }
    
    def generate_sales_analysis(self, data: Dict) -> Dict:
        """Генерирует анализ продаж с объяснениями"""
        try:
            # Подготавливаем данные для промпта
            prompt_data = self._prepare_sales_data(data)
            
            # Генерируем объяснение
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Ты - эксперт по анализу продаж ресторанов в Бали."},
                    {"role": "user", "content": self.prompts['sales_analysis'].format(**prompt_data)}
                ],
                temperature=0.7,
                max_tokens=1000
            )
            
            explanation = response.choices[0].message.content
            
            # Извлекаем рекомендации
            recommendations = self._extract_recommendations(explanation)
            
            # Определяем уровень критичности
            criticality = self._calculate_criticality(data)
            
            return {
                'explanation': explanation,
                'recommendations': recommendations,
                'criticality': criticality,
                'confidence': self._calculate_confidence(data),
                'generated_at': datetime.now().isoformat(),
                'tokens_used': response.usage.total_tokens
            }
            
        except Exception as e:
            return self._generate_fallback_analysis(data, str(e))
    
    def generate_weekly_summary(self, weekly_data: List[Dict]) -> Dict:
        """Генерирует еженедельный отчет"""
        try:
            # Подготавливаем данные
            summary_data = self._prepare_weekly_data(weekly_data)
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Ты - аналитик продаж ресторанов."},
                    {"role": "user", "content": self.prompts['weekly_summary'].format(
                        weekly_data=json.dumps(summary_data, ensure_ascii=False, indent=2)
                    )}
                ],
                temperature=0.6,
                max_tokens=1500
            )
            
            summary = response.choices[0].message.content
            
            return {
                'summary': summary,
                'key_insights': self._extract_key_insights(summary),
                'action_items': self._extract_action_items(summary),
                'generated_at': datetime.now().isoformat(),
                'tokens_used': response.usage.total_tokens
            }
            
        except Exception as e:
            return self._generate_fallback_summary(weekly_data, str(e))
    
    def generate_emergency_alert(self, data: Dict) -> Dict:
        """Генерирует экстренное уведомление"""
        try:
            # Определяем основную причину проблемы
            main_reason = self._identify_main_issue(data)
            
            prompt_data = {
                'restaurant_name': data.get('restaurant_name', 'Unknown'),
                'difference_percent': data.get('sales_difference_percent', 0),
                'main_reason': main_reason,
                'timestamp': datetime.now().strftime('%H:%M %d.%m.%Y')
            }
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Ты - система мониторинга продаж."},
                    {"role": "user", "content": self.prompts['emergency_alert'].format(**prompt_data)}
                ],
                temperature=0.3,  # Более консервативная генерация для алертов
                max_tokens=500
            )
            
            alert = response.choices[0].message.content
            
            return {
                'alert': alert,
                'urgency': self._calculate_urgency(data),
                'immediate_actions': self._extract_immediate_actions(alert),
                'generated_at': datetime.now().isoformat(),
                'tokens_used': response.usage.total_tokens
            }
            
        except Exception as e:
            return self._generate_fallback_alert(data, str(e))
    
    def generate_custom_analysis(self, data: Dict, custom_prompt: str) -> Dict:
        """Генерирует анализ по кастомному промпту"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Ты - эксперт по анализу ресторанного бизнеса."},
                    {"role": "user", "content": f"{custom_prompt}\n\nДанные: {json.dumps(data, ensure_ascii=False, indent=2)}"}
                ],
                temperature=0.7,
                max_tokens=1000
            )
            
            return {
                'analysis': response.choices[0].message.content,
                'generated_at': datetime.now().isoformat(),
                'tokens_used': response.usage.total_tokens
            }
            
        except Exception as e:
            return {'error': str(e), 'analysis': 'Ошибка генерации анализа'}
    
    def _prepare_sales_data(self, data: Dict) -> Dict:
        """Подготавливает данные для промпта анализа продаж"""
        return {
            'restaurant_name': data.get('restaurant_name', 'Unknown'),
            'region': data.get('region', 'Unknown'),
            'date': data.get('date', datetime.now().strftime('%Y-%m-%d')),
            'actual_sales': data.get('actual_sales', 0),
            'predicted_sales': data.get('predicted_sales', 0),
            'difference_percent': data.get('sales_difference_percent', 0),
            'weather_summary': data.get('weather', {}).get('summary', 'Данные недоступны'),
            'weather_impact': data.get('weather', {}).get('impact_percent', 0),
            'holidays': ', '.join(data.get('holidays', {}).get('current_holidays', ['Нет'])),
            'holiday_impact': data.get('holidays', {}).get('impact_percent', 0),
            'orders': data.get('analysis', {}).get('orders', 0),
            'avg_order_value': data.get('analysis', {}).get('avg_order_value', 0),
            'ads_status': 'Включена' if data.get('analysis', {}).get('ads_enabled', False) else 'Выключена',
            'rating': data.get('analysis', {}).get('rating', 0),
            'delivery_time': data.get('analysis', {}).get('delivery_time', 0)
        }
    
    def _prepare_weekly_data(self, weekly_data: List[Dict]) -> Dict:
        """Подготавливает недельные данные"""
        total_sales = sum(day.get('actual_sales', 0) for day in weekly_data)
        avg_sales = total_sales / len(weekly_data) if weekly_data else 0
        
        best_day = max(weekly_data, key=lambda x: x.get('actual_sales', 0)) if weekly_data else {}
        worst_day = min(weekly_data, key=lambda x: x.get('actual_sales', 0)) if weekly_data else {}
        
        return {
            'total_sales': total_sales,
            'avg_daily_sales': avg_sales,
            'best_day': {
                'date': best_day.get('date', ''),
                'sales': best_day.get('actual_sales', 0)
            },
            'worst_day': {
                'date': worst_day.get('date', ''),
                'sales': worst_day.get('actual_sales', 0)
            },
            'daily_breakdown': [
                {
                    'date': day.get('date', ''),
                    'sales': day.get('actual_sales', 0),
                    'deviation': day.get('sales_difference_percent', 0)
                }
                for day in weekly_data
            ]
        }
    
    def _extract_recommendations(self, text: str) -> List[str]:
        """Извлекает рекомендации из текста"""
        recommendations = []
        lines = text.split('\n')
        
        in_recommendations = False
        for line in lines:
            line = line.strip()
            if '💡 РЕКОМЕНДАЦИИ' in line or 'РЕКОМЕНДАЦИИ' in line:
                in_recommendations = True
                continue
            
            if in_recommendations and line and not line.startswith('📊') and not line.startswith('🎯'):
                if line.startswith('-') or line.startswith('•') or line.startswith('1.'):
                    recommendations.append(line.lstrip('- •123456789.').strip())
                elif len(line) > 10:  # Фильтруем короткие строки
                    recommendations.append(line)
        
        return recommendations[:5]  # Максимум 5 рекомендаций
    
    def _extract_key_insights(self, text: str) -> List[str]:
        """Извлекает ключевые инсайты"""
        insights = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if any(keyword in line.lower() for keyword in ['тренд', 'рост', 'падение', 'причина']):
                if len(line) > 20:
                    insights.append(line)
        
        return insights[:3]
    
    def _extract_action_items(self, text: str) -> List[str]:
        """Извлекает пункты действий"""
        actions = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if any(keyword in line.lower() for keyword in ['рекомендую', 'следует', 'нужно', 'стоит']):
                if len(line) > 15:
                    actions.append(line)
        
        return actions[:4]
    
    def _extract_immediate_actions(self, text: str) -> List[str]:
        """Извлекает немедленные действия"""
        actions = []
        lines = text.split('\n')
        
        in_immediate = False
        for line in lines:
            line = line.strip()
            if '⚡ НЕМЕДЛЕННЫЕ ДЕЙСТВИЯ' in line:
                in_immediate = True
                continue
            
            if in_immediate and line and not line.startswith('🔧'):
                if line.startswith('-') or line.startswith('•'):
                    actions.append(line.lstrip('- •').strip())
                elif len(line) > 10:
                    actions.append(line)
        
        return actions[:3]
    
    def _identify_main_issue(self, data: Dict) -> str:
        """Определяет основную проблему"""
        difference = abs(data.get('sales_difference_percent', 0))
        
        if difference > 30:
            return "Критическое отклонение продаж"
        elif difference > 20:
            return "Значительное отклонение продаж"
        elif difference > 10:
            return "Умеренное отклонение продаж"
        else:
            return "Незначительное отклонение продаж"
    
    def _calculate_criticality(self, data: Dict) -> int:
        """Рассчитывает уровень критичности (1-5)"""
        difference = abs(data.get('sales_difference_percent', 0))
        
        if difference > 40:
            return 5  # Критично
        elif difference > 25:
            return 4  # Высокая важность
        elif difference > 15:
            return 3  # Средняя важность
        elif difference > 5:
            return 2  # Низкая важность
        else:
            return 1  # Информационно
    
    def _calculate_urgency(self, data: Dict) -> int:
        """Рассчитывает срочность (1-5)"""
        difference = abs(data.get('sales_difference_percent', 0))
        
        if difference > 35:
            return 5  # Немедленно
        elif difference > 25:
            return 4  # В течение часа
        elif difference > 15:
            return 3  # В течение дня
        elif difference > 10:
            return 2  # В течение недели
        else:
            return 1  # Плановое
    
    def _calculate_confidence(self, data: Dict) -> float:
        """Рассчитывает уверенность в анализе"""
        base_confidence = 0.8
        
        # Учитываем наличие данных
        if data.get('weather'):
            base_confidence += 0.1
        if data.get('holidays'):
            base_confidence += 0.1
        if data.get('analysis', {}).get('orders', 0) > 0:
            base_confidence += 0.05
        
        return min(base_confidence, 1.0)
    
    def _generate_fallback_analysis(self, data: Dict, error: str) -> Dict:
        """Генерирует анализ по умолчанию при ошибке API"""
        difference = data.get('sales_difference_percent', 0)
        
        if difference > 10:
            explanation = f"📊 Продажи превысили прогноз на {difference:.1f}%. Это хороший результат!"
            recommendations = ["Проанализируйте факторы успеха", "Повторите успешную стратегию"]
        elif difference < -10:
            explanation = f"📊 Продажи ниже прогноза на {abs(difference):.1f}%. Требуется анализ причин."
            recommendations = ["Проверьте качество сервиса", "Проанализируйте конкурентов"]
        else:
            explanation = f"📊 Продажи близки к прогнозу (отклонение {difference:+.1f}%)."
            recommendations = ["Продолжайте текущую стратегию"]
        
        return {
            'explanation': explanation,
            'recommendations': recommendations,
            'criticality': self._calculate_criticality(data),
            'confidence': 0.5,
            'generated_at': datetime.now().isoformat(),
            'error': f"Ошибка AI: {error}"
        }
    
    def _generate_fallback_summary(self, weekly_data: List[Dict], error: str) -> Dict:
        """Генерирует сводку по умолчанию"""
        total_sales = sum(day.get('actual_sales', 0) for day in weekly_data)
        
        return {
            'summary': f"📈 Общие продажи за неделю: {total_sales:,.0f} IDR",
            'key_insights': ["Данные обработаны"],
            'action_items': ["Проверьте детальную статистику"],
            'generated_at': datetime.now().isoformat(),
            'error': f"Ошибка AI: {error}"
        }
    
    def _generate_fallback_alert(self, data: Dict, error: str) -> Dict:
        """Генерирует алерт по умолчанию"""
        return {
            'alert': f"🚨 Обнаружено отклонение продаж: {data.get('sales_difference_percent', 0):+.1f}%",
            'urgency': self._calculate_urgency(data),
            'immediate_actions': ["Проверьте данные вручную"],
            'generated_at': datetime.now().isoformat(),
            'error': f"Ошибка AI: {error}"
        }

# Пример использования
def main():
    # Замените на ваш API ключ от OpenAI
    api_key = "YOUR_OPENAI_API_KEY"
    
    ai_explainer = AIExplainer(api_key)
    
    # Тестовые данные
    test_data = {
        'restaurant_name': 'Warung Bali Asli',
        'region': 'Seminyak',
        'date': '2024-01-15',
        'actual_sales': 5500000,
        'predicted_sales': 5000000,
        'sales_difference_percent': 10.0,
        'weather': {
            'summary': 'Ясно, 28°C',
            'impact_percent': 5.0
        },
        'holidays': {
            'current_holidays': [],
            'impact_percent': 0
        },
        'analysis': {
            'orders': 95,
            'avg_order_value': 57895,
            'ads_enabled': True,
            'rating': 4.2,
            'delivery_time': 28
        }
    }
    
    # Генерируем анализ
    result = ai_explainer.generate_sales_analysis(test_data)
    print("📊 АНАЛИЗ ПРОДАЖ:")
    print(result['explanation'])
    print(f"\n💡 Рекомендации: {len(result['recommendations'])}")
    for i, rec in enumerate(result['recommendations'], 1):
        print(f"{i}. {rec}")

if __name__ == "__main__":
    main()