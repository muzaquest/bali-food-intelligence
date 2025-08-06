import sqlite3
import json

def check_ads_and_holidays():
    # Проверяем рекламу и ROAS
    with sqlite3.connect('database.sqlite') as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT 
            g.stat_date, 
            g.ads_spend as grab_ads_spend, 
            g.ads_sales as grab_ads_sales,
            gj.ads_spend as gojek_ads_spend, 
            gj.ads_sales as gojek_ads_sales,
            g.sales as grab_sales,
            gj.sales as gojek_sales
        FROM grab_stats g 
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE r.name = "Ika Canggu" AND g.stat_date = "2025-04-18"
        ''')
        
        result = cursor.fetchone()
        
        if result:
            print("🎯 ПРОВЕРКА РЕКЛАМЫ IKA CANGGU 18 АПРЕЛЯ 2025:")
            print("=" * 60)
            
            grab_ads_spend = result[1] or 0
            grab_ads_sales = result[2] or 0
            gojek_ads_spend = result[3] or 0
            gojek_ads_sales = result[4] or 0
            grab_sales = result[5] or 0
            gojek_sales = result[6] or 0
            
            print(f"📊 GRAB:")
            print(f"   💰 Общие продажи: {grab_sales:,} IDR")
            print(f"   📢 Реклама потрачено: {grab_ads_spend:,} IDR")
            print(f"   📈 Продажи от рекламы: {grab_ads_sales:,} IDR")
            if grab_ads_spend > 0:
                grab_roas = grab_ads_sales / grab_ads_spend
                print(f"   🎯 ROAS: {grab_roas:.2f}")
                if grab_roas >= 3:
                    print("   ✅ ROAS отличный!")
                elif grab_roas >= 2:
                    print("   🟢 ROAS хороший")
                else:
                    print("   🟡 ROAS низкий")
            else:
                print("   ❌ Реклама не работала")
                
            print(f"\n📊 GOJEK:")
            print(f"   💰 Общие продажи: {gojek_sales:,} IDR")
            print(f"   📢 Реклама потрачено: {gojek_ads_spend:,} IDR")
            print(f"   📈 Продажи от рекламы: {gojek_ads_sales:,} IDR")
            if gojek_ads_spend > 0:
                gojek_roas = gojek_ads_sales / gojek_ads_spend
                print(f"   🎯 ROAS: {gojek_roas:.2f}")
                if gojek_roas >= 3:
                    print("   ✅ ROAS отличный!")
                elif gojek_roas >= 2:
                    print("   🟢 ROAS хороший")
                else:
                    print("   🟡 ROAS низкий")
            else:
                print("   ❌ Реклама не работала")
        else:
            print("❌ Нет данных")
    
    # Проверяем праздники
    print("\n🎉 ПРОВЕРКА ПРАЗДНИКОВ 18 АПРЕЛЯ 2025:")
    print("=" * 60)
    
    try:
        with open('data/comprehensive_holiday_analysis.json', 'r', encoding='utf-8') as f:
            holidays_data = json.load(f)
            
        target_date = "2025-04-18"
        holidays_found = []
        
        for holiday in holidays_data:
            if holiday.get('date') == target_date:
                holidays_found.append(holiday)
        
        if holidays_found:
            print("🚨 НАЙДЕНЫ ПРАЗДНИКИ:")
            for holiday in holidays_found:
                print(f"   🎉 {holiday['name']} ({holiday['type']})")
                if holiday.get('description'):
                    print(f"      📝 {holiday['description']}")
        else:
            print("✅ Праздников не найдено - обычный день")
            
    except Exception as e:
        print(f"❌ Ошибка при проверке праздников: {e}")

if __name__ == "__main__":
    check_ads_and_holidays()