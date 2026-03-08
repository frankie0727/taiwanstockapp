import pandas as pd
import requests
import io
import json
import time
from datetime import datetime, timedelta

def get_stock_history(code):
    """抓取單一股票最近一個月的 K 線數據"""
    print(f"   📈 抓取 {code} 的歷史行情...")
    try:
        # 證交所個股日收盤行情 API (本月)
        date_str = datetime.now().strftime('%Y%m%d')
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date_str}&stockNo={code}"
        r = requests.get(url, timeout=15)
        data = r.json()
        
        if data.get('stat') == 'OK':
            history = []
            # 欄位: 日期,成交股數,成交金額,開盤價,最高價,最低價,收盤價,漲跌價差,成交筆數
            for row in data['data'][-30:]: # 取最近 30 天
                try:
                    history.append({
                        'date': row[0],
                        'open': float(row[3].replace(',', '')),
                        'high': float(row[4].replace(',', '')),
                        'low': float(row[5].replace(',', '')),
                        'close': float(row[6].replace(',', '')),
                        'vol': int(int(row[1].replace(',', '')) / 1000)
                    })
                except: continue
            return history
    except Exception as e:
        print(f"   ⚠️ 抓取 {code} 歷史失敗: {e}")
    return []

def get_last_trading_day_info():
    print("🔍 正在尋找最近一個交易日的行情資料...")
    today = datetime.now()
    info_dict = {}
    found_date = ""

    for i in range(10):
        target_date = today - timedelta(days=i)
        date_str = target_date.strftime('%Y%m%d')
        date_str_tw = f"{target_date.year - 1911}/{target_date.strftime('%m/%d')}"
        
        try:
            twse_url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALLBUT0999"
            r = requests.get(twse_url, timeout=15)
            data = r.json()
            
            if data.get('stat') == 'OK':
                found_date = date_str
                for table in data.get('tables', []):
                    if "每日收盤行情" in table.get('title', ''):
                        for row in table.get('data', []):
                            code = str(row[0]).strip()
                            name = str(row[1]).strip()
                            try:
                                vol = int(int(row[2].replace(',', '')) / 1000)
                                price = float(row[12].replace(',', '')) if row[12] != '--' else 0
                                info_dict[code] = {'name': name, 'vol': vol, 'price': price}
                            except: continue
                        break
                
                # 同步抓上櫃
                tpex_url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_quotes_result.php?l=zh-tw&o=json&d={date_str_tw}"
                rt = requests.get(tpex_url, timeout=15)
                data_tpex = rt.json()
                if data_tpex.get('aaData'):
                    for row in data_tpex['aaData']:
                        code = str(row[0]).strip()
                        name = str(row[1]).strip()
                        try:
                            vol = int(int(row[7].replace(',', '')) / 1000)
                            price = float(row[2].replace(',', '')) if row[2] != '--' else 0
                            info_dict[code] = {'name': name, 'vol': vol, 'price': price}
                        except: continue
                break
        except: continue

    return info_dict, found_date

def scan_high_concentration():
    market_info, trading_date = get_last_trading_day_info()
    if not market_info: return

    print(f"🔄 正在從集保中心抓取股權數據...")
    url = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
    df = pd.read_csv(io.StringIO(response.text))
        
    df.columns = df.columns.str.strip()
    df['證券代號'] = df['證券代號'].astype(str).str.strip()
    df['持股分級'] = df['持股分級'].astype(str).str.strip()
    df = df[df['證券代號'].str.match(r'^\d{4}$')]
    tdcc_date = str(df['資料日期'].iloc[0])
    
    level_17 = df[df['持股分級'] == '17'][['證券代號', '人數', '股數']].rename(columns={'人數': '總人數', '股數': '總股數'})
    level_15 = df[df['持股分級'] == '15'][['證券代號', '股數']].rename(columns={'股數': '大戶股數'})
    
    market = pd.merge(level_17, level_15, on='證券代號', how='left').fillna(0)
    market['大戶比例'] = round((market['大戶股數'].astype(float) / market['總股數'].astype(float)) * 100, 2)
    
    results = []
    # 篩選大戶 > 70 且 人數 > 100 且 量 > 2000
    candidates = market[(market['大戶比例'] >= 70.0) & (market['總人數'] > 100)].copy()
    
    print(f"🔎 發現 {len(candidates)} 檔初步達標股票，準備抓取歷史行情...")

    processed_count = 0
    for _, row in candidates.iterrows():
        code = row['證券代號']
        info = market_info.get(code)
        
        if info and info['vol'] >= 2000:
            # 🚀 關鍵：抓取真實歷史 K 線
            history = get_stock_history(code)
            if history:
                results.append({
                    '股票代號': code,
                    '股票名稱': info['name'],
                    '總人數': int(row['總人數']),
                    '大戶比例': float(row['大戶比例']),
                    '今日成交量': info['vol'],
                    '今日收盤': info['price'],
                    'history': history # 真實 30 天走勢
                })
                processed_count += 1
                # 避免頻繁請求被封鎖
                time.sleep(1) 
            
            if processed_count >= 40: # 最多抓取前 40 檔，避免執行太久
                break
            
    results = sorted(results, key=lambda x: x['大戶比例'], reverse=True)
    for i, item in enumerate(results): item['排名'] = i + 1
        
    output_result = {
        '集保更新日期': tdcc_date,
        '最後交易日期': trading_date,
        '高集中度清單': results
    }
    
    with open('high_concentration_stocks.json', 'w', encoding='utf-8') as f:
        json.dump(output_result, f, ensure_ascii=False, indent=2)
        
    print(f"🎯 處理完畢！已產出 {len(results)} 檔含真實走勢的股票資料。")

if __name__ == "__main__":
    scan_high_concentration()
