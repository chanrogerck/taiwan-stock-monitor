# -*- coding: utf-8 -*-
import os, sys, time, random, json, subprocess
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "cn-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")
CACHE_LIST_PATH = os.path.join(LIST_DIR, "cn_stock_list_cache.json")

# 🛡️ 穩定性優先：保持 4 個執行緒，避免觸發封鎖
THREADS_CN = 4 
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

# 💡 定義數據過期時間 (3600 秒 = 1 小時)
DATA_EXPIRY_SECONDS = 3600

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_pkg(pkg: str):
    """自動檢查並安裝必要的套件"""
    try:
        __import__(pkg)
    except ImportError:
        log(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

def get_cn_list():
    """獲取 A 股清單：整合 EM 接口與多重保底機制"""
    ensure_pkg("akshare")
    import akshare as ak
    threshold = 4500  
    
    # 1. 檢查今日快取
    if os.path.exists(CACHE_LIST_PATH):
        try:
            file_mtime = os.path.getmtime(CACHE_LIST_PATH)
            if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
                with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if len(data) >= threshold:
                        log(f"📦 載入今日快取 (共 {len(data)} 檔)")
                        return data
        except Exception:
            pass

    log("📡 嘗試從 Akshare EM 接口獲取清單...")
    try:
        df_sh = ak.stock_sh_a_spot_em()
        df_sz = ak.stock_sz_a_spot_em()
        df = pd.concat([df_sh, df_sz], ignore_index=True)
        
        df['code'] = df['代码'].astype(str).str.zfill(6)
        valid_prefixes = ('000','001','002','003','300','301','600','601','603','605','688')
        df = df[df['code'].str.startswith(valid_prefixes)]
        
        name_col = '名称' if '名称' in df.columns else '名稱'
        res = [f"{row['code']}&{row[name_col]}" for _, row in df.iterrows()]
        
        if len(res) >= threshold:
            with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False)
            log(f"✅ 成功獲取 {len(res)} 檔標的")
            return res
    except Exception as e:
        log(f"⚠️ EM 接口失敗: {e}")

    if os.path.exists(CACHE_LIST_PATH):
        with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    return ["600519&貴州茅台", "000001&平安銀行", "300750&寧德時代"]

def download_one(item):
    """單檔下載邏輯：具備時效檢查與強化防封鎖"""
    try:
        code, name = item.split('&', 1)
        symbol = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        out_path = os.path.join(DATA_DIR, f"{code}_{name}.csv")

        # 💡 智慧時效檢查
        if os.path.exists(out_path):
            file_age = time.time() - os.path.getmtime(out_path)
            # 若檔案存在且小於 1 小時則跳過
            if file_age < DATA_EXPIRY_SECONDS and os.path.getsize(out_path) > 1000:
                return {"status": "exists", "code": code}

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 🛡️ 隨機延遲保護
                time.sleep(random.uniform(0.7, 1.5)) 
                
                tk = yf.Ticker(symbol)
                hist = tk.history(period="2y", timeout=25)
                
                if hist is not None and not hist.empty:
                    hist.reset_index(inplace=True)
                    hist.columns = [c.lower() for c in hist.columns]
                    if 'date' in hist.columns:
                        hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None)
                    
                    hist.to_csv(out_path, index=False, encoding='utf-8-sig')
                    return {"status": "success", "code": code}
                
                if attempt == max_retries - 1:
                    return {"status": "empty", "code": code}
                    
            except Exception:
                if attempt == max_retries - 1:
                    return {"status": "error", "code": code}
                time.sleep(random.randint(5, 12)) 
    except Exception:
        return {"status": "error", "code": item.split('&')[0]}
            
    return {"status": "error", "code": code}

def main():
    start_time = time.time()
    log("🇨🇳 中國 A 股同步器 (時效檢查模式)")
    
    items = get_cn_list()
    log(f"🚀 目標總數: {len(items)} 檔")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=THREADS_CN) as executor:
        futures = {executor.submit(download_one, it): it for it in items}
        pbar = tqdm(total=len(items), desc="下載進度")
        
        for f in as_completed(futures):
            res = f.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
        pbar.close()

    total_expected = len(items)
    effective_success = stats['success'] + stats['exists']
    fail_count = stats['error'] + stats['empty']

    download_stats = {
        "total": total_expected,
        "success": effective_success,
        "fail": fail_count
    }

    duration = (time.time() - start_time) / 60
    log(f"📊 執行報告: 成功(含效期內)={effective_success}, 失敗={fail_count}, 耗時={duration:.1f}分鐘")
    
    return download_stats

if __name__ == "__main__":
    main()
