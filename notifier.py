# -*- coding: utf-8 -*-
import os
import resend
from datetime import datetime

def send_stock_report(market_name, img_data, report_df, text_reports, stats=None):
    """
    發送包含分布圖、智慧技術線圖連結及【數據下載統計】的專業電子郵件
    支援市場：台灣 (TW), 美國 (US), 香港 (HK), 中國 (CN), 日本 (JP), 韓國 (KR)
    """
    # 1. 檢查 API Key
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        print("❌ 錯誤：找不到環境變數 RESEND_API_KEY，郵件發送中斷。")
        return
    resend.api_key = api_key

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # 2. 判斷市場屬性（智慧識別六大市場）
    market_upper = market_name.upper()
    is_us = any(x in market_upper for x in ["美國", "US"])
    is_hk = any(x in market_upper for x in ["香港", "HK"])
    is_cn = any(x in market_upper for x in ["中國", "CN"])
    is_tw = any(x in market_upper for x in ["台灣", "TW"])
    is_jp = any(x in market_upper for x in ["日本", "JP"])
    is_kr = any(x in market_upper for x in ["韓國", "KR"])

    # 3. 建立數據健康度儀表板 HTML
    health_html = ""
    if stats:
        total = stats.get("total", 0)
        success = stats.get("success", 0)
        # 防止除以零
        rate = (success / total * 100) if total > 0 else 0
        
        # 顏色邏輯：成功率低於 85% 顯示橘色，低於 70% 顯示紅色
        status_color = "#27ae60" # 綠色
        status_text = "✅ 數據完整度優良"
        if rate < 85:
            status_color = "#f39c12" # 橘色
            status_text = "⚠️ 部分數據缺失"
        if rate < 70:
            status_color = "#e74c3c" # 紅色
            status_text = "🚨 數據嚴重缺失 (建議重跑)"

        health_html = f"""
        <div style="background-color: #f8f9fa; border: 1px solid #e9ecef; padding: 15px; border-radius: 8px; margin: 20px 0; display: flex; align-items: center;">
            <div style="flex: 1; text-align: center; border-right: 1px solid #dee2e6;">
                <div style="font-size: 12px; color: #6c757d; margin-bottom: 5px;">市場標的總數</div>
                <div style="font-size: 20px; font-weight: bold; color: #2c3e50;">{total}</div>
            </div>
            <div style="flex: 1; text-align: center; border-right: 1px solid #dee2e6;">
                <div style="font-size: 12px; color: #6c757d; margin-bottom: 5px;">成功下載檔案</div>
                <div style="font-size: 20px; font-weight: bold; color: {status_color};">{success}</div>
            </div>
            <div style="flex: 1; text-align: center; border-right: 1px solid #dee2e6;">
                <div style="font-size: 12px; color: #6c757d; margin-bottom: 5px;">成功率</div>
                <div style="font-size: 20px; font-weight: bold; color: {status_color};">{rate:.1f}%</div>
            </div>
            <div style="flex: 1.5; text-align: center; padding-left: 10px;">
                <div style="font-size: 14px; font-weight: bold; color: {status_color};">{status_text}</div>
            </div>
        </div>
        """

    # 4. 建立 Top 50 連結區塊邏輯
    def get_top50_links(df, col_name):
        if col_name not in df.columns:
            return "目前無數據"
        
        top50 = df.sort_values(by=col_name, ascending=False).head(50)
        links = []
        
        for _, r in top50.iterrows():
            ticker = str(r["Ticker"])
            if is_us:
                url = f"https://stockcharts.com/sc3/ui/?s={ticker}"
            elif is_hk:
                clean_code = ticker.replace(".HK", "").strip().zfill(5)
                url = f"https://www.aastocks.com/tc/stocks/quote/quick-quote.aspx?symbol={clean_code}"
            elif is_cn:
                prefix = "sh" if ticker.startswith('6') else "sz"
                url = f"https://quote.eastmoney.com/{prefix}{ticker}.html"
            elif is_jp:
                clean_ticker = ticker if ".T" in ticker.upper() else f"{ticker.split('.')[0]}.T"
                url = f"https://www.rakuten-sec.co.jp/web/market/search/quote.html?ric={clean_ticker}"
            elif is_kr:
                clean_code = ticker.split('.')[0]
                url = f"https://finance.naver.com/item/main.naver?code={clean_code}"
            elif is_tw:
                clean_tkr = ticker.split('.')[0]
                url = f"https://www.wantgoo.com/stock/{clean_tkr}/technical-chart"
            else:
                clean_tkr = ticker.split('.')[0]
                url = f"https://www.wantgoo.com/stock/{clean_tkr}/technical-chart"
            
            display_name = r.get("Full_Name", ticker)
            links.append(f'<a href="{url}" style="text-decoration:none; color:#0366d6;">{ticker}({display_name})</a>')
        
        return " | ".join(links)

    target_site = 'StockCharts' if is_us else 'AASTOCKS' if is_hk else '東方財富' if is_cn else '樂天證券' if is_jp else 'Naver Finance' if is_kr else '玩股網'
    
    # 5. 組合 HTML 郵件內容
    html_content = f"""
    <div style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; max-width: 850px; margin: auto; border: 1px solid #eee; padding: 20px; border-radius: 10px;">
        <h2 style="color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; margin-bottom: 10px;">
            🚀 {market_name} 全方位市場監控報表
        </h2>
        <p style="color: #7f8c8d; font-size: 14px; margin-bottom: 20px;">報告生成時間: {now_str}</p>
        
        {health_html}  <div style="background-color: #fdfefe; border-left: 5px solid #e74c3c; padding: 10px; margin: 20px 0; font-size: 14px;">
            💡 提示：點擊下方表格中的<b>股票代號</b>，可直接跳轉至 <b>{target_site}</b> 查看即時技術線圖。
        </div>
    """
    
    # 插入 9 張分布圖
    for img in img_data:
        html_content += f"<h3 style='color: #2980b9; margin-top: 30px;'>📍 {img['label']}</h3>"
        html_content += f'<img src="cid:{img["id"]}" style="width:100%; max-width:800px; border-radius: 5px; box-shadow: 0 4px 8px rgba(0,0,0,0.1);">'

    # 插入分箱清單文字
    html_content += "<div style='background-color: #f4f7f6; padding: 15px; border-radius: 8px; margin-top: 40px;'>"
    for period, report in text_reports.items():
        p_name = {"Week": "週", "Month": "月", "Year": "年"}.get(period, period)
        html_content += f"<h4 style='color: #16a085; margin-bottom: 5px;'>📊 {p_name}K 報酬分布明細 (含飆股清單)</h4>"
        html_content += f"<pre style='background-color: #ffffff; padding: 10px; border: 1px solid #ddd; font-size: 12px; white-space: pre-wrap; word-wrap: break-word;'>{report}</pre>"
    html_content += "</div>"

    # 插入 Top 50 飆股區塊
    html_content += f"""
        <hr style="border: 0; border-top: 1px solid #eee; margin: 40px 0;">
        <h4 style="color: #c0392b;">🔥 本週表現最強動能 Top 50 (點擊跳轉線圖)</h4>
        <div style="line-height: 2; font-size: 13px; color: #34495e;">
            {get_top50_links(report_df, 'Week_High')}
        </div>
        <p style="margin-top: 50px; font-size: 12px; color: #bdc3c7; text-align: center;">
            此報表由系統自動生成，僅供研究參考。
        </p>
    </div>
    """

    # 6. 準備附件 (Inline Embedding)
    attachments = []
    for img in img_data:
        try:
            with open(img['path'], "rb") as f:
                attachments.append({
                    "content": list(f.read()),
                    "filename": f"{img['id']}.png",
                    "content_id": img['id'],
                    "disposition": "inline"
                })
        except Exception as e:
            print(f"⚠️ 讀取圖片失敗 {img['path']}: {e}")

    # 7. 執行寄送
    to_emails = ["grissomlin643@gmail.com"]

    try:
        resend.Emails.send({
            "from": "StockMonitor <onboarding@resend.dev>",
            "to": to_emails,
            "subject": f"🚀 {market_name} 監控報告 - {now_str}",
            "html": html_content,
            "attachments": attachments
        })
        print(f"✅ 郵件發送成功！市場：{market_name}")
    except Exception as e:
        print(f"❌ 郵件發送失敗 ({market_name}): {e}")
