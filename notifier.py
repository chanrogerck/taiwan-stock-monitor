# -*- coding: utf-8 -*-
import os
import requests
import resend
import pandas as pd
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        
        if self.resend_api_key:
            resend.api_key = self.resend_api_key

    def get_now_time_str(self):
        """獲取 UTC+8 台北時間"""
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_telegram(self, message):
        """發送 Telegram 即時簡報"""
        if not self.tg_token or not self.tg_chat_id:
            return False
        ts = self.get_now_time_str().split(" ")[1]
        full_message = f"{message}\n\n🕒 <i>Sent at {ts} (UTC+8)</i>"
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {"chat_id": self.tg_chat_id, "text": full_message, "parse_mode": "HTML"}
        try:
            requests.post(url, json=payload, timeout=10)
            return True
        except:
            return False

    def send_stock_report(self, market_name, img_data, report_df, text_reports, stats=None):
        """
        🚀 專業版更新：包含智慧快取統計、九張動能分布圖、以及玩股網跳轉功能
        """
        if not self.resend_api_key:
            print("⚠️ 缺少 Resend API Key，無法寄信。")
            return False

        report_time = self.get_now_time_str()
        
        # --- 1. 處理下載統計數據 ---
        total_count = stats.get('total', 'N/A') if stats else 'N/A'
        success_count = stats.get('success', len(report_df)) if stats else len(report_df)
        fail_count = stats.get('fail', 0) if stats else 0
        success_rate = f"{(success_count/total_count)*100:.1f}%" if isinstance(total_count, (int, float)) and total_count > 0 else "N/A"

        # --- 2. 構建 HTML 內容 ---
        html_content = f"""
        <html>
        <body style="font-family: 'Microsoft JhengHei', sans-serif; color: #333; line-height: 1.6;">
            <div style="max-width: 800px; margin: auto; border: 1px solid #ddd; border-top: 10px solid #28a745; border-radius: 10px; padding: 25px;">
                <h2 style="color: #1a73e8; border-bottom: 2px solid #eee; padding-bottom: 10px;">{market_name} 全方位監控報告</h2>
                <p style="color: #666;">生成時間: <b>{report_time} (台北時間)</b></p>
                
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0; display: flex; justify-content: space-around; border: 1px solid #eee; text-align: center;">
                    <div>
                        <div style="font-size: 12px; color: #888;">應收標的</div>
                        <div style="font-size: 18px; font-weight: bold;">{total_count}</div>
                    </div>
                    <div>
                        <div style="font-size: 12px; color: #888;">成功更新(含快取)</div>
                        <div style="font-size: 18px; font-weight: bold; color: #28a745;">{success_count}</div>
                    </div>
                    <div>
                        <div style="font-size: 12px; color: #888;">今日覆蓋率</div>
                        <div style="font-size: 18px; font-weight: bold; color: #1a73e8;">{success_rate}</div>
                    </div>
                </div>

                <p style="background-color: #fff9db; padding: 10px; border-left: 4px solid #fcc419; font-size: 14px; color: #666; margin: 20px 0;">
                    💡 <b>提示：</b>下方的數據報表若包含股票代號，點擊可直接跳轉至 
                    <a href="https://www.wantgoo.com/" target="_blank" style="color: #e67e22; text-decoration: none; font-weight: bold;">玩股網</a> 
                    查看即時技術線圖。
                </p>
        """

        # --- 3. 插入九張核心動能圖表 ---
        html_content += "<div style='margin-top: 30px;'>"
        for img in img_data:
            html_content += f"""
            <div style="margin-bottom: 40px; text-align: center; border-bottom: 1px dashed #eee; padding-bottom: 25px;">
                <h3 style="color: #2c3e50; text-align: left; font-size: 16px; border-left: 4px solid #3498db; padding-left: 10px;">📍 {img['label']}</h3>
                <img src="cid:{img['id']}" style="width: 100%; max-width: 750px; border-radius: 5px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); margin-top: 10px;">
            </div>
            """
        html_content += "</div>"

        # --- 4. 插入文字明細報表 ---
        html_content += "<div style='margin-top: 20px;'>"
        for period, report in text_reports.items():
            p_name = {"Week": "週", "Month": "月", "Year": "年"}.get(period, period)
            html_content += f"""
            <div style="margin-bottom: 20px;">
                <h4 style="color: #16a085; margin-bottom: 8px;">📊 {p_name} 報酬分布明細</h4>
                <pre style="background-color: #2d3436; color: #dfe6e9; padding: 15px; border-radius: 5px; font-size: 13px; white-space: pre-wrap; font-family: 'Courier New', monospace;">{report}</pre>
            </div>
            """
        html_content += "</div>"

        html_content += """
                <p style="margin-top: 40px; font-size: 12px; color: #999; text-align: center; border-top: 1px solid #eee; padding-top: 20px;">
                    此郵件由 Global Stock Monitor 自動發送。數據僅供參考，不構成投資建議。
                </p>
            </div>
        </body>
        </html>
        """

        # --- 5. 準備附件 (Inline Embedding) ---
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

        # --- 6. 執行寄送 ---
        try:
            resend.Emails.send({
                "from": "StockMonitor <onboarding@resend.dev>",
                "to": "grissomlin643@gmail.com",
                "subject": f"📊 {market_name} 全方位監控報表 - {report_time.split(' ')[0]}",
                "html": html_content,
                "attachments": attachments
            })
            print(f"✅ {market_name} 專業報表已寄送至電子信箱！")
            
            # Telegram 同步通知
            self.send_telegram(f"📊 <b>{market_name} 監控報表已送達</b>\n今日數據成功率: {success_rate}\n有效樣本: {success_count} 檔")
            return True
        except Exception as e:
            print(f"❌ 寄送失敗: {e}")
            return False
