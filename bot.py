import asyncio
import json
import os
import signal
import sys
from datetime import datetime
from dotenv import load_dotenv
import websockets
from telegram import Bot

load_dotenv()

# ====================== CẤU HÌNH ======================
SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt']

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

VOLUME_SPIKE_THRESHOLD = 8000
DELTA_PERCENT_THRESHOLD = 30

data = {sym: {'buy': 0.0, 'sell': 0.0, 'trades': 0, 'minute': None} for sym in SYMBOLS}

bot = Bot(token=TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None

async def send_telegram(message: str):
    if not bot or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram chưa cấu hình!")
        return
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='HTML')
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}")

async def handle_message(message: str):
    try:
        payload = json.loads(message)
        stream = payload.get('stream', '')
        d = payload.get('data', payload)

        symbol = stream.split('@')[0].lower() if '@' in stream else None
        if not symbol or symbol not in data:
            return

        qty = float(d['q'])
        is_buyer_maker = d['m']
        ts = int(d['T'])
        trade_minute = datetime.utcfromtimestamp(ts / 1000).replace(second=0, microsecond=0)

        current = data[symbol]

        if current['minute'] is None or trade_minute > current['minute']:
            if current['minute'] is not None and (current['buy'] + current['sell'] > 0):
                buy = current['buy']
                sell = current['sell']
                total = buy + sell
                delta = buy - sell
                delta_pct = (delta / total * 100) if total > 0 else 0
                time_str = current['minute'].strftime('%Y-%m-%d %H:%M')

                print(f"\n🕒 {time_str} UTC | {symbol.upper()} Futures")
                print(f"   Buy  : {buy:,.2f}")
                print(f"   Sell : {sell:,.2f}")
                print(f"   Delta: {delta:,.2f} ({delta_pct:+.1f}%)")
                print(f"   Total: {total:,.2f} | Trades: {current['trades']}")

                alert = f"""
🕒 <b>{time_str}</b> | <b>{symbol.upper()}</b> Futures

Buy : <b>{buy:,.0f}</b>
Sell: <b>{sell:,.0f}</b>
Delta: <b>{delta:,.0f}</b> (<b>{delta_pct:+.1f}%</b>)
Total: <b>{total:,.0f}</b>
                """.strip()

                if total > VOLUME_SPIKE_THRESHOLD or abs(delta_pct) > DELTA_PERCENT_THRESHOLD:
                    alert += "\n\n🔥 STRONG MOVEMENT"
                    await send_telegram(alert)
                elif abs(delta_pct) > 18:
                    await send_telegram(alert)

            current['buy'] = current['sell'] = 0.0
            current['trades'] = 0
            current['minute'] = trade_minute

        if is_buyer_maker:
            current['sell'] += qty
        else:
            current['buy'] += qty
        current['trades'] += 1

    except Exception as e:
        print(f"Lỗi xử lý: {e}")

async def main():
    streams = [f"{s}@trade" for s in SYMBOLS]
    uri = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
    
    print(f"🤖 Bot đang khởi động - Theo dõi {SYMBOLS}")
    
    reconnect = 0
    while reconnect < 50:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=30) as ws:
                reconnect = 0
                print("✅ Kết nối WebSocket thành công!")
                async for msg in ws:
                    await handle_message(msg)
        except Exception as e:
            reconnect += 1
            print(f"⚠️ Mất kết nối (lần {reconnect}), thử lại sau...")
            await asyncio.sleep(min(reconnect * 3, 30))

def graceful_shutdown(signum, frame):
    print("Nhận tín hiệu tắt từ Render...")
    sys.exit(0)

signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

if __name__ == "__main__":
    asyncio.run(main())