import asyncio
import json
import os
import signal
import sys
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import websockets
from telegram import Bot

load_dotenv()

# ====================== CẤU HÌNH ======================
SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt']

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

VOLUME_SPIKE_THRESHOLD = 100000      # Tổng volume > 8000 USDT → cảnh báo mạnh
DELTA_PERCENT_THRESHOLD = 30       # |Delta%| > 30% → cảnh báo mạnh

# Dữ liệu theo dõi (bây giờ lưu theo USDT)
data = {sym: {'buy': 0.0, 'sell': 0.0, 'trades': 0, 'minute': None} for sym in SYMBOLS}

bot = Bot(token=TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None


async def send_telegram(message: str):
    if not bot or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram chưa được cấu hình!")
        return
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='HTML')
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}")


async def handle_message(message: str):
    try:
        payload = json.loads(message)
        d = payload.get('data', payload)

        # Lấy symbol
        stream = payload.get('stream', '')
        symbol = stream.split('@')[0].lower() if '@' in stream else None
        if not symbol or symbol not in data:
            return

        qty = float(d['q'])
        price = float(d['p'])                    # ← THÊM DÒNG NÀY
        volume_usdt = price * qty                # ← Tính volume theo USDT
        is_buyer_maker = d['m']
        ts = int(d['T'])

        # ==================== CHUYỂN VỀ GIỜ VIỆT NAM (+7) ====================
        trade_time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        vn_time = trade_time.astimezone(timezone(timedelta(hours=7)))
        trade_minute = vn_time.replace(second=0, microsecond=0)

        current = data[symbol]

        # Khi sang phút mới → xử lý báo cáo phút trước
        if current['minute'] is None or trade_minute > current['minute']:
            if current['minute'] is not None and (current['buy'] + current['sell'] > 0):
                buy = current['buy']
                sell = current['sell']
                total = buy + sell
                delta = buy - sell
                delta_pct = (delta / total * 100) if total > 0 else 0
                buy_pct = (buy / total * 100) if total > 0 else 0
                sell_pct = (sell / total * 100) if total > 0 else 0

                time_str = current['minute'].strftime('%Y-%m-%d %H:%M')

                print(f"\n🕒 {time_str} (VN) | {symbol.upper()} Futures")
                print(f"   Buy  : {buy:,.2f} USDT ({buy_pct:5.1f}%)")
                print(f"   Sell : {sell:,.2f} USDT ({sell_pct:5.1f}%)")
                print(f"   Total: {total:,.2f} USDT | Delta: {delta:,.2f} ({delta_pct:+.1f}%) | Trades: {current['trades']}")

                # ==================== TIN NHẮN TELEGRAM ====================
                alert = f"""
🕒 <b>{time_str}</b> 🇻🇳 | <b>{symbol.upper()}</b> Perpetual

<b>BUY</b>  : <b>{buy:,.0f}</b> USDT  (<b>{buy_pct:.1f}%</b>)
<b>SELL</b> : <b>{sell:,.0f}</b> USDT  (<b>{sell_pct:.1f}%</b>)
<b>TOTAL</b>: <b>{total:,.0f}</b> USDT

Delta: <b>{delta:,.0f}</b> USDT (<b>{delta_pct:+.1f}%</b>)
Trades: <b>{current['trades']}</b>
                """.strip()

                if total > VOLUME_SPIKE_THRESHOLD or abs(delta_pct) > DELTA_PERCENT_THRESHOLD:
                    alert += "\n\n🔥 <b>STRONG MOVEMENT DETECTED!</b>"
                    await send_telegram(alert)
                elif abs(delta_pct) > 18:
                    await send_telegram(alert)

            # Reset dữ liệu cho phút mới
            current['buy'] = current['sell'] = 0.0
            current['trades'] = 0
            current['minute'] = trade_minute

        # Cộng dồn volume theo USDT
        if is_buyer_maker:
            current['sell'] += volume_usdt      # Taker bán
        else:
            current['buy'] += volume_usdt       # Taker mua

        current['trades'] += 1

    except Exception as e:
        print(f"Lỗi xử lý message: {e}")


async def main():
    streams = [f"{s}@trade" for s in SYMBOLS]
    uri = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
    
    print(f"🤖 Bot Volume Delta (USDT) đang chạy - Theo dõi: {SYMBOLS}")
    print(f"📍 Múi giờ: Việt Nam (GMT+7)\n")

    ws_task = asyncio.create_task(run_websocket(uri))
    await keep_alive()


# Các hàm run_websocket, keep_alive, graceful_shutdown giữ nguyên như cũ
async def run_websocket(uri):
    reconnect = 0
    while reconnect < 50:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=30) as ws:
                reconnect = 0
                print("✅ Kết nối WebSocket Binance Futures thành công!")
                async for msg in ws:
                    await handle_message(msg)
        except Exception as e:
            reconnect += 1
            print(f"⚠️ Mất kết nối (lần {reconnect}), thử kết nối lại sau {min(reconnect*3, 30)}s...")
            await asyncio.sleep(min(reconnect * 3, 30))


async def keep_alive():
    from aiohttp import web
    async def health(request):
        return web.Response(text="Bot Volume Delta (USDT) is running - " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    app = web.Application()
    app.router.add_get('/', health)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 10000)))
    await site.start()
    print(f"🌐 Keep-alive server đang chạy trên port {os.environ.get('PORT', 10000)}")
    
    await asyncio.Event().wait()


def graceful_shutdown(signum, frame):
    print("Nhận tín hiệu tắt từ Render...")
    sys.exit(0)


signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

if __name__ == "__main__":
    asyncio.run(main())