"""
中継サーバー
-----------
ホスト(操作される側)とコントローラー(操作する側)の間でメッセージを橋渡しするだけの
軽量なWebSocketサーバーです。どちらも「このサーバーに自分から接続しにいく」だけなので、
ホスト・コントローラー側のPCは受信ポートを開ける必要がなく、管理者権限も不要です。

使い方:
  1. このファイルを Render / Railway / Fly.io などの無料枠、または自前のVPSにデプロイする。
  2. 起動すると ws://<デプロイ先のURL> (HTTPSが前提のサービスなら wss://) で待ち受けます。
  3. 同じ room 名・password を指定したホストとコントローラーだけが通信できます。

必要なライブラリ: pip install websockets
起動: python relay_server.py
(多くのPaaSはPORT環境変数でポート番号を渡してくるので、それに対応しています)
"""

import asyncio
import json
import os
import websockets

# room名 -> {"password": str, "host": websocket|None, "controllers": set()}
ROOMS: dict[str, dict] = {}


async def handler(ws):
    room_name = None
    role = None
    try:
        first_raw = await ws.recv()
        join = json.loads(first_raw)

        if join.get("type") != "join":
            await ws.close()
            return

        room_name = join.get("room")
        role = join.get("role")
        password = join.get("password", "")

        if not room_name or role not in ("host", "controller"):
            await ws.close()
            return

        room = ROOMS.setdefault(room_name, {"password": password, "host": None, "controllers": set()})

        if room["password"] != password:
            await ws.send(json.dumps({"type": "error", "message": "パスワードが違います"}))
            await ws.close()
            return

        if role == "host":
            room["host"] = ws
        else:
            room["controllers"].add(ws)

        async for raw in ws:
            if role == "host":
                # 画面フレーム -> 全コントローラーに転送
                dead = set()
                for c in room["controllers"]:
                    try:
                        await c.send(raw)
                    except Exception:
                        dead.add(c)
                room["controllers"] -= dead
            else:
                # 操作メッセージ -> ホストに転送
                host_ws = room.get("host")
                if host_ws is not None:
                    try:
                        await host_ws.send(raw)
                    except Exception:
                        room["host"] = None

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if room_name and room_name in ROOMS:
            room = ROOMS[room_name]
            if role == "host" and room.get("host") is ws:
                room["host"] = None
            elif role == "controller":
                room["controllers"].discard(ws)


async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port, max_size=None, ping_interval=20, ping_timeout=20):
        print(f"中継サーバー起動: ポート {port}")
        await asyncio.Future()  # 永久に待機


if __name__ == "__main__":
    asyncio.run(main())
