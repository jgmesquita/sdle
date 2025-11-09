import zmq
import json
import uuid
import sqlite3
import time
import sys

def init_db(server_number: int):
    db_file = f"server{server_number}.db"
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS shopping_lists (
                    id TEXT PRIMARY KEY
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    list_id TEXT,
                    name TEXT,
                    current_qtd INTEGER,
                    target_qtd INTEGER,
                    acquired_flag INTEGER,
                    FOREIGN KEY(list_id) REFERENCES shopping_lists(id)
                )''')
    conn.commit()
    return conn

def handle_request(conn, req):
    c = conn.cursor()
    op = req.get("op")
    list_id = req.get("list_id")
    payload = req.get("payload", {})

    if op == "ping":
        return {"status": "pong"}

    elif op == "create_list":
        try:
            c.execute("INSERT INTO shopping_lists(id) VALUES (?)", (list_id,))
            conn.commit()
            return {"status": "ok"}
        except sqlite3.IntegrityError:
            return {"status": "error", "message": "List already exists"}

    elif op == "create_item":
        try:
            c.execute("""
                INSERT INTO items(list_id, name, current_qtd, target_qtd, acquired_flag)
                VALUES (?, ?, ?, ?, 0)
            """, (list_id, payload["item_name"], payload["current"], payload["total"]))
            conn.commit()
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    elif op == "update_item":
        c.execute("""
            UPDATE items
            SET current_qtd=?, target_qtd=?
            WHERE list_id=? AND name=?
        """, (payload["current"], payload["total"], list_id, payload["item_name"]))
        conn.commit()
        return {"status": "ok"}

    elif op == "delete_item":
        c.execute("DELETE FROM items WHERE list_id=? AND name=?", (list_id, payload["item_name"]))
        conn.commit()
        return {"status": "ok"}

    elif op == "get_info":
        c.execute("SELECT id FROM shopping_lists WHERE id=?", (list_id,))
        list_row = c.fetchone()
        if not list_row:
            return {"status": "error", "message": "List not found"}

        c.execute("""
            SELECT name, current_qtd, target_qtd, acquired_flag
            FROM items
            WHERE list_id=?
        """, (list_id,))
        items = [
            {
                "name": r[0],
                "current_qtd": r[1],
                "target_qtd": r[2],
                "acquired_flag": bool(r[3])
            } for r in c.fetchall()
        ]
        return {"status": "ok", "list": {"id": list_row[0], "items": items}}

    else:
        return {"status": "error", "message": "Unknown operation"}

def connect_to_proxy(context, proxies, server_number, timeout=2.0):
    """Try to connect to one of the proxies (with ping verification)."""
    for p in proxies:
        try:
            sock_front = context.socket(zmq.DEALER)
            sock_front.setsockopt_string(zmq.IDENTITY, f"server{server_number}-ping-{uuid.uuid4()}")
            sock_front.connect(p["frontend"])
            poller = zmq.Poller()
            poller.register(sock_front, zmq.POLLIN)

            sock_front.send_json({"op": "ping"})
            socks = dict(poller.poll(timeout * 1000))
            if socks.get(sock_front) == zmq.POLLIN:
                reply = sock_front.recv_json()
                if reply.get("status") == "pong":
                    print(f"Proxy frontend alive at {p['frontend']}. Connecting to backend {p['backend']}...")
                    sock_front.close()
                    sock_backend = context.socket(zmq.DEALER)
                    sock_backend.setsockopt_string(zmq.IDENTITY, f"server{server_number}-{uuid.uuid4()}")
                    sock_backend.connect(p["backend"])
                    return sock_backend
            sock_front.close()
        except Exception as e:
            print(f"Failed to ping frontend {p['frontend']}: {e}")
    return None

def main():
    try:
        server_number = int(input("Enter this server's number (1–5): "))
        if not (1 <= server_number <= 5):
            print("Invalid number. Please choose between 1 and 5.")
            return
    except ValueError:
        print("Invalid input. Please enter a number between 1 and 5.")
        return

    conn = init_db(server_number)
    context = zmq.Context()

    proxies = [
        {"frontend": "tcp://localhost:5558", "backend": "tcp://localhost:5559"},
        {"frontend": "tcp://localhost:5560", "backend": "tcp://localhost:5561"},
    ]

    sock = connect_to_proxy(context, proxies, server_number)
    if not sock:
        raise Exception("Could not connect to any proxy!")

    print(f"Server {server_number} connected and ready to handle requests.")

    last_ping = 0
    PING_INTERVAL = 3

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    while True:
        try:
            now = time.time()
            if now - last_ping > PING_INTERVAL:
                sock.send_json({"op": "ping"})
                last_ping = now

            socks = dict(poller.poll(1000))
            if socks.get(sock) == zmq.POLLIN:
                msg = sock.recv_multipart()
                client_id = msg[0]
                req = json.loads(msg[-1].decode())

                reply = handle_request(conn, req)
                sock.send_multipart([client_id, json.dumps(reply).encode()])

        except KeyboardInterrupt:
            print(f"\nServer {server_number} shutting down...")
            break
        except Exception as e:
            print(f"Error on server {server_number}: {e}")


if __name__ == "__main__":
    main()
