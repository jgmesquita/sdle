import zmq
import uuid
import sqlite3
import json
import time

DB_FILE = "shopping_lists.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS shopping_lists (
                    id TEXT PRIMARY KEY)''')
    c.execute('''CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    list_id TEXT,
                    name TEXT,
                    current_qtd INTEGER,
                    target_qtd INTEGER,
                    acquired_flag INTEGER,
                    FOREIGN KEY(list_id) REFERENCES shopping_lists(id))''')
    conn.commit()
    return conn

class Client:
    def __init__(self, proxy_addrs=["tcp://localhost:5558", "tcp://localhost:5560"]):
        self.conn = init_db()
        self.connected = False
        self.ctx = zmq.Context()

        for addr in proxy_addrs:
            try:
                sock = self.ctx.socket(zmq.DEALER)
                sock.setsockopt_string(zmq.IDENTITY, f"client-{uuid.uuid4()}")
                poller = zmq.Poller()
                poller.register(sock, zmq.POLLIN)
                sock.connect(addr)

                print(f"Connecting to proxy: {addr}...")
                if self._test_connection(sock, poller):
                    print(f"Connected to proxy: {addr}")
                    self.sock = sock
                    self.poller = poller
                    self.connected = True
                    break
                else:
                    print(f"Failed to connect to the proxy: {addr}")
                    sock.close()
            except Exception as e:
                print(f"Error trying to connect to proxy: {addr} -> {e}")
                continue 

        if not self.connected:
            raise Exception("It wasn't possible to connect to any proxy.")

    def _test_connection(self, sock, poller, retries=3):
        """Send a ping to proxy to test connectivity"""
        for _ in range(retries):
            try:
                sock.send_json({"op": "ping"})
                socks = dict(poller.poll(2000))  
                if socks.get(sock) == zmq.POLLIN:
                    response = sock.recv_json()
                    if response.get("status") == "pong":
                        return True
            except Exception:
                pass
            time.sleep(0.1)
        return False

    def send_request(self, op, list_id, payload=None):
        if payload is None:
            payload = {}
        self.sock.send_json({"op": op, "list_id": list_id, "payload": payload})
        socks = dict(self.poller.poll(2000))
        if socks.get(self.sock) == zmq.POLLIN:
            return self.sock.recv_json()
        return {"status": "timeout"}

    def save_list_local(self, list_id):
        c = self.conn.cursor()
        try:
            c.execute("INSERT INTO shopping_lists(id) VALUES (?)", (list_id,))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass

    def save_item_local(self, list_id, item_name, current, total):
        c = self.conn.cursor()
        c.execute("INSERT INTO items(list_id, name, current_qtd, target_qtd, acquired_flag) VALUES (?,?,?,?,0)",
                  (list_id, item_name, current, total))
        self.conn.commit()

    def create_list(self, list_id):
        self.save_list_local(list_id)
        return self.send_request("create_list", list_id)

    def create_item(self, list_id, item_name, current, total):
        self.save_item_local(list_id, item_name, current, total)
        return self.send_request("create_item", list_id, {"item_name": item_name, "current": current, "total": total})

    def update_item(self, list_id, item_name, current, total):
        c = self.conn.cursor()
        c.execute("UPDATE items SET current_qtd=?, target_qtd=? WHERE list_id=? AND name=?",
                  (current, total, list_id, item_name))
        self.conn.commit()
        return self.send_request("update_item", list_id, {"item_name": item_name, "current": current, "total": total})

    def delete_item(self, list_id, item_name):
        c = self.conn.cursor()
        c.execute("DELETE FROM items WHERE list_id=? AND name=?", (list_id, item_name))
        self.conn.commit()
        return self.send_request("delete_item", list_id, {"item_name": item_name})

    def get_info(self, list_id):
        c = self.conn.cursor()
        c.execute("SELECT id FROM shopping_lists WHERE id=?", (list_id,))
        list_row = c.fetchone()
        c.execute("SELECT name, current_qtd, target_qtd, acquired_flag FROM items WHERE list_id=?", (list_id,))
        items = [{"name": r[0], "current_qtd": r[1], "target_qtd": r[2], "acquired_flag": bool(r[3])} for r in c.fetchall()]
        local = {"id": list_row[0], "items": items} if list_row else None
        server = self.send_request("get_info", list_id)
        return {"local": local, "server": server}
    
def main():
    client = Client()
    print("Commands: create_list, create_item, update_item, delete_item, get_info, exit")
    while True:
        cmd = input(">>> ").strip().split()
        if not cmd:
            continue
        try:
            if cmd[0] == "exit":
                break
            elif cmd[0] == "create_list":
                print(client.create_list(cmd[1]))
            elif cmd[0] == "create_item":
                print(client.create_item(cmd[1], cmd[2], int(cmd[3]), int(cmd[4])))
            elif cmd[0] == "update_item":
                print(client.update_item(cmd[1], cmd[2], int(cmd[3]), int(cmd[4])))
            elif cmd[0] == "delete_item":
                print(client.delete_item(cmd[1], cmd[2]))
            elif cmd[0] == "get_info":
                print(json.dumps(client.get_info(cmd[1]), indent=2))
            else:
                print("Invalid command")
        except Exception as e:
            print("Error:", e)

if __name__ == "__main__":
    main()
