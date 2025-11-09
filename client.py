import zmq
import uuid
import sqlite3
import json
import time

DB_FILE = "client.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS shopping_lists (
                    id TEXT PRIMARY KEY,
                    synced INTEGER DEFAULT 0
                )''')

    c.execute('''CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    list_id TEXT,
                    name TEXT,
                    current_qtd INTEGER,
                    target_qtd INTEGER,
                    acquired_flag INTEGER,
                    synced INTEGER DEFAULT 0,
                    FOREIGN KEY(list_id) REFERENCES shopping_lists(id)
                )''')
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
                    print(f"Failed to connect to proxy: {addr}")
                    sock.close()
            except Exception as e:
                print(f"Error trying to connect to proxy {addr}: {e}")
                continue

        if not self.connected:
            raise Exception("Could not connect to any proxy.")

    def _test_connection(self, sock, poller, retries=3):
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
            c.execute("INSERT INTO shopping_lists(id, synced) VALUES (?, 0)", (list_id,))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass

    def save_item_local(self, list_id, item_name, current, total):
        c = self.conn.cursor()
        c.execute("""INSERT INTO items(list_id, name, current_qtd, target_qtd, acquired_flag, synced)
                     VALUES (?, ?, ?, ?, 0, 0)""",
                  (list_id, item_name, current, total))
        self.conn.commit()

    def create_list(self, list_id):
        self.save_list_local(list_id)
        resp = self.send_request("create_list", list_id)
        if resp.get("status") == "ok":
            c = self.conn.cursor()
            c.execute("UPDATE shopping_lists SET synced=1 WHERE id=?", (list_id,))
            self.conn.commit()
            print(f"List '{list_id}' synced with server.")
        elif resp.get("status") == "timeout":
            print("Server not reachable. Saved locally (unsynced).")
        return resp

    def create_item(self, list_id, item_name, current, total):
        self.save_item_local(list_id, item_name, current, total)
        resp = self.send_request("create_item", list_id,
                                 {"item_name": item_name, "current": current, "total": total})
        if resp.get("status") == "ok":
            c = self.conn.cursor()
            c.execute("UPDATE items SET synced=1 WHERE list_id=? AND name=?", (list_id, item_name))
            self.conn.commit()
            print(f"Item '{item_name}' synced with server.")
        elif resp.get("status") == "timeout":
            print("Server not reachable. Saved locally (unsynced).")
        return resp

    def update_item(self, list_id, item_name, current, total):
        c = self.conn.cursor()
        c.execute("""UPDATE items
                     SET current_qtd=?, target_qtd=?, synced=0
                     WHERE list_id=? AND name=?""",
                  (current, total, list_id, item_name))
        self.conn.commit()
        resp = self.send_request("update_item", list_id,
                                 {"item_name": item_name, "current": current, "total": total})
        if resp.get("status") == "ok":
            c.execute("UPDATE items SET synced=1 WHERE list_id=? AND name=?", (list_id, item_name))
            self.conn.commit()
            print(f"Item '{item_name}' updated on server.")
        elif resp.get("status") == "timeout":
            print("Server not reachable. Change saved locally (unsynced).")
        return resp

    def delete_item(self, list_id, item_name):
        c = self.conn.cursor()
        c.execute("DELETE FROM items WHERE list_id=? AND name=?", (list_id, item_name))
        self.conn.commit()
        resp = self.send_request("delete_item", list_id, {"item_name": item_name})
        if resp.get("status") == "ok":
            print(f"Item '{item_name}' deleted on server.")
        elif resp.get("status") == "timeout":
            print("Server not reachable. Deleted locally only.")
        return resp

    def get_info(self, list_id):
        c = self.conn.cursor()
        c.execute("SELECT id FROM shopping_lists WHERE id=?", (list_id,))
        list_row = c.fetchone()
        c.execute("SELECT name, current_qtd, target_qtd, acquired_flag FROM items WHERE list_id=?", (list_id,))
        items = [{"name": r[0], "current_qtd": r[1], "target_qtd": r[2], "acquired_flag": bool(r[3])} for r in c.fetchall()]
        local = {"id": list_row[0], "items": items} if list_row else None
        server = self.send_request("get_info", list_id)
        return {"local": local, "server": server}

    def commit_all(self):
        c = self.conn.cursor()

        # Commit unsynced lists
        c.execute("SELECT id FROM shopping_lists WHERE synced=0")
        unsynced_lists = [r[0] for r in c.fetchall()]

        # Commit unsynced items (may belong to already synced lists)
        c.execute("SELECT DISTINCT list_id FROM items WHERE synced=0")
        lists_with_unsynced_items = [r[0] for r in c.fetchall()]

        if not unsynced_lists and not lists_with_unsynced_items:
            print("Nothing to commit - all data is synced.")
            return

        print("Starting commit...")

        for list_id in unsynced_lists:
            print(f"-> Committing list '{list_id}'")
            resp = self.send_request("create_list", list_id)
            if resp.get("status") == "ok":
                c.execute("UPDATE shopping_lists SET synced=1 WHERE id=?", (list_id,))
                print(f"-> List '{list_id}' synced.")
            elif resp.get("status") == "timeout":
                print(f"Server not reachable for '{list_id}'. Skipping.")
                continue

        for list_id in lists_with_unsynced_items:
            print(f"-> Committing unsynced items in list '{list_id}'")
            c.execute("SELECT name, current_qtd, target_qtd FROM items WHERE list_id=? AND synced=0", (list_id,))
            items = c.fetchall()
            for name, current, total in items:
                resp = self.send_request("create_item", list_id,
                                         {"item_name": name, "current": current, "total": total})
                if resp.get("status") == "ok":
                    c.execute("UPDATE items SET synced=1 WHERE list_id=? AND name=?", (list_id, name))
                    print(f"-> Item '{name}' synced.")
                else:
                    print(f"-> Failed to sync '{name}': {resp}")

        self.conn.commit()
        print("Commit complete - unsynced changes pushed.")

    def sync(self):
        print("Syncing from servers...")
        c = self.conn.cursor()

        resp = self.send_request("list_all_lists", None)
        if resp.get("status") != "ok":
            print("Could not fetch list of lists from server.")
            return

        server_lists = set(resp["lists"])
        c.execute("SELECT id FROM shopping_lists")
        local_lists = set(r[0] for r in c.fetchall())

        for list_id in local_lists - server_lists:
            print(f"Removing deleted list '{list_id}' locally.")
            c.execute("DELETE FROM items WHERE list_id=?", (list_id,))
            c.execute("DELETE FROM shopping_lists WHERE id=?", (list_id,))

        for list_id in server_lists:
            resp = self.send_request("get_info", list_id)
            if resp.get("status") == "ok":
                print(f"-> Syncing list '{list_id}'")
                c.execute("DELETE FROM items WHERE list_id=?", (list_id,))
                for item in resp["list"]["items"]:
                    c.execute("""INSERT INTO items(list_id, name, current_qtd, target_qtd, acquired_flag, synced)
                                 VALUES (?, ?, ?, ?, ?, 1)""",
                              (list_id, item["name"], item["current_qtd"], item["target_qtd"], int(item["acquired_flag"])))
                c.execute("INSERT OR REPLACE INTO shopping_lists(id, synced) VALUES (?, 1)", (list_id,))

        self.conn.commit()
        print("Sync complete.")


def main():
    client = Client()
    print("Commands: create_list, create_item, update_item, delete_item, get_info, commit, sync, exit")

    while True:
        cmd = input(">>> ").strip().split()
        if not cmd:
            continue
        op = cmd[0]
        try:
            if op == "exit":
                break
            elif op == "create_list":
                client.create_list(cmd[1])
            elif op == "create_item":
                client.create_item(cmd[1], cmd[2], int(cmd[3]), int(cmd[4]))
            elif op == "update_item":
                client.update_item(cmd[1], cmd[2], int(cmd[3]), int(cmd[4]))
            elif op == "delete_item":
                client.delete_item(cmd[1], cmd[2])
            elif op == "get_info":
                print(json.dumps(client.get_info(cmd[1]), indent=2))
            elif op == "commit":
                client.commit_all()
            elif op == "sync":
                client.sync()
            else:
                print("Invalid command.")
        except Exception as e:
            print("Error:", e)


if __name__ == "__main__":
    main()
