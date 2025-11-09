import zmq
import json
import time
from hashring import HashRing   


def start_proxy(proxy_port_clients, proxy_port_servers, proxy_name):
    context = zmq.Context()

    frontend = context.socket(zmq.ROUTER)  
    frontend.bind(f"tcp://*:{proxy_port_clients}")

    backend = context.socket(zmq.ROUTER)  
    backend.bind(f"tcp://*:{proxy_port_servers}")

    print(f"{proxy_name} started.")
    print(f"Listening for clients on port {proxy_port_clients}")
    print(f"Listening for servers on port {proxy_port_servers}\n")

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    ring = HashRing()
    servers = {}        
    MAX_SERVERS = 5 

    while True:
        try:
            events = dict(poller.poll(1000))

            if backend in events and events[backend] == zmq.POLLIN:
                frames = backend.recv_multipart()

                if len(frames) == 3:
                    server_id, client_id, msg = frames
                    frontend.send_multipart([client_id, msg])
                    continue

                server_id, msg = frames[0], frames[-1]
                try:
                    req = json.loads(msg.decode())
                    if req.get("op") == "ping":
                        backend.send_multipart([server_id, json.dumps({"status": "pong"}).encode()])

                        if server_id not in servers:
                            if len(servers) < MAX_SERVERS:
                                servers[server_id] = time.time()
                                ring.add_node(server_id.decode())
                                print(f"[+] Added {server_id.decode()} to hash ring. ({len(servers)}/{MAX_SERVERS})")
                            else:
                                print(f"[!] Ignored extra server {server_id.decode()} (limit reached).")
                        else:
                            servers[server_id] = time.time()
                        continue
                except Exception:
                    pass

            if frontend in events and events[frontend] == zmq.POLLIN:
                frames = frontend.recv_multipart()
                client_id, msg = frames[0], frames[-1]

                try:
                    req = json.loads(msg.decode())
                except Exception:
                    frontend.send_multipart([
                        client_id,
                        json.dumps({"status": "error", "message": "Invalid JSON"}).encode()
                    ])
                    continue

                if req.get("op") == "ping":
                    frontend.send_multipart([client_id, json.dumps({"status": "pong"}).encode()])
                    continue

                list_id = req.get("list_id")
                target_server_name = ring.get_node(list_id)

                if not target_server_name:
                    frontend.send_multipart([
                        client_id,
                        json.dumps({"status": "error", "message": "No servers available"}).encode()
                    ])
                    continue

                target_server = None
                for sid in servers.keys():
                    if sid.decode() == target_server_name:
                        target_server = sid
                        break

                if not target_server:
                    frontend.send_multipart([
                        client_id,
                        json.dumps({"status": "error", "message": "Server not found"}).encode()
                    ])
                    continue

                backend.send_multipart([target_server, client_id, msg])
                print(f"→ Routed list '{list_id}' to {target_server.decode()}")

            now = time.time()
            for sid, last in list(servers.items()):
                if now - last > 10: 
                    ring.remove_node(sid.decode())
                    del servers[sid]
                    print(f"[-] Removed {sid.decode()} (heartbeat timeout)")

        except KeyboardInterrupt:
            print("\nProxy shutting down...")
            break
        except Exception as e:
            print(f"Error in {proxy_name}: {e}")

if __name__ == "__main__":
    proxies = [
        {"proxy_name": "Proxy 1", "proxy_port_clients": 5558, "proxy_port_servers": 5559},
        {"proxy_name": "Proxy 2", "proxy_port_clients": 5560, "proxy_port_servers": 5561},
    ]

    print("Choose which proxy to start:")
    for i, proxy in enumerate(proxies):
        print(f"{i + 1}. {proxy['proxy_name']} (Clients: {proxy['proxy_port_clients']}, Servers: {proxy['proxy_port_servers']})")

    try:
        choice = int(input("Enter 1 or 2: "))
        if choice in [1, 2]:
            proxy = proxies[choice - 1]
            start_proxy(proxy["proxy_port_clients"], proxy["proxy_port_servers"], proxy["proxy_name"])
        else:
            print("Invalid selection.")
    except ValueError:
        print("Please enter a valid number (1 or 2).")
