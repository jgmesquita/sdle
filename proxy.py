import zmq
import json

def start_proxy(proxy_port_clients, proxy_port_servers, proxy_name):
    context = zmq.Context()

    frontend = context.socket(zmq.ROUTER)
    frontend.bind(f"tcp://*:{proxy_port_clients}")

    backend = context.socket(zmq.DEALER)
    backend.bind(f"tcp://*:{proxy_port_servers}")

    print(f"{proxy_name} initialized. Clients: {proxy_port_clients}, Servers: {proxy_port_servers}")

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    while True:
        try:
            events = dict(poller.poll(1000))

            if frontend in events and events[frontend] == zmq.POLLIN:
                frames = frontend.recv_multipart()
                client_id = frames[0]
                msg = frames[-1]
                req = json.loads(msg.decode())

                if req.get("op") == "ping":
                    frontend.send_multipart([client_id, json.dumps({"status": "pong"}).encode()])
                else:
                    backend.send_multipart([client_id, msg])

            if backend in events and events[backend] == zmq.POLLIN:
                frames = backend.recv_multipart()
                server_id = frames[0]
                msg = frames[-1]

                try:
                    req = json.loads(msg.decode())
                    if req.get("op") == "ping":
                        backend.send_multipart([server_id, json.dumps({"status": "pong"}).encode()])
                        continue
                except:
                    pass

                frontend.send_multipart([server_id, msg])

        except Exception as e:
            print(f"Erro no {proxy_name}: {e}")


if __name__ == "__main__":
    proxies = [
        {"proxy_name": "Proxy 1", "proxy_port_clients": 5558, "proxy_port_servers": 5559},
        {"proxy_name": "Proxy 2", "proxy_port_clients": 5560, "proxy_port_servers": 5561},
    ]

    print("Escolhe o proxy para iniciar:")
    for i, proxy in enumerate(proxies):
        print(f"{i + 1}. {proxy['proxy_name']} (Clientes: {proxy['proxy_port_clients']}, Servidores: {proxy['proxy_port_servers']})")

    try:
        choice = int(input("Escolha o número do proxy (1 ou 2): "))
        if choice in [1, 2]:
            proxy = proxies[choice - 1]
            start_proxy(proxy["proxy_port_clients"], proxy["proxy_port_servers"], proxy["proxy_name"])
        else:
            print("Escolha inválida. Por favor, escolha 1 ou 2.")
    except ValueError:
        print("Entrada inválida. Por favor, insira um número válido.")
