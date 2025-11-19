import socket
import sys
import time

SERVER_IP = "127.0.0.1"
SERVER_PORT = 8080
BUFFER_SIZE = 4096


def send_and_receive(sock: socket.socket, command: str):
    """Send a line to the server and print its response."""
    if not command.endswith("\n"):
        command += "\n"

    print(f"\n>>> {command.strip()}")
    sock.sendall(command.encode("utf-8"))

    try:
        response = sock.recv(BUFFER_SIZE)
        if response:
            print("<<<", response.decode("utf-8", errors="replace").strip())
        else:
            print("(no response)")
    except socket.timeout:
        print("(timeout waiting for response)")


def connect_to_ziggy(ip: str, port: int):
    """Connect to ZiggyDB TCP server and run a small test sequence."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(3)
        try:
            print(f"Connecting to {ip}:{port} ...")
            s.connect((ip, port))
        except ConnectionRefusedError:
            print(f"Connection refused. Is the ZiggyDB server running on {ip}:{port}?")
            sys.exit(1)

        # initial server greeting
        greeting = s.recv(BUFFER_SIZE)
        if greeting:
            print(greeting.decode("utf-8", errors="replace").strip())

        # test sequence
        send_and_receive(s, "SET foo bar")
        send_and_receive(s, "GET foo")
        send_and_receive(s, "SET x 42")
        send_and_receive(s, "GET x")
        send_and_receive(s, "DEL x")
        send_and_receive(s, "GET x")
        send_and_receive(s, "BEGIN")
        send_and_receive(s, "SET txn 99")
        send_and_receive(s, "COMMIT")
        send_and_receive(s, "GET txn")

        # exit the session cleanly
        send_and_receive(s, "EXIT")

        print("\nTest sequence complete. Closing connection.")
        try:
            s.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        s.close()


if __name__ == "__main__":
    connect_to_ziggy(SERVER_IP, SERVER_PORT)