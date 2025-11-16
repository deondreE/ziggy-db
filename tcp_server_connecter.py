import socket
from time import sleep

SERVER_IP = "127.0.0.1"
SERVER_PORT = 8080
BUFFER_SIZE = 1024


def connect_to_tcp_server(ip, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        try:
            print(f"Connecting to {ip}:{port}...")
            client_socket.connect((ip, port))
            print("Connected! Waiting for initial server message...\n")

            # First, print the server greeting
            initial = client_socket.recv(BUFFER_SIZE)
            if initial:
                print("Server:", initial.decode("utf-8", errors="replace").strip())

            # Send our command to the server
            cmd = 'SET data "data"\n'
            print(f"Sending command: {cmd.strip()}")
            client_socket.sendall(cmd.encode("utf-8"))

            # Wait for a response from the server
            response = client_socket.recv(BUFFER_SIZE)
            if response:
                print("Server:", response.decode("utf-8", errors="replace").strip())
            else:
                print("No response from server.")

            # Optional: Keep listening for additional messages
            print("\nListening for more messages (Ctrl+C to exit)...")
            while True:
                data = client_socket.recv(BUFFER_SIZE)
                if not data:
                    print("Server closed the connection.")
                    break
                print("Server:", data.decode("utf-8", errors="replace").strip())
                sleep(0.1)

        except ConnectionRefusedError:
            print(f"Error: Connection refused. Is the server running on {ip}:{port}?")
        except socket.timeout:
            print("Error: Connection timed out.")
        except KeyboardInterrupt:
            print("\nDisconnected by user.")
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            print("Connection closed.")


if __name__ == "__main__":
    connect_to_tcp_server(SERVER_IP, SERVER_PORT)