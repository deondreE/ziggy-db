import socket
from time import sleep

SERVER_IP = "127.0.0.1"
SERVER_PORT = 8080
BUFFER_SIZE = 1024

def connect_to_tcp_server(ip, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket: 
        try:
            print(f"Attempting to connect to {ip}:{port}...")
            client_socket.connect((ip, port))
            print(f"Successfully connected to {ip}:{port}!")
            
            while (True):
               sleep(1) 
                
            
            
        
        except ConnectionRefusedError:
            print(f"Error: Connection refused. Is the server running on {ip}:{port}?")
        except socket.timeout:
            print("Error: Connection timed out.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        finally:
            print("Connection closed.")

if __name__ == "__main__":
    connect_to_tcp_server(SERVER_IP, SERVER_PORT)