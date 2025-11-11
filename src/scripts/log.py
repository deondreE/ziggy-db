import os
import re
from colorama import Fore, Style, init

# Initialize colorama for color support on all platforms
init(autoreset=True)


def read_log_file(file_path):
    """
    Reads a .log file and returns its contents as a list of lines.

    Args:
        file_path (str): Path to the .log file.

    Returns:
        list[str]: Lines from the log file.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            lines = file.readlines()
        return [line.strip() for line in lines if line.strip()]
    except FileNotFoundError:
        print(f"{Fore.RED}Error: The file '{file_path}' was not found.{Style.RESET_ALL}")
        return []
    except Exception as e:
        print(f"{Fore.RED}An error occurred while reading '{file_path}': {e}{Style.RESET_ALL}")
        return []


def pretty_print_log(lines, file_name):
    """
    Prints log lines with simple color-coded formatting.
    Recognizes common log patterns like ERROR, WARNING, and INFO.
    """
    print(f"\n{Fore.GREEN}=== {file_name} ==={Style.RESET_ALL}\n")
    for line in lines:
        if re.search(r"\bERROR\b", line, re.IGNORECASE):
            print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} {line}")
        elif re.search(r"\bWARNING\b", line, re.IGNORECASE):
            print(f"{Fore.YELLOW}[WARNING]{Style.RESET_ALL} {line}")
        elif re.search(r"\bINFO\b", line, re.IGNORECASE):
            print(f"{Fore.CYAN}[INFO]{Style.RESET_ALL} {line}")
        elif re.search(r"\bDEBUG\b", line, re.IGNORECASE):
            print(f"{Fore.MAGENTA}[DEBUG]{Style.RESET_ALL} {line}")
        else:
            print(f"{Fore.WHITE}{line}{Style.RESET_ALL}")


def read_all_logs_in_directory(directory_path):
    """
    Reads and prints all .log files from a given directory.
    """
    if not os.path.isdir(directory_path):
        print(f"{Fore.RED}Error: '{directory_path}' is not a valid directory.{Style.RESET_ALL}")
        return

    log_files = [f for f in os.listdir(directory_path) if f.endswith(".log")]

    if not log_files:
        print(f"{Fore.YELLOW}No .log files found in {directory_path}.{Style.RESET_ALL}")
        return

    for log_file in log_files:
        file_path = os.path.join(directory_path, log_file)
        lines = read_log_file(file_path)
        pretty_print_log(lines, log_file)


if __name__ == "__main__":
    logs_directory = "C:/Workspace/ziggy-db/tests"
    print(f"\n{Fore.GREEN}=== Reading all .log files from: {logs_directory} ==={Style.RESET_ALL}")
    read_all_logs_in_directory(logs_directory)