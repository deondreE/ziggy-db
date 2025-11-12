import os
import re
from colorama import Fore, Style, init

# Initialize colorama for color support on all platforms
init(autoreset=True)


def read_file_content(file_path):
    """
    Reads a file and returns its content as a list of lines.

    Args:
        file_path (str): Path to the file.

    Returns:
        list[str]: Lines from the file.
    """
    try:
        # Attempt to read as UTF-8, which is common for logs and text files
        with open(file_path, "r", encoding="utf-8") as file:
            lines = file.readlines()
        return [line.strip() for line in lines]
    except UnicodeDecodeError:
        # If UTF-8 fails, try reading as binary and handle it differently
        print(f"{Fore.YELLOW}Warning: '{file_path}' is not a valid UTF-8 text file. Reading as binary.{Style.RESET_ALL}")
        try:
            with open(file_path, "rb") as file:
                # For binary files, you might want to display a hex dump or simply acknowledge it
                # For now, we'll just indicate it's binary and not attempt to parse line by line
                content = file.read()
                return [f"{Fore.BLUE}--- Binary Content ({len(content)} bytes) ---{Style.RESET_ALL}"]
        except Exception as e:
            print(f"{Fore.RED}An error occurred while reading binary file '{file_path}': {e}{Style.RESET_ALL}")
            return []
    except FileNotFoundError:
        print(f"{Fore.RED}Error: The file '{file_path}' was not found.{Style.RESET_ALL}")
        return []
    except Exception as e:
        print(f"{Fore.RED}An error occurred while reading '{file_path}': {e}{Style.RESET_ALL}")
        return []


def pretty_print_db_log(lines, file_name):
    """
    Prints database log lines with structured color-coded formatting.
    Recognizes WAL headers, table formatting, and transaction messages.
    """
    print(f"\n{Fore.GREEN}{'=' * 5} {file_name} {'=' * 5}{Style.RESET_ALL}\n")

    in_table = False
    for line in lines:
        if line.startswith("--- Binary Content"):
            print(line)
            in_table = False
        elif line.startswith("✓ Valid WAL file"):
            print(f"{Fore.CYAN}{line}{Style.RESET_ALL}")
            in_table = False
        elif line.startswith("+---"):  # Table border line
            print(f"{Fore.BLUE}{line}{Style.RESET_ALL}")
            in_table = True
        elif line.startswith("| Operation | Key           | Value"):  # Table header
            print(f"{Fore.MAGENTA}{line}{Style.RESET_ALL}")
            in_table = True
        elif in_table and line.startswith("|"):  # Table content line
            parts = line.split('|')
            if len(parts) >= 4:  # Ensure it has operation, key, value
                op_col = parts[1].strip()
                key_col = parts[2].strip()
                value_col = parts[3].strip()

                color_op = Fore.WHITE
                if op_col.lower() == "set" or op_col.lower() == "listpush":
                    color_op = Fore.GREEN
                elif op_col.lower() == "delete" or op_col.lower() == "listpop":
                    color_op = Fore.RED

                # Attempt to colorize numerical/boolean values
                color_value = Fore.YELLOW
                if value_col.lower() == "-" or not value_col: # Delete placeholder or empty value
                    color_value = Fore.WHITE
                elif value_col.lower() == "true" or value_col.lower() == "false":
                    color_value = Fore.MAGENTA # Bool
                elif value_col.replace('.', '', 1).isdigit(): # Integer or float
                    color_value = Fore.CYAN

                # Reconstruct the line with colors
                formatted_line = (
                    f"{Fore.BLUE}|{Style.RESET_ALL} {color_op}{op_col:<9}{Style.RESET_ALL} "
                    f"{Fore.BLUE}|{Style.RESET_ALL} {Fore.WHITE}{key_col:<13}{Style.RESET_ALL} "
                    f"{Fore.BLUE}|{Style.RESET_ALL} {color_value}{value_col:<21}{Style.RESET_ALL} "
                    f"{Fore.BLUE}|{Style.RESET_ALL}"
                )
                print(formatted_line)
            else: # Fallback for malformed table lines
                print(f"{Fore.WHITE}{line}{Style.RESET_ALL}")
        elif "Transaction started" in line:
            print(f"{Fore.BLUE}{line}{Style.RESET_ALL}")
            in_table = False
        elif "Transaction Committed" in line:
            print(f"{Fore.GREEN}{line}{Style.RESET_ALL}")
            in_table = False
        elif "Transaction rolled back" in line:
            print(f"{Fore.RED}{line}{Style.RESET_ALL}")
            in_table = False
        elif line: # Any other non-empty line
            print(f"{Fore.WHITE}{line}{Style.RESET_ALL}")
        # else: ignore empty lines, as they are stripped by read_file_content

    print(f"{Fore.GREEN}{'=' * (len(file_name) + 12)}{Style.RESET_ALL}\n")


def read_and_print_all_files_in_directory(directory_path):
    """
    Reads and prints all files (attempting to auto-detect format) from a given directory.
    """
    if not os.path.isdir(directory_path):
        print(f"{Fore.RED}Error: '{directory_path}' is not a valid directory.{Style.RESET_ALL}")
        return

    # Get all files, not just .log
    files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]

    if not files:
        print(f"{Fore.YELLOW}No files found in {directory_path}.{Style.RESET_ALL}")
        return

    for file_name in files:
        file_path = os.path.join(directory_path, file_name)
        lines = read_file_content(file_path)

        # Heuristic to detect if it's a ZiggyDB log file
        if any(line.startswith("✓ Valid WAL file") or line.startswith("+---") for line in lines):
            pretty_print_db_log(lines, file_name)
        elif any(re.search(r"\bERROR\b|\bWARNING\b|\bINFO\b|\bDEBUG\b", line, re.IGNORECASE) for line in lines):
            # Fallback to general log parsing if standard keywords are found
            pretty_print_generic_log(lines, file_name)
        else:
            # For other files, just print the content without special formatting,
            # or handle them as text/binary as read_file_content dictates
            print(f"\n{Fore.GREEN}{'=' * 5} {file_name} (Plain Text) {'=' * 5}{Style.RESET_ALL}\n")
            for line in lines:
                print(f"{Fore.WHITE}{line}{Style.RESET_ALL}")
            print(f"\n{Fore.GREEN}{'=' * (len(file_name) + 21)}{Style.RESET_ALL}\n")


def pretty_print_generic_log(lines, file_name):
    """
    Prints generic log lines with simple color-coded formatting based on keywords.
    (This is your original pretty_print_log function, renamed and integrated)
    """
    print(f"\n{Fore.GREEN}{'=' * 5} {file_name} (Generic Log) {'=' * 5}{Style.RESET_ALL}\n")
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
    print(f"\n{Fore.GREEN}{'=' * (len(file_name) + 21)}{Style.RESET_ALL}\n")


if __name__ == "__main__":
    logs_directory = "C:/Workspace/ziggy-db/tests"  # Or your project's root "C:/Workspace/ziggy-db/"
    print(f"\n{Fore.GREEN}=== Reading all files from: {logs_directory} ==={Style.RESET_ALL}")
    read_and_print_all_files_in_directory(logs_directory)