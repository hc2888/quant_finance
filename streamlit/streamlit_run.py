"""Run Streamlit App."""

import os
import socket
import subprocess
from typing import List


# ----------------------------------------------------------------------------------------------------------------------
def check_port_in_use(port_num: int) -> bool:
    """Check if a given port is in use.

    :param port: int: The port number to check.
    :return: bool: True if the port is in use, False otherwise.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex((r"localhost", port_num)) == 0


# ----------------------------------------------------------------------------------------------------------------------
def kill_process_on_port(port_num: int) -> None:
    """Kill the process using the given port on Windows.

    :param port: (int): The port number whose process needs to be killed.
    """
    try:
        command: str = rf"netstat -ano | findstr :{port_num}"
        result: str = subprocess.check_output(command, shell=True).decode()
        lines: List[str] = result.strip().split(r"\n")
        line: str
        for line in lines:
            if line:
                parts: List[str] = line.split()
                pid: str = parts[-1]
                os.system(rf"taskkill /F /PID {pid}")
                print(rf"Process with PID {pid} on port {port_num} killed successfully.")
    except subprocess.CalledProcessError:
        print(rf"No process found running on port {port_num}.")


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    target_port: int = 8501
    if check_port_in_use(port_num=target_port):
        kill_process_on_port(port_num=target_port)
    os.system(rf"streamlit run streamlit_main.py --server.port {target_port}")
