"""Shut down a local running port."""

import os
import signal
import subprocess
from typing import List

# ----------------------------------------------------------------------------------------------------------------------
# The local port to shut down.
TARGET_PORT: int = 9999
# TARGET_PORT: int = 8501
# ----------------------------------------------------------------------------------------------------------------------


def find_process_id_by_port(port: int) -> int:
    """Find the process ID (PID) using the specified port.

    :param port: The port number to check for a running process.
    :return: The PID of the process using the specified port, or -1 if no process is found.
    """
    result: subprocess.CompletedProcess[str] = subprocess.run(
        args=["netstat", "-ano", "|", "findstr", f":{port}"],
        capture_output=True,
        text=True,
        shell=True,
    )
    result_lines: List[str] = result.stdout.splitlines()
    line: str
    for line in result_lines:
        if rf":{port}" in line:
            parts: List[str] = line.split()
            pid: int = int(parts[-1])
            return pid
    return -1  # Return -1 if no process is found on the specified port


def kill_process(pid: int) -> None:
    """Kill the process with the specified PID.

    :param pid: The process ID to kill.
    """
    os.kill(pid, signal.SIGTERM)


def shutdown_port(port: int) -> None:
    """Find and kill the process running on the specified port.

    :param port: The port number to shut down.
    """
    try:
        pid: int = find_process_id_by_port(port=port)
        if pid != -1:
            kill_process(pid=pid)
            print(f"PROCESS ON PORT {port} WITH PID {pid} HAS BEEN TERMINATED.")
        else:
            print(f"NO PROCESS FOUND ON PORT {port}.")
    except Exception as error_msg:
        print(f"ERROR: {error_msg}")


if __name__ == "__main__":
    shutdown_port(port=TARGET_PORT)
