"""Various utility functions used throughout `tests_airflow` py files."""

import inspect
import logging
import textwrap
from multiprocessing import Process
from typing import Any, Callable, List, Optional

from airflow.models.dagbag import DagBag
from global_utils.develop_secrets import QUANT_PROJ_PATH
from global_utils.general_utils import COLOR_END, GREEN_START, RED_START

# ----------------------------------------------------------------------------------------------------------------------
TARGET_DAG_FOLDERS: List[str] = [
    "dags_air_env",
    "dags_backtest_runs",
    "dags_pytorch_models",
    "dags_stock_market",
]


# ----------------------------------------------------------------------------------------------------------------------
def test_dag_parsing() -> None:
    """Verify DAGs parse correctly and all top-level file imports are correct."""
    dag_count: int = 0
    import_error_count: int = 0

    folder: str
    for folder in TARGET_DAG_FOLDERS:
        print(f"TARGET DAG FOLDER PROCESSING: {folder}")

        dag_bag: DagBag = DagBag(dag_folder=rf"{QUANT_PROJ_PATH}/ml_airflow/dags/{folder}", include_examples=False)

        dag_count += len(dag_bag.dags)
        import_error_count += len(dag_bag.import_errors)

        # The part after the comma in an assert statement is the error message that gets printed if the assertion fails
        assert dag_bag.dags, "No DAGs were loaded. Check if the folder is correct or contains valid DAGs."
        assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"

    print(f"Total DAGs Parsed: {dag_count}")
    print(f"Total DAG Import Errors: {import_error_count}")


# ----------------------------------------------------------------------------------------------------------------------
def run_import_statements(py_func: Optional[Callable[[], Any]], target_source_str: str) -> None:
    """Run only import statements from a function's source code to ensure they are valid.

    :param py_func: Optional[Callable[[], Any]]: The Python function to inspect for imports.
    :param target_source_str: str: Identifier for logging.
    """
    try:
        source_code: str = inspect.getsource(object=py_func)
        lines: List[str] = source_code.splitlines()

        import_blocks: List[str] = []
        current_block: str = ""
        open_parens: int = 0

        line: str
        for line in lines:
            stripped: str = line.strip()

            if stripped.startswith("import ") or stripped.startswith("from "):
                current_block: str = line
                open_parens: int = line.count("(") - line.count(")")
                if open_parens == 0:
                    import_blocks.append(current_block)
                    current_block: str = ""
                continue

            if current_block:
                current_block += f"\n{line}"
                open_parens += line.count("(") - line.count(")")  # Do NOT use keyword arguments for `line.count()`
                if open_parens == 0:
                    import_blocks.append(current_block)
                    current_block: str = ""

        import_block: str
        for import_block in import_blocks:
            exec(textwrap.dedent(text=import_block), {"__builtins__": __builtins__})

        logging.info(msg=GREEN_START + rf"SUCCESS: {target_source_str}" + COLOR_END)

    except Exception:
        logging.error(msg=RED_START + rf"FAILED: {target_source_str}" + COLOR_END, exc_info=True)
        raise


# ----------------------------------------------------------------------------------------------------------------------
def multi_execute_funcs(
    process_functions: List[Callable[[], None]],
    max_concurrent: int = 64,
) -> None:
    """Run Python functions across multiple CPU cores.

    :param process_functions: List[Callable[[], None]]: Test functions to run
    :param max_concurrent: int: Maximum number of processes to run at once; default value is the MAX CPU cores avail.
    """
    index: int = 0
    total: int = len(process_functions)

    while index < total:
        # Launch up to max_concurrent processes in this batch
        batch_funcs: List[Callable[[], None]] = process_functions[index : index + max_concurrent]
        processes: List[Process] = [Process(target=func, name=func.__name__) for func in batch_funcs]
        # Start all processes in this batch
        process: Process
        for process in processes:
            # Creates a separate OS-level process.
            # That new process executes the target function you passed when creating the Process object.
            # This happens asynchronously; meaning the main program does not wait for the new process to finish.
            # It continues running.
            process.start()
        # Wait for all processes in this batch to finish
        process: Process
        for process in processes:
            # Tells the main program to wait (block) until the specific process has finished running.
            process.join()

        index += max_concurrent
