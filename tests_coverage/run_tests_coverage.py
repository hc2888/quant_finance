"""PyTest Coverage."""

import os
from typing import List

# Change directory up one level to properly find files
os.chdir(path="..")

# ----------------------------------------------------------------------------------------------------------------------
# The --cov option expects a module or package name, not a file path.
# When using --cov, you should provide the module name using Python's dot notation as you would when
# importing a module in Python, without the .py extension.

"""OVERALL MODULES."""
path_to_tests: str = "tests_coverage"
target_modules: List[str] = ["algo_metrics", "backtest_metrics"]
cover_str_list: List[str] = []
target_path: str
for target_path in target_modules:
    cover_str_list.append(rf"--cov={target_path}")
path_to_modules: str = " ".join(cover_str_list)

"""SPECIFIC MODULES."""
# path_to_tests: str = "tests/algo_metrics/trade_methods"
# target_modules: List[str] = ["algo_metrics.trade_methods"]
# cover_str_list: List[str] = []
# target_path: str
# for target_path in target_modules:
#     cover_str_list.append(rf"--cov={target_path}")
# path_to_modules: str = " ".join(cover_str_list)
# ----------------------------------------------------------------------------------------------------------------------
# Run the pytest command to generate the coverage report
cmd_1: str = rf"pytest -W ignore::DeprecationWarning {path_to_tests} {path_to_modules}"
cmd_2: str = r"--cov-report=html:coverage_report"
cmd_3: str = r"-n auto --dist=loadscope"  # CMD to use ALL CPU Cores
os.system(command=rf"{cmd_1} {cmd_2} {cmd_3}")
# Open the coverage report in the default browser (assuming it's on Windows)
os.system(command="start coverage_report/index.html")
