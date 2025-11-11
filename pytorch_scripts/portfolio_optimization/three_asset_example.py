"""Example of optimize asset weight distributions using PyTorch."""

from typing import Dict, Tuple

import torch


def expected_return_a(interest_rate: torch.Tensor, stock_price_a: torch.Tensor) -> torch.Tensor:
    """Compute the expected return for asset A.

    :param interest_rate: torch.Tensor: Interest rate.
    :param stock_price_a: torch.Tensor: Stock price of asset A.

    :return: torch.Tensor: Expected return for asset A.
    """
    return 100 - 10 * interest_rate + 2 * stock_price_a


def expected_return_b(interest_rate: torch.Tensor, stock_price_b: torch.Tensor) -> torch.Tensor:
    """Compute the expected return for asset B.

    :param interest_rate: torch.Tensor: Interest rate.
    :param stock_price_b: torch.Tensor: Stock price of asset B.

    :return: torch.Tensor: Expected return for asset B.
    """
    return 200 + 5 * interest_rate + 3 * stock_price_b


def expected_return_c(interest_rate: torch.Tensor, stock_price_c: torch.Tensor) -> torch.Tensor:
    """Compute the expected return for asset C.

    :param interest_rate: torch.Tensor: Interest rate.
    :param stock_price_c: torch.Tensor: Stock price of asset C.

    :return: torch.Tensor: Expected return for asset C.
    """
    return 150 + interest_rate + 4 * stock_price_c


def portfolio_expected_return(
    weights: torch.Tensor,
    interest_rate: torch.Tensor,
    stock_price_a: torch.Tensor,
    stock_price_b: torch.Tensor,
    stock_price_c: torch.Tensor,
) -> torch.Tensor:
    """Compute the portfolio's expected return given weights, interest rate, and stock prices of assets.

    :param weights: torch.Tensor: Weights of the assets.
    :param interest_rate: torch.Tensor: Interest rate.
    :param stock_price_a: torch.Tensor: Stock price of asset A.
    :param stock_price_b: torch.Tensor: Stock price of asset B.
    :param stock_price_c: torch.Tensor: Stock price of asset C.

    :return: torch.Tensor: Expected return of the portfolio.
    """
    return_asset_a: torch.Tensor = expected_return_a(interest_rate=interest_rate, stock_price_a=stock_price_a)
    return_asset_b: torch.Tensor = expected_return_b(interest_rate=interest_rate, stock_price_b=stock_price_b)
    return_asset_c: torch.Tensor = expected_return_c(interest_rate=interest_rate, stock_price_c=stock_price_c)
    return weights[0] * return_asset_a + weights[1] * return_asset_b + weights[2] * return_asset_c


def gradient_descent(
    interest_rate: torch.Tensor,
    stock_price_a: torch.Tensor,
    stock_price_b: torch.Tensor,
    stock_price_c: torch.Tensor,
    alpha: float,
    iterations: int,
) -> torch.Tensor:
    """Perform gradient descent to maximize the portfolio's expected return.

    :param interest_rate: torch.Tensor: Interest rate.
    :param stock_price_a: torch.Tensor: Stock price of asset A.
    :param stock_price_b: torch.Tensor: Stock price of asset B.
    :param stock_price_c: torch.Tensor: Stock price of asset C.
    :param alpha: float: Learning rate.
    :param iterations: int: Number of iterations.

    :return: torch.Tensor: Optimal weights.
    """
    weights: torch.Tensor = torch.randn(3, device=interest_rate.device)
    weights: torch.Tensor = torch.abs(weights)
    weights: torch.Tensor = weights / weights.sum()
    weights.requires_grad_(True)

    optimizer: torch.optim.Optimizer = torch.optim.NAdam(
        params=[weights],
        lr=alpha,
        betas=(0.9, 0.999),
        eps=0.00000001,
        weight_decay=0,
        momentum_decay=0.004,
    )

    iter_num: int
    for iter_num in range(iterations):
        optimizer.zero_grad(set_to_none=False)
        expected_return: torch.Tensor = portfolio_expected_return(
            weights=weights,
            interest_rate=interest_rate,
            stock_price_a=stock_price_a,
            stock_price_b=stock_price_b,
            stock_price_c=stock_price_c,
        )
        loss: torch.Tensor = -expected_return
        loss.backward(retain_graph=False, create_graph=False)
        optimizer.step()
        with torch.no_grad():
            weights: torch.Tensor = weights / weights.sum()
            weights.requires_grad_(True)
        print(f"Iteration {iter_num+1}/{iterations}, Weights: {weights.detach().cpu().numpy()}")

    return weights


# Parameters
interest_rate_tensor: torch.Tensor = torch.tensor(data=0.05, requires_grad=False)
stock_price_A_tensor: torch.Tensor = torch.tensor(data=100.0, requires_grad=False)  # Stock price of asset A
stock_price_B_tensor: torch.Tensor = torch.tensor(data=150.0, requires_grad=False)  # Stock price of asset B
stock_price_C_tensor: torch.Tensor = torch.tensor(data=200.0, requires_grad=False)  # Stock price of asset C
alpha_num: float = 0.01
iterations_num: int = 100

# Specify which GPU to use (0 or 1 for your case, or adjust as needed)
gpu_index: int = 0  # Change this to 1 if you want to use the second GPU

# Set the device based on the specified GPU index
gpu_device: torch.device = torch.device(f"cuda:{gpu_index}")

# Move tensors to the GPU
interest_rate_tensor: torch.Tensor = interest_rate_tensor.to(device=gpu_device, non_blocking=False)
stock_price_A_tensor: torch.Tensor = stock_price_A_tensor.to(device=gpu_device, non_blocking=False)
stock_price_B_tensor: torch.Tensor = stock_price_B_tensor.to(device=gpu_device, non_blocking=False)
stock_price_C_tensor: torch.Tensor = stock_price_C_tensor.to(device=gpu_device, non_blocking=False)

# Perform gradient descent using NAdam
optimal_weights: torch.Tensor = gradient_descent(
    interest_rate=interest_rate_tensor,
    stock_price_a=stock_price_A_tensor,
    stock_price_b=stock_price_B_tensor,
    stock_price_c=stock_price_C_tensor,
    alpha=alpha_num,
    iterations=iterations_num,
)
print(f"Optimal Weights: {optimal_weights}")

# Create a dictionary of the optimal weights with asset names as keys, rounded to 2 decimal places
assets: Tuple[str, str, str] = ("Asset A", "Asset B", "Asset C")
optimal_weights_dict: Dict[str, float] = {assets[i]: round(optimal_weights[i].item(), 2) for i in range(len(assets))}
print(f"Optimal Weights Dictionary: {optimal_weights_dict}")
