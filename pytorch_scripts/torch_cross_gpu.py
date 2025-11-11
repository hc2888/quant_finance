"""Dummy example for Streamlit."""

import torch

print(r"CUDA Available:", torch.cuda.is_available())

if torch.cuda.is_available():
    # THIS IS WHERE YOU POINT THE TENSORS & MODEL TO THE GPU
    device1: torch.device = torch.device("cuda:0")
    device2: torch.device = torch.device("cuda:1")
    print(r"CUDA Device 0:", torch.cuda.get_device_name(0))
    print(r"CUDA Device 1:", torch.cuda.get_device_name(1))

    # Dummy tensors
    tensor1: torch.Tensor = torch.randn(1000, 1000).to(device1)
    tensor2: torch.Tensor = torch.randn(1000, 1000).to(device2)

    # Perform some dummy operations
    result1: torch.Tensor = tensor1 + 1
    result2: torch.Tensor = tensor2 * 2

    # Move results to device1 and add them
    result1 = result1.to(device1)
    final_result: torch.Tensor = result1 + result2.to(device1)

    print("Computation Result (first element):", final_result[0, 0])

    print(r"IT WORKED!!!!")
else:
    print("NOTHING WORKED")
