"""Testing Data Parallel Method."""

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

print(r"CUDA Available:", torch.cuda.is_available())

"""Https://pytorch.org/tutorials/beginner/blitz/data_parallel_tutorial.html."""
torch.cuda.init()
print(rf"torch.cuda.is_available(): {torch.cuda.is_available()}")

# Parameters and DataLoaders
main_input_size: int = 5
main_output_size: int = 2

batch_size: int = 128
data_size: int = 500000

device: torch.device = torch.device("cuda")


class RandomDataset(Dataset):
    """RandomDataset."""

    def __init__(self, size: int, length: int) -> None:
        """Initializes the RandomDataset with the given size and length.

        :param size: (int): The size of each data point.
        :param length: (int): The total number of data points.
        :return: None
        """
        self.len: int = length
        self.data: torch.Tensor = torch.randn(length, size)

    def __getitem__(self, index: int) -> torch.Tensor:
        """Returns the data at the specified index.

        :param index: (int): The index of the data point to retrieve.
        :return: torch.Tensor: The data point at the specified index.
        """
        return self.data[index]

    def __len__(self) -> int:
        """Returns the length of the dataset.

        :return: int: The length of the dataset.
        """
        return self.len


rand_loader: DataLoader = DataLoader(
    dataset=RandomDataset(size=main_input_size, length=data_size), batch_size=batch_size, shuffle=True
)


class Model(nn.Module):
    """A simple neural network model with a single linear layer."""

    def __init__(self, input_size: int, output_size: int) -> None:
        """Initializes the Model with the given input and output sizes.

        :param input_size: (int): The size of the input layer.
        :param output_size: (int): The size of the output layer.
        :return: None
        """
        super(Model, self).__init__()
        self.forward_conv: nn.Linear = nn.Linear(in_features=input_size, out_features=output_size)

    def forward(self, input_var: torch.Tensor) -> torch.Tensor:
        """Defines the forward pass of the model.

        :param input_var: (torch.Tensor): The input tensor to the model.
        :return: Any: The output tensor from the model.
        """
        output_var: torch.Tensor = self.forward_conv(input=input_var)
        print("\tIn Model: input size", input_var.size(), "output size", output_var.size())
        return output_var


model: Model = Model(input_size=main_input_size, output_size=main_output_size)
if torch.cuda.device_count() > 1:
    print("Let's use", torch.cuda.device_count(), "GPUs!")
    # dim = 0 [30, xxx] -> [10, ...], [10, ...], [10, ...] on 3 GPUs
    # noinspection PyTypeChecker
    # NOTE: THIS IS WHAT ACTIVATES DUAL GPU USAGE
    model: Model = nn.DataParallel(module=model)
model.to(device=device)

for data in rand_loader:
    input_tensor: torch.Tensor = data.to(device=device)
    # noinspection PyCallingNonCallable
    output: torch.Tensor = model(input_var=input_tensor)
    print("Outside: input size", input_tensor.size(), "output_size", output.size())
