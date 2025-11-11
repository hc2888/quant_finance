"""Utility functions having to do with getting things ready for PyTorch."""

from typing import List, Tuple

import pandas as pd
import torch
from torch import Tensor
from torch.utils.data import DataLoader, Dataset


# ----------------------------------------------------------------------------------------------------------------------
class RandomTensorDataset(Dataset):
    """Generate dataset of random tensors of shape 3 * pixel dimension * pixel dimension."""

    def __init__(self, pixel_dim: int = 32, num_samples: int = 1000, num_classes: int = 10) -> None:
        """Initializes the dataset with a specified number of samples and classes.

        :param pixel_dim: int: The pixel value in width and height.
        :param num_samples: int: The number of samples to generate.
        :param num_classes: int: The number of classes.
        """
        self.num_samples: int = num_samples
        self.num_classes: int = num_classes
        # Random 3 x `pixel_dim` x `pixel_dim` tensors
        self.data: Tensor = torch.randn(size=(num_samples, 3, pixel_dim, pixel_dim))
        self.labels: Tensor = torch.randint(low=0, high=num_classes, size=(num_samples,))  # Random labels

    def __len__(self) -> int:
        """Returns the number of samples in the dataset.

        Method is called when using the `len()` function on an instance of the class.

        :return: int: The number of samples.
        """
        return self.num_samples

    def __getitem__(self, index_num: int) -> Tuple[Tensor, Tensor]:
        """Retrieves the data and label at a given index.

        Method is called when using indexing syntax (e.g., dataset[index]) on an instance of the class.
        It allows the object to support indexed access and iteration.

        :param index_num: int: The index of the data and label to retrieve.

        :return: Tuple[Tensor, Tensor]: The data and label at the specified index.
        """
        return self.data[index_num], self.labels[index_num]


# ----------------------------------------------------------------------------------------------------------------------
def pandas_to_pytorch_dataload(
    pandas_df: pd.DataFrame,
    target_col: str,
    batch_size: int = 32,
    num_workers: int = 16,
    three_dim_array: bool = False,
    timestep: int = 30,
    shuffle_data_rows: bool = True,
) -> DataLoader:
    """Converts a Pandas Dataframe into a PyTorch Dataloader.

    :param pandas_df: pd.DataFrame: A Pandas Dataframe with all the features and a target column. The features must be
        converted into numerical datatypes before passing into this function.
    :param target_col: str: The name of the column with the "target" / "dependant" labels.
    :param batch_size: int: The number of data rows to pass into the PyTorch model for each iteration in a given epoch.
    :param num_workers: int: The number of subprocesses to use for data loading during a PyTorch
        training / inference run.
    :param three_dim_array: bool: Whether you want the dataset to be 3-dimensional.
    :param timestep: int: The number of time-steps if you want the dataset to be if the dataset is 3-dimensional.
    :param shuffle_data_rows: bool: Whether to shuffle the dataset "rows" at every EPOCH run during training.

    :return: DataLoader: PyTorch DataLoader ready to be used for a PyTorch training / inference run.
    """
    import numpy as np
    import torch
    from torch.utils.data import TensorDataset

    # Separate features and target columns
    df_features: pd.DataFrame = pandas_df.drop(columns=target_col)
    df_targets: pd.Series = pandas_df[target_col]

    # Convert features and targets columns to NumPy arrays
    np_features: np.ndarray = df_features.values
    np_targets: np.ndarray = df_targets.values

    # Reshape the features to 3D format (batch, timestep, features) if `three_dim_array` is `True`.
    if three_dim_array:
        # Calculate new shape
        num_samples: int = len(pandas_df) // timestep
        num_features: int = df_features.shape[1]

        # Reshape features and targets
        np_features: np.ndarray = np_features[: num_samples * timestep].reshape(num_samples, timestep, num_features)
        # Select the last target of each timestep chunk.
        # `[timestep - 1::timestep]` ensures that for each sequence of timestep rows in np_features,
        # we are picking the target value at the end of that sequence.
        # `[:num_samples]` ensures that the number of targets matches the number of samples in np_features after
        # reshaping, eliminating any extra target values that might arise due to division.

        np_targets: np.ndarray = np_targets[timestep - 1 :: timestep][:num_samples]

    # Convert NumPy arrays to PyTorch tensors
    tensor_features: torch.Tensor = torch.tensor(data=np_features, dtype=torch.float32)
    tensor_targets: torch.Tensor = torch.tensor(data=np_targets, dtype=torch.float32)

    # Create a TensorDataset
    torch_dataset: TensorDataset = TensorDataset(tensor_features, tensor_targets)

    # Create a DataLoader
    torch_dataloader: DataLoader = DataLoader(
        dataset=torch_dataset,
        # `batch_size` & `num_workers` values have to be adjusted depending on dimensions of the Tensor.
        # For example, a high dimensional object like `3 * 224 * 224` images might cause out-of-memory errors unless the
        # `batch_size` & `num_workers` values are reduced.
        batch_size=batch_size,
        # `num_workers` specifies the number of subprocesses to use for data loading.
        # By setting it to a number greater than `0`, data loading is done in parallel by multiple worker subprocesses.
        # This allows the data to be loaded faster, reducing the idle time of the GPU or CPU during training.
        # While one batch of data is being used for training, the next batch is loaded and prepared by other workers.
        num_workers=num_workers,
        # Specify whether the data should be SHUFFLED at every EPOCH.
        shuffle=shuffle_data_rows,
        # Indicates whether the data loader should copy Tensors into CUDA pinned-memory before returning them.
        pin_memory=True,
    )

    return torch_dataloader


# ----------------------------------------------------------------------------------------------------------------------
def linear_in_features_size(
    pixel_dim: int,
    last_conv_layer_num: int,
    pad_kern_stride_seq_list: List[List[int]],
) -> int:
    """Calculate the total number of features / dims for the first Linear Layer's `in_features` parameter.

    :param pixel_dim: int: The pixel value in width and height of the images that the model will be trained on.
    :param last_conv_layer_num: int: Desired `out_channels` parameter value for the last convolutional (NOT pooling)
        layer.
    :param pad_kern_stride_seq_list: List[List[int]]: A list of convolutional and / or pooling layers
        (in sequential order) that have been put together in the Model Class. Values within each list must be
        the `padding`, `kernel_size`, and `stride` parameters (in that order).

    :return: int: Total number of features / dims for the first Linear Layer's `in_features` parameter.
    """
    # Initial placeholder value for `output_size`.
    output_size: int = 0

    index: int
    param_list: List[int]
    for index, param_list in enumerate(pad_kern_stride_seq_list):
        padding: int = param_list[0]
        kernel_size: int = param_list[1]
        stride: int = param_list[2]

        if index == 0:
            input_size: int = pixel_dim
        else:
            input_size: int = output_size

        # Output size calculation for a convolutional or pooling layer.
        output_size: int = ((input_size - kernel_size + 2 * padding) // stride) + 1

    # Total number of features / dims for the first Linear Layer's `in_features` parameter.

    linear_in_features_dims: int = (output_size**2) * last_conv_layer_num

    return linear_in_features_dims
