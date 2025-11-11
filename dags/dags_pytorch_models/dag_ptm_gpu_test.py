"""Dummy PyTorch model example to test it is running on the GPU."""

from typing import Any, Dict, List, Tuple, Union

import numpy as np
import pandas as pd
from airflow.sdk import dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, store_dag_info
from torch import Tensor, device
from torch.nn import Linear, Module
from torch.utils.data import DataLoader

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "ptm_gpu_test"


# ----------------------------------------------------------------------------------------------------------------------
def generate_trend_data(
    n_samples: int = 2_000,
    seq_len: int = 128,
    n_features: int = 3,
    n_classes: int = 3,
) -> Tuple[Tensor, Tensor]:
    """Generate synthetic temporal data with clear uptrend/flat/downtrend patterns.

    Rule:
    - Class 0: decreasing trend in feature[0]
    - Class 1: flat feature[0]
    - Class 2: increasing trend in feature[0]

    Other features remain pure noise.

    :param: n_samples: int: Number of samples.
    :param: seq_len: int: Sequence length (must equal past_length).
    :param: n_features: int: Features per timestep.
    :param: n_classes: int: Number of classes.

    :returns: Tuple[torch.Tensor, torch.Tensor]: x (B, T, F), y (B)
    """
    from torch import float32, long, tensor

    samples_per_class = n_samples // n_classes
    xs_var, ys_var = [], []

    timesteps = np.arange(seq_len, dtype=np.float32)

    c_num: int
    for c_num in range(n_classes):
        x_var = np.random.randn(samples_per_class, seq_len, n_features).astype(np.float32)

        trend = 0
        if c_num == 0:  # downtrend
            trend = -0.05 * timesteps
        elif c_num == 1:  # flat
            trend = np.zeros(seq_len, dtype=np.float32)
        elif c_num == 2:  # uptrend
            trend = 0.05 * timesteps

        # Add trend to first feature
        x_var[:, :, 0] += trend

        y = np.full(samples_per_class, c_num, dtype=np.int64)
        xs_var.append(x_var)
        ys_var.append(y)

    x_np = np.vstack(xs_var)
    y_np = np.concatenate(ys_var)

    # Shuffle
    idx = np.random.permutation(len(y_np))
    x_np, y_np = x_np[idx], y_np[idx]

    x_var = tensor(data=x_np, dtype=float32)
    y_var = tensor(data=y_np, dtype=long)
    return x_var, y_var


def evaluate(
    loader: DataLoader,
    model: Module,
    classifier_head: Linear,
    gpu_device: device,
    past_length: int,
    input_size: int,
    time_feat_dim: int,
    prediction_length: int,
    split_name: str,
) -> None:
    """Run evaluation on a given dataloader and print metrics.

    :param loader: DataLoader: PLACEHOLDER.
    :param model: Module: PLACEHOLDER.
    :param classifier_head: Linear: PLACEHOLDER.
    :param gpu_device: torch.device: PLACEHOLDER.
    :param past_length: int: PLACEHOLDER.
    :param input_size: int: PLACEHOLDER.
    :param time_feat_dim: int: PLACEHOLDER.
    :param prediction_length: int: PLACEHOLDER.
    :param split_name: str: PLACEHOLDER.
    """
    from sklearn.metrics import classification_report, confusion_matrix
    from torch import no_grad, ones, zeros

    model.eval()
    y_true: list[int] = []
    y_pred: list[int] = []
    with no_grad():
        for bx_batch, by_batch in loader:
            bx_batch = bx_batch.to(gpu_device)
            by_batch = by_batch.to(gpu_device)

            past_time_features = zeros(
                bx_batch.shape[0],
                past_length,
                time_feat_dim,
                dtype=bx_batch.dtype,
                device=gpu_device,
            )
            future_time_features = zeros(
                bx_batch.shape[0],
                prediction_length,
                time_feat_dim,
                dtype=bx_batch.dtype,
                device=gpu_device,
            )
            past_observed_mask = ones(
                bx_batch.shape[0],
                past_length,
                input_size,
                dtype=bx_batch.dtype,
                device=gpu_device,
            )

            outputs: Any = model(
                past_values=bx_batch,
                past_time_features=past_time_features,
                future_time_features=future_time_features,
                past_observed_mask=past_observed_mask,
            )

            last_hidden: Tensor = outputs[0]
            hidden: Tensor = last_hidden[:, -1, :]
            logits: Tensor = classifier_head(hidden)
            preds: Tensor = logits.argmax(dim=-1)

            y_true.extend(by_batch.cpu().numpy().tolist())
            y_pred.extend(preds.cpu().numpy().tolist())

    print(f"\n=== {split_name} Metrics ===")
    print("Confusion Matrix:")
    print(confusion_matrix(y_true, y_pred, labels=[0, 1, 2]))
    print("Classification Report:")
    print(classification_report(y_true, y_pred, labels=[0, 1, 2]))


@task(task_id="auto_former", trigger_rule=DEFAULT_TRIGGER)
def auto_former() -> None:
    """Train/evaluate Auto Former + linear head with trend-based synthetic data."""
    from sklearn.model_selection import train_test_split
    from torch import ones, optim, zeros
    from torch.cuda import device_count
    from torch.nn import CrossEntropyLoss, DataParallel
    from torch.utils.data import Subset, TensorDataset
    from transformers import AutoformerConfig, AutoformerModel

    model_device: device = device(device="cuda")
    print(f"Device: {model_device}")

    # ----- hyperparameters -----
    input_size: int = 3
    n_classes: int = 3
    prediction_length: int = 1
    context_length: int = 128
    lags_sequence: List[int] = [1]

    d_model: int = 64
    n_heads: int = 4
    num_encoder_layers: int = 2
    num_decoder_layers: int = 2
    num_epochs: int = 3  # NOTE: longer training

    max_lag: int = max(lags_sequence)  # 1
    past_length: int = context_length + max_lag  # 129
    time_feat_dim: int = 6
    expected_channels: int = (len(lags_sequence) + 1) * input_size + time_feat_dim
    print(f"Expected input channels = {expected_channels}")

    # ----- config -----
    config: AutoformerConfig = AutoformerConfig(
        input_size=input_size,
        prediction_length=prediction_length,
        context_length=context_length,
        d_model=d_model,
        n_heads=n_heads,
        num_encoder_layers=num_encoder_layers,
        num_decoder_layers=num_decoder_layers,
        lags_sequence=lags_sequence,
        num_time_features=time_feat_dim,
    )

    # ----- data -----
    bx, by = generate_trend_data(seq_len=past_length, n_features=input_size, n_classes=n_classes)
    dataset: TensorDataset = TensorDataset(bx, by)

    # Stratified train/val split
    indices = list(range(len(dataset)))
    train_idx, val_idx, _, _ = train_test_split(
        indices,
        by.numpy(),
        test_size=0.2,
        random_state=42,
        stratify=by.numpy(),
    )

    train_dataset = Subset(dataset, train_idx)
    val_dataset = Subset(dataset, val_idx)

    train_loader: DataLoader = DataLoader(train_dataset, batch_size=32, shuffle=True)
    val_loader: DataLoader = DataLoader(val_dataset, batch_size=32, shuffle=False)

    # ----- model -----
    model: Union[AutoformerModel, DataParallel, Module] = AutoformerModel(config)
    if device_count() > 1:
        print(f"Using {device_count()} GPUs with DataParallel")
        model = DataParallel(model)
    model = model.to(model_device)

    optimizer: optim.Adam = optim.Adam(model.parameters(), lr=1e-3)
    classifier_head: Linear = Linear(config.d_model, n_classes).to(model_device)

    # ----- training -----
    for epoch in range(num_epochs):
        model.train()
        epoch_loss: float = 0.0
        for bx_batch, by_batch in train_loader:
            bx_batch = bx_batch.to(model_device)
            by_batch = by_batch.to(model_device)

            past_time_features = zeros(
                bx_batch.shape[0],
                past_length,
                time_feat_dim,
                dtype=bx_batch.dtype,
                device=model_device,
            )
            future_time_features = zeros(
                bx_batch.shape[0],
                prediction_length,
                time_feat_dim,
                dtype=bx_batch.dtype,
                device=model_device,
            )
            past_observed_mask = ones(
                bx_batch.shape[0],
                past_length,
                input_size,
                dtype=bx_batch.dtype,
                device=model_device,
            )

            optimizer.zero_grad()

            outputs: Any = model(
                past_values=bx_batch,
                past_time_features=past_time_features,
                future_time_features=future_time_features,
                past_observed_mask=past_observed_mask,
            )

            last_hidden: Tensor = outputs[0]
            hidden: Tensor = last_hidden[:, -1, :]
            logits: Tensor = classifier_head(hidden)
            loss: Tensor = CrossEntropyLoss()(logits, by_batch)
            loss.backward()
            optimizer.step()
            epoch_loss += float(loss.item())

        print(f"Epoch {epoch + 1}/{num_epochs}, Loss: {epoch_loss / len(train_loader):.4f}")

    # ----- evaluation -----
    evaluate(
        train_loader,
        model,
        classifier_head,
        model_device,
        past_length,
        input_size,
        time_feat_dim,
        prediction_length,
        split_name="Train",
    )
    evaluate(
        val_loader,
        model,
        classifier_head,
        model_device,
        past_length,
        input_size,
        time_feat_dim,
        prediction_length,
        split_name="Validation",
    )


# ----------------------------------------------------------------------------------------------------------------------
def generate_temporal_data(n_samples: int = 5_000, seq_len: int = 30) -> pd.DataFrame:
    """Generate temporal timestep data.

    :param n_samples: int: Number of data samples.
    :param seq_len: int: How long the sequence is.

    :return: pd.DataFrame: The dataframe with the model data.
    """
    time_idx: np.ndarray = np.arange(start=0, stop=n_samples)
    symbol: List[str] = ["AAPL"] * n_samples

    # features with temporal structure
    # `np.cos, np.sin, np.radians, np.exp` does not accept keyword arguments
    feat_0: np.ndarray = np.sin(time_idx / 10) + np.random.normal(scale=0.1, size=n_samples)
    feat_1: np.ndarray = np.cos(time_idx / 20) + np.random.normal(scale=0.1, size=n_samples)
    feat_2: np.ndarray = np.random.normal(size=n_samples)

    # target rule:
    # class 0 if rolling mean of feat_0 last seq_len < -0.2
    # class 1 if rolling mean of feat_0 last seq_len > 0.2
    # class 2 otherwise
    roll_mean: np.ndarray = pd.Series(data=feat_0).rolling(window=seq_len, min_periods=1).mean().values
    target: np.ndarray = np.where(roll_mean < -0.2, 0, np.where(roll_mean > 0.2, 1, 2))

    data_df: pd.DataFrame = pd.DataFrame(
        data={
            "time_idx": time_idx,
            "symbol": symbol,
            "feat_0": feat_0,
            "feat_1": feat_1,
            "feat_2": feat_2,
            "target": target,
        }
    )

    return data_df


# ----------------------------------------------------------------------------------------------------------------------
def invoke_tft(gpu_nomen: int = 0, num_epochs: int = 5) -> None:
    """Invoke a Temporal Fusion Transformer.

    :param gpu_nomen: int: The target GPU to be used.
    :param num_epochs: int: The number of epochs to invoke.
    """
    from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
    from pytorch_forecasting.data.encoders import NaNLabelEncoder
    from pytorch_forecasting.metrics import CrossEntropy
    from pytorch_forecasting.utils import TupleOutputMixIn
    from pytorch_lightning import LightningModule
    from sklearn.metrics import classification_report, confusion_matrix
    from torch import no_grad, optim

    model_device: device = device(device=rf"cuda:{gpu_nomen}")
    print(f"Using device: {model_device.type}")
    # Generate synthetic temporal dataset
    temporal_df: pd.DataFrame = generate_temporal_data()
    # Cutoff for train/val split
    training_cutoff: int = int(temporal_df["time_idx"].max() * 0.8)
    feature_cols: List[str] = ["feat_0", "feat_1", "feat_2"]
    # Define a training dataset for the Temporal Fusion Transformer.
    # TimeSeriesDataSet automatically transforms the raw DataFrame into sliding windows of encoder (past context)
    # and decoder (future target), groups series by "symbol", extracts the target column, handles real-valued
    # time-varying features, and applies categorical encodings/normalization so the model can be trained directly on
    # the structured time-series data.
    training_dataset: TimeSeriesDataSet = TimeSeriesDataSet(
        data=temporal_df[temporal_df["time_idx"] <= training_cutoff],
        # Integer typed column denoting the time index within data.
        # This column is used to determine the sequence of samples.
        # If there are no missing observations, the time index should increase by +1 for each subsequent sample.
        # The first time_idx for each series does not necessarily have to be 0 but any value is allowed.
        time_idx="time_idx",
        target="target",
        group_ids=["symbol"],
        # maximum length to encode. This is the maximum history length used by the time series dataset.
        max_encoder_length=20,
        # maximum prediction/decoder length (choose this not too short as it can help convergence)
        max_prediction_length=1,
        # list of continuous variables that change over time and are known in the future
        # (e.g. price of a product, but not demand of a product).
        time_varying_unknown_reals=feature_cols,
        # Encode the categorical "symbol" column into integer labels using NaNLabelEncoder.
        # PyTorch models can only work with numeric tensors, not string IDs.
        # This step converts each symbol (e.g., stock ticker) into an integer class index,
        # and also handles missing values safely. Without this, the model could not process
        # categorical series identifiers as inputs.
        categorical_encoders={"symbol": NaNLabelEncoder().fit(y=temporal_df["symbol"])},
        # Disable target normalization (use raw target values).
        # Normally, target_normalizer rescales the target (e.g., via StandardScaler or EncoderNormalizer)
        # to stabilize training and improve convergence. Setting it to None means the model will
        # directly predict on the raw scale of the target variable, which can be fine for classification
        # tasks or when the target is already in a well-scaled range.
        target_normalizer=None,
    )
    # Define a validation dataset for the Temporal Fusion Transformer.
    # TimeSeriesDataSet automatically transforms the raw DataFrame into sliding windows of encoder (past context)
    # and decoder (future target), groups series by "symbol", extracts the target column, handles real-valued
    # time-varying features, and applies categorical encodings/normalization so the model can be trained directly on
    # the structured time-series data.
    validation_dataset: TimeSeriesDataSet = TimeSeriesDataSet(
        data=temporal_df[temporal_df["time_idx"] > training_cutoff],
        # Integer typed column denoting the time index within data.
        # This column is used to determine the sequence of samples.
        # If there are no missing observations, the time index should increase by +1 for each subsequent sample.
        # The first time_idx for each series does not necessarily have to be 0 but any value is allowed.
        time_idx="time_idx",
        target="target",
        group_ids=["symbol"],
        # maximum length to encode. This is the maximum history length used by the time series dataset.
        max_encoder_length=20,
        # maximum prediction/decoder length (choose this not too short as it can help convergence)
        max_prediction_length=1,
        # list of continuous variables that change over time and are known in the future
        # (e.g. price of a product, but not demand of a product).
        time_varying_unknown_reals=feature_cols,
        # Encode the categorical "symbol" column into integer labels using NaNLabelEncoder.
        # PyTorch models can only work with numeric tensors, not string IDs.
        # This step converts each symbol (e.g., stock ticker) into an integer class index,
        # and also handles missing values safely. Without this, the model could not process
        # categorical series identifiers as inputs.
        categorical_encoders={"symbol": NaNLabelEncoder().fit(y=temporal_df["symbol"])},
        # Disable target normalization (use raw target values).
        # Normally, target_normalizer rescales the target (e.g., via StandardScaler or EncoderNormalizer)
        # to stabilize training and improve convergence. Setting it to None means the model will
        # directly predict on the raw scale of the target variable, which can be fine for classification
        # tasks or when the target is already in a well-scaled range.
        target_normalizer=None,
    )
    # Convert the training TimeSeriesDataSet into a PyTorch DataLoader.
    # This handles batching, shuffling (since train=True), and yields batches of (encoder/decoder inputs, targets)
    # so they can be fed directly into the Temporal Fusion Transformer during training.
    training_dataloader: DataLoader = training_dataset.to_dataloader(train=True, batch_size=64, num_workers=0)
    # Convert the validation TimeSeriesDataSet into a PyTorch DataLoader.
    # Since train=False, data is not shuffled — batches are drawn sequentially.
    # Provides (encoder/decoder inputs, targets) for model evaluation.
    validation_dataloader: DataLoader = validation_dataset.to_dataloader(train=False, batch_size=64, num_workers=0)
    # Initialize the Temporal Fusion Transformer model directly from the training dataset.
    # This configures the network architecture using dataset metadata (features, encoders, etc.),
    # sets the loss function (CrossEntropy for 3-class classification), specifies output size,
    # hidden layer dimensions, attention head count, and dropout rate, then moves the model to the GPU/CPU device.
    tft_model: LightningModule = TemporalFusionTransformer.from_dataset(
        dataset=training_dataset,
        loss=CrossEntropy(),
        output_size=3,  # 3 classes
        hidden_size=16,
        attention_head_size=2,
        dropout=0.1,
    ).to(model_device)
    # NOTE: OPTIMIZER: https://docs.pytorch.org/docs/stable/optim.html#algorithms
    model_optimizer: optim.Optimizer = optim.AdamW(params=tft_model.parameters(), lr=0.001)
    # NOTE: TRAINING LOOP
    epoch: int
    for epoch in range(num_epochs):
        # NOTE: Put model back into training mode at the start of each epoch.
        # NOTE: This re-enables dropout and batch-norm updates, which are required during training.
        # NOTE: If we switched the model to .eval() for validation at the end of the last epoch,
        # NOTE: calling .train() here ensures we don’t accidentally stay in eval mode.
        tft_model.train()
        epoch_loss: float = 0.0
        # batch_x: dictionary of input tensors (encoder/decoder features, categorical/continuous variables)
        batch_x: Dict[str, Tensor]
        # Iterate through validation dataloader, yielding input-target batches
        batch_y: Union[Tensor, Tuple[Tensor]]
        for batch_x, batch_y in training_dataloader:
            # Move batch inputs to the model device (CPU/GPU)
            batch_x: Dict[str, Tensor] = {key_var: value_var.to(model_device) for key_var, value_var in batch_x.items()}
            # If target is a tuple, extract the first element
            if isinstance(batch_y, tuple):
                batch_y: Tensor = batch_y[0]
            batch_y: Tensor = batch_y.to(model_device)
            # Reset all model parameter gradients to zero before backpropagation
            model_optimizer.zero_grad()
            # Run a forward pass through the model
            # Returns a NamedTuple-like object containing predictions and other outputs
            output: TupleOutputMixIn = tft_model(batch_x)
            # noinspection PyUnresolvedReferences
            training_loss: Tensor = tft_model.loss(output.prediction, batch_y)
            # Compute gradients of the loss with respect to model parameters via backpropagation
            training_loss.backward()
            # Update model parameters using the computed gradients according to the optimization algorithm
            model_optimizer.step()
            # Accumulate the scalar value of the current batch loss into the running total for this epoch
            epoch_loss += training_loss.item()
        print(f"Epoch {epoch+1}/5, Train Loss: {epoch_loss / len(training_dataloader):.4f}")
    # NOTE: Set the model to evaluation mode (disables dropout, batch norm updates, etc.)
    # NOTE: Switch layers like dropout / batch-norm into eval mode
    tft_model.eval()
    y_true_full: List[Union[int, float]] = []
    y_pred_full: List[Union[int, float]] = []
    # Temporarily disable gradient calculation to speed up inference and reduce memory usage
    with no_grad():  # don’t track gradients, save memory, speed up etc.
        # batch_x: dictionary of input tensors (encoder/decoder features, categorical/continuous variables)
        batch_x: Dict[str, Tensor]
        # Iterate through validation dataloader, yielding input-target batches
        batch_y: Union[Tensor, Tuple[Tensor]]
        for batch_x, batch_y in validation_dataloader:
            # Move batch inputs to the model device (CPU/GPU)
            batch_x: Dict[str, Tensor] = {key_var: value_var.to(model_device) for key_var, value_var in batch_x.items()}
            # If target is a tuple, extract the first element
            if isinstance(batch_y, tuple):
                batch_y: Tensor = batch_y[0]
            # Move target tensor to the model device
            batch_y: Tensor = batch_y.to(model_device)
            # Run a forward pass through the model
            # Returns a NamedTuple-like object containing predictions and other outputs
            model_output: TupleOutputMixIn = tft_model(batch_x)
            # Extract prediction tensor from model output
            # noinspection PyUnresolvedReferences
            model_predictions: Tensor = model_output.prediction
            # If predictions are 3D (batch, horizon, classes), keep last horizon step
            if model_predictions.ndim == 3:  # (batch, horizon, classes)
                model_predictions: Tensor = model_predictions[:, -1, :]
            # Take the class index with the highest probability
            model_predictions: Tensor = model_predictions.argmax(dim=-1)
            # Add ground-truth values from batch to full list
            y_true_full.extend(batch_y.cpu().numpy().tolist())
            # Add predicted values from batch to full list
            y_pred_full.extend(model_predictions.cpu().numpy().tolist())
    # NOTE: `np.ravel()` flattens an array into 1D view whenever possible.
    # NOTE: Unlike `np.flatten()`, which always returns a new copy,
    # NOTE: `np.ravel()` will return a VIEW of the same memory if possible (faster, less memory).
    # NOTE: `np.flatten()` always returns a copy.
    # NOTE: `np.ravel()` tries to return a view (shared memory) if the data is already laid out nicely in memory.
    # NOTE: If not, it falls back to making a copy.
    # Convert collected ground-truth values to a 1D NumPy array
    y_true_full: np.ndarray = np.array(object=y_true_full).ravel()
    # Convert collected predictions to a 1D NumPy array
    y_pred_full: np.ndarray = np.array(object=y_pred_full).ravel()

    confusion_matrix: np.ndarray = confusion_matrix(y_true=y_true_full, y_pred=y_pred_full, labels=[0, 1, 2])
    classification_report: str = classification_report(y_true=y_true_full, y_pred=y_pred_full, labels=[0, 1, 2])
    print("Confusion Matrix:")
    print(confusion_matrix)
    print("Classification Report:")
    print(classification_report)


@task(task_id="invoke_tft_0", trigger_rule=DEFAULT_TRIGGER)
def invoke_tft_0() -> None:
    """Run test on first GPU."""
    invoke_tft(gpu_nomen=0)


@task(task_id="invoke_tft_1", trigger_rule=DEFAULT_TRIGGER)
def invoke_tft_1() -> None:
    """Run test on second GPU."""
    invoke_tft(gpu_nomen=1)


# ----------------------------------------------------------------------------------------------------------------------
def gpu_avail_test(gpu_nomen: int = 0) -> None:
    """Https://pytorch.org/tutorials/beginner/blitz/data_parallel_tutorial.html.

    :param gpu_nomen: int: The target GPU to be used.
    """
    from typing import Any

    from torch import randn
    from torch.cuda import init, is_available
    from torch.utils.data import DataLoader, Dataset

    init()
    print(rf"torch.cuda.is_available(): {is_available()}")

    # Parameters and DataLoaders
    main_input_size: int = 5
    main_output_size: int = 2

    batch_size: int = 128
    data_size: int = 500_000

    model_device: device = device(device=rf"cuda:{gpu_nomen}")

    class RandomDataset(Dataset):
        """RandomDataset."""

        def __init__(self, size: int, length: int) -> None:
            """Initializes the RandomDataset with the given size and length.

            :param size: (int): The size of each data point.
            :param length: (int): The total number of data points.
            :return: None
            """
            self.len: int = length
            self.data: Tensor = randn(length, size)

        def __getitem__(self, index: int) -> Tensor:
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
        dataset=RandomDataset(size=main_input_size, length=data_size),
        batch_size=batch_size,
        shuffle=True,
    )

    class Model(Module):
        """A simple neural network model with a single linear layer."""

        def __init__(self, input_size: int, output_size: int) -> None:
            """Initializes the Model with the given input and output sizes.

            :param input_size: (int): The size of the input layer.
            :param output_size: (int): The size of the output layer.
            :return: None
            """
            super(Model, self).__init__()
            self.forward_conv: Linear = Linear(in_features=input_size, out_features=output_size)

        def forward(self, input_var: Tensor) -> Any:
            """Defines the forward pass of the model.

            :param input_var: (torch.Tensor): The input tensor to the model.
            :return: Any: The output tensor from the model.
            """
            output_var: Tensor = self.forward_conv(input=input_var)
            print("\tIn Model: input size", input_var.size(), "output size", output_var.size())
            return output_var

    model: Model = Model(input_size=main_input_size, output_size=main_output_size).to(device=model_device)

    for data in rand_loader:
        input_tensor: Tensor = data.to(device=model_device)
        # noinspection PyCallingNonCallable
        output: Any = model(input_var=input_tensor)
        print("Outside: input size", input_tensor.size(), "output_size", output.size())


@task(task_id="gpu_device_0", trigger_rule=DEFAULT_TRIGGER)
def gpu_device_0() -> None:
    """Run test on first GPU."""
    gpu_avail_test(gpu_nomen=0)


@task(task_id="gpu_device_1", trigger_rule=DEFAULT_TRIGGER)
def gpu_device_1() -> None:
    """Run test on second GPU."""
    gpu_avail_test(gpu_nomen=1)


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="multi_gpu_test", trigger_rule=DEFAULT_TRIGGER)
def multi_gpu_test() -> None:
    """Https://pytorch.org/tutorials/beginner/blitz/data_parallel_tutorial.html."""
    from torch import randn
    from torch.cuda import device_count, init, is_available
    from torch.nn import DataParallel
    from torch.utils.data import Dataset

    init()
    print(rf"torch.cuda.is_available(): {is_available()}")

    # Parameters and DataLoaders
    main_input_size: int = 5
    main_output_size: int = 2

    batch_size: int = 128
    data_size: int = 500_000

    model_device: device = device(device="cuda")

    class RandomDataset(Dataset):
        """RandomDataset."""

        def __init__(self, size: int, length: int) -> None:
            """Initializes the RandomDataset with the given size and length.

            :param size: (int): The size of each data point.
            :param length: (int): The total number of data points.
            :return: None
            """
            self.len: int = length
            self.data: Tensor = randn(length, size)

        def __getitem__(self, index: int) -> Tensor:
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
        dataset=RandomDataset(size=main_input_size, length=data_size),
        batch_size=batch_size,
        shuffle=True,
    )

    class Model(Module):
        """A simple neural network model with a single linear layer."""

        def __init__(self, input_size: int, output_size: int) -> None:
            """Initializes the Model with the given input and output sizes.

            :param input_size: (int): The size of the input layer.
            :param output_size: (int): The size of the output layer.
            :return: None
            """
            super(Model, self).__init__()
            self.forward_conv: Linear = Linear(in_features=input_size, out_features=output_size)

        def forward(self, input_var: Tensor) -> Any:
            """Defines the forward pass of the model.

            :param input_var: (torch.Tensor): The input tensor to the model.
            :return: Any: The output tensor from the model.
            """
            output_var: Tensor = self.forward_conv(input=input_var)
            print("\tIn Model: input size", input_var.size(), "output size", output_var.size())
            return output_var

    model: Model = Model(input_size=main_input_size, output_size=main_output_size)
    if device_count() > 1:
        print("Let's use", device_count(), "GPUs!")
        # dim = 0 [30, xxx] -> [10, ...], [10, ...], [10, ...] on 3 GPUs
        # noinspection PyTypeChecker
        # NOTE: THIS IS WHAT ACTIVATES DUAL GPU USAGE
        model: Model = DataParallel(module=model)
    model.to(device=model_device)

    for data in rand_loader:
        input_tensor: Tensor = data.to(device=model_device)
        # noinspection PyCallingNonCallable
        output: Any = model(input_var=input_tensor)
        print("Outside: input size", input_tensor.size(), "output_size", output.size())


# ----------------------------------------------------------------------------------------------------------------------
@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["pytorch_models"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    node_bridge_1: EmptyOperator = EmptyOperator(task_id="node_bridge_1", trigger_rule=DEFAULT_TRIGGER)
    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences
    (
        store_dag_info()
        >> auto_former()
        >> [invoke_tft_0(), invoke_tft_1()]
        >> node_bridge_1
        >> [gpu_device_0(), gpu_device_1()]
        >> multi_gpu_test()
        >> dag_finish
    )


# ----------------------------------------------------------------------------------------------------------------------
main()
