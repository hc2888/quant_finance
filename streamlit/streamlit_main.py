"""Stock Finance Streamlit Example."""

import pandas as pd
import yfinance as yf

import streamlit as st


@st.cache_data  # type: ignore[attr-defined]
def load_data() -> pd.DataFrame:
    """
    Load the list of S&P 500 companies from Wikipedia and return a DataFrame.

    :return: pd.DataFrame: DataFrame containing S&P 500 companies data.
    """
    components: pd.DataFrame = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
    if "SEC filings" in components.columns:
        components = components.drop("SEC filings", axis=1)
    return components.set_index("Symbol")


@st.cache_data  # type: ignore[attr-defined]
def load_quotes(asset: str) -> pd.DataFrame:
    """
    Load historical quotes for the given asset using yfinance.

    :param asset: str: The ticker symbol of the asset to load quotes for.
    :return: pd.DataFrame: DataFrame containing historical quotes for the asset.
    """
    return yf.download(asset)


def main() -> None:
    """Main function to run the Streamlit app."""
    components: pd.DataFrame = load_data()
    title = st.empty()  # type: ignore[attr-defined]
    st.sidebar.title("Options")  # type: ignore[attr-defined]

    def label(symbol: str) -> str:
        """
        Generate a label for the dropdown select box.

        :param symbol: str: The ticker symbol of the company.
        :return: str: The label combining the symbol and the company's name.
        """
        attribute_var = components.loc[symbol]
        return f"{symbol} - {attribute_var.Security}"

    if st.sidebar.checkbox("View companies list"):  # type: ignore[attr-defined]
        columns_to_display = ["Security", "GICS Sector", "Date first added", "Founded"]
        available_columns = [col for col in columns_to_display if col in components.columns]
        if available_columns:
            st.dataframe(components[available_columns])  # type: ignore[attr-defined]
        else:
            st.warning("None of the selected columns are available in the data.")  # type: ignore[attr-defined]

    st.sidebar.subheader("Select asset")  # type: ignore[attr-defined]
    asset = st.sidebar.selectbox(  # type: ignore[attr-defined]
        "Click below to select a new asset", components.index.sort_values(), index=3, format_func=label
    )
    title.title(components.loc[asset].Security)
    if st.sidebar.checkbox("View company info", True):  # type: ignore[attr-defined]
        st.table(components.loc[asset])  # type: ignore[attr-defined]
    data0 = load_quotes(asset)
    data = data0.copy().dropna()
    data.index.name = None

    section = st.sidebar.slider(  # type: ignore[attr-defined]
        "Number of quotes", min_value=30, max_value=min([2000, data.shape[0]]), value=500, step=10
    )

    data2 = data[-section:]["Adj Close"].to_frame("Adj Close")

    sma = st.sidebar.checkbox("SMA")  # type: ignore[attr-defined]
    if sma:
        period = st.sidebar.slider(  # type: ignore[attr-defined]
            "SMA period", min_value=5, max_value=500, value=20, step=1
        )
        data[f"SMA {period}"] = data["Adj Close"].rolling(period).mean()
        data2[f"SMA {period}"] = data[f"SMA {period}"].reindex(data2.index)

    sma2 = st.sidebar.checkbox("SMA2")  # type: ignore[attr-defined]
    if sma2:
        period2 = st.sidebar.slider(  # type: ignore[attr-defined]
            "SMA2 period", min_value=5, max_value=500, value=100, step=1
        )
        data[f"SMA2 {period2}"] = data["Adj Close"].rolling(period2).mean()
        data2[f"SMA2 {period2}"] = data[f"SMA2 {period2}"].reindex(data2.index)

    st.subheader("Chart")  # type: ignore[attr-defined]
    st.line_chart(data2)  # type: ignore[attr-defined]

    if st.sidebar.checkbox("View statistics"):  # type: ignore[attr-defined]
        st.subheader("Statistics")  # type: ignore[attr-defined]
        st.table(data2.describe())  # type: ignore[attr-defined]

    if st.sidebar.checkbox("View quotes"):  # type: ignore[attr-defined]
        st.subheader(f"{asset} historical data")  # type: ignore[attr-defined]
        st.write(data2)  # type: ignore[attr-defined]

    st.sidebar.title("About")  # type: ignore[attr-defined]
    st.sidebar.info(  # type: ignore[attr-defined]
        "This app is a simple example of "
        "using Streamlit to create a financial data web app.\n"
        "\nIt is maintained by [Paduel]("
        "https://twitter.com/paduel_py).\n\n"
        "Check the code at https://github.com/paduel/streamlit_finance_chart"
    )


if __name__ == "__main__":
    main()
