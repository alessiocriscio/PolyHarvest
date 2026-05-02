import pandas as pd
import numpy as np

def calculate_obi(order_book: pd.DataFrame) -> float:
    """
    Calculates the Order Book Imbalance (OBI) from a pandas DataFrame representing the Limit Order Book levels.

    Mathematical formula: OBI = (Bid_Volume - Ask_Volume) / (Bid_Volume + Ask_Volume).

    If Bid_Volume + Ask_Volume == 0, OBI is 0.

    :param order_book: pandas DataFrame with columns 'bid_size' and 'ask_size'
    :return: OBI
    """
    bid_volume = order_book['bid_size'].sum()
    ask_volume = order_book['ask_size'].sum()

    # Handling division by zero
    denominator = bid_volume + ask_volume
    obi = np.where(denominator == 0, 0, (bid_volume - ask_volume) / denominator)

    return float(obi)

def save_obi(obi, filename):
    with open(filename, 'a') as f:
        f.write(str(obi) + '\n')

if __name__ == "__main__":
    # Generating a dummy DataFrame for testing
    data = {
        'bid_size': [10, 20, 30, 40, 50],
        'ask_size': [15, 25, 35, 45, 55]
    }
    order_book = pd.DataFrame(data)

    # Calculation and printing of OBI
    obi = calculate_obi(order_book)
    print("OBI:", obi)
    save_obi(obi, 'obi_data.txt')
