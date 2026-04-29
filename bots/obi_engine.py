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

def parse_ibkr_depth(dom_bids, dom_asks, top_n=5):
    """
    Takes lists of ib_insync.DOMLevel objects for bids and asks,
    takes the top top_n levels by size, aggregates bid and ask volumes, and returns a DataFrame
    with columns bid_size and ask_size.
    """
    # Function to extract size safely from DOMLevel object or dict
    def get_size(level):
        return getattr(level, 'size', level.get('size', 0) if isinstance(level, dict) else 0)
        
    # Sort by size descending and take top_n
    bids_sorted = sorted(dom_bids, key=get_size, reverse=True)[:top_n]
    asks_sorted = sorted(dom_asks, key=get_size, reverse=True)[:top_n]
    
    bid_vol = sum(get_size(b) for b in bids_sorted)
    ask_vol = sum(get_size(a) for a in asks_sorted)
    
    return pd.DataFrame({"bid_size": [bid_vol], "ask_size": [ask_vol]})

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