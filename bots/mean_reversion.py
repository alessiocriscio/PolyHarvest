import pandas as pd
import numpy as np

print("--- BACKTEST ---")

try:
    # 1. Loading dataset
    df = pd.read_csv('BTC_data.csv')
    
    # 2. Pulizia e formattazione dei dati
    df['z_score'] = pd.to_numeric(df['z_score'], errors='coerce')
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['z_score', 'timestamp'])

    # Static var for the simulation
    in_trade = False
    trade_start_time = None
    trade_durations = []

    # 3. Chronological scan of the market
    for index, row in df.iterrows():
        z = row['z_score']
        t = row['timestamp']
        
        # Entry Logic (Trigger |Z| > 2.0)
        if not in_trade and abs(z) > 2.0:
            in_trade = True
            trade_start_time = t
            
        # Exit Logic (Rebalancing |Z| < 0.5)
        elif in_trade and abs(z) < 0.5:
            in_trade = False
            # Calculates the duration of the trade in seconds
            duration = (t - trade_start_time).total_seconds()
            
            # Security filter for wrong timestamps
            if duration >= 0: 
                trade_durations.append(duration)

    # 4. Performance Metrics
    num_trades = len(trade_durations)
    if num_trades > 0:
        avg_dur = np.mean(trade_durations)
        med_dur = np.median(trade_durations)
        max_dur = np.max(trade_durations)
        
        print(f"Operazioni Chiuse: {num_trades}")
        print(f"Media (secondi): {avg_dur:.2f}")
        print(f"Mediana (secondi): {med_dur:.2f}")
        print(f"Max Drawdown Temporale (secondi): {max_dur:.2f}")
    else:
        print("Nessun trade elaborato.")

except Exception as e:
    print(f"Errore durante l'esecuzione: {e}")