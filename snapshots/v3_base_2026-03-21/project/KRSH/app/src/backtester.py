def run_backtest(df, strategy):

    trades = strategy(df)

    balance = 10000
    risk = 0.01

    results = []

    for trade in trades:

        entry = trade["price"]

        if trade["type"] == "BUY":
            exit_price = entry * 1.02
        else:
            exit_price = entry * 0.98

        pnl = (exit_price - entry)

        balance += pnl

        results.append(pnl)

    win_rate = len([r for r in results if r>0]) / len(results)

    return {
        "balance":balance,
        "win_rate":win_rate,
        "trades":len(results)
    }