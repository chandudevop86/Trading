def run_trading_cycle(context):
    data = fetch_data(context)

    signals = run_strategy(context, data)

    validated = validate_signals(signals)

    reviewed = review_trades(validated)

    executable = apply_execution_guard(reviewed)

    result = execute_trades(executable)

    log_cycle(context, signals, validated, reviewed, executable, result)

    return result