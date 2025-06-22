import os
import pandas as pd
import numpy as np
import requests
import time
import logging
import pytz
import datetime
import dateutil.parser
from dogpile.cache.region import make_region

api_key = os.environ.get("POLYGON_API_KEY")
timezone = pytz.timezone("US/Eastern")

logging.basicConfig(level=logging.INFO)

frequency_map = {12: "Monthly", 4: "Quarterly", 1: "Yearly"}


def create_region():
    cache_dir = os.path.join(os.environ.get("HOME"), ".div_cache")
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    return make_region().configure(
        "dogpile.cache.dbm",
        arguments={"filename": os.path.join(cache_dir, "cachefile.dbm")},
        expiration_time=datetime.timedelta(days=30),
    )


region = create_region()


@region.cache_on_arguments()
def get_dividend_announcements(
    symbol: str, start: datetime.date, end: datetime.date
) -> list:
    uri_template = "https://api.polygon.io/v3/reference/dividends?ticker={symbol}&ex_dividend_date.gte={date}&limit=1000&order=asc&sort=ex_dividend_date&apiKey={apikey}"

    repeat = True
    date = start.strftime("%Y-%m-%d")
    uri = uri_template.format(apikey=api_key, date=date, symbol=symbol)
    last_date = start
    events = []
    while repeat and last_date <= end:
        try:
            r = requests.get(uri, timeout=(3, 10))
            r.raise_for_status()
            r = r.json()
            if "next_url" in r:
                uri = r["next_url"] + "&apiKey=" + api_key
                repeat = True
            else:
                repeat = False
            if "results" in r:
                if r["results"]:
                    last_date = dateutil.parser.parse(
                        r["results"][-1]["ex_dividend_date"]
                    ).date()
                    events.extend(r["results"])
                    logging.info(
                        f"size: {len(events)}, ex_dividend_date: {last_date}, symbol: {symbol}"
                    )

        except requests.exceptions.HTTPError as err:
            if r.status_code != 429:
                logging.error(err)
                raise err
            logging.info(err)
            time.sleep(60)
            repeat = True
        except Exception as e:
            logging.error(repr(e))
            repeat = False
    return events


def gather_dividends(symbol, start, end):
    dividend_events = get_dividend_announcements(symbol, start, end)
    events = pd.DataFrame.from_records(
        dividend_events, columns=dividend_events[0].keys()
    )
    # find outlier dividends
    special_dividends = events[events["frequency"] == 0].index
    events = events.drop(special_dividends)
    events["pay_date"] = pd.to_datetime(events["pay_date"])
    events["pay_date"] = events["pay_date"].dt.tz_localize(timezone)
    events = events.rename(columns={"pay_date": "Date", "ticker": "Symbol"})
    events = events.set_index("Date")
    return events


def trim_div_events(df, end):
    future_events = df[df.index > pd.to_datetime(end)].index
    return df.drop(future_events)


def dividend_keep_the_cash(prices, div_events, initial_cash):
    dividends_gathered = prices[["Close"]].merge(
        div_events[["cash_amount"]], left_index=True, right_index=True, how="outer"
    )
    dividends_gathered = dividends_gathered[["Close", "cash_amount"]]
    dividends_gathered["quantity"] = initial_cash / prices.iloc[0]["Close"]
    dividends_gathered["total_dividend"] = (
        dividends_gathered["cash_amount"] * dividends_gathered["quantity"]
    )
    dividends_gathered["value"] = (
        dividends_gathered["Close"] * dividends_gathered["quantity"]
    )
    return dividends_gathered


def dividend_snowball_with_contributions(
    prices,
    div_events,
    initial_cash,
    contribution_amount=0,
    contribution_interval_days=0,
):
    """
    Calculates the dividend snowball effect on stock quantity and portfolio value,
    including initial cash used for purchasing, and regular cash contributions.

    Args:
        prices (pd.DataFrame): DataFrame with 'Close' and 'quantity' columns,
                               indexed by Date. 'quantity' should be the initial quantity
                               *before* any initial cash purchase.
        div_events (pd.DataFrame): DataFrame with 'cash_amount' column,
                                   indexed by Date, representing dividend payments.
        initial_cash (float): An initial amount of cash to be used to buy shares
                              at the very first date in the prices DataFrame.
        contribution_amount (float): The amount of cash contributed at each interval.
        contribution_interval_days (int): The number of days between cash contributions.
                                          Set to 0 or None to disable contributions.

    Returns:
        pd.DataFrame: A DataFrame with 'Close', 'quantity', 'total_dividend',
                      'cash_contribution', 'new_shares_from_contribution', and 'value' columns,
                      indexed by Date, showing the dividend snowball effect with contributions.
    """

    # Ensure indices are datetime for proper merging and reindexing
    prices.index = pd.to_datetime(prices.index)
    div_events.index = pd.to_datetime(div_events.index)

    # 1. Prepare the base DataFrame with all dates from prices
    # And merge dividend events onto it.
    df = prices[["Close"]].copy()
    df = df.merge(
        div_events["cash_amount"], left_index=True, right_index=True, how="left"
    )

    # Initialize new columns
    df["quantity"] = np.nan
    df["daily_dividend"] = 0.0
    df["cash_contribution"] = 0.0
    df["new_shares_from_contribution"] = (
        0.0  # Shares from contributions (including initial cash)
    )

    # Determine the very first trading date
    first_trading_date = df.index[0]
    initial_close_price = df.loc[first_trading_date, "Close"]

    # Shares purchased with initial cash
    shares_from_initial_cash = 0
    if initial_cash > 0 and initial_close_price > 0:
        shares_from_initial_cash = initial_cash / initial_close_price
        df.loc[
            first_trading_date, "new_shares_from_contribution"
        ] += shares_from_initial_cash
        df.loc[
            first_trading_date, "cash_contribution"
        ] += initial_cash  # Mark initial cash as a contribution

    # Set the effective initial quantity
    effective_initial_quantity = shares_from_initial_cash
    df.loc[first_trading_date, "quantity"] = effective_initial_quantity

    # Handle initial dividend if applicable (similar to previous version)
    # This dividend is based on the effective_initial_quantity
    initial_total_div = 0
    if df.loc[first_trading_date, "cash_amount"] > 0:
        initial_total_div = (
            effective_initial_quantity * df.loc[first_trading_date, "cash_amount"]
        )
        df.loc[first_trading_date, "daily_dividend"] = initial_total_div
        df.loc[first_trading_date, "quantity"] += (
            initial_total_div / initial_close_price
        )

    # Fill NaN values in 'cash_amount' with 0 (no dividend on those days)
    df.fillna({"cash_amount": 0}, inplace=True)

    # 2. Determine contribution dates (excluding the very first date if initial_cash was used there)
    contribution_dates = []
    if contribution_amount > 0 and contribution_interval_days > 0:
        # Start contributions from the date *after* the first trading date
        # if initial cash was used, otherwise from the first trading date.
        start_contribution_calc_date = first_trading_date
        # If initial cash was added, the first contribution interval starts from the first date.
        # So the *next* contribution will be after the interval.
        if initial_cash > 0:
            start_contribution_calc_date += pd.Timedelta(
                days=contribution_interval_days
            )

        current_contribution_date = start_contribution_calc_date
        while current_contribution_date <= df.index[-1]:
            # Ensure the contribution date exists in the DataFrame's index
            if current_contribution_date in df.index:
                contribution_dates.append(current_contribution_date)
            else:
                next_valid_date_loc = df.index.searchsorted(
                    current_contribution_date, side="left"
                )
                if next_valid_date_loc < len(df.index):
                    contribution_dates.append(df.index[next_valid_date_loc])
                else:
                    break
            current_contribution_date += pd.Timedelta(days=contribution_interval_days)

    # Ensure contribution_dates are unique and sorted
    contribution_dates = pd.DatetimeIndex(contribution_dates)
    contribution_dates = contribution_dates.unique().sort_values()

    # Create a combined set of "event" dates: dividend dates and contribution dates
    # Make sure the first_trading_date is always included as an event if it's not already
    event_dates = sorted(
        list(
            set(
                df[df["cash_amount"] > 0].index.tolist()
                + contribution_dates.tolist()
                + [first_trading_date]
            )
        )
    )
    event_dates = [
        d for d in event_dates if d >= first_trading_date
    ]  # Ensure no dates before start

    # Initialize a temporary series to hold calculated quantity at event dates
    quantity_at_event_dates = pd.Series(index=df.index, dtype=float)
    quantity_at_event_dates.loc[first_trading_date] = df.loc[
        first_trading_date, "quantity"
    ]  # Initial quantity already set

    # Process events in chronological order
    for i, current_date in enumerate(event_dates):
        if (
            current_date < first_trading_date
        ):  # Skip events before the start of the price data
            continue
        if (
            current_date == first_trading_date and i == 0
        ):  # Already handled initial calculation
            continue

        # Get the quantity from the last calculated event date
        # Find the most recent calculated quantity before the current_date
        previous_calculated_dates = quantity_at_event_dates.index[
            (quantity_at_event_dates.index < current_date)
            & (quantity_at_event_dates.notna())
        ].tolist()

        if previous_calculated_dates:
            last_calculated_quantity = quantity_at_event_dates.loc[
                previous_calculated_dates[-1]
            ]
        else:
            # Fallback if no previous calculated quantity (shouldn't happen often if first_trading_date is set)
            last_calculated_quantity = df.loc[first_trading_date, "quantity"]

        current_quantity = last_calculated_quantity

        # Handle Dividend Event
        if current_date in df.index and df.loc[current_date, "cash_amount"] > 0:
            dividend_per_share = df.loc[current_date, "cash_amount"]
            total_dividend_received = dividend_per_share * current_quantity
            df.loc[current_date, "daily_dividend"] = total_dividend_received
            if df.loc[current_date, "Close"] > 0:  # Avoid division by zero
                current_quantity += (
                    total_dividend_received / df.loc[current_date, "Close"]
                )

        # Handle Cash Contribution Event (excluding the very first date if initial_cash was used there, as it's pre-calculated)
        if current_date in contribution_dates and current_date in df.index:
            # Check if this is a subsequent contribution, not the initial cash on first day
            if not (current_date == first_trading_date and initial_cash > 0):
                df.loc[current_date, "cash_contribution"] = contribution_amount
                if df.loc[current_date, "Close"] > 0:  # Avoid division by zero
                    new_shares = contribution_amount / df.loc[current_date, "Close"]
                    df.loc[current_date, "new_shares_from_contribution"] = new_shares
                    current_quantity += new_shares

        quantity_at_event_dates.loc[current_date] = current_quantity

    # Propagate the calculated quantities from event dates forward
    df["quantity"] = quantity_at_event_dates.ffill()

    # Fill 'quantity' for dates before the first event date with the initial quantity
    df.fillna({"quantity": df.loc[first_trading_date, "quantity"]}, inplace=True)

    # Calculate total_dividend and value
    df["total_dividend"] = df[
        "daily_dividend"
    ]  # Renaming for clarity based on original output columns

    df["value"] = df["Close"] * df["quantity"]

    # Select and reorder columns as in the original output, plus new ones
    final_df = df[
        [
            "Close",
            "quantity",
            "total_dividend",
            "cash_contribution",
            "new_shares_from_contribution",
            "value",
        ]
    ]
    return final_df


def create_approach_summary(name, final_value, initial_cash, cash_in_bank):
    profit = final_value - initial_cash
    gain = profit / initial_cash * 100
    total_profit = profit + cash_in_bank
    total_gain = total_profit / initial_cash * 100
    return pd.Series(
        [
            name,
            round(initial_cash, 2),
            round(final_value, 2),
            round(profit, 2),
            round(gain, 2),
            round(cash_in_bank, 2),
            round(total_profit, 2),
            round(total_gain, 2),
        ],
        index=[
            "Approach",
            "Cash Contributed($)",
            "Final Market Amount($)",
            "Market Profit($)",
            "Market Gain(%)",
            "Cash Kept($)",
            "Total Profit($)",
            "Total Gain(%)",
        ],
    )
