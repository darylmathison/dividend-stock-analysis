import os
import pandas as pd
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
    future_events = events[events["pay_date"] > pd.to_datetime(end)].index
    events = events.drop(future_events)
    events = events.rename(columns={"pay_date": "Date", "ticker": "Symbol"})
    events = events.set_index("Date")
    return events


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


def dividend_snowball(prices, div_events, initial_cash=10000):
    dividend_dates = div_events.index.unique().tolist()
    bought_stock = prices.loc[prices.index.isin(dividend_dates)]
    bought_stock = bought_stock.merge(
        div_events, left_index=True, right_index=True, how="left"
    )
    bought_stock = bought_stock[["Close", "cash_amount"]]
    initial_quantity = initial_cash / prices.iloc[0]["Close"]
    initial_total_div = initial_quantity * bought_stock.iloc[0]["cash_amount"]

    columns = ["quantity", "total_dividend", "Date"]
    values = [
        pd.Series(
            [
                initial_quantity + initial_total_div / bought_stock.iloc[0]["Close"],
                initial_total_div,
                bought_stock.iloc[0].name,
            ],
            index=columns,
        )
    ]
    for index in range(1, len(bought_stock)):
        current_row = bought_stock.iloc[index]
        dividend = current_row["cash_amount"]
        last_quantity = values[-1]["quantity"]
        total_dividend = dividend * last_quantity
        quantity = last_quantity + total_dividend / current_row["Close"]
        values.append(
            pd.Series([quantity, total_dividend, current_row.name], index=columns)
        )

    final_df = pd.DataFrame(values)
    final_df.set_index("Date", inplace=True)
    final_df = prices[["Close"]].merge(
        final_df, left_index=True, right_index=True, how="outer"
    )
    final_df.iloc[0, final_df.columns.get_loc("quantity")] = initial_quantity
    quantity = final_df.iloc[0]["quantity"]
    final_df.loc[final_df.index < dividend_dates[0], "quantity"] = quantity

    for index, start_date in enumerate(dividend_dates[:-1]):
        end_date = dividend_dates[index + 1]
        quantity = final_df.loc[start_date]["quantity"]
        date_filter = (start_date < final_df.index) & (final_df.index < end_date)
        try:
            final_df.loc[date_filter, "quantity"] = quantity
        except ValueError as ve:
            print(
                f"index {index}, start_date:{start_date}, end_date: {end_date}, quantity: {quantity}"
            )
            raise ve
    quantity = final_df.loc[dividend_dates[-1]]["quantity"]
    final_df.loc[final_df.index > dividend_dates[-1], "quantity"] = quantity
    return final_df


def create_approach_summary(name, final_value, initial_cash, cash_in_bank):
    profit = final_value - initial_cash
    gain = profit / initial_cash * 100
    total_profit = profit + cash_in_bank
    total_gain = total_profit / initial_cash * 100
    return pd.Series(
        [
            name,
            round(final_value, 2),
            round(profit, 2),
            round(gain, 2),
            round(cash_in_bank, 2),
            round(total_profit, 2),
            round(total_gain, 2),
        ],
        index=[
            "Approach",
            "Final Market Amount",
            "Market Profit",
            "Market Gain",
            "Cash Kept",
            "Total Profit",
            "Total Gain",
        ],
    )
