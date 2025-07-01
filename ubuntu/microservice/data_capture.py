import argparse
import os.path
import shutil
from time import sleep
import logging
from logging.handlers import TimedRotatingFileHandler

import datafeed.okexfeed as okf
import datafeed.binancefeed as bnf
import datafeed.bybitfeed as bbf
import datafeed.bitgetfeed as bgf
import datafeed.dummyfeed as dyf
import datafeed.hyperliquidfeed as hlf
from datafeed.utils_online import *

'''
Module that updates historic database for given perimeter (market, ticker)
It can fill holes when tickers are added
It takes a depth as parameter

'''

LOGGER = logging.getLogger()


def make_perimeter(database, first_index, coin_perimeter):
    LOGGER.info(f'Audit for DB {database.shape[1]} columns')
    coin_report = {}
    full_report = {}
    close_col = [name for name in database.columns if 'close' in name]
    missing_recent = {}
    missing = []
    ok = []

    for name in close_col:
        coin, _ = parse_column(name)
        report = {}
        subdata = database[name]
        bad_count = subdata.isna().sum()
        report.update({'total NA': int(bad_count)})
        recent_index = [ts for ts in subdata.index if ts > first_index]
        recent = subdata.loc[recent_index]
        recent_bad_count = recent.isna().sum()
        if recent_bad_count > 0:
            recent_lines = recent[recent.isna()]
            missing_recent[coin] = [recent_bad_count, recent_lines.index[0], recent_lines.index[-1]]

            if coin in coin_perimeter:
                LOGGER.warning(f'Incomplete histo for {coin} in perimeter')
                missing.append(coin)

        report.update({'recent NA': int(recent_bad_count)})

        subdata = database[name].dropna()
        if len(subdata) > 0:
            if recent_bad_count == 0 and bad_count == 0:
                ok.append(coin)
            report.update({'start': subdata.index[0], 'end':subdata.index[-1]})

        coin_report[coin] = report

    full_report['current_incomplete_data'] = missing
    full_report['missing_recent_data'] = missing_recent
    full_report['ok'] = ok
    full_report['coins'] = coin_report

    return full_report


def prepare_db(filename, coin_perimeter, time_zone, min_depth, max_depth=None):
    '''
    Create file, trim obsolete data, returns end_date for fill up
    :param filename: str, name of file containing data
    :param min_depth: timedelta, mandatory depth of histo
    :param max_depth: timedelta, depth of histo to keep
    :return: type pd.timestamps, the expected first and last rows
    '''

    first_index = pd.Timestamp(hour_rounder(datetime.today()) - min_depth).tz_localize(time_zone).tz_convert('UTC')

    if os.path.exists(filename):
        current_db = pd.read_pickle(filename)
        current_db = current_db.astype('float32')
        replace = False

        if max_depth is not None:
            obsolete_index = pd.Timestamp(hour_rounder(datetime.today()) - max_depth).tz_localize(time_zone).tz_convert('UTC')
            to_drop = [idx for idx in current_db.index if idx < obsolete_index]
            if len(to_drop) > 0:
                replace = True
                LOGGER.info(f'Loaded DB, dropped {len(to_drop)} lines, removed {current_db.index.duplicated().sum()} duplicated index')
                current_db.drop(inplace=True, index=to_drop)
                current_db = current_db.loc[~current_db.index.duplicated(keep='last')]
        btc = [col for col in current_db.columns if 'BTC' in col and 'close' in col]
        if len(btc) > 0:
            last_index = current_db[btc].dropna().index[-1]
            last_iloc = current_db.index.get_loc(last_index)
            LOGGER.info(f'Found BTC last index {last_index}')
            if last_iloc < len(current_db) - 1:
                LOGGER.info(f'Trimming a bit last nan')
                current_db = current_db.iloc[:last_iloc]
                replace = True
        elif len(current_db.index) > 0:
            last_index = current_db.index[-1]
        else:
            last_index = first_index

        if replace:
            LOGGER.info(f'Saved cleaned DB')
            current_db.to_pickle(filename)
        coin_report = make_perimeter(current_db, first_index, coin_perimeter)

        return current_db, first_index, last_index, coin_report
    else:
        current_db = pd.DataFrame()
        current_db.to_pickle(filename)

        return current_db, first_index, first_index, None


def histo_filler(current_db, access_point, market, perimeter, tf, start_time, end_time, just_holes):
    LOGGER.info(f'Fetching {len(perimeter)} from {market} in mode just_holes={just_holes}')
    data = pd.DataFrame(columns=current_db.columns)
    initial_size = len(current_db.index)
    # create new index from BTC
    symbol = perimeter[0]
    bar, _ = access_point.read_bars(symbol, tf, start_time, None, tf)
    new_index = current_db.index.append(bar.index)
    new_index = new_index[~new_index.duplicated(keep='last')]
    new_db = pd.DataFrame(current_db, index=new_index).astype('float32')
    del current_db
    LOGGER.info(f'Fetched {symbol} and found {len(new_db) - initial_size} new lines')

    for symbol in perimeter:
        info = access_point.market_info(symbol)

        if info is not None:
            capture = True
            test_col = make_column(symbol, 'close')
            coin_start_time = start_time
            if test_col in new_db.columns:
                current_close = new_db[test_col].dropna()

                # starts where it should
                if len(current_close) > 0 and current_close.index[0].date() < start_time.date():
                    coin_start_time = current_close.index[-1]

                    if (dt2ts(datetime.today()) - coin_start_time.timestamp()) / 3600 < 3:
                        # too fresh, we skip
                        if just_holes:
                            capture = False
                            LOGGER.info(f'Column for {symbol} will not be refreshed')
                        else:
                            LOGGER.info(f'Column for {symbol} will be force-refreshed')
                    else:
                        LOGGER.info(f'Column for {symbol} more than 3h old, will be refreshed')
                else:
                    LOGGER.info(f'Column for {symbol} exists but is all nan, will be refreshed')
            else:
                LOGGER.info(f'Adding column for {symbol}')
                coin_start_time = start_time

            if capture:
                bars, _ = access_point.read_bars(symbol, tf, coin_start_time, None, tf)  # TODO async

                sleep(0.1)
                if bars is not None:
                    bars.rename(columns={col: make_column(symbol, col) for col in bars.columns}, inplace=True)

                    if make_column(symbol, 'close') not in data.columns:
                        new_db = pd.concat([new_db, bars], axis=1).astype('float32')
                    else:
                        # Merge new rows
                        new_db.fillna(bars.astype('float32'), inplace=True)
                else:
                    LOGGER.info(f'No bar retrieved for {symbol} in {market}')
            else:
                bars = None
        else:
            LOGGER.info(f'No {symbol} in {market}')

    return new_db




# Update fill_data to use DummyMarket when use_dummy is True
def fill_data(feed, coin_perimeter, time_zone, audit_mode=False, trimming=False, use_dummy=False):
    market = feed['exchange']
    datafile = feed['database']
    tf = feed['timeframe']
    LOGGER.info(f'Starting process for {market}')

    if trimming:
        dirname = os.path.dirname(datafile)
        basename = os.path.basename(datafile)
        basename_without_ext = os.path.splitext(basename)[0]
        basename_ext = os.path.splitext(basename)[1]
        today = datetime.today().isoformat().split('T')[0]
        backup_name = os.path.join(dirname, f'{basename_without_ext}_{today}{basename_ext}')
        LOGGER.info(f'Copying file {market}')
        shutil.copyfile(datafile, backup_name)
        LOGGER.info(f'Trimming database {market} to {feed["min_depth_months"]} months')
        min_depth = timedelta(weeks=feed['min_depth_months'] * 52 / 12)
        max_depth = min_depth
        current_db, start_time, end_time, coin_report = prepare_db(datafile, coin_perimeter, time_zone, min_depth, max_depth)
        return

    min_depth = timedelta(weeks=feed['min_depth_months'] * 4.5)
    max_depth = timedelta(weeks=feed['max_depth_months'] * 4.4)

    current_db, start_time, end_time, coin_report = prepare_db(datafile, coin_perimeter, time_zone, min_depth, max_depth)

    if audit_mode:
        with open(datafile + '_audit.json', 'w') as myfile:
            j = json.dumps(coin_report, indent=4, cls=NpEncoder)
            print(j, file=myfile)
        return
    
    # if latests bar is less than 3h old, we only fill old holes in histo
    just_holes = (dt2ts(datetime.today()) - end_time.timestamp()) / 3600 < 3

    # Use DummyMarket if --dummy is specified or market is 'dummy'
    if use_dummy or market == 'dummy':
        ap = dyf.DummyMarket()
    else:
        if market == 'binance':
            ap = bnf.BinanceMarket()
        elif market == 'binancefut':
            ap = bnf.BinanceFutureMarket()
        elif market == 'okex':
            ap = okf.OkexMarket()
        elif market == 'bybit':
            ap = bbf.BybitMarket()
        elif market == 'bitget':
            ap = bgf.BitgetMarket()
        elif market == 'hyperliquid':
            ap = hlf.HyperliquidMarket()
        else:
            ap = dyf.DummyMarket()

    new_db = histo_filler(current_db, ap, market, coin_perimeter, tf, start_time, end_time, just_holes)
    new_db = new_db.loc[~new_db.index.duplicated(keep='last')]
    new_db.to_pickle(datafile)
    return



# In ubuntu/microservice/data_capture.py
# In the main() function, add the --dummy argument
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="input file", required=True)
    parser.add_argument("--audit", required=False, action='store_true')
    parser.add_argument("--trimming", required=False, action='store_true')
    parser.add_argument("--dummy", required=False, action='store_true', help="Use dummy market and broker for local testing")
    args = parser.parse_args()
    config_file = args.config
    audit_mode = args.audit
    trimming = args.trimming
    use_dummy = args.dummy  # New flag

    with open(config_file, 'r') as myfile:
        params = json.load(myfile)
    working_dir = params['working_dir']
    recup_file = params['recup_file']
    output = params['output_file']
    feeds = params['feeds']

    fmt = logging.Formatter('{asctime}:{levelname}:{name}:{message}', style='{')
    handler = TimedRotatingFileHandler(filename=os.path.join(working_dir, output),
                                       when="midnight", interval=1, backupCount=7)
    handler.setFormatter(fmt)
    LOGGER.setLevel(logging.INFO)
    LOGGER.addHandler(handler)

    with open(recup_file, 'r') as myfile:
        perimeters = json.load(myfile)

    with open(working_dir + 'dc_params.json', 'w') as myfile:
        j = json.dumps(params, indent=4, cls=NpEncoder)
        print(j, file=myfile)

    tz_string = datetime.now().astimezone().tzinfo
    for feed in feeds:
        if feed['perimeter'] in perimeters:
            coin_perimeter = perimeters[feed['perimeter']]
            fill_data(feed, coin_perimeter, tz_string, audit_mode, trimming, use_dummy=use_dummy)  # Pass use_dummy
    return



if __name__ == '__main__':
    main()

