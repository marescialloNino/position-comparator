from enum import Enum
from csv import writer
import logging

EventType = Enum("EventType", "BEAT ACCOUNT MARKET COINSIGNAL SIGNAL ORDER PAIRORDER FILL COMMAND STOP")

SignalType = Enum('SignalType', [('STOPLOSS', 'forced_stoploss'),
                                 ('TAKEPROFIT', 'forced_takeprofit'),
                                 ('EXPIRED', 'forced_expired'),
                                 ('EXIT', 'forced_exit'),
                                 ('SIGNAL', 'signal')])


class Event(object):
    """
    Event is base class providing an interface for all subsequent
    (inherited) events, that will trigger further events in the
    trading infrastructure.
    """
    @property
    def typename(self):
        return self.type.name


class BeatEvent(Event):
    """
    Handles the heartbeat event for a given market and timeframe
    """
    def __init__(self):
        """

        Initialises the BeatEvent.
        """
        self.type = EventType.BEAT

class UpdateAccountEvent(Event):
    """
    Handles the event of receiving a new market bars from common data providers or exchange
    """
    def __init__(self):
        self.type = EventType.ACCOUNT

    def __str__(self):
        format_str = 'Type: %s' % (str(self.type))
        return format_str

    def __repr__(self):
        return str(self)


class MarketEvent(Event):
    """
    Handles the event of receiving a new market bar
    """
    def __init__(self, time, period, index, size):
        """
        Initialise the BarEvent.

        :param time: timestamp
        :type time: timestamp
        :param period: periodicity of data
        :type period: str
        :param index: index of last new data
        :type index: int
        :param size: number of new data lines
        :type size: int
        """

        self.type = EventType.MARKET
        self.time = time
        self.period = period
        self.index = index
        self.size = size
        self.period_readable = self._readable_period()

    def _readable_period(self):
        """
        Creates a human-readable period from the number
        of seconds specified for 'period'.

        For instance, converts:
        * 1 -> '1sec'
        * 5 -> '5secs'
        * 60 -> '1min'
        * 300 -> '5min'

        If no period is found in the lookup table, the human
        readable period is simply passed through from period,
        in seconds.
        """
        lut = {
            1: "1sec",
            5: "5sec",
            10: "10sec",
            15: "15sec",
            30: "30sec",
            60: "1min",
            300: "5min",
            600: "10min",
            900: "15min",
            1800: "30min",
            3600: "1hr",
            86400: "1day",
            604800: "1wk"
        }
        if self.period in lut:
            return lut[self.period]
        else:
            return "%s" % str(self.period)

    def __str__(self):
        format_str = 'Type: %s, symbol: %s, Time: %s' % (
                str(self.type), str(self.ticker), str(self.time))
        return format_str

    def __repr__(self):
        return str(self)


class PairSignalEvent(Event):
    """
    Signal sent by Strategy object to Portfolio
    """
    def __init__(self, timestamp, pair_name, action, prices, comment, strat_name, nature):
        """
        Initialises the SignalEvent.

        :param timestamp: time
        :type timestamp: pd.timestamp
        :param pair_name: The pair name
        :type pair_name: str
        :param action: +1 for buy, -1 for sell 0 for close
        :type action: int
        :param prices: pair of current prices
        :type prices: Pair[float]
        :param nature: nature of signal (x for exit, n for entry) xx/xn/nx/nn
        :type nature: str
        """
        self.timestamp = timestamp
        self.type = EventType.SIGNAL
        self.pair_name = pair_name
        self.pair_prices = prices
        self.action = action
        self.comment = comment
        self.strat_name = strat_name
        self.nature = nature

    def __str__(self):
        if self.action == 0:
            action = 'CLOSE'
        elif self.action == 1:
            action = 'BUYLEG1'
        else:
            action = 'SELLLEG1'
        format_str = f'{self.strat_name};{action};{self.pair_name};' \
                     f';{self.pair_prices[0]},{self.pair_prices[1]};{self.nature};{self.comment}'
        return format_str


class CoinSignalEvent(Event):
    """
    Handles the event of sending an Order to a smart spread execution system
    containing both legs
    """
    def __init__(self, timestamp, coin_name, action, price, comment, strat_name, nature):
        """
        Initialises the OrderEvent.

        Parameters:
        ticker - The ticker symbol, e.g. 'GOOG'.
        action - int
        """
        self.type = EventType.COINSIGNAL
        self.timestamp = timestamp
        self.coin_name = coin_name
        self.action = action
        self.price = price
        self.comment = comment
        self.strat_name = strat_name
        self.nature = nature

    def __str__(self):
        format_str = f'{self.strat_name};{self.action};{self.coin_name};{self.price};{self.nature};{self.comment}'

        return format_str

class PairOrderEvent(Event):
    """
    Handles the event of sending an Order to a smart spread execution system
    containing both legs
    """
    def __init__(self, timestamp, pair_name, action, quantity, prices, comment, strat_name, nature='', leverage=None):
        """
        Initialises the OrderEvent.

        Parameters:
        ticker - The ticker symbol, e.g. 'GOOG'.
        action - int
        quantity - The unsigned quantity of shares to transact.
        """
        self.type = EventType.PAIRORDER
        self.timestamp = timestamp
        self.ticker = pair_name
        self.action = action
        self.quantity = quantity
        self.price = prices
        self.comment = comment
        self.nature = nature
        self.strat_name = strat_name
        self.leverage = leverage

    def __str__(self):
        format_str = (f'{self.strat_name};{self.action};{self.ticker};{self.quantity[0]},{self.quantity[1]};'
                      f'{self.price[0]},{self.price[1]};{self.nature};{self.comment};{self.leverage}')

        return format_str

    @property
    def simple(self):
        if self.action == 1:
            action = 'BUYLEG1'
        else:
            action = 'SELLLEG1'
        line = [action, self.ticker, 'CLOSE' if 'exit' in self.comment else 'OPEN']
        return ';'.join(line)



class CoinOrderEvent(Event):
    """
    Handles the event of sending an Order to an execution system.
    The order contains a symbol (e.g. BTCUSDT), action (-1 or 1)
    and quantity.
    """
    def __init__(self, timestamp, ticker, action, quantity, price, comment, strat_name, nature, leverage=None):
        """
        Initialises the OrderEvent.

        Parameters:
        ticker - The ticker symbol, e.g. 'BTCUSDT'.
        action - int
        quantity - The unsigned quantity of shares to transact.
        """
        self.type = EventType.ORDER
        self.ticker = ticker
        self.action = action
        self.quantity = quantity
        self.comment = comment
        self.timestamp = timestamp
        self.price = price
        self.strat_name = strat_name
        self.leverage = leverage
        self.nature = nature

    def __str__(self):
        format_str = (f'{self.strat_name};{self.action};{self.ticker};{self.quantity};{self.price};'
                      f'{self.nature};{self.comment};{self.leverage}')
        return format_str

    @property
    def simple(self):
        if self.action == 1:
            action = 'BUY'
        else:
            action = 'SELL'
        line = [action, self.ticker, 'CLOSE' if 'exit' in self.comment else 'OPEN']
        return ','.join(line)

class FillEvent(Event):
    """
    Encapsulates the notion of a filled order, as returned
    from a brokerage. Stores the quantity of an instrument
    actually filled and at what price. In addition, stores
    the commission of the trade from the brokerage.

    """

    def __init__(self, timestamp, ticker, quantity, exchange, price, commission, strat_name):
        """
        Initialises the FillEvent object.

        timestamp - The timestamp when the order was filled.
        ticker - The symbol ticker, e.g. 'GOOG'.
        quantity - The signed filled quantity.
        exchange - The exchange where the order was filled.
        price - The price at which the trade was filled
        commission - The brokerage commission for carrying out the trade.
        """
        self.type = EventType.FILL
        self.timestamp = timestamp
        self.ticker = ticker
        self.quantity = quantity
        self.exchange = exchange
        self.price = price
        self.commission = commission
        self.strat_name = strat_name

    def __str__(self):
        format_str = 'Type: %s, symbol: %s, qty: %s @ price %s and com %s' % (
                self.type, self.ticker, self.quantity, self.price, self.commission)
        return format_str

class CommandEvent(Event):
    """
    Handles the event of receiving a web command
    """
    def __init__(self, strategy_name, action):
        """
        Initialises the OrderEvent.

        Parameters:
        ticker - The ticker symbol, e.g. 'GOOG'.
        action - str
        quantity - The quantity of shares to transact.
        """
        self.type = EventType.COMMAND
        self.strategy_name = strategy_name
        self.action = action

    def __str__(self):
        format_str = 'Type: %s' % (str(self.type))
        return format_str

    def __repr__(self):
        return str(self)


class StopEvent(Event):
    def __init__(self):
        self.type = EventType.STOP


def append_order(csv_file, order):
    """

    :param csv_file:
    :type csv_file: str
    :param order:
    :type order: [CoinOrderEvent, PairOrderEvent]
    :return:
    :rtype:
    """
    with open(csv_file, 'a') as f_object:
        writer_object = writer(f_object)
        writer_object.writerow(order.simple)
        f_object.close()


class OrderFilter(logging.Filter):
    def __init__(self, strat_name):
        super().__init__()
        self.strat_name = strat_name
    def filter(self, record):
        order = record.args.get('order', None)
        if order is not None and order.strat_name == self.strat_name:
            record.order = order.simple
            return True
        else:
            return False
