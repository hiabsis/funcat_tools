"""
第一版代码
实现功能
自定义数据结构
完成指标一与指标二的选股
"""
import datetime
import backtrader as bt
import pandas
import main
import main.common as cm
import main.data_util as data_util
import pandas as pd

MIN_PERIOD = 30
# 股票池最大股票数目
MAX_STOCK_NUM = 10
# 最大股票价格
MAX_STOCK_PRICE = 30
# 最大流通量
MAX_AMOUNT = 100000000
# 测试股票开始时间
BACKTRADER_START_TIME = '2021-01-01'
#
# 测试股票结束时间时间
# 默认是今天
BACKTRADER_END_TIME = None


class Strategy(bt.Strategy):
    params = dict(
        buy_size=10,  # 买入的大小
        callback=0,
        stop_loss=0.05,
        position=0.5,
        take_profit=0.1,
        validity_day=3,
        expired_day=1000,
    )

    # 日志函数
    def log(self, txt, dt=None):
        # 以第一个数据data0，即指数作为时间基准
        dt = dt or self.data0.datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self, **kwargs):

        # 下单的股票
        self.rank = []
        # 回测股票
        for stock in self.datas:
            print('回测股票', stock._name)
        pass

    def to_buy(self, stock):
        """
        买入
        :return:
        """

        # 买入价格 开盘价
        buy_price = stock.open[0]
        sell_price = 0
        # 计算收盘价
        if stock.volume[-1] >= stock.volume[-2]:
            # 当天交易量 > 昨天交易量
            sell_price = stock.high[0]
        elif stock.volume[-1] < stock.volume[-2]:
            sell_price = stock.close[0]

        size = self.p.buy_size
        self.buy_bracket(size=size,
                         data=stock,
                         price=buy_price,
                         limitprice=sell_price, )

    def next(self):

        # 开盘检查是否有需要平仓的股票
        for stock in self.rank:
            position = int(stock.txd_filter_position[-1])
            if stock.close[-1] < stock.close[-position]:
                self.close(data=stock)
                self.rank.remove(stock)
        for stock in self.datas:
            # 满足选股条件和二添加待选股票
            if stock.filter[-1] == 1 and stock.txd_filter[-2] == 1:
                if stock not in self.rank:
                    self.rank.append(stock)
                    self.to_buy(stock)

    # 记录交易收益情况
    def notify_trade(self, trade):
        if trade.isclosed:
            print('毛收益 %0.2f, 扣佣后收益 % 0.2f, 佣金 %.2f, 市值 %.2f, 现金 %.2f' %
                  (trade.pnl, trade.pnlcomm, trade.commission, self.broker.getvalue(), self.broker.getcash()))


def _calculate_index_one(stock_codes, frequency):
    """
    计算指标一

    :return:
    """
    for stock_code in stock_codes:
        data_path = data_util.get_stock_path(stock_code, frequency, resource='baostock')
        df = pandas.read_csv(data_path)
        # 指标
        index = []
        for i, row in df.iterrows():
            # 流通盘小于最大流通盘量
            volume = row.amount / row.turn

            if row.close > MAX_STOCK_PRICE \
                    or row.tradestatus == 1 \
                    or row.tradestatus == 1 \
                    or row.isST == 1 \
                    or volume > MAX_AMOUNT:
                index.append(0)
            else:
                index.append(1)
        df['filter'] = index
        df.to_csv(data_path)


def tdx_filter(close, openp, high, low, volume, n=1):
    """
    通达信计算公式
    :return:
    """
    xg = []
    # 位置
    position = []
    for i in range(len(close)):
        xg.append(1)
        position.append(10)
    return xg, position
    # 公式存在问题,无法计算,你自己实现吧

    # a1 = tu.REF(high, n) == tu.HHV(high, 2 * n + 1)
    # b1 = tu.FILTER(a1, n)
    # c1 = tu.BACKSET(b1, n + 1)
    # hd = tu.FILTER(c1, n)
    #
    # a2 = tu.REF(low, n) == tu.HHV(low, 2 * n + 1)
    # b2 = tu.FILTER(a2, n)
    # c2 = tu.BACKSET(b2, n + 1)
    # hl = tu.FILTER(c2, n)
    # h1 = tu.BARSLAST(hd)
    # crest = tu.REF(high, h1)
    # hh1 = tu.CONST(h1)
    # crest_h = tu.IF(hd, high, low)
    # precise = tu.COUNT(tu.ABS(hh1 - crest_h) <= 0.03, 360) > 1
    # breakthrough = tu.FILTER(tu.CROSS(close, crest), 3)
    # position = tu.BARSLAST(breakthrough)
    # gold = tu.LLV(tu.IF(close > openp, openp, close), position) and tu.REF(volume, position) > tu.REF(volume,
    #                                                                                                   position + 1) * 1.9
    # shrink = volume < tu.REF(volume, 1) and tu.HHV(volume, position) < tu.REF(volume, position)
    # step_back = tu.IF(close > openp, tu.BETWEEN(hh1, openp, low), tu.BETWEEN(hh1, close, low))
    #
    # xg = breakthrough and gold and shrink and step_back and precise
    # return xg


def _calculate_index_second(stock_codes, frequency):
    """
    计算指标二

    :return:
    """
    for stock_code in stock_codes:
        data_path = data_util.get_stock_path(stock_code, frequency, resource='baostock')
        df = pandas.read_csv(data_path)
        # 指标
        index = []
        # 计算通信达的指标
        # 收盘价
        close = df.close
        # 开盘价
        popen = df.open
        # 最高价
        high = df.high
        # 最低价
        low = df.low
        # 交易量
        volume = df.volume
        # 计算指标
        index, txf_position = tdx_filter(close, popen, high, low, volume)

        df['txd_filter'] = index
        df['txd_filter_position'] = txf_position
        df.to_csv(data_path)


def _feeds_data(cerebro: bt.Cerebro, stocks=None, frequencys=None, fromdate=datetime.datetime(2022, 1, 1),
                todate=datetime.date.today()):
    """
    加载数据
    :param cerebro:
    :param stocks:
    :param frequencys:
    :param fromdate:
    :param todate:
    :return:
    """
    if stocks is None:
        stocks = data_util.get_stock_codes()
    if frequencys is None:
        frequencys = ['d']

    for stock_code in stocks:
        for frequency in frequencys:
            # 数据名称
            data_name = stock_code + f"{frequency}"
            # 加载数据
            data = load_strategy_v1_generic_csv(stock_code, frequency)
            cerebro.adddata(data, name=data_name)

    return cerebro


class StrategyV1CSDataExtend(cm.BaoStockDayGenericCSDataExtend):
    """
    版本一:
    添加自定义因子
    """
    # 增加线
    lines = ("filter", 'txd_filter', 'txd_filter_position')
    params = (('filter', 0),  # 指标一
              ('txd_filter', 1),  # 指标二  通信达
              ('txd_filter_position', 1),  # 指标二  位置
              )


def load_strategy_v1_generic_csv(stock_code: str, frequency: str, start_time=None,
                                 end_time=None, dformat='%Y-%m-%d %H:%M:%S'):
    """
    加载策略所需要的数据
    :return:
    """
    if start_time is None:
        start_time = datetime.datetime.strptime(BACKTRADER_START_TIME, '%Y-%m-%d')
    if end_time is None:
        end_time = datetime.datetime.now()

    # 时间级别
    timeframe = data_util.get_timeframe(frequency)
    # 文件名称
    file_path = data_util.get_stock_path(stock_code, frequency)
    df = pd.read_csv(
        file_path,
        skiprows=0,  # 不忽略行
        header=0,  # 列头在0行
    )
    return StrategyV1CSDataExtend(
        dataname=file_path,
        nullvalue=0.0,
        fromdate=start_time,
        todate=end_time,
        dtformat=dformat,
        timeframe=timeframe,
        datetime=data_util.get_columns_index(df, 'time'),
        high=data_util.get_columns_index(df, 'high'),
        low=data_util.get_columns_index(df, 'low'),
        open=data_util.get_columns_index(df, 'open'),
        close=data_util.get_columns_index(df, 'close'),
        volume=data_util.get_columns_index(df, 'volume'),
        amount=data_util.get_columns_index(df, 'amount'),
        adjustflag=data_util.get_columns_index(df, 'adjustflag'),
        turn=data_util.get_columns_index(df, 'turn'),
        tradestatus=data_util.get_columns_index(df, 'tradestatus'),
        pctChg=data_util.get_columns_index(df, 'pctChg'),
        isST=data_util.get_columns_index(df, 'isST'),
        filter=data_util.get_columns_index(df, 'filter'),
        txd_filter=data_util.get_columns_index(df, 'txd_filter'),
        txd_filter_position=data_util.get_columns_index(df, 'txd_filter_position'),
        openinterest=-1
    )


def run(params=None):
    # 创建控制器
    cerebro = cm.get_default_cerebro()
    # 获取股票代码
    stocks = data_util.get_stock_codes()
    # 时间级别
    frequencys = ['d']
    # 更新数据
    data_util.update_date(stocks=stocks[:MAX_STOCK_NUM], frequencys=frequencys)
    # 数据预处理 计算指标一
    _calculate_index_one(stocks[:MAX_STOCK_NUM], 'd')
    # 数据预处理 计算指标二
    _calculate_index_second(stocks[:MAX_STOCK_NUM], 'd')
    # 填数据
    _feeds_data(cerebro, stocks[:MAX_STOCK_NUM], frequencys)
    # 添加数据
    cerebro.addstrategy(Strategy, kwargs=params)
    # 运行
    cerebro.run()
    # print(type(cerebro))
    # 显示

    return cerebro


if __name__ == '__main__':
    # 运行策略
    cerebro = run()
    cm.simple_analyze(cerebro)
    cerebro.plot()
    # 分析策略
