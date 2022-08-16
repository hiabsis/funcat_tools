"""
第一版代码
实现功能
自定义数据结构
完成指标一与指标二的选股
"""
import datetime
import backtrader as bt
import main.common as cm
import main.data_util as data_util
import pandas as pd

# 最小周期
from main.config import data_root_dir
from main.stategy import PandasDataExtend

MIN_PERIOD = 30
# 股票池最大股票数目
MAX_STOCK_NUM = 100


def bar_size(datapath, fromdate, todate):
    """
    统计回测周期内K线数量
    :param datapath:
    :param fromdate:
    :param todate:
    :return:
    """
    df = pd.read_csv(datapath)
    return len(df[(df['date'] >= fromdate.strftime('%Y-%m-%d'))
                  & (df['date'] <= todate.strftime('%Y-%m-%d'))])


def _feeds_data(cerebro: bt.Cerebro, fromdate=datetime.datetime(2022, 1, 1), todate=datetime.date.today()):
    # 获取股票代码
    stocks = data_util.get_stock_codes()
    # 日线级别股票数据路径
    day_k_lines_paths = list()
    for i in range(MAX_STOCK_NUM):
        day_k_lines_paths.append(data_util.get_stock_path(stocks[i], 'd'))
    # 5分钟级别股票数据路径
    five_m_k_lines_paths = list()
    for i in range(MAX_STOCK_NUM):
        five_m_k_lines_paths.append(data_util.get_stock_path(stocks[i], '5'))

    # 加载数据
    for file_path in five_m_k_lines_paths:
        # 股票名称
        stock_code = file_path[-13:-4]
        # 数据名称
        data_name = stock_code + "_1d"
        # 读取数据
        data = data_util.load_bao_stock_day__generic_csv(stock_code, 'd')
        cerebro.adddata(data, name=data_name)
    # 加载数据
    for file_path in day_k_lines_paths:
        # 股票名称
        stock_code = file_path[-13:-4]
        # 数据名称
        data_name = stock_code + "_1d"
        # 读取数据
        data = data_util.load_generic_csv_data(stock_code, 'd')
        cerebro.adddata(data, name=data_name)
    return cerebro


def run():
    # 创建控制器
    cerebro = cm.get_default_cerebro()
    # 填数据
    _feeds_data(cerebro)
    # 运行
    cerebro.run()
    # 显示
    cerebro.plot()


class Strategy(bt.Strategy):
    """

    """
    params = dict(
        rebal_monthday=[1],  # 每月1日执行再平衡
        num_volume=100,  # 成交量取前100名
        period=5,
    )

    # 日志函数
    def log(self, txt, dt=None):
        # 以第一个数据data0，即指数作为时间基准
        dt = dt or self.data0.datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):

        self.ranks = None
        self.currDate = None
        self.lastRanks = []  # 上次交易股票的列表
        # 0号是指数，不进入选股池，从1号往后进入股票池
        self.stocks = self.datas[1:]
        # 记录以往订单，在再平衡日要全部取消未成交的订单
        self.order_list = []

        # 移动平均线指标
        self.sma = {d: bt.ind.SMA(d, period=self.p.period) for d in self.stocks}

        # 定时器
        self.add_timer(
            when=bt.Timer.SESSION_START,
            monthdays=self.p.rebal_monthday,  # 每月1号触发再平衡
            monthcarry=True,  # 若再平衡日不是交易日，则顺延触发notify_timer

        )

    def notify_timer(self, timer, when, *args, **kwargs):
        # 只在5，9，11月的1号执行再平衡
        if self.data0.datetime.date(0).month in [5, 9, 11]:
            self.rebalance_portfolio()  # 执行再平衡

    # def next(self):

    #     print('next 账户总值', self.data0.datetime.datetime(0), self.broker.getvalue())
    #     for d in self.stocks:
    #         if(self.getposition(d).size!=0):
    #             print(d._name, '持仓' ,self.getposition(d).size)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # 订单状态 submitted/accepted，无动作
            return

        # 订单完成
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('买单执行,%s, %.2f, %i' % (order.data._name,
                                                    order.executed.price, order.executed.size))

            elif order.issell():
                self.log('卖单执行, %s, %.2f, %i' % (order.data._name,
                                                     order.executed.price, order.executed.size))

        else:
            self.log('订单作废 %s, %s, isbuy=%i, size %i, open price %.2f' %
                     (order.data._name, order.getstatusname(), order.isbuy(), order.created.size, order.data.open[0]))

    # 记录交易收益情况
    def notify_trade(self, trade):
        if trade.isclosed:
            print('毛收益 %0.2f, 扣佣后收益 % 0.2f, 佣金 %.2f, 市值 %.2f, 现金 %.2f' %
                  (trade.pnl, trade.pnlcomm, trade.commission, self.broker.getvalue(), self.broker.getcash()))

    def rebalance_portfolio(self):
        # 从指数取得当前日期
        self.currDate = self.data0.datetime.date(0)
        print('rebalance_portfolio currDate', self.currDate, len(self.stocks))

        # 如果是指数的最后一本bar，则退出，防止取下一日开盘价越界错
        if len(self.datas[0]) == self.data0.buflen():
            return

            # 取消以往所下订单（已成交的不会起作用）
        for o in self.order_list:
            self.cancel(o)
        self.order_list = []  # 重置订单列表

        # for d in self.stocks:
        #     print('sma', d._name, self.sma[d][0],self.sma[d][1], d.marketdays[0])

        # 最终标的选取过程
        # 1 先做排除筛选过程
        self.ranks = [d for d in self.stocks if
                      len(d) > 0  # 重要，到今日至少要有一根实际bar
                      and d.marketdays > 3 * 365  # 到今天至少上市
                      # 今日未停牌 (若去掉此句，则今日停牌的也可能进入，并下订单，次日若复牌，则次日可能成交）（假设原始数据中已删除无交易的记录)
                      and d.datetime.date(0) == self.currDate
                      and d.roe >= 0.1
                      and 100 > d.pe > 0
                      and len(d) >= self.p.period
                      and d.close[0] > self.sma[d][1]
                      ]

        # 2 再做排序挑选过程
        self.ranks.sort(key=lambda d: d.volume, reverse=True)  # 按成交量从大到小排序
        self.ranks = self.ranks[0:self.p.num_volume]  # 取前num_volume名

        if len(self.ranks) == 0:  # 无股票选中，则返回
            return

        # 3 以往买入的标的，本次不在标的中，则先平仓
        data_toclose = set(self.lastRanks) - set(self.ranks)
        for d in data_toclose:
            print('sell 平仓', d._name, self.getposition(d).size)
            o = self.close(data=d)
            self.order_list.append(o)  # 记录订单

        # 4 本次标的下单
        # 每只股票买入资金百分比，预留2%的资金以应付佣金和计算误差
        buypercentage = (1 - 0.02) / len(self.ranks)

        # 得到目标市值
        targetvalue = buypercentage * self.broker.getvalue()
        # 为保证先卖后买，股票要按持仓市值从大到小排序
        self.ranks.sort(key=lambda d: self.broker.getvalue([d]), reverse=True)
        self.log('下单, 标的个数 %i, targetvalue %.2f, 当前总市值 %.2f' %
                 (len(self.ranks), targetvalue, self.broker.getvalue()))

        for d in self.ranks:
            # 按次日开盘价计算下单量，下单量是100的整数倍
            size = int(
                abs((self.broker.getvalue([d]) - targetvalue) / d.open[1] // 100 * 100))
            validday = d.datetime.datetime(1)  # 该股下一实际交易日
            if self.broker.getvalue([d]) > targetvalue:  # 持仓过多，要卖
                # 次日跌停价近似值
                lowerprice = d.close[0] * 0.9 + 0.02

                o = self.sell(data=d, size=size, exectype=bt.Order.Limit,
                              price=lowerprice, valid=validday)
            else:  # 持仓过少，要买
                # 次日涨停价近似值
                upperprice = d.close[0] * 1.1 - 0.02
                o = self.buy(data=d, size=size, exectype=bt.Order.Limit,
                             price=upperprice, valid=validday)

            self.order_list.append(o)  # 记录订单

        self.lastRanks = self.ranks  # 跟踪上次买入的标的


if __name__ == '__main__':
    run()
