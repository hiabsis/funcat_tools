"""
公共部分代码
"""
import os
import webbrowser

import backtrader as bt

import backtrader
import pandas
import pyfolio
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
import quantstats

import config


def simple_analyze(cerebro, name="DEFAULT_NAME"):
    """
    对策略分析
    :param cerebro:
    :param name:
    :return:
    """

    cerebro.addanalyzer(backtrader.analyzers.TimeReturn, _name='_TimeReturn')
    result = cerebro.run()

    # 提取收益序列
    pnl = pandas.Series(result[0].analyzers._TimeReturn.get_analysis())
    # 计算累计收益
    cumulative = (pnl + 1).cumprod()
    # 计算回撤序列
    max_return = cumulative.cummax()
    drawdown = (cumulative - max_return) / max_return
    # 按年统计收益指标
    perf_stats_year = pnl.groupby(pnl.index.to_period('y')).apply(
        lambda data: pyfolio.timeseries.perf_stats(data)).unstack()
    # 统计所有时间段的收益指标
    perf_stats_all = pyfolio.timeseries.perf_stats(pnl).to_frame(name='all')
    perf_stats = pandas.concat([perf_stats_year, perf_stats_all.T], axis=0)
    perf_stats_ = round(perf_stats, 4).reset_index()
    # 支持中文
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

    # plt.style.use('seaborn')
    plt.style.use('dark_background')

    fig, (ax0, ax1) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [1.5, 4]}, figsize=(20, 8))
    cols_names = ['date\n', 'Annual\nreturn', 'Cumulative\nreturns', 'Annual\nvolatility',
                  'Sharpe\nratio', 'Calmar\nratio', 'Stability\n', 'Max\ndrawdown',
                  'Omega\nratio', 'Sortino\nratio', 'Skew\n', 'Kurtosis\n', 'Tail\nratio',
                  'Daily value\nat risk']

    # 绘制表格
    ax0.set_axis_off()  # 除去坐标轴
    table = ax0.table(cellText=perf_stats_.values,
                      bbox=(0, 0, 1, 1),  # 设置表格位置， (x0, y0, width, height)
                      rowLoc='right',  # 行标题居中
                      cellLoc='right',
                      colLabels=cols_names,  # 设置列标题
                      colLoc='right',  # 列标题居中
                      edges='open'  # 不显示表格边框
                      )
    table.set_fontsize(13)

    # 绘制累计收益曲线
    ax2 = ax1.twinx()
    ax1.yaxis.set_ticks_position('right')  # 将回撤曲线的 y 轴移至右侧
    ax2.yaxis.set_ticks_position('left')  # 将累计收益曲线的 y 轴移至左侧
    # 绘制回撤曲线
    drawdown.plot.area(ax=ax1, label='drawdown (right)', rot=0, alpha=0.3, fontsize=13, grid=False)
    # 绘制累计收益曲线
    cumulative.plot(ax=ax2, color='#F1C40F', lw=3.0, label='cumret (left)', rot=0, fontsize=13, grid=False)
    # 不然 x 轴留有空白
    ax2.set_xbound(lower=cumulative.index.min(), upper=cumulative.index.max())
    # 主轴定位器：每 5 个月显示一个日期：根据具体天数来做排版
    ax2.xaxis.set_major_locator(ticker.MultipleLocator(100))
    # 同时绘制双轴的图例
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    plt.legend(h1 + h2, l1 + l2, fontsize=12, loc='upper left', ncol=1)
    fig.tight_layout()  # 规整排版
    plt.show()


def get_default_strategy_name(resource):
    """
    获取默认的策略名称
    :param strategy: 策略类
    :param resource: 数据源
    :return:
    """
    return str(resource._dataname).split('\\')[-1].split('.')[0]


def pyfolio_analyze_plot(cerebro, title='Returns_Sentiment', output=None,
                         is_show=True):
    """
    可视化分析 财务数据
    :param cerebro:
    :param output:
    :param title:
    :param is_show:
    :return:
    """

    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    back = cerebro.run()

    portfolio = back[0].analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = portfolio.get_pf_items()
    returns.index = returns.index.tz_convert(None)
    if output is None:
        file_name = title
        output = config.DATA_ROOT_DIR + "\\pyfolio"
        if not os.path.exists(output):
            os.makedirs(output)
        output = output + "\\" + file_name + ".html"
    report_path = config.DATA_ROOT_DIR + "\\pyfolio\\html\\report.html"
    if not os.path.exists(report_path):
        report_path = None
    quantstats.reports.html(returns, output=output, template_path=report_path, download_filename=output, title=title)
    if is_show:
        webbrowser.open(output)
    return output


class StampDutyCommissionScheme(bt.CommInfoBase):
    """
    本佣金模式下，买入股票仅支付佣金，卖出股票支付佣金和印花税.
    """
    params = (
        ('stamp_duty', 0.005),  # 印花税率
        ('commission', 0.001),  # 佣金率
        ('stocklike', True),
        ('commtype', bt.CommInfoBase.COMM_PERC),
    )

    def _getcommission(self, size, price, pseudoexec):
        """
        If size is greater than 0, this indicates a long / buying of shares.
        If size is less than 0, it idicates a short / selling of shares.
        """

        if size > 0:  # 买入，不考虑印花税
            return size * price * self.p.commission
        elif size < 0:  # 卖出，考虑印花税
            return size * price * (self.p.stamp_duty + self.p.commission)
        else:
            return 0  # just in case for some reason the size is 0.


class BaoStockDayGenericCSDataExtend(bt.feeds.GenericCSVData):
    """
    日线级别的数据
    """
    # 增加线
    lines = ("amount", 'adjustflag', 'turn', 'tradestatus', 'pctChg', 'isST')
    params = (('amount', 0),  # 成交额
              ('adjustflag', 0),  # 是否复权
              ('turn', 0),  # 换手率
              ('tradestatus', 0),  # 交易状态 1：正常交易 0：停牌
              ('pctChg', 0),  # 涨跌幅
              ('isST', 3),)  # 是否ST	1是，0否


class BaoStockMinuteGenericCSDataExtend(bt.feeds.GenericCSVData):
    """
    分钟级别的数据
    """
    # 增加线
    lines = ("amount", 'adjustflag')
    params = (('amount', 0),  # 成交额
              ('adjustflag', 0),  # 是否复权
              )


def get_default_cerebro(startcash=100000):
    """
    获取默认的控制器
    :return:
    """
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addobserver(bt.observers.Broker)
    cerebro.addobserver(bt.observers.Trades)
    # cerebro.broker.set_coc(True)  # 以订单创建日的收盘价成交
    # cerebro.broker.set_coo(True) # 以次日开盘价成交
    cerebro.broker.setcash(startcash)
    # 防止下单时现金不够被拒绝。只在执行时检查现金够不够。
    cerebro.broker.set_checksubmit(False)
    comminfo = StampDutyCommissionScheme(stamp_duty=0.001, commission=0.001)
    cerebro.broker.addcommissioninfo(comminfo)
    return cerebro
