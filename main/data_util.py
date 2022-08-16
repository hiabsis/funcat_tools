"""
数据获取的代码
"""
import os
import baostock as bs
import datetime
import sys
import pandas as pd
import backtrader as bt
# 数据保存的根目录位置
from main.config import data_root_dir
import main.common as cm

# BaoStock日线数据字段
g_baostock_data_day_fields = 'date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,' \
                             'pctChg,isST '
# BaoStock分钟线数据字段
g_baostock_data_minute_fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"
# BaoStock 周月线指标
g_baostock_data_weeks_fields = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"


def get_stock_path(stock_code, frequency):
    """
    股票的保存路径
    :param stock_code:
    :param frequency:
    :return:
    """
    return data_root_dir + f"\\{frequency}\\{stock_code}.csv"


def get_stock_codes(date=None):
    """
    获取指定日期的A股代码列表

    若参数date为空，则返回最近1个交易日的A股代码列表
    若参数date不为空，且为交易日，则返回date当日的A股代码列表
    若参数date不为空，但不为交易日，则打印提示非交易日信息，程序退出

    :param date: 日期
    :return: A股代码的列表
    """

    # 登录baostock
    bs.login()

    # 从BaoStock查询股票数据
    stock_df = bs.query_all_stock(date).get_data()

    # 如果获取数据长度为0，表示日期date非交易日
    if 0 == len(stock_df):

        # 如果设置了参数date，则打印信息提示date为非交易日
        if date is not None:
            print('当前选择日期为非交易日或尚无交易数据，请设置date为历史某交易日日期')
            sys.exit(0)

        # 未设置参数date，则向历史查找最近的交易日，当获取股票数据长度非0时，即找到最近交易日
        delta = 1
        while 0 == len(stock_df):
            stock_df = bs.query_all_stock(datetime.date.today() - datetime.timedelta(days=delta)).get_data()
            delta += 1

    # 注销登录
    bs.logout()

    # 筛选股票数据，上证和深证股票代码在sh.600000与sz.39900之间
    stock_df = stock_df[(stock_df['code'] >= 'sh.600000') & (stock_df['code'] < 'sz.399000')]

    # 返回股票列表
    return stock_df['code'].tolist()


def download_bao_stock_data(stock_codes, frequency='d', from_date='1990-12-19',
                            to_date=datetime.date.today().strftime('%Y-%m-%d'),
                            adjustflag='3'):
    """
    下载指定日期内，指定股票的日线数据

    :param frequency: 数据时间级别
    :param stock_codes: 待下载数据的股票代码
    :param from_date: 日线开始日期
    :param to_date: 日线结束日期
    :param adjustflag: 复权选项 1：后复权  2：前复权  3：不复权  默认为前复权
    :return: None
    """

    # 下载股票循环
    for code in stock_codes:
        print(f'正在下载 {code}-{frequency}...')
        # 数据保存的位置
        save_path = get_stock_path(code, frequency)

        bs.login()

        # 下载日线数据
        if frequency == 'd':
            out_df = bs.query_history_k_data_plus(code,
                                                  g_baostock_data_day_fields,
                                                  start_date=from_date, end_date=to_date,
                                                  frequency=frequency, adjustflag=adjustflag)
            out_df = bs.query_history_k_data_plus(code,
                                                  g_baostock_data_day_fields,
                                                  start_date=from_date, end_date=to_date,
                                                  frequency=frequency, adjustflag=adjustflag)
        # 下载分钟级别的数据
        else:
            out_df = bs.query_history_k_data_plus(code,
                                                  g_baostock_data_minute_fields,
                                                  start_date=from_date, end_date=to_date,
                                                  frequency=frequency, adjustflag=adjustflag)

        # 注销登录
        bs.logout()
        data_list = []
        # 解析数据
        while (out_df.error_code == '0') & out_df.next():
            row = out_df.get_row_data()
            fmt = '%Y-%m-%d'
            d = datetime.datetime.strptime(row[0], fmt)
            fmt = '%Y-%m-%d %H:%M:%S'
            row[0] = d.strftime(fmt)
            # 数据时间格式进行转换格式进行
            if frequency != 'd':
                row = out_df.get_row_data()
                fmt = '%Y%m%d%H%M%S'
                t = datetime.datetime.strptime(row[1][:-3], fmt)
                fmt = '%Y-%m-%d %H:%M:%S'
                row[1] = t.strftime(fmt)
                row[0] = t.strftime(fmt)
            else:
                row.append(row[0])
            # 获取一条记录，将记录合并在一起
            data_list.append(row)
        csv_head = out_df.fields
        if frequency == 'd':
            csv_head.append('time')
        result = pd.DataFrame(data_list, columns=csv_head)
        # 保存数据
        try:
            result.to_csv(save_path, index=False)
        except OSError as r:
            print("OSError", r)
            os.makedirs(data_root_dir + f"\\{frequency}")
            result.to_csv(save_path, index=False)


def update_date(frequencys=None):
    """
    # 待完成
    更新本地股票数据
    :param frequencys: 更新的时间级别
    :return:
    """
    # 获取所有股票的代码
    if frequencys is None:
        frequencys = ['d', '5', '15', '30']
    stock_codes = get_stock_codes()
    for frequency in frequencys:
        download_bao_stock_data(stock_codes, frequency=frequency)


def load_generic_csv_data(stock_code: str, frequency: str, start_time=datetime.datetime(2021, 1, 1),
                          end_time=datetime.datetime.now(), dformat='%Y-%m-%d %H:%M:%S'):
    """
    按照generic的格式加载数据

    :return:
    """
    if frequency == 'd':
        timeframe = bt.TimeFrame.Days
    else:
        timeframe = bt.TimeFrame.Minutes
    # 文件名称
    file_path = get_stock_path(stock_code, frequency)
    df = pd.read_csv(
        file_path,
        skiprows=0,  # 不忽略行
        header=0,  # 列头在0行
    )
    return bt.feeds.GenericCSVData(
        dataname=file_path,
        nullvalue=0.0,
        fromdate=start_time,
        todate=end_time,
        dtformat=dformat,
        timeframe=timeframe,
        datetime=get_columns_index(df, 'time'),
        high=get_columns_index(df, 'high'),
        low=get_columns_index(df, 'low'),
        open=get_columns_index(df, 'open'),
        close=get_columns_index(df, 'close'),
        volume=get_columns_index(df, 'volume'),
        openinterest=-1
    )


def load_bao_stock_day__generic_csv(stock_code: str, frequency: str, start_time=datetime.datetime(2021, 1, 1),
                                    end_time=datetime.datetime.now(), dformat='%Y-%m-%d %H:%M:%S'):
    """
    按照generic的格式加载数据

    :return:
    """
    if frequency == 'd':
        timeframe = bt.TimeFrame.Days
    else:
        timeframe = bt.TimeFrame.Minutes
    # 文件名称
    file_path = get_stock_path(stock_code, frequency)
    df = pd.read_csv(
        file_path,
        skiprows=0,  # 不忽略行
        header=0,  # 列头在0行
    )
    return cm.BaoStockDayGenericCSDataExtend(
        dataname=file_path,
        nullvalue=0.0,
        fromdate=start_time,
        todate=end_time,
        dtformat=dformat,
        timeframe=timeframe,
        datetime=get_columns_index(df, 'time'),
        high=get_columns_index(df, 'high'),
        low=get_columns_index(df, 'low'),
        open=get_columns_index(df, 'open'),
        close=get_columns_index(df, 'close'),
        volume=get_columns_index(df, 'volume'),
        amount=get_columns_index(df, 'amount'),
        adjustflag=get_columns_index(df, 'adjustflag'),
        turn=get_columns_index(df, 'turn'),
        tradestatus=get_columns_index(df, 'tradestatus'),
        pctChg=get_columns_index(df, 'pctChg'),
        isST=get_columns_index(df, 'isST'),
        plot=False,
        openinterest=-1
    )


def load_bao_stock_minute__generic_csv(stock_code: str, frequency: str, start_time=datetime.datetime(2021, 1, 1),
                                       end_time=datetime.datetime.now(), dformat='%Y-%m-%d %H:%M:%S'):
    """
    按照generic的格式加载数据

    :return:
    """
    if frequency == 'd':
        timeframe = bt.TimeFrame.Days
    else:
        timeframe = bt.TimeFrame.Minutes
    # 文件名称
    file_path = get_stock_path(stock_code, frequency)
    df = pd.read_csv(
        file_path,
        skiprows=0,  # 不忽略行
        header=0,  # 列头在0行
    )
    return cm.BaoStockDayGenericCSDataExtend(
        dataname=file_path,
        nullvalue=0.0,
        fromdate=start_time,
        todate=end_time,
        dtformat=dformat,
        timeframe=timeframe,
        datetime=get_columns_index(df, 'time'),
        high=get_columns_index(df, 'high'),
        low=get_columns_index(df, 'low'),
        open=get_columns_index(df, 'open'),
        close=get_columns_index(df, 'close'),
        volume=get_columns_index(df, 'volume'),
        amount=get_columns_index(df, 'amount'),
        adjustflag=get_columns_index(df, 'adjustflag'),
        plot=False,
        openinterest=-1
    )


def get_columns_index(data, column_name):
    """
    获取表头所在位置
    :param data:
    :param column_name:
    :return:
    """
    index = 0
    for column in data.columns:
        if column == column_name:
            return index
        index += 1

    return -1

    pass


if __name__ == '__main__':
    download_bao_stock_data(['sh.600000'], frequency='d')
