## 2014: 11839
from rqalpha import run_func
from rqalpha.apis import update_universe, logger, order_percent
import jqdatasdk as jq
import talib, numpy, math, random, copy, pytz, time, json, requests, pandas as pd
from pandas.io.json import json_normalize
#import statsmodels.api as sm
from pandas.core.frame import DataFrame
from datetime import datetime, tzinfo, timedelta
from collections import Counter
#from six import BytesIO

def get_all_stocks(context):
    return all_instruments(type='CS')['order_book_id'].tolist()

def order_target(stock, shares): order_to(stock, shares)

def do_message(m): print(m) #, send_message(m, channel='weixin')

def current_date(context): return context.now


def port_total(context): return context.portfolio.total_value


def port_cash(context): return context.portfolio.cash


def port_value(context): return context.portfolio.market_value


def port_size(context): return len(context.portfolio.positions)


def port_ratio(context): return port_value(context) / port_total(context)


def base_money(context): return port_total(context) * 0.01


def stock_name(stock): return instruments(stock).symbol


def stock_code(s):
    if s == '': return ''
    s += '.XSHG' if s.startswith('6') else '.XSHE'
    return s


def stock_value(context, stock):
    return get_position(stock).market_value

def stock_price(context, stock):
    return context.data[stock].last

def stock_open(context, stock):
    return context.data[stock].open

def stock_amount(context, stock):
    return get_position(stock).quantity

def stock_cost(context, stock):
    return stock_value(context,stock) - get_position(stock).pnl

def stock_inport(context, stock): return stock in context.portfolio.positions

def stock_rsi(stock, unit='1d', period=14, data='close'):
    return stock_ersi(stock, unit='1d', period=14, data='close')[0]


def stime(context): return str(context.now.time())


def rsi_ok(stock, unit='1d', period=14, data='close'):
    rsi = stock_rsi(stock, unit, period, data)
    return (rsi < 35 or 45 < rsi < 85)


def rsi_good(stock, unit='1d', period=14, data='close'):
    rsi = stock_rsi(stock, unit, period, data)
    return (rsi < 30 or 60 < rsi < 90)


def rsi_strong(stock, unit='1d', period=14, data='close'):
    rsi = stock_rsi(stock, unit, period, data)
    return (rsi < 25 or 80 < rsi < 90)


def rsi_weak(stock, unit='1d', period=6, data='close'):
    rsi_day = stock_rsi(stock, '1w', period, data)
    rsi_week = stock_rsi(stock, unit, period, data)
    return (30 < rsi_day < 60 or rsi_day > 90) and (30 < rsi_week < 60 or rsi_week > 90)


def rsi_peaked(stock, unit='1d', period=14):
    rsi = stock_rsi(stock, unit, period)
    vrsi = stock_rsi(stock, unit, period, data='volume')
    return ((rsi > 90 and vrsi > 65) or rsi > 95)


def order_limit(stock, shares, price):
    return order(stock, shares, LimitOrderStyle(price))


def my_bars(stock, count=100, unit='1d', include_now=False,
            fields=['volume', 'close', 'open','high','low']):
    return history_bars(stock, count, unit, fields)


def hold_info(context):
    m = '\t'
    for stock in list(context.portfolio.positions.keys()):
        name = stock_name(stock)
        cost = stock_cost(context,stock)
        profit = stock_profit(context,stock)
        ratio = stock_value(context,stock)/port_total(context)
        m += '%s=%2.f(%.f) ' %(name, ratio*100, profit*100)
    return m


def port_info(context):
    total = context.portfolio.total_value
    profit = (total - context.total[-2]) / context.total[-2]
    ratio = context.ratio / context.days * 100
    holds = context.holds / context.days
    return '\t现仓=%2.f 均仓=%.2f 现股=%d 均股=%.2f 收益=%.2f RSI=%.2f 总值=%d' % (
        port_ratio(context) * 100, ratio, len(context.portfolio.positions), holds,
        profit, context.rsi, total)


def stock_info(context, stock):
    profit = stock_profit(context,stock)
    price = stock_price(context,stock)
    net = stock_value(context,stock) - stock_cost(context,stock)
    return '  %s: 现价=%.2f 收益=%2.0f%% 利润=%d 股数=%d' % (
        stock_name(stock), price, profit * 100,
        stock_profit(context, stock) * stock_cost(context, stock),
        stock_amount(context, stock))


def stock_mode(context, stock, unit='1d', days=6):
    bd = my_bars(stock, unit=unit)
    if len(bd) < days: return 0
    volumes = bd['volume']
    vma = talib.MA(volumes, days)[-days:]
    volumes = volumes[-days:]  # 对齐
    volumes = volumes[volumes > vma]
    return len(volumes) / days  # 1：情绪很高，0：情绪很低


def stock_gain(context, stock, days=1, data='close'):
    bd = my_bars(stock)
    if len(bd) < days: return 0
    return stock_price(context, stock) / bd[data][-days - 1] - 1


def stock_profit(context, stock):
    cost = stock_cost(context,stock)
    return get_position(stock).pnl/cost if cost else 0

def stock_days(context, stock):
    sdate = context.portfolio.positions[stock].init_time
    return (current_date(context) - sdate).days


def stock_ersi(stock, unit='1w', period=14, data='close', count=100):
    a = my_bars(stock, count, unit, fields=[data])
    if len(a) < period: return 50, 0
    rsi = talib.RSI(numpy.array(a[data]), period)
    return rsi[-1], rsi[-1] - rsi[-2]


def stock_myrsi(stock, unit='1d', count=14):
    bd = my_bars(stock, count, unit)
    if len(bd) < count: return 50
    psum = nsum = 0
    for i in range(1, count):
        x = bd['close'][i] - bd['close'][i - 1]
        if x > 0: psum += x
        if x < 0: nsum += x
    if psum - nsum == 0: return 50
    return psum / (psum - nsum) * 100


def order_money(context, stock, money):
    price = stock_price(context, stock)
    if money>0: money = min(money,port_cash(context))
    if abs(money) < 100 * price: return None
    return order_value(stock, money)


def order_ratio(context, stock, ratio):
    shares = stock_amount(context, stock) * ratio
    price = stock_price(context, stock)
    if shares>0: shares = min(shares, port_cash(context)/price)
    if abs(shares) < 100: return None
    return order(stock, shares)


def remove_loss(context, tobuy, percent=-0.12):  ## remove those losing stocks
    for stock in list(tobuy):
        if stock_gain(context, stock, 20) < percent:
            if stock in context.portfolio.positions:
                order_target(stock, 0)
            tobuy.remove(stock)
    return tobuy


def hold_clear(context, ratio=1):
    print('账户清仓 ' + port_info(context))
    for stock in context.portfolio.positions:
        shares = stock_amount(context, stock)
        order_target(stock, -shares * (1 - ratio))


def initialize(context):
    global_setup(context)
    set_benchmark(context.BENCH)  # 设定沪深300作为基准
    log.set_level('system', 'error')  # 只打印系统信息与错误
    set_option('use_real_price', True)  # 使用真实价格交易
    set_option('order_volume_ratio', 1)  # 设定成交量比例
    set_option('avoid_future_data', True)  # 避免未来数据
    # 股票类佣金：买入时万二，卖出时万二加千一印花税, 每笔交易最低5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0002,
                             close_commission=0.0002, min_commission=3), type='stock')


def global_setup(context):
    context.BENCH = '000001.XSHG'  # 基准指数
    context.stocknum = 10   #持仓股票数限制
    context.days = 0  # 交易日计数.
    context.holds = 0  # 持有股票数累计
    context.ratio = 0  # 每日持仓累计
    context.rsi = 50  # 大盘相对强弱
    context.data = None  # get_current_data()
    context.new = []  # 新股池
    context.new = []  # 要换仓的股票
    context.pending = [] # 要买没买的股票
    context.tosell = []  # 要卖没卖的股票
    context.oids = {}  # 订单ID及次数
    context.stocks = {}  # 股票基本面数据集
    context.total = [port_total(context)]  # 账户价值记录


def after_code_changed(context):
    unschedule_all()  # 重置，方便代码升级
    global_setup(context)
    run_monthly(period_start, 1, time='8:30')
    # run_weekly(do_weekly, 1, time='open')
    run_daily(do_daily, time='open')


def period_start(context, bar_dict):
    context.data = bar_dict
    context.total.append(port_total(context))
    tobuy = stocks_get(context)
    context.new = tobuy[:context.stocknum]  ##+ah_get(context)[:5]
    if len(context.new) == 0: return
    do_message('潜力榜: ' + ' '.join(map(stock_name, context.new)))
    # rebalance(context,context.new),
    # context.new = []


def do_weekly(context, stocknum=1):
    update = stocks_get(context)
    if len(update) and update[0] not in context.portfolio.positions:
        order_money(context, update[0], port_cash(context))


def sell_pending(context):
    for stock in list(context.tosell):
        if stock in context.portfolio.positions:
            if stock_price(context, stock) > 0.01 or not rsi_strong(stock):
                order_target(stock, 0)
                context.tosell.remove(stock)
        else:
            context.tosell.remove(stock)


def rebalance(context, newset=[], ratio=1):
    holding = list(context.portfolio.positions.keys())
    tosell = [stock for stock in holding if stock not in newset]
    for stock in list(tosell): order_target(stock, 0)
    tobuy = remove_loss(context, newset)
    # if port_cash(context)>40000000: inout_cash(10000000)
    ### 不买已买的股票,效果不如重复买！
    # tobuy = [stock for stock in tobuy if stock not in holding]
    if len(tobuy) == 0: return []
    money = port_total(context) / len(tobuy) * ratio
    for stock in tobuy:
        order_money(context, stock, money)
    #return []
    #return [stock for stock in tobuy if stock not in context.portfolio.positions]


def do_daily(context, bar_dict):
    context.data = bar_dict
    context.days += 1
    context.ratio += port_ratio(context)
    context.holds += len(context.portfolio.positions)
    context.total.append(port_total(context))
    context.rsi = stock_rsi(context.BENCH, '1d', period=6)
    if len(context.new):
        rebalance(context, context.new)
        context.new = []
        #context.pending = [stock for stock in context.new if stock not in context.portfolio.positions]
    if len(context.pending):
        money = port_total(context)/len(context.pending)
        for stock in context.pending:
            order_money(context,stock,money)
            context.pending = [] #[stock for stock in context.pending if stock not in context.portfolio.positions]
    print(port_info(context))
    print(hold_info(context))
    #day_optimize(context)

def day_optimize(context):
    sell_pending(context)
    dapan_mode = stock_mode(context,context.BENCH)
    for stock in context.portfolio.positions:
        mode = stock_mode(context, stock, days=7)
        if mode == 0 and dapan_mode == 0:
            order_ratio(context, stock, -0.2)
        elif mode == 1 and dapan_mode == 1:
            order_ratio(context, stock, 0.2)
        elif rsi_peaked(stock): order_target(stock,0)
        elif rsi_strong(stock, period=6): order_ratio(context, stock, 1)
        else :
            score = trend(context, stock)
            if score:
                order_ratio(context, stock, score*0.5)
                continue
            score = stock_gain(context, stock, days=5)
            if abs(score)>0.2 and mode>0.5:
                order_ratio(context, stock, score*1)

def trend(context,stock):
    mrs = stock_rsi(stock,'1M')
    wrs = stock_rsi(stock,'1w')
    drs = stock_rsi(stock,'1d')
    if (50<mrs<85) and (wrs>mrs) and (drs>wrs):  return 1
    if (30<mrs<50) and (wrs<mrs) and (drs<wrs):  return -1
    else: return 0

def pool_filter(context,pool):
    curr_data = context.data
    pool = [stock for stock in pool if not (
            is_suspended(stock)   # 停牌
            or is_st_stock(stock)   # ST
            or ('ST' in stock_name(stock))
            or ('*' in stock_name(stock))
            or ('退' in stock_name(stock))
            #or (curr_data[stock].last == curr_data[stock].limit_up)     # 涨停开盘
            #or (curr_data[stock].last == curr_data[stock].limit_down)   # 跌停开盘
            #or stock.startswith('300')     # 创业
            #stock.startswith('688') # 科创
            )]
    return pool

jq.auth('13135685382', 'Fqm120103011125')

def get_q4(context, stock):
    """
    获取指定股票指定日期的连续5个 roe5 ,cf5,gross5的数据
    环比 result['600309.XSHG'][-2]
    同比 result['600309.XSHG'][0]
    :param context:
    :param stock: 注意需要传递数组
    :return:
    """
    param = dict(date=context.now.strftime("%Y-%m-%d"), codes=stock)
    r = requests.get(url="http://1000stock.com/config/find_Q4", params=param)
    data = pd.DataFrame(json_normalize(r.json()))
    data.index = data['code']
    for index, row in data.iterrows():
        if len(row['pubDate']) < 5:
            tmp = []
            fields = ['pubDate', 'incRevenueYearOnYear', 'incRevenueAnnual', 'incNetProfitYearOnYear',
                      'incNetProfitAnnual', 'netProfit', 'roe', 'grossProfitMargin', 'netProfitMargin',
                      'netOperateCashFlow', 'operatingProfit']
            for i in range(0, 5 - len(row['pubDate'])):
                tmp = [numpy.nan] + tmp
            for field in fields:
                data.at[row['code'], field] = tmp + row[field]
    return data

def stocks_get(context, pool=[], cap_req=10, rev_req=15, pe_req=50,
            gross_req=15, roe_req=3, roe_ratio=1.2, cf_ratio=1.5):
    date = current_date(context).strftime('%Y-%m-%d')
    if pool==[]: pool = get_all_stocks(context)
    pool = pool_filter(context,pool)
    q = jq.query(
        jq.cash_flow.pubDate,
        jq.valuation.code,
        jq.valuation.pe_ratio,
        jq.valuation.market_cap,
        jq.indicator.roe,
        jq.cash_flow.net_operate_cash_flow,
        jq.income.net_profit,
        jq.income.operating_revenue,
        jq.indicator.gross_profit_margin,
    ).filter(
        jq.valuation.code.in_(pool),
        jq.valuation.market_cap>cap_req,
        jq.indicator.roe>roe_req,
        jq.valuation.pe_ratio.between(0, pe_req),
        jq.valuation.pe_ratio<(jq.indicator.inc_operation_profit_year_on_year
                    +jq.indicator.inc_operation_profit_annual)*0.75,
        jq.indicator.gross_profit_margin>gross_req,
        jq.indicator.inc_revenue_year_on_year>rev_req,
        jq.indicator.inc_operation_profit_year_on_year>jq.indicator.inc_revenue_year_on_year,
        jq.cash_flow.net_operate_cash_flow>jq.income.net_profit*cf_ratio,
    ).order_by(jq.valuation.pe_ratio-jq.indicator.gross_profit_margin/10)
    stocks = jq.get_fundamentals(q, date=date)
    slist = stocks['code'].tolist()
    if len(slist)==0: return []
    buy = []
    q4 = get_q4(context, slist)
    q4.index = q4.code
    for stock in slist:
        if stock not in q4.roe: continue
        roe5 = q4.roe[stock]
        gross5 = q4.grossProfitMargin[stock]
        cf5 = q4.netOperateCashFlow[stock]
        if (roe5[0]!=None and roe5[-1] > roe5[0] * roe_ratio
            and roe5[-2]!=None and roe5[-1] > roe5[-2]
            and cf5[0]!=None and cf5[-1] > cf5[0] * cf_ratio
            and cf5[-2]!=None and cf5[-1] > cf5[-2]
            and gross5[0]!=None and gross5[-1] > gross5[0]
            and gross5[-2]!=None and gross5[-1] > gross5[-2]
            and rsi_ok(stock, '1w')
            and stock_mode(context, stock) > 0
        ): buy.append(stock)
    return buy


# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):
    global_setup(context)
    #rscheduler.run_weekly(do_weekly,1,time_rule=market_open(minute=0)
    scheduler.run_monthly(period_start, tradingday=-1, time_rule=market_open(minute=1))
    scheduler.run_daily(do_daily, time_rule=market_open(minute=1))

# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_barrr(context, bar_dict):
    do_daily(context, bar_dict)
    context.data = bar_dict
    print('%s %.2f'%(stock, stock_price(context, '600720.XSHG')))

# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
config = {
  "base": {
    "start_date": "2013-12-31",
    "end_date": "2021-04-24",
    "frequency": "1d",
    "data_bundle_path":"D:\python\data\\bundle",
    "accounts": {
        "stock": 1000000
    }
  },
  "extra": {
    "log_level": "info",
  },
  "mod": {
    "sys_analyser": {
      "enabled": True,
      "plot": True,
      "benchmark": "000001.XSHG",
    },
  }
}

# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
results = run_func(config=config, init=init)