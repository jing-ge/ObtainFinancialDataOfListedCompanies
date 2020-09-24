import pandas as pd
import baostock as bs
from tqdm import tqdm
from multiprocessing import Pool,freeze_support
import time
import os
from concurrent.futures import ThreadPoolExecutor

df = pd.read_csv("stock_industry.csv",encoding = "gbk")
companycode = df["code"].values.tolist()

l = os.listdir("./companydata")
for i in range(len(l)):
    l[i] = l[i][:-4]

filtercompanycode = []
for code in companycode:
    if code in l:
        continue
    else:
        filtercompanycode.append(code)
# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

def getquarterdata(companycode,year,quarter):
    profit_list = []
    rs_profit = bs.query_profit_data(code=companycode, year=year, quarter=quarter)
    while (rs_profit.error_code == '0') & rs_profit.next():
        profit_list.append(rs_profit.get_row_data())
    if len(profit_list)==0:
        return [],[]
    # 营运能力
    operation_list = []
    rs_operation = bs.query_operation_data(code=companycode, year=year, quarter=quarter)
    while (rs_operation.error_code == '0') & rs_operation.next():
        operation_list.append(rs_operation.get_row_data())

    # 成长能力
    growth_list = []
    rs_growth = bs.query_growth_data(code=companycode, year=year, quarter=quarter)
    while (rs_growth.error_code == '0') & rs_growth.next():
        growth_list.append(rs_growth.get_row_data())

    # 偿债能力
    balance_list = []
    rs_balance = bs.query_balance_data(code=companycode, year=year, quarter=quarter)
    while (rs_balance.error_code == '0') & rs_balance.next():
        balance_list.append(rs_balance.get_row_data())   

    # 季频现金流量
    cash_flow_list = []
    rs_cash_flow = bs.query_cash_flow_data(code=companycode, year=year, quarter=quarter)
    while (rs_cash_flow.error_code == '0') & rs_cash_flow.next():
        cash_flow_list.append(rs_cash_flow.get_row_data())

    # 查询杜邦指数
    dupont_list = []
    rs_dupont = bs.query_dupont_data(code=companycode, year=year, quarter=quarter)
    while (rs_dupont.error_code == '0') & rs_dupont.next():
        dupont_list.append(rs_dupont.get_row_data())
    if len(profit_list)*len(operation_list)*len(growth_list)*len(balance_list)*len(cash_flow_list)*len(dupont_list)==0:
        return [],[]
    datalist = [companycode + "-" + year +"-" + quarter]
    datalist += profit_list[0] + operation_list[0] + growth_list[0] + balance_list[0] + cash_flow_list[0] + dupont_list[0]
    fieldslist = ["quaryContent"]
    fieldslist += rs_profit.fields + rs_operation.fields + rs_growth.fields + rs_balance.fields + rs_cash_flow.fields + rs_dupont.fields
    return datalist,fieldslist

def getdata(companycode):
    print("正在下载上市公司："+companycode+"的财务数据......")
    data = []
    fieldslist = ['quaryContent', 'code', 'pubDate', 'statDate', 'roeAvg', 'npMargin', 'gpMargin', 'netProfit', 'epsTTM', 'MBRevenue', 'totalShare', 'liqaShare', 'code', 'pubDate', 'statDate', 'NRTurnRatio', 'NRTurnDays', 'INVTurnRatio', 'INVTurnDays', 'CATurnRatio', 'AssetTurnRatio', 'code', 'pubDate', 'statDate', 'YOYEquity', 'YOYAsset', 'YOYNI', 'YOYEPSBasic', 'YOYPNI', 'code', 'pubDate', 'statDate', 'currentRatio', 'quickRatio', 'cashRatio', 'YOYLiability', 'liabilityToAsset', 'assetToEquity', 'code', 'pubDate', 'statDate', 'CAToAsset', 'NCAToAsset', 'tangibleAssetToAsset', 'ebitToInterest', 'CFOToOR', 'CFOToNP', 'CFOToGr', 'code', 'pubDate', 'statDate', 'dupontROE', 'dupontAssetStoEquity', 'dupontAssetTurn', 'dupontPnitoni', 'dupontNitogr', 'dupontTaxBurden', 'dupontIntburden', 'dupontEbittogr']
    for y in range(2007,2020):
        year = str(y)
        for q in range(1,5):
            quarter = str(q)
            datalist,_ = getquarterdata(companycode,year,quarter)
            if len(datalist)!=0:
                data.append(datalist)  
    datalist,_= getquarterdata(companycode,"2020","1") 
    if len(datalist)!=0:
        data.append(datalist)
    datalist,_ = getquarterdata(companycode,"2020","2")
    if len(datalist)!=0:
        data.append(datalist)
    print(len(data))
    if len(data)!=0:
        print(companycode+":writing to file....")
        result = pd.DataFrame(data, columns=fieldslist)
        result.to_csv("./companydata/"+companycode+".csv", encoding="utf-8", index=False)

if __name__=='__main__':
    freeze_support()
    pool = Pool(processes=10) 
    print(os.getpid())
    for code in filtercompanycode: 
        pool.apply_async(func=getdata, args=(code,)) 
    print('end')
    pool.close()
    pool.join()


bs.logout()