# -*- coding:utf-8 -*- 
import sys
import pymysql
sys.path.append("..")
reload(sys)
sys.setdefaultencoding('utf8')
import time;  # 引入time模块
import tushare as ts
import pandas as pd
import numpy as np
from tushare.stock import cons as ct
import tushare.stock.classifying as fd
import tushare.stock.fundamental as sf

def getConnAndCur():
	# 连接数据库  
	conn = pymysql.connect(
				# host="192.168.12.188",
				# port=3306,user="root",
				host="172.18.84.232",
				port=3311,user="root",
				passwd="123456",
				db="mydb",
				charset="utf8")
	cur = conn.cursor()
	return conn,cur


def get_stock_id(item,code,current_time,conn,cur):
	#判断股票是否存在
	stock_id=0
	stock_id_sql="select stock_id from base_stock_info where code = '%s'" % code
	count = cur.execute(stock_id_sql)
	if count == 0:
		try:  
			#插入股票信息 
			cur.execute("insert into base_stock_info(code,name,create_time,update_time) values('%s','%s','%s','%s')" % (item['code'],item['name'],current_time,current_time))
			conn.commit()  
		except Exception as e:  
			print(e) 
			conn.rollback() 
		cur.execute(stock_id_sql)
		stock_id = cur.fetchone()[0]
	else:
		stock_id = cur.fetchone()[0]
	return stock_id


def save_classified_data(df,year,quarter):
	'''
	保存行业信息
	'''
	conn,cur=getConnAndCur()
	current_time = getCurrentTime()
	param=[]
	for index,item in df.iterrows():
		stock_id=get_stock_id(item,item['code'],current_time,conn,cur)
		#插入业绩报表
		stock_id_sql="select * from stock_classified where classify_type = '%s' and stock_id='%s'" % (classify_type,stock_id)
		count = cur.execute(stock_id_sql)
		if count == 0:
			try:  
				#插入具体分类信息 
				cur.execute("insert into stock_classified(classify_name,classify_type,stock_id,create_time,update_time) values('%s','%s','%d','%s','%s')" % (item['c_name'],classify_type,stock_id,current_time,current_time))
				conn.commit()  
			except Exception as e:  
				print(e) 
				conn.rollback() 
		else:
			print("classify is exist,stock_id is %s",stock_id)
	# 关闭数据库连接
	conn.close()


def save_stock_basics(df):
	'''
	保存stock 列表信息
	'''
	conn,cur=getConnAndCur()
	param=[]
	for index,item in df.iterrows():
		stock_id=''
		current_time = getCurrentTime()
		#判断股票是否存在
		stock_id_sql="select stock_id from base_stock_info where code = '%s'" % item.name
		count = cur.execute(stock_id_sql)
		try:  
			if count == 0:
				#插入股票信息 
				cur.execute('''insert into base_stock_info
					(code,name,area,pe,outstanding,totals,totalAssets,liquidAssets,fixedAssets,reserved,reservedPerShare,esp,bvps,pb,timeToMarket,undp,perundp,rev,profit,gpr,npr,holders,create_time,update_time)
					 values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
					 ''' % (item.name,item['name'],item['area'],item['pe'],item['outstanding'],item['totals'],item['totalAssets'],item['liquidAssets'],item['fixedAssets'],item['reserved'],item['reservedPerShare'],item['esp'],item['bvps'],item['pb'],item['timeToMarket'],item['undp'],item['perundp'],item['rev'],item['profit'],item['gpr'],item['npr'],item['holders'],current_time,current_time))
			else:
				#更新股票信息 
				cur.execute('''update base_stock_info
								set pe='%s',
									area='%s',
									outstanding='%s',
									totals='%s',
									totalAssets='%s',
									liquidAssets='%s',
									fixedAssets='%s',
									reserved='%s',
									reservedPerShare='%s',
									esp='%s',
									bvps='%s',
									pb='%s',
									timeToMarket='%s',
									undp='%s',
									perundp='%s',
									rev='%s',
									profit='%s',
									gpr='%s',
									npr='%s',
									holders='%s',
									update_time='%s' 
								where code = '%s'
					 		''' % (item['area'],item['pe'],item['outstanding'],item['totals'],item['totalAssets'],item['liquidAssets'],item['fixedAssets'],item['reserved'],item['reservedPerShare'],item['esp'],item['bvps'],item['pb'],item['timeToMarket'],item['undp'],item['perundp'],item['rev'],item['profit'],item['gpr'],item['npr'],item['holders'],current_time,item.name))
			conn.commit()  
		except Exception as e:  
			print(e) 
			conn.rollback() 
		
		
	# 关闭数据库连接
	conn.close()


def save_basic_data(table_name,df,year,quarter):
	'''
	保存基本面信息
	'''
	conn,cur=getConnAndCur()
	current_time = getCurrentTime()
	for index,item in df.iterrows():
		stock_id=get_stock_id(item,item['code'],current_time,conn,cur)
		# 查询数据是否存在
		count = cur.execute("select stock_id from %s where year = '%s' and quarter='%s' and stock_id='%s'" % (table_name,year,quarter,stock_id))
		if count == 0:
			try: 
				#插入基本面信息 
				append_dict = {'year':year,'quarter':quarter,"stock_id":stock_id,"create_time":current_time,'update_time':current_time}
				cur.execute(join_insert_sql(table_name,item,append_dict))
				conn.commit()  
			except Exception as e:  
				print(e) 
				conn.rollback() 
		else:
			print("%s exist,stock_id =%s and year =%s and quarter=%s" % (table_name,stock_id,year,quarter))
	# 关闭数据库连接
	conn.close()

def join_insert_sql(table,item,append_dict):
	'''
	组拼基本面的insert语句
	'''
	insert = ''' insert into %s (''' % table
	values = []
	valueParams = ''
	_dict = item.to_dict()
	_dict = dict(_dict,**append_dict)
	for k,v in _dict.iteritems():
		if pd.notnull(v) and k!='name' and v!='--':
			insert = insert+k+','
			values.append(v)

	for _ in values:
		valueParams= valueParams+"'%s',"
	insert = insert[0:-1]+")"+" values("+valueParams[0:-1]+")"

	insert = insert % tuple(values)
	print(insert)
	return insert




def getCurrentTime():
	'''
	获取当前时间， 时间格式为 2017-09-13 14:30:34
	'''
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def initClassifiedData():
	# 行业分类
	df = fd.get_industry_classified()
	save_classified_data(df,"行业分类")

	# 行业分类
	df = fd.get_concept_classified()
	save_classified_data(df,"概念分类")

	# 地域分类数据
	df = fd.get_area_classified()
	df.rename(columns={'area':'c_name'}, inplace = True)
	save_classified_data(df,"地域分类")

	# 中小板分类数据
	df = fd.get_sme_classified()
	df.loc[:,'c_name']=''
	save_classified_data(df,"中小板分类")

	# 创业板分类数据
	df = fd.get_gem_classified()
	df.loc[:,'c_name']=''
	save_classified_data(df,"创业板分类")


def init_basic_data():
	# 添加股票列表
	df = sf.get_stock_basics()
	# print(df)
	save_stock_basics(df)


	# 添加基本面信息
	'''
	performance_repor:业绩报告
	stock_profit：盈利能力
	stock_operation：运营能力
	stock_growth:成长能力
	stock_debtpay:偿债能力
	stock_cashflow：现金流量
	'''
	# df = sf.get_operation_data(2016,4)
	# save_basic_data('stock_operation',df,2016,4)




if __name__ == '__main__':
	init_basic_data()