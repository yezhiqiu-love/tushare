# -*- coding:utf-8 -*- 
import sys
import pymysql
sys.path.append("..")
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import tushare as ts
from tushare.stock import cons as ct
import tushare.stock.classifying as fd

def getConnAndCur():
	# 连接数据库  
	conn = pymysql.connect(
				host="192.168.1.119",
				port=3306,user="root",
				passwd="123456",
				db="mydb",
				charset="utf8")
	cur = conn.cursor()
	return conn,cur


# 批量插入executemany  
def insert_by_many(df,code,date): 
	conn,cur=getConnAndCur()
	sel = "select count(*) from my_tick_data where code = '%s' and date = '%s' and time= '%s'"
	# TICK_COLUMNS = ['time', 'price', 'change', 'volume', 'amount', 'type']
	insert = '''INSERT INTO my_tick_data(time,price,type,code,date) values(%s,%s,%s,%s,%s) '''
	param=[]
	success_count=0  
	for index,item in df.iterrows():
		#判断数据是否已经存在
		cur.execute(sel % (code,date,item['time']))
		count =cur.fetchone()
		if count[0] == 0:
			param.append([item['time'], item['price'],item['type'],code,date])  
		else:
			print("数据已经存在：code = %s and date = %s and time= %s" % (code,date,item['time']))
	
	try:  
		# 批量插入  
		success_count=cur.executemany(insert, param)  
		conn.commit()  
	except Exception as e:  
		print e  
		conn.rollback()   
	print("DataFrame count:",len(df))
	print ('[insert_by_many executemany] total:',success_count)


def saveClassified(df,classify_type):
	conn,cur=getConnAndCur()
	sel = "select count(*) from my_tick_data where code = '%s' and date = '%s' and time= '%s'"
	# TICK_COLUMNS = ['time', 'price', 'change', 'volume', 'amount', 'type']
	insert = '''INSERT INTO my_tick_data(time,price,type,code,date) values(%s,%s,%s,%s,%s) '''
	param=[]
	success_count=0  
	for index,item in df.iterrows():
		stock_id=''
		#判断股票是否存在
		stock_id_sql="select stock_id from base_stock_info where code = '%s'" % item['code']
		count = cur.execute(stock_id_sql)
		if count == 0:
			try:  
				#插入股票信息 
				cur.execute("insert into base_stock_info(code,name) values('%s','%s')" % (item['code'],item['name']))
				conn.commit()  
			except Exception as e:  
				print(e) 
				conn.rollback() 
			cur.execute(stock_id_sql)
			stock_id = cur.fetchone()[0]
		else:
			stock_id = cur.fetchone()[0]
		
		#插入分类信息
		stock_id_sql="select * from stock_classified where classify_type = '%s' and stock_id='%s'" % (classify_type,stock_id)
		count = cur.execute(stock_id_sql)
		if count == 0:
			try:  
				#插入具体分类信息 
				cur.execute("insert into stock_classified(classify_name,classify_type,stock_id) values('%s','%s','%d')" % (item['c_name'],classify_type,stock_id))
				conn.commit()  
			except Exception as e:  
				print(e) 
				conn.rollback() 
		else:
			print("classify is exist,stock_id is %s",stock_id)



def testSql():
	conn,cur=getConnAndCur()
	item={'code':'00003','name':'哈哈更改'}
	stock_id=''
	#判断股票是否存在
	stock_id_sql="select stock_id from base_stock_info where code = '%s'" % item['code']
	count = cur.execute(stock_id_sql)
	if count == 0:
		try:  
			#插入股票信息 
			cur.execute("insert into base_stock_info(code,name) values('%s','%s')" % (item['code'],item['name']))
			conn.commit()  
		except Exception as e:  
			print(e) 
			conn.rollback() 
		cur.execute(stock_id_sql)
		stock_id = cur.fetchone()[0]
	else:
		stock_id = cur.fetchone()[0]
	print("stock_id:",stock_id)


if __name__ == '__main__':
	# code = '600848'
	# date = '2014-12-22'
	# df = ts.get_tick_data(code,date=date)
	# insert_by_many(df,code,date)

	# 中小板分类数据
	# df = fd.get_sme_classified()
	# df.loc[:,'c_name']=''
	# saveClassified(df,"中小板分类")

	# 创业板分类数据
	# df = fd.get_gem_classified()
	# df.loc[:,'c_name']=''
	# saveClassified(df,"创业板分类")


	# 地域分类数据
	df = fd.get_area_classified()
	df.rename(columns={'area':'c_name'}, inplace = True)
	saveClassified(df,"地域分类")
