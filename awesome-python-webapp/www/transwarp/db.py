#! /usr/bin/env python
# -*- coding:utf-8 -*-

__author__='sniper'

'''
Database operation module.
'''

import time,uuid,functools,threading,logging

#Dict object:
#继承dict对象
class Dict(dict):
	'''
	test
	'''
	#将属性名，元祖连接,转换为字典
	def __init__(self,names=(),values=(),**kw):
		super(Dict,self).__init__(**kw)
		for k,v in zip(names,values):
			self[k]=v
	
	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Dict' object has no attribute '%s'" % key)
	
	def __setattr__(self,key,value):
		self[key]=value
	#end of Dict


#生成uuid信息，根据时间信息	
def next_id(t=None):
	'''
	Return next id as 50-char string.
	Args:
		t:unix timestamp,default to None and using time.time()
	'''

	if t is None:
		t=time.time()
	return '%015d%s000' % (int(t*1000),uuid.uuid4().hex)

def _profiling(start,sql=''):
	t=time.time()-start
	if t>0.1:
		logging.warning('[PROFILING][DB]%s:%s' % (t,sql))
	else:
		logging.info('[PROFILING][DB]%s:%s' % (t,sql))


#两个错误类定义
class DBError(Exception):
	pass

class MultiColumnsError(DBError):
	pass


#封装了数据库的基本操作
class _LasyConnection(object):
	'''
	已经测试完毕
	'''
	def __init__(self):
		self.connection=None

	def cursor(self):
		if self.connection is None:
			connection=engine.connect() #生成一个mysql的connector
			logging.info('open connection <%s>...' % hex(id(connection)) )
			self.connection=connection
		#记住_LazyConnection的cursor函数会返回一个cursor对象
		return self.connection.cursor()

	def commit(self):
		self.connection.commit()

	def rollback(self):
		self.connection.rollback()
	
	def cleanup(self):
		if self.connection:
			connection=self.connection
			self.connection=None
			logging.info('close connection <%s>...' % hex(id(connection)))
			connection.close()


#创建一个线程局部变量作为数据库上下文		
#_DbCtx变成一个线程局部变量，属于某个特定线程
#提供一个连接 transactions表示当前的事物数目
class _DbCtx(threading.local):
	'''
	Threading local object that holds connection info.
	已经测试完毕
	'''
	
	def __init__(self):
		self.connection=None
		self.transactions=0

	def is_init(self):
		return not self.connection is None
	
	def init(self):
		logging.info('open lazy connection...')
		
		#注意_DbCtx的属性connection是_LasyConnection对象
		self.connection=_LasyConnection()
		self.transactions=0

	def cleanup(self):
		self.connection.cleanup()
		self.connection=None

	def cursor(self):
		'''
		Return cursor
		'''
		return self.connection.cursor()

#thread-local db context
_db_ctx=_DbCtx()

#global engine object:
engine=None #engine是一个_Engine类的对象，这个类实际上只有一个属性--mysql.connector.close函数的指针，只有调用engine类的connect函数时，才真正调用函数进行数据库的连接

class _Engine(object):
	'''
	已经测试完毕
	'''
	#注意，这里的参数connect是一个函数指针，由lambda指定
	def __init__(self,connect):
		self._connect=connect
	
	def connect(self):
		return self._connect()


#创建一个engine对象（全局），engine对象用于封装数据库连接信息
def create_engine(user='root',password='123',database='python_web',host='127.0.0.1',port=3306,**kw):
	'''
	已经测试完毕
	'''
	import mysql.connector
	global engine
	if engine is not None:
		raise DBError('Engine is already initialized.')

	params=dict(user=user,password=password,database=database,host=host,port=port)
	#默认的一些参数（可能会被覆盖）
	defaults=dict(use_unicode=True,charset='utf8',collation='utf8_general_ci',autocommit=False)
	
	#将默认参数设置和指定参数整合起来
	for k,v in defaults.iteritems():
		params[k]=kw.pop(k,v)#先设置默认的一些参数，如果该参数没有被覆盖
		#即kw中没有,则用默认值代替，否则k会从kw中取出参数值覆盖默认值
	params.update(kw);#再设置非默认的参数
	params['buffered']=True

	#传递一个函数指针，lambda用于声明一个函数
	engine=_Engine(lambda:mysql.connector.connect(**params))
	
	logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))#转换为16进制

#with语句的enter和exit函数
class _ConnectionCtx(object):
	'''

	_ConnectionCtx object that can open and close connection context.

	with connection():
		pass
		with connection():
			pass
	'''

	def __enter__(self):
		global _db_ctx #全局化
		self.should_cleanup=False #自身只有一个属性--标记是否应该清空
		if not _db_ctx.is_init():#未初始化的时候，则生成_LasyjConnection对象
			_db_ctx.init()
			self.should_cleanup=True
		return self

	def __exit__(self,exctype,exvalue,traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()


def connection():
	'''
	Return _ConnectionCtx object that can be used by 'with' statement:
	
	with connection():
		pass

	'''
	return _ConnectionCtx()

#with语句使得每次对数据库操作时，都有一个connection
def with_connection(func):
	'''
	装饰器的使用
	'''
	@functools.wraps(func)#保存原函数的一些属性
	def _wrapper(*args,**kw):
		with _ConnectionCtx():
			return func(*args,**kw)
	return _wrapper

#with，使得每次事物管理操作时，确保有一个connect对象用于事物的提交、回滚，同时在线程里面的事物数目加1

class _TransactionCtx(object):
	'''
	_TransactionCtx object that can handle transactions

	with_Transaction():
		pass	
	'''

	def __enter__(self):
		global _db_ctx
		self.should_close_conn=False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn=True
		
		_db_ctx.transactions=_db_ctx.transactions+1#事物数目加1

		logging.info('begin transaction...' if _db_ctx.transactions==1 else 'join current transaction...')
		return self

	def __exit__(self,extype,exvalue,traceback):
		global _db_ctx
		_db_ctx.transactions=_db_ctx.transactions-1#事物数目减1

		#如果没有错误发生，提交事物，否则回滚
		try:
			if _db_ctx.transactions==0:
				if extype is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):
		global _db_ctx
		logging.info('commit transaction...')
		
		try:
			_db_ctx.connection.commit()
			logging.info('commit ok.')
		except:
			logging.warning('commit failed.try rollback...')
			_db_ctx.connection.rollback()
			logging.warning('rollback ok.')
			raise

	def rollback(self):
		global _db_ctx
		logging.warning('rollback transaction...')
		_db_ctx.connection.rollback()
		logging.info('rollback ok.')

def transaction():
	'''
	create a transaction object so can use with statement:
	
	with transaction():
		pass

	'''
	return _TransactionCtx()



def with_transaction(func):
	'''
		
	A decorator that makes function around transaction.
	'''
	@functools.wraps(func)#保存原函数func的一些属性
	def _wrapper(*args,**kw):
		_start=time.time()
		with _TransactionCtx():#with语句,with语句包围事物处理过程中必须的一些步骤
			return func(*args,**kw)#执行目标函数
		_profiling(_start)#日志记录事物处理流程
	return _wrapper

def _select(sql,first,*args):
	'execute select SQL and return unique result or list results.'
	global _db_ctx
	cursor=None
	sql=sql.replace('?','%s')
	logging.info('SQL:%s,ARGS:%s' % (sql,args))
	
	try:
		cursor=_db_ctx.connection.cursor()
		cursor.execute(sql,args)#注意，exexcute函数的关键部分可以用%s代替
		if cursor.description:
			names=[ x[0] for x in cursor.description]#cursor.description()列出各列的信息,x[0]取出各列的名字，即属性名列表
		if first:
			values=cursor.fetchone()#取出结果集的第一行
			if not values:
				return None
			return Dict(names,values)#将第一行结果集和属性名列表合起来（zip函数），形成字典
		return [Dict(names,x) for x in cursor.fetchall()]#将每一行结果集和属性名结合起来，形成字典列表
	
	finally:
		if cursor:
			cursor.close()


#用with_connection装饰器函数修饰_select
@with_connection
def select_one(sql,*args):#查询结果只有一行
	return _select(sql,True,*args)

@with_connection
def select_int(sql,*args):
	d=_select(sql,True,*args)
	if len(d)!=1:
		raise MultiColumnsError('Except only one column.')
	return d.values()[0]

@with_connection
def select(sql,*args):
	return _select(sql,False,*args)

@with_connection
def _update(sql,*args):
	global _db_ctx
	cursor=None
	sql=sql.replace('?','%s')
	logging.info('SQL: %s,ARGS:%s ' % (sql,args))

	try:
		cursor=_db_ctx.connection.cursor()
		cursor.execute(sql,args)
		r=cursor.rowcount
		if _db_ctx.transactions==0:
			logging.info('auto commit')
			_db_ctx.connection.commit()
		return r
	finally:
		if cursor:
			cursor.close()

def insert(table,**kw):
	cols,args=zip(*kw.iteritems())	
	sql='insert into %s (%s) values(%s)' % (table,','.join(['%s' % cols for cols in cols]),','.join(['?' for i in range(len(cols))]))
	return _update(sql,*args)

def update(sql,*args):
	return _update(sql,*args)


#for test
if __name__=='__main__':
	logging.basicConfigj(level=logging.DEBUG)
	create_engine('www-data','www-data','test')
	update('drop table if exits user')
	updat('create table user (id int primart key,name text,email text,passwd text,last_modified real)')
	import doctest
	doctest.testmod()

