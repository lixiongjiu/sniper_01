#!/usr/bin/env python
#coding=utf-8

__author__='sniper'

'''
Database operation module. This module is independent with web module.
object-relation mapping
'''

import time,logging
import db

class Field(object):
	_count=0
	
	def __init__(self,**kw):
		self.name=kw.get('name',None)
		self._default=kw.get('default',None)  #_default（默认值）可能是函数，也可能是其他类型
		self.primary_key=kw.get('primary_key',False) #是否为主键
		self.nullable=kw.get('nullable',False)#是否可为空
		self.updatable=kw.get('updatable',True)
		self.insertable=kw.get('insertable',True)
		self.ddl=kw.get('ddl','') #字段类型
		self._order=Field._count #记录字段类型定义的顺序
		Field._count=Field._count+1 #每初始化一个字段类型，就加一

	#注意注解，相当于_default属性的getter
	@property
	def default(self):
		d=self._default
		return d() if callable(d) else d #_default可能是函数，callable()函数检查对象是否可以被调用，可区分函数

	#类似于java中的toString函数，注意返回str类型的对象
	def __str__(self):
		#一下依次输出类型：字段名称，字段类型，默认值
		s=['<%s:%s,%s,default(%s),' % (self.__class__.__name__,self.name,self.ddl,self._default)]#注意__class__.__name__返回类名
		self.nullable and s.append('N')
		self.updatable and s.append('U')
		self.insertable and s.append('I')
		s.append('>')
		return ''.join(s)


class StringField(Field):
	def __init__(self,**kw):
		if not 'default' in kw:
			kw['default']='' #没有定义默认值，则置为’‘
		if not 'ddl' in kw:
			kw['ddl']='varchar(225)'#没有指定字段类型
		super(StringField,self).__init__(**kw)

class IntegerField(Field):
	def __init__(self,**kw):
		if not 'default' in kw:
			kw['default']=0
		if not 'ddl' in kw:
			kw['ddl']='bigint'
		super(IntegerField,self).__init__(**kw)

class FloatField(Field):
	def __init__(self,**kw):
		if not 'default' in kw:
			kw['default']=0.0
		if not 'ddl' in kw:
			kw['ddl']='real'
		super(FloatField,self).__init__(**kw)
class BooleanField(Field):
	def __init__(self,**kw):
		if not 'default' in kw:
			kw['default']=False
		if not 'ddl' in kw:
			kw['ddl']='bool'
		super(BooleanField,self).__init__(**kw)
class TextField(Field):
	def __init__(self,**kw):
		if not 'default' in kw:
			kw['default']=''
		if not 'ddl' in kw:
			kw['ddl']='text'
		super(TextField,self).__init__(**kw)

class BlobField(Field):
	def __init__(self,**kw):
		if not 'default' in kw:
			kw['default']=''
		if not 'ddl' in kw:
			kw['ddl']='blob'
		super(BlobField,self).__init__(**kw)

class VersionField(Field):
	def __init__(self,name=None):
		super(VersionField,self).__init__(name=name,default=0,ddl='bigint')

#frozenset是set的一种，但是和set的不同之处在于：frozenset不可以更改，我理解为类似于枚举类型
_triggers=frozenset(['pre_insert','pre_update','pre_delete'])

#该函数用于检查和管理一个对象（或者说一个数据库表格）的所有字段类型
def _gen_sql(table_name,mappings):
	pk=None
	sql=['-- generating SQL for %s:' % table_name,'create table \'%s\' (' % table_name]
	for f in sorted(mappings.values(),lambda x,y:cmp(x._order,y._order)):
		if not hasattr(f,'ddl'):#如果该字段类型没有类型名称，则报错
			raise StandardError('no ddl in field "%s".' % n)
		ddl=f.ddl
		nullable=f.nullable
		if f.primary_key:#该字段是主键
			pk=f.name	#pk记录主键的名称
		sql.append(nullable and ' \'%s\' %s,' % (f.name,ddl) or \
		' \'%s\' %s not null,' % (f.name,ddl))#类似于 age bigint not null
	sql.append(' primary key(\'%s\')' % pk)
	sql.append(');')
	return '\n'.join(sql)	#换行隔开


#最重要的元类
class ModelMetaclass(type):
	'''
	Metaclass,用于建立模型类和数据库字段类型的映射
	'''

	def __new__(cls,name,bases,attrs):

		#子类和父类Model都会调用metaclass，如果是Model调用的，则忽略以下步骤
		if name=='Model':
			return type.__new__(cls,name,bases,attrs)

		if not hasattr(cls,'subclasses'):
			cls.subclasses={}
		if not name in cls.subclasses:
			cls.subclasses[name]=name

		else:
			logging.warning('Redefine class:%s' % name)
		
		logging.info('Scan ORMaping %s...' % name);
		mappings=dict()
		primart_key=None
		for k,v in attrs.iteritems():
			if isinstance(v,Field):
				if not v.name:
					v.name=k
				logging.info('Found mapping:%s=>%s' % (k,v))

				if v.primary_key:
					if primary_key:
						raise TypeError('Cannot define more than 1 primary key in class:%s' % name)
					if v.updatable:
						logging.warning('NOTE:change primary key to non-updatable.')
					if v.nullable:
						logging.warning('NOTE:change primary key to not-nullable.')
					primary_key=v
				mappings[k]=v
		if not primary_key:
			raise TypeError('Primary key not defined in class %s.' % name)
		for k in mappings.iterkeys():
			attrs.pop(k)
		if not '__table__' in attrs:
			attrs['__table__']=name.lower()
		attrs['__mappings__']=mappings
		attrs['__primary_key__']=primary_key
		attrs['__sql__']=lambda self:_gen_sql(attrs['__table__'],mappings)
		for trigger in _trigger:
			if not trigger in attrs:
				attrs[trigger]=None
		return type.__new__(cls,name,bases,attrs)

class Model(dict):
	__metaclasd__=ModelMetaclass
	
	def __init__(self,**kw):
		super(Model,self).__init__(**kw)
	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Dict' object has no attribute '%s'" % key)
	def __setattr__(self,key,value):
		self[key]=value

	@classmethod
	def get(cls,pk):
		'''
		Get by primary key.
		'''
		d=db.select_one('select * from %s where %s=?' % (cls.__table__,cls.__primary_key__.name),pk)
		return cls(**d) if d else None

	@classmethod
	def find_first(cls,where,*args):
		d=db.select_one(r'select * from %s %s' % (cls.__table__,where),*args)
		return cls(**d) id d else None

	@classmethod
	def find_all(cls,*args):
		L=db.select('select * from %s' % cls.__table__)
		return [cls(**d) for d in L]

	@classmethod
	def fing_by(cls,where,*args):
		L=db.select('select * from %s %s' % (cls.__table__,where),*args)
		return [cls(**d) for d in L]

	@classmethod
	def couny_all(cls):
		return db.select_int('select count(\'%s\') from %s' % (cls.__primary_key__.name,cls.__table__))
		

	@classmethod
	def count_by(cls,where,*args):
		return db.select_int('select count(\'%s\') from %s %s' % (cls.__primary_key__.name,cls.__table__,where),*args)

	def update(self):
		self.pre_update and self.pre_update()
		L=[]
		args=[]
		for k,v in self.__mappings__.iteritems(():
			if v.updatable:
				if hasattr(self,k);
					args=getattr(self,k)
				else:
					arg=v.default
					setattr(self,k,arg)
				L.append('%s=?' % k)
				args.append(arg)
		pk=self.__primary_key__.name
		args.append(getattr(self,pk))
		db.update('update %s set %s where %s=?' % (self.__table__,','.join(L),pk),*args)
		return self

	def delete(self):
		self.pre_delete and self.pre_delete()
		pk=self.__primary_key__
		args=(getattr(self,pk),)
		db.update('delete from %s where %s=?' % (self.__table__,pk),×args)
		return self

	def insert(self):
		self.pre_insert and self.pre_insert()
		params={}
		for k,v in self.__mappings__.iteritems():
			if not hasattr(self,k):
				setattr(self,k,v.default)
			params[v.name]=getattr((self,k)
		db.insert('%s' % self.__table__,**params)
		return self


