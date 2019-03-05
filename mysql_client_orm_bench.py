#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import timeit
import functools

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import Column, Integer, DateTime, SmallInteger, Boolean, DECIMAL, TIMESTAMP
from sqlalchemy.sql import func

cfg = {'user': 'user_00', 'host': '127.0.0.1', 'dbname': 'sakila', 'passwd': 'test123', 'charset': 'utf8', 'port': 3306}

# 使用mysql connector/python
connector_url = "mysql+mysqlconnector://{}:{}@{}:{}/{}?charset={}".format(cfg["user"],
                                                                          cfg["passwd"],
                                                                          cfg["host"],
                                                                          cfg["port"],
                                                                          cfg["dbname"],
                                                                          cfg["charset"])
# 使用pymysql
pymysql_url = "mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(cfg["user"],
                                                                 cfg["passwd"],
                                                                 cfg["host"],
                                                                 cfg["port"],
                                                                 cfg["dbname"],
                                                                 cfg["charset"])

# 使用mysqlclient
mysqlclient_url = "mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(cfg["user"],
                                                                     cfg["passwd"],
                                                                     cfg["host"],
                                                                     cfg["port"],
                                                                     cfg["dbname"],
                                                                     cfg["charset"])

Base = declarative_base()
DBSession = scoped_session(sessionmaker())
engin = None


class Payment(Base):
    __tablename__ = "payment"
    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8"}
    # 表结构
    payment_id = Column(SmallInteger, primary_key=True, autoincrement=True)
    customer_id = Column(SmallInteger, nullable=False)
    staff_id = Column(Boolean, nullable=False)
    rental_id = Column(Integer)
    amount = Column(DECIMAL, nullable=False)
    payment_date = Column(DateTime, nullable=False)
    last_update = Column(TIMESTAMP, server_default=func.now())  # 初始化当前时间


def init_sqlalchemy(url='sqlite:///sqlalchemy.db'):
    global engine
    engine = create_engine(url, echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=True, expire_on_commit=False)
    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def simple_select():
    res = DBSession.query(Payment.payment_id, Payment.customer_id).filter(Payment.payment_id.between(1, 100)).all()
    assert len(res) == 100
    DBSession.commit()


def raw_simple_select():
    res = engine.execute(
        "SELECT payment_id,customer_id,staff_id FROM payment where payment_id between 1 and 100 limit 100")
    assert res.rowcount == 100


def get_db_url(connector):
    url = None
    if connector == 'connector':
        url = connector_url
    elif connector == 'pymysql':
        url = pymysql_url
    elif connector == 'mysqlclient':
        url = mysqlclient_url

    return url


def bench_one(client_name, n, func=simple_select):
    connector = client_name

    url = get_db_url(connector)

    if url is None:
        sys.exit("connector, pymysql or mysqlclient")

    init_sqlalchemy(url)
    print("{}:{}".format(client_name, timeit.timeit(functools.partial(func), number=n)))


def bench_all():
    n = 100
    bench_one('connector', n)
    bench_one('pymysql', n)
    bench_one('mysqlclient', n)

    bench_one('connector', n, raw_simple_select)
    bench_one('pymysql', n, raw_simple_select)
    bench_one('mysqlclient', n, raw_simple_select)


if __name__ == '__main__':
    bench_all()
