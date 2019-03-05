#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

import MySQLdb  # mysqlclient fork from MySQLdb, fastest implementation, as it is C based
import pymysql
import mysql.connector

cfg = {'user': 'root', 'host': '127.0.0.1', 'database': 'sakila', 'password': 'tencent'}


def one_plus_one(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1+1")
    x = cur.fetchall()[0][0]
    assert x == 2
    cur.close()


def simple_select(conn):
    cur = conn.cursor()
    cur.execute("SELECT payment_id,customer_id,staff_id FROM payment where payment_id between 1 and 100 limit 100")
    rs = cur.fetchall()
    assert len(rs) == 100
    cur.close()


import timeit
import functools


def bench_one(client_name):
    connector = sys.argv[1]

    if connector == 'connector':
        con = mysql.connector.connect(**cfg)
    elif connector == 'pymysql':
        con = pymysql.connect(**cfg)
    elif connector == 'mysqlclient':
        cfg['db'] = cfg.pop('database')
        con = MySQLdb.connect(**cfg)
    else:
        sys.exit("connector, pymysql or mysqlclient")

    print("{}:{}".format(client_name, timeit.timeit(functools.partial(one_plus_one, con), number=10000)))


def bench_all():
    n = 10000

    # connector
    con = mysql.connector.connect(**cfg)
    print("{}:{}".format("connector", timeit.timeit(functools.partial(one_plus_one, con), number=n)))

    # pymysql
    con = mysql.connector.connect(**cfg)
    print("{}:{}".format("pymysql", timeit.timeit(functools.partial(one_plus_one, con), number=n)))

    # mysqlclient
    cfg['db'] = cfg.pop('database')
    con = MySQLdb.connect(**cfg)
    print("{}:{}".format("mysqlclient", timeit.timeit(functools.partial(one_plus_one, con), number=n)))


def bench_all2():
    n = 100

    # connector
    con = mysql.connector.connect(**cfg)
    print("{}:{}".format("connector", timeit.timeit(functools.partial(simple_select, con), number=n)))

    # pymysql
    con = mysql.connector.connect(**cfg)
    print("{}:{}".format("pymysql", timeit.timeit(functools.partial(simple_select, con), number=n)))

    # mysqlclient
    cfg['db'] = cfg.pop('database')
    con = MySQLdb.connect(**cfg)
    print("{}:{}".format("mysqlclient", timeit.timeit(functools.partial(simple_select, con), number=n)))


if __name__ == '__main__':
    # bench_all()
    bench_all2()
