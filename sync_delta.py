#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Write by yxb at 2015-09-08


import ConfigParser
import string, os, sys
import tempfile
import argparse
import MySQLdb
import encodings
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from pymysqlreplication.event import (
    RotateEvent,
    XidEvent,
)
encodings._aliases["utf8mb4"] = "utf_8"

#
# 全局参数 
#
MYSQL_SETTINGS   = {}
sourcedb_conf    = {}
targetdb_conf    = {}
sharerule_conf   = {}
masterinfo_conf  = {}
master_info_file = ""
serverid         = 1
sync_binlog      = 1 ##  暂时不要做这个判断，文件是强同步

auto_position    = False
binlogFilename   = ""
binlogPosition   = ""
binlog_info      = {}

#
# 解析命令行传值
#
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--conf", help="the confilger file.")
    args = parser.parse_args()

    return args
#
# 解析binlog文件名和位置配置
# 需要判断 master_info_file 文件是存在的
# 需要判断 sync_binlog 的值不为空，是int，且大于0
#
def parse_binlog_conf():
    ## master_info_file 存在，则解析文件中的内容
    if is_file_exist(masterinfo_conf["master_info_file"]):
        conf=ConfigParser.ConfigParser()
        conf.read(masterinfo_conf["master_info_file"])
        set_conf("Binlog_Info",  conf, binlog_info)


#
# 同步binlog的同步信息到本地文件, 写文件也遵守原子性
#
def sync_binlog_to_file(binlog, position):
    filename = masterinfo_conf["master_info_file"]
    strs  = "[Binlog_Info]" + "\n" 
    strs += "binlog = " + binlog + "\n" 
    strs += "position = " + str(position)
    with tempfile.NamedTemporaryFile('w', dir=os.path.dirname(filename), delete=False) as tf:
        tf.writelines(strs)
        tempname = tf.name
        os.rename(tempname, filename)
        
    tf.close()


#
# 解析配置文件，然后通过set_conf()函数，将配置信息存放到全局变量
# sourcedb_conf，targetdb_conf，sharerule_conf 
#
def parse_conf():
    args = parse_args()
    if is_file_exist(args.conf):
        conf=ConfigParser.ConfigParser()
        conf.read(args.conf)
        set_conf("SourceDB",       conf, sourcedb_conf)
        set_conf("TargetDB",       conf, targetdb_conf)
        set_conf("ShareRule",      conf, sharerule_conf)
        set_conf("MasterInfoFile", conf, masterinfo_conf)

#
# 将相应值存到全局变量sourcedb_conf，targetdb_conf，sharerule_conf
#
def set_conf(sections, conf, conf_dict):
    if conf.options(sections):
        for attr in conf.options(sections):
            conf_dict[attr] = conf.get(sections, attr)

def set_mysql_setting():
    MYSQL_SETTINGS["host"]   = sourcedb_conf["db_host"]
    MYSQL_SETTINGS["port"]   = int(sourcedb_conf["db_port"])
    MYSQL_SETTINGS["user"]   = sourcedb_conf["db_user"]
    MYSQL_SETTINGS["passwd"] = sourcedb_conf["db_pass"]
        
#
# 判断文件是否存在, 存在返回1，否则返回0
#
def is_file_exist(filename):
    isexist = 0
    if os.path.exists(filename):
        isexist = 1
    else:
        print "The confige file %s not exist" % filename
        os._exit(2)
    return isexist

def share_range(idx):
    return (int(idx) / int(sharerule_conf['shareval']))

def share_module(idx):
    return (int(idx) % int(sharerule_conf['shareval']))


def splitRule(sharerule, idx):
    if sharerule_conf['isshare'] == "NO":
        targetTableName = targetdb_conf['db_table_pre']
    elif sharerule_conf['isshare'] == "YES":
        if sharerule_conf['rule'] == "INTRANGE":
            targetTableName  = targetdb_conf['db_table_pre']
            targetTableName += str(share_range(idx))
        elif sharerule_conf['rule'] == "MODULE":
            targetTableName  = targetdb_conf['db_table_pre']
            targetTableName += str(share_module(idx))
        elif sharerule_conf['rule'] == "MD5":
            targetTableName  = targetdb_conf['db_table_pre']
            pre8str          = idx[0:8]
            str16toint       = int(pre8str, 16)
            targetTableName += str(share_module(str16toint))

    return targetTableName


def syncDelete(rows):
    #print("---- sync delete event ----")
    args = []
    targetTableName = splitRule(sharerule_conf['rule'], rows['values'][sharerule_conf['sharecolum']])
    rowsourcecolums = targetdb_conf['db_sync_colums'].split(',')
    rowtargetcolums = targetdb_conf['db_target_colums'].split(',')
    sql  = "DELETE FROM " + targetTableName + " WHERE  1=1 "
    for colx in rowtargetcolums:
        sql += " AND "
        sql += colx + '=%s'

    for col in rowsourcecolums:
        args.append(rows['values'][col.strip()])
    execSQL(sql,args)

def syncUpdate(rows):
    #print("---- sync update event ----")
    args   = []
    after  = []
    before = []
    targetTableName = splitRule(sharerule_conf['rule'], rows['before_values'][sharerule_conf['sharecolum']])
    rowsourcecolums = targetdb_conf['db_sync_colums'].split(',')
    rowtargetcolums = targetdb_conf['db_target_colums'].split(',')
    sql = "UPDATE " + targetTableName + " SET "
    i = 0 
    for colx in rowtargetcolums:
        i += 1
        if i < len(rowtargetcolums):
            sql += colx + "=%s,"
        else:
            sql += colx + "=%s"
    sql += " WHERE "

    j = 0
    for colx in rowtargetcolums:
        j += 1
        if j < len(rowtargetcolums):
            sql += colx + "=%s" + " AND "
        else:
            sql += colx + "=%s"

    for col in rowsourcecolums:
        after.append(rows['after_values'][col.strip()])

    for col in rowsourcecolums:
        before.append(rows['before_values'][col.strip()])

    args = after + before
    execSQL(sql, args)

def syncInsert(rows):
    #print("---- sync insert event ----")
    args = []
    targetTableName = splitRule(sharerule_conf['rule'], rows['values'][sharerule_conf['sharecolum']])
    rowcolums = targetdb_conf['db_sync_colums'].split(',')
    sql  = "INSERT INTO " + targetTableName + "("
    sql +=  targetdb_conf['db_target_colums']
    sql += ") VALUES ("
    i = 0
    for col in rowcolums:
        i += 1
        if i < len(rowcolums):
            sql += "%s,"
        else:
            sql += "%s)"
        args.append(rows['values'][col.strip()])

    execSQL(sql, args)

def execSQL(sql, args):
    try:
        conn = MySQLdb.connect(host=targetdb_conf['db_host'],
                               port=int(targetdb_conf['db_port']),
                               user=targetdb_conf['db_user'],
                               passwd=targetdb_conf['db_pass'],
                               db=targetdb_conf['db_dbname'],
                               charset=targetdb_conf['db_encoding'], 
                               connect_timeout=1000)
        conn.ping()
        curs = conn.cursor()
        curs.execute(sql, args)
        conn.commit()
        curs.close()
        conn.close()
    except MySQLdb.Error,e:
        print "MySQLdb Error",e
        sys.exit(2)

def main():
    ## 几个初始化工作
    parse_conf()
    set_mysql_setting()
    parse_binlog_conf()

    stream = BinLogStreamReader(
        connection_settings= MYSQL_SETTINGS,
        server_id          = serverid,
        resume_stream      = True,
        log_file           = binlog_info["binlog"],
        log_pos            = int(binlog_info["position"]),
        only_events        = [DeleteRowsEvent, 
                              WriteRowsEvent, 
                              UpdateRowsEvent,
                              RotateEvent,
                              XidEvent],
        blocking           = True)

    currentbinlogfilename = ""
    currentbinlogposition = ""
    for binlogevent in stream:
        if isinstance(binlogevent, RotateEvent):
            currentbinlogfilename = binlogevent.next_binlog
            currentbinlogposition = binlogevent.position
            print currentbinlogfilename, currentbinlogposition
            #sync_binlog_to_file(currentbinlogfilename,currentbinlogposition)
        elif isinstance(binlogevent, XidEvent):
            currentbinlogposition = binlogevent.packet.log_pos
            print currentbinlogfilename, currentbinlogposition
            #sync_binlog_to_file(currentbinlogfilename,currentbinlogposition)
        elif (binlogevent.schema == sourcedb_conf["db_dbname"] and 
                binlogevent.table == sourcedb_conf["db_table"]):
            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    syncDelete(row)
                elif isinstance(binlogevent, UpdateRowsEvent):
                    syncUpdate(row)
                elif isinstance(binlogevent, WriteRowsEvent):
                    syncInsert(row)
    stream.close()

if __name__ == "__main__":
    main()
