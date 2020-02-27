#!/usr/bin/env python
# coding=utf-8
# Author: wangbo
# Mail: bowang@xiaojubianli.com
# Created Time: 2018-05-28 13:44:35

import sys
sys.path.append('../')
import traceback
import paramiko
from warnings import filterwarnings
from datetime import datetime, date, timedelta
from common.db_bi import DatabaseBI
from common.database2 import Database2
db2 = Database2()
def fetch_log(ip, cmd):
    logs = []
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip, 22, 'root', 'ctw6kDdiC>knpvsj5kge', timeout=5)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        for line in stdout.readlines():
            logs.append(line.strip())
    except:
        print '%s\tError' % (ip)
    return logs

def get_goods_log(day):
    data = []
    cmd = "cat /data/logs/goods-web/app.log.%s|grep submitRequest|sed s/\\\\[INFO\\ ]//g" % (
        day)
    logs1 = fetch_log('120.55.41.0', cmd)
    logs2 = fetch_log('47.97.122.83', cmd)
    for line in logs1 + logs2:
        try:
            cols = line.strip().split("|")
            add_time = line.strip().split(",")[0]
            module = cols[1]
            user_tel = cols[2] if cols[2] != 'null' else None
            user_id = cols[3] if cols[3] != 'null' else None
            shelf_sn = cols[4] if cols[4] != 'null' else None
            driver_id = int(cols[5]) if cols[5] != 'null' else None
            data.append((module, user_tel, user_id, shelf_sn, driver_id, add_time))
        except:
            print "parse log error:", line.strip()
            continue
    return data

def get_banner_log(day):
    data = []
    cmd = "cat /data/logs/banner-web/app.log.%s|grep submitRequest|sed s/\\\\[INFO\\ ]//g" % (
        day)
    logs1 = fetch_log('120.55.41.0', cmd)
    logs2 = fetch_log('47.97.122.83', cmd)
    for line in logs1 + logs2:
        try:
            cols = line.strip().split("|")
            add_time = line.strip().split(",")[0]
            module = cols[1]
            user_tel = cols[2] if cols[2] != 'null' else None
            user_id = cols[3] if cols[3] != 'null' else None
            shelf_sn = cols[4] if cols[4] != 'null' else None
            driver_id = int(cols[5]) if cols[5] != 'null' else None
            data.append((module, user_tel, user_id, shelf_sn, driver_id, add_time))
        except:
            print "parse log error:", line.strip()
            continue
    return data

def get_order_log(day):
    data = []
    cmd = "cat /data/logs/order-web//app.log.%s|grep submitRequest|sed s/\\\\[INFO\\ ]//g" % (
        day)
    logs1 = fetch_log('47.96.176.119', cmd)
    logs2 = fetch_log('121.43.167.71', cmd)
    for line in logs1 + logs2:
        try:
            cols = line.strip().split("|")
            add_time = line.strip().split(",")[0]
            module = cols[1]
            user_tel = cols[2] if cols[2] != 'null' else None
            user_id = cols[3] if cols[3] != 'null' else None
            shelf_sn = cols[4] if cols[4] != 'null' else None
            driver_id = int(cols[5]) if cols[5] != 'null' else None
            data.append((module, user_tel, user_id, shelf_sn, driver_id, add_time))
        except:
            print "parse log error:", line.strip()
            continue
    return data

# 维表管理
class Dimdata:
    def __init__(self):
        self._db = DatabaseBI()

    # 依赖order_info,一定要先执行order_info
    def order(self, day):
        sql1 = "delete from dim_mgo_order where date(add_time)='%s'" % day
        sql2 = ''' insert into dim_mgo_order
        select a.id,a.order_sn,a.coupon_seq,a.driver_id,
        a.driver_tel,c.city_sn,b.leasing_company_sn,d.work_type,
        a.user_id,a.user_tel,e.driver_tag,
        a.price,a.discount_amount,a.pay_amount,f.cost,
        a.num,a.`status`,a.pay_type,a.pay_order_sn,a.is_first,a.add_time,a.update_time
        from mgo_order a left join mgo_driver_info b on a.driver_id=b.driver_id
        left join mgo_city c on b.distric_sn=c.distric_sn
        left join mgo_driver_tag d on a.driver_id=d.driver_id
        left join mgo_user_tag e on a.user_tel=e.mobile
        left join (select order_sn,sum(cost_price) as cost
        from dim_mgo_order_info group by order_sn) f on a.order_sn=f.order_sn
        where date(a.add_time)='%s' ''' % day

        self._db.commit([sql1, sql2])

    def order_info(self, day):
        sql1 = "delete from dim_mgo_order_info where date(add_time)='%s'" % day
        sql2 = ''' insert into dim_mgo_order_info
        select a.id,a.order_sn,a.driver_id,a.driver_tel,c.city_sn,
        b.leasing_company_sn,d.work_type,
        a.user_tel,e.driver_tag,a.box_sn,a.sku_id,g.name,
        g.content,g.img,a.price,f.cost_price,a.status,
        a.add_time,a.update_time
        from mgo_order_info a left join mgo_driver_info b on a.driver_id=b.driver_id
        left join mgo_city c on b.distric_sn=c.distric_sn
        left join mgo_driver_tag d on a.driver_id=d.driver_id
        left join mgo_user_tag e on a.user_tel=e.mobile
        left join (select sku_id,sum(goods_purchase_price*goods_num) as cost_price
        from mgo_sku_goods where sku_id is not null
        and is_deleted=0 group by sku_id) f on a.sku_id=f.sku_id
        left join mgo_sku g on a.sku_id=g.id
        where date(a.add_time)='%s' ''' % (day)

        self._db.commit([sql1, sql2])

    def order_comment(self, day):
        sql = "delete from dim_mgo_order_comment where date(add_time)='%s'" % day
        self._db.commit([sql])

        # 评价标签定义
        tag_data = {}
        sql = "select id, tag from mgo_order_comment_tag"
        for item in self._db.fetchall(sql):
            id = str(item['id'])
            tag_data[id] = item['tag']

        comment_list = []
        sql = "select order_sn,appraise,tag_id,`comment`,add_time,update_time \
            from mgo_order_comment where date(add_time)='%s'" % day
        for item in self._db.fetchall(sql):
            sn = item['order_sn']
            appraise = int(item['appraise'])
            add_time = item['add_time']
            update_time = item['update_time']
            msg = item['comment'] if item['comment'] is not None else ''
            tags_text = item['tag_id'].strip(
            ) if item['tag_id'] is not None else ''
            if tags_text == '':
                comment_list.append(
                    (sn, appraise, 'null', msg, add_time, update_time))
                continue

            for tag in tags_text.split(','):
                comment_list.append((sn, appraise, tag_data.get(
                    tag, 'null'), msg, add_time, update_time))

        cursor = self._db.conn().cursor()
        cursor.executemany(
            'insert into dim_mgo_order_comment value(%s,%s,%s,%s,%s,%s)', comment_list)
        self._db.conn().commit()
        cursor.close()

    def scan_log(self, day):
        sql = "delete from dim_scan_log where date(add_time)='%s'" % day
        self._db.commit([sql])

        #goods
        data = get_goods_log(day)
        cursor = self._db.conn().cursor()
        cursor.executemany(
            'insert into dim_scan_log(module,user_tel,user_id,shelf_sn,driver_id,add_time) \
            value(%s,%s,%s,%s,%s,%s)', data)
        self._db.conn().commit()
        cursor.close()

        #banner
        data = get_banner_log(day)
        cursor = self._db.conn().cursor()
        cursor.executemany(
            'insert into dim_scan_log(module,user_tel,user_id,shelf_sn,driver_id,add_time) \
            value(%s,%s,%s,%s,%s,%s)', data)
        self._db.conn().commit()
        cursor.close()

        #order
        data = get_order_log(day)
        cursor = self._db.conn().cursor()
        cursor.executemany(
            'insert into dim_scan_log(module,user_tel,user_id,shelf_sn,driver_id,add_time) \
            value(%s,%s,%s,%s,%s,%s)', data)
        self._db.conn().commit()
        cursor.close()

    def statis(self, day):
        result = {}
        sql = "select leasing_company_sn,city_name,leasing_company_abbreviation from mgo_leasing_companies where is_deleted!=1"
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn] = {}
            result[company_sn]['city'] = item['city_name']
            result[company_sn]['company_name'] = item['leasing_company_abbreviation']

        #可接单司机数
        sql = '''SELECT leasing_company_sn,count(1) AS cnt
        FROM mgo_driver a left join mgo_driver_info b on a.id=b.driver_id
        WHERE a.is_checked = 1 AND a.shelf_sn IS NOT NULL AND a.shelf_sn!=''
        and date(checked_time) <= '%s'
        group by b.leasing_company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn]['online_driver_cnt'] = item['cnt']

        #订单售价
        sql = '''select company_sn,sum(price)/100.0 as amount
        FROM dim_mgo_order where status=1 and date(add_time)='%s'
        group by company_sn''' %(day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['origin_gmv'] = item['amount']

        #订单实付营业额
        sql = '''select company_sn,sum(pay_amount)/100.0 as amount
        FROM dim_mgo_order where status=1 and date(add_time)='%s'
        group by company_sn''' %(day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['gmv'] = item['amount']

        # 开单司机数
        sql = '''select company_sn,count(distinct driver_id) as cnt
        from dim_mgo_order where status=1 and date(add_time)='%s'
        group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['order_driver_cnt'] = item['cnt']
        # 营收订单司机数
        sql = '''select company_sn,count(distinct driver_id) as cnt
        from dim_mgo_order where status=1 and price>0 and date(add_time)='%s'
        group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['gmv_order_driver_cnt'] = item['cnt']
        # 免费订单司机数
        sql = '''select company_sn,count(distinct driver_id) as cnt
        from dim_mgo_order where status=1 and price=0 and date(add_time)='%s'
        group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['free_order_driver_cnt'] = item['cnt']

        # 购买用户数
        sql = '''select company_sn,count(distinct user_id) as cnt
        from dim_mgo_order where status=1 and date(add_time)='%s'
        group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['order_user_cnt'] = item['cnt']
        # 营收订单购买用户数
        sql = '''select company_sn,count(distinct user_id) as cnt
        from dim_mgo_order where status=1 and price>0 and date(add_time)='%s'
        group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['gmv_order_user_cnt'] = item['cnt']
        # 免费订单购买用户数
        sql = '''select company_sn,count(distinct user_id) as cnt
        from dim_mgo_order where status=1 and price=0 and date(add_time)='%s'
        group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['free_order_user_cnt'] = item['cnt']

        # 订单数
        sql = '''select company_sn,count(1) as cnt
        from dim_mgo_order where status=1 and date(add_time)='%s'
        group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['order_cnt'] = item['cnt']
        # 营收订单数
        sql = '''select company_sn,count(1) as cnt
        from dim_mgo_order where status=1 and date(add_time)='%s'
        and price>0 group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['gmv_order_cnt'] = item['cnt']
        # 免费订单数
        sql = '''select company_sn,count(1) as cnt
        from dim_mgo_order where status=1 and date(add_time)='%s'
        and price=0 group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['free_order_cnt'] = item['cnt']

        # 货品成本
        sql = '''select company_sn,sum(cost)/100.0 as amount
        from dim_mgo_order where status=1 and date(add_time)='%s'
        group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['cost'] = item['amount']
        # 营收订单货品成本
        sql = '''select company_sn,sum(cost)/100.0 as amount
        from dim_mgo_order where status=1 and date(add_time)='%s'
        and price>0 group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['gmv_order_cost'] = item['amount']
        # 免费订单货品成本
        sql = '''select company_sn,sum(cost)/100.0 as amount
        from dim_mgo_order where status=1 and date(add_time)='%s'
        and price=0 group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['free_order_cost'] = item['amount']

        # sku售卖数量
        sql = '''select company_sn,sum(num) as cnt
        from dim_mgo_order where status=1 and date(add_time)='%s'
        group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['sku_cnt'] = item['cnt']
        #营收订单sku售卖数量
        sql = '''select company_sn,sum(num) as cnt
        from dim_mgo_order where status=1 and date(add_time)='%s'
        and price>0 group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['gmv_sku_cnt'] = item['cnt']
        #免费订单sku售卖数量
        sql = '''select company_sn,sum(num) as cnt
        from dim_mgo_order where status=1 and date(add_time)='%s'
        and price=0 group by company_sn''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['company_sn']
            result[company_sn]['free_sku_cnt'] = item['cnt']

        #司机扫码-司机人数
        sql = '''SELECT c.leasing_company_sn,count(distinct a.shelf_sn) as cnt  FROM   mgo. mgo_driver_info c  
         left join mgo_driver_shelf e on c.driver_id = e.driver_id
         left join mgo.dim_scan_log  a  on a.shelf_sn = e.shelf_sn
         where  date(a.add_time) = '%s' and  a.module = 'home' group by c.leasing_company_sn;
         ''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn]['scan_drivers_cnt'] = item['cnt']

        # 司机扫码-UV
        sql = '''SELECT c.leasing_company_sn,count(distinct concat(a.shelf_sn,"-",a.user_id)) as cnt  FROM   mgo. mgo_driver_info c  
         left join mgo_driver_shelf e on c.driver_id = e.driver_id
         left join mgo.dim_scan_log  a  on a.shelf_sn = e.shelf_sn
         where  date(a.add_time) = '%s' and  a.module = 'home' group by c.leasing_company_sn;''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn]['scan_uv_cnt'] = item['cnt']

        #累计激活司机数
        sql = '''SELECT c.leasing_company_sn,count(1) as cnt  FROM   mgo.mgo_driver_info c
              left join mgo.mgo_driver a  on c.driver_id = a.id
              where date(checked_time) <= '%s'  group by c.leasing_company_sn;''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn]['active_alldriver_cnt'] = item['cnt']

        #当天激活司机数
        sql = '''SELECT c.leasing_company_sn,count(1) as cnt  FROM  mgo.mgo_driver_info c
              left join mgo.mgo_driver a  on c.driver_id = a.id
              where date(checked_time) = '%s'  group by c.leasing_company_sn;''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn]['active_dailydriver_cnt'] = item['cnt']

        #周活司机数
        sql = '''SELECT c.leasing_company_sn,count(distinct a.driver_id) as cnt  FROM  mgo.mgo_driver_info c  
              left join mgo.mgo_order a  on c.driver_id = a.driver_id
              where a.status = 1 and date(a.add_time) between date_sub(current_date(),interval 7 day) and '%s'  group by c.leasing_company_sn; ''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn]['weekwork_driver_cnt'] = item['cnt']

        #补货司机人次
        sql = '''SELECT c.leasing_company_sn,count(distinct a.driver_id) as cnt FROM    mgo. mgo_driver_info c 
              left join mgo.mgo_scm_replenishment_stream  a  on c.driver_id = a.driver_id
              where  date(a.add_time) = '%s' and  a.rep_type <> 2 group by c.leasing_company_sn;''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn]['replenishment_driver_cnt'] = item['cnt']

        # 补货订单数
        sql = '''SELECT c.leasing_company_sn,count( distinct a.rep_sn) as cnt FROM   mgo. mgo_driver_info c  
              left join mgo.mgo_scm_replenishment_stream  a  on c.driver_id = a.driver_id
              where  date(a.add_time) = '%s' and  a.rep_type <> 2 group by c.leasing_company_sn;''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn]['replenishment_order_cnt'] = item['cnt']

        # 补货评价数
        sql = ''' SELECT c.leasing_company_sn,count( distinct e.rep_sn)as cnt  FROM    mgo. mgo_driver_info c 
             left join mgo.mgo_scm_replenishment_stream  a  on c.driver_id = a.driver_id
             left join mgo.mgo_driver_replenishment_estimate e on a.rep_sn = e.rep_sn 
             where  date(e.add_time) = '%s' and  a.rep_type <> 2 and e.level >0  group by c.leasing_company_sn;''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn]['replenishment_comment_cnt'] = item['cnt']

        # 缺货司机数
        sql = ''' SELECT c.leasing_company_sn,count( a.driver_id) as cnt FROM  mgo. mgo_driver_info c 
            left join mgo.mgo_driver_tag  a  on c.driver_id = a.driver_id
            where a.driver_id in (select driver_id from mgo_driver_info where distric_sn in (select distric_sn from mgo_city where city_sn = 310100 )) and a.stock_warming = 1
            and c.leasing_company_sn like '%SH%'
            group by c.leasing_company_sn;''' % (day)
        for item in self._db.fetchall(sql):
            company_sn = item['leasing_company_sn']
            result[company_sn]['short_supply_cnt'] = item['cnt']


        #汇总统计结果
        sql = "delete from statis_metrics where day='%s'" %(day)
        self._db.commit([sql])

        record = []
        for key, value in result.iteritems():
            if value.get('online_driver_cnt', 0) == 0:
                continue

            record.append([day,key,value['company_name'],value['city'],
            value.get('online_driver_cnt', 0),
            value.get('origin_gmv', 0),
            value.get('gmv', 0),
            value.get('order_driver_cnt', 0),
            value.get('order_user_cnt', 0),
            value.get('order_cnt', 0),
            value.get('cost', 0),
            value.get('sku_cnt', 0),
            value.get('gmv_order_driver_cnt', 0),
            value.get('gmv_order_user_cnt', 0),
            value.get('gmv_order_cnt', 0),
            value.get('gmv_order_cost', 0),
            value.get('gmv_sku_cnt', 0),
            value.get('free_order_driver_cnt', 0),
            value.get('free_order_user_cnt', 0),
            value.get('free_order_cnt', 0),
            value.get('free_order_cost', 0),
            value.get('free_sku_cnt', 0),
            value.get('scan_drivers_cnt', 0),
            value.get('scan_uv_cnt', 0),
            value.get('active_alldriver_cnt', 0),
            value.get('active_dailydriver_cnt', 0),
            value.get('weekwork_driver_cnt', 0),
            value.get('replenishment_driver_cnt', 0),
            value.get('replenishment_order_cnt', 0),
            value.get('replenishment_comment_cnt', 0),
            value.get('short_supply_cnt', 0),
            ])

        cursor = self._db.conn().cursor()
        cursor.executemany(
            'insert into statis_metrics(day,company_sn,company,city,online_driver_cnt, \
            origin_gmv,gmv,order_driver_cnt,order_user_cnt,order_cnt,cost,sku_cnt, \
            gmv_order_driver_cnt,gmv_order_user_cnt,gmv_order_cnt,gmv_order_cost,gmv_sku_cnt, \
            free_order_driver_cnt,free_order_user_cnt,free_order_cnt,free_order_cost,free_sku_cnt,\
            scan_drivers_cnt,scan_uv_cnt,active_alldriver_cnt,active_dailydriver_cnt,weekwork_driver_cnt,\
           replenishment_driver_cnt, replenishment_order_cnt,replenishment_comment_cnt,short_supply_cnt) \
            value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', record)
        self._db.conn().commit()
        cursor.close()

if __name__ == '__main__':
    db = Dimdata()
    day = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    db.order_info(day)
    db.order(day)
    db.order_comment(day)
    db.statis(day)
    db.scan_log(day)
