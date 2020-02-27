# -*- coding: utf-8 -*-
"""
Created on Tue Dec 31 18:17:43 2019

@author: xucoo
"""

from sqlalchemy import create_engine
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import sys
import os
import requests
import json
import pymysql 
from urllib3 import request
from pyhive import hive

#建立hive连接
conn = hive.connect(host='mogo-002.hadoop', port=10000, database='mring',
                                   auth='KERBEROS', kerberos_service_name='hive')
#建立游标
cursor = conn.cursor()

#建立mgo库连接
engine=create_engine("mysql+pymysql://bi:VS6w@6wiv@rr-bp18237k924jt6e70bo.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format('mgo'))
#查询mgo库方法
def reader(query):
    sql=query
    df=pd.read_sql(sql,engine)
    return df

#建立mring库连接
engine1=create_engine("mysql+pymysql://bi:VS6w@6wiv@rr-bp18237k924jt6e70bo.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format('mring'))
#查询mring库方法
def reader_mring(query):
    sql=query
    df=pd.read_sql(sql,engine1)
    return df

#查询hive方法(完成数值float类型转换)
def get_sensor_info(sql):
    cursor.execute(sql)
    result = cursor.fetchall()
    df=pd.DataFrame(result)
    df=df.applymap(lambda x:'0' if x=='' else x)#将空字符转成字符'0'
    for column in df.columns:
        try:df[[column]]=df[[column]].astype('float')#将所有列的类型转成float
        except:continue
    return df

#查询hive指定date区间每日数据方法，可用于计算累计数据
def get_total_sensor(sql,start_date,end_date):
    df=pd.DataFrame()
    for day in pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        #从神策API返回数据
        sub_df=get_sensor_info(sql.format(test=test_dev_list,day=day))
        df=pd.concat([df,sub_df],ignore_index=True)
    df=df.sort_values('date').reset_index(drop=True)
    return df

#查询hive方法(字符串)
def get_sensor_info2(sql):
    cursor.execute(sql)
    result = cursor.fetchall()
    df=pd.DataFrame(result)
    df=df.applymap(lambda x:'0' if x=='' else x)
    return df

#查询mring库指定date区间每日数据方法，可用于计算累计数据
def get_mring_total_info(sql,start_date,end_date):
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        sub_df=reader_mring(sql.format(day,day,day,day,day))
        df=pd.concat([df,sub_df],ignore_index=True)
    return df

#设备信息表
sql0='''SELECT  a.third_code,b.owner_sn parent_id
    FROM  mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND  b.main_app IN ('com.mgo.gad','com.mgo.walle')
    AND b.desc NOT LIKE '%%测试%%'
    '''
df_agent=reader_mring(sql0)

#获取代理商日汇总计数，cnames：列名
def get_mring_agent_count_info(sql,cnames=[]):
    cursor.execute(sql)
    result = cursor.fetchall()
    df=pd.DataFrame(result)
    df.columns=cnames
    df=df.applymap(lambda x:'0' if x=='' else x)
    df=df.merge(df_agent,left_on='devicename',right_on='third_code',how='inner').drop('third_code',axis=1)
    L_group=['date','parent_id']
    df=df.groupby(L_group).devicename.count().reset_index()
    return df

#获取代理商日汇总加和，cnames：列名
def get_mring_agent_sum_info(sql,cnames=[]):
    cursor.execute(sql)
    result = cursor.fetchall()
    df=pd.DataFrame(result)
    df.columns=cnames
    df=df.applymap(lambda x:'0' if x=='' else x)
    df=df.merge(df_agent,left_on='devicename',right_on='third_code',how='inner').drop('third_code',axis=1)
    L_group=['date','parent_id']
    L_sum=[column for column in df.columns if column not in ['date','parent_id','devicename']]
    df[L_sum]=df[L_sum].astype('float')
    df=df.groupby(L_group).sum()[L_sum].reset_index()
    df=df.sort_values('date').reset_index(drop=True)
    return df
#获取代理商date区间内每日数据计数，可用于计算累计数据
def get_total_sensor_agent_count_info(sql,start_date,end_date,cnames=[]):
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        #从神策API返回数据
        sub_df=get_mring_agent_count_info(sql.format(day=day,test=test_dev_list),cnames)
        df=pd.concat([df,sub_df],ignore_index=True)
    df=df.sort_values('date').reset_index(drop=True)
    return df
#获取代理商date区间内每日数据加和，可用于计算累计数据
def get_total_sensor_agent_sum_info(sql,start_date,end_date,cnames):
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        #从神策API返回数据
        sub_df=get_mring_agent_sum_info(sql.format(day=day,test=test_dev_list),cnames)
        df=pd.concat([df,sub_df],ignore_index=True)
    df=df.sort_values('date').reset_index(drop=True)
    return df
#测试设备列表
test_dev_list=''' '1d5559d3cb1dc2ad24051519cf546a18',
'5fbe08d956f7abed75f2e0773e628ccf',
'a85293469e50afddb55c9bbcdfccb566',
'64261dc5ff5e1ffe5f8b3a296bc7ef12',
'3dcbbcbefbc3b4565ea491268105ae77',
'3d288ed8ced8010349f2e68a7d9e7563',
'79e1a35bc7a5191e413d8ffa26dcfbaf',
'a40357a62156df25938d10aa8056138e',
'44f748d3fb8ae6f6dd908bc02361c00b',
'973a857ab199ba24160739e053244476',
'6890aa362c0fbb55dd34ab44d442bb33',
'29cfe8445736a92a135dfc24de8f2ecf',
'fe9deacee9a4422b23fb27847a69442d',
'a184f8ee18fa537b8c16e669a0176c65',
'ef0f1af97ebf957245ccc0ff4f7e103b',
'5fe096954887b19b04c599a18322f0bf',
'61630dbceea975e8e515c03b5470cdf4',
'98bd1611cb7d55b2ff1f644f50a22b90',
'ef8485b1f33a5c35cc3d38bb9a0d5c8e',
'375a71e781b0f1cde8c806dc637bc476',
'8aefa608f9d61bd8aa243d67cf0fe687',
'97f9865b95ef1081dada65256d7c4c92',
'9429d9629e6ac2c835e17fa2024742d7',
'1ab4774ca4a5df0812d743f61d3a98dd',
'4d3bdefb14c9ab044bef0050a66400f1',
'86f977932ecd06e32ab192c924a724a3',
'954f771399af715a3698d48b6aeda6d1',
'd6ff8958367644d5fd299be2b39be79f' '''
#设备城市车型信息表
sql0='''SELECT  a.third_code,c.city_sn,c.car_type
    FROM  mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND  b.main_app IN ('com.mgo.gad','com.mgo.walle')
    AND b.desc NOT LIKE '%%测试%%'
    '''
df_city=reader_mring(sql0)
#计算按城市、车型分类的每日数据加和
def get_sensor_info_mgo_sum(sql,cnames=[]):
    cursor.execute(sql)
    result = cursor.fetchall()
    df=pd.DataFrame(result)
    df.columns=cnames
    df=df.applymap(lambda x:'0' if x=='' else x)
    df=df.merge(df_city,left_on='devicename',right_on='third_code',how='inner').drop('third_code',axis=1)
    L_group=['date','city_sn','car_type']
    L_sum=[column for column in df.columns if column not in ['date','city_sn','car_type','devicename']]
    df[L_sum]=df[L_sum].astype('float')
    df=df.groupby(L_group).sum()[L_sum].reset_index()
    df=df.sort_values('date').reset_index(drop=True)
    return df
#计算按城市、车型分类的每日数据计数
def get_sensor_info_mgo_count(sql,cnames=[]):
    cursor.execute(sql)
    result = cursor.fetchall()
    df=pd.DataFrame(result)
    df.columns=cnames
    df=df.applymap(lambda x:'0' if x=='' else x)
    df=df.merge(df_city,left_on='devicename',right_on='third_code',how='inner').drop('third_code',axis=1).\
    groupby(['date','city_sn','car_type']).count().reset_index()
    return df
#计算按城市、车型分类的指定区间内每日数据加和，可用于计算累计数据
def get_total_sensor_mgo_sum(sql,start_date,end_date,cnames=[]):
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        #从神策API返回数据
        sub_df=get_sensor_info_mgo_sum(sql.format(test=test_dev_list,day=day),cnames)
        df=pd.concat([df,sub_df],ignore_index=True)
    return df
#计算按城市、车型分类的指定区间内每日数据计数，可用于计算累计数据
def get_total_sensor_mgo_count(sql,start_date,end_date,cnames=[]):
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        #从神策API返回数据
        sub_df=get_sensor_info_mgo_count(sql.format(test=test_dev_list,day=day),cnames)
        df=pd.concat([df,sub_df],ignore_index=True)
    df=df.sort_values('date').reset_index(drop=True)
    return df
#根据聚合需求，在L=[]里输入聚合字段，汇总算出聚合后求和数据
def get_sensor_info_mgo_group_sum(sql,L=[],cnames=[]):
    cursor.execute(sql)
    result = cursor.fetchall()
    df=pd.DataFrame(result)
    df.columns=cnames
    df=df.applymap(lambda x:'0' if x=='' else x)
    df=df.merge(df_city,left_on='devicename',right_on='third_code',how='inner').drop('third_code',axis=1)
    L_sum=[column for column in df.columns if column not in (L+['devicename'])]
    for column in L_sum:
        try:df[[column]]=df[[column]].astype('float')
        except:continue
    #df[L_sum]=df[L_sum].astype('float')
    df=df.groupby(L).sum()[L_sum].reset_index()
    df=df.sort_values('date').reset_index(drop=True)
    return df
#根据聚合需求，在L=[]里输入聚合字段，汇总算出聚合数据后指定日期区间按日加和(可计算累计数据)
def get_total_sensor_mgo_group_sum(sql,L,start_date,end_date,cnames):
    df=pd.DataFrame()
    for day in  pd.date_range(start=start_date,end=end_date,freq='1D'):
        day=day.date()
        #从神策API返回数据
        sub_df=get_sensor_info_mgo_group_sum(sql.format(test=test_dev_list,day=day),L,cnames)
        df=pd.concat([df,sub_df],ignore_index=True)
    df=df.sort_values('date').reset_index(drop=True)
    return df
#设备类型列表
sql01='''SELECT  a.third_code,CASE WHEN b.desc LIKE '%%弗朗%%' THEN '弗朗' ELSE 'ELSE' END `type`
    FROM  mgo.mgo_driver_third_service a
    left join mgo.mgo_screen_info b on a.third_code=b.device_name
    LEFT JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND  (b.main_app='com.mgo.gad')
    AND b.desc NOT LIKE '%%测试%%' '''
df_type=reader(sql01)
#设备厂商聚合数据
def get_sensor_info_type_group_sum(sql,L=[],cnames=[]):
    cursor.execute(sql)
    result = cursor.fetchall()
    df=pd.DataFrame(result)
    df.columns=cnames
    df=df.applymap(lambda x:'0' if x=='' else x)
    df=df.merge(df_type,left_on='devicename',right_on='third_code',how='inner').drop('third_code',axis=1)
    L_sum=[column for column in df.columns if column not in (L+['devicename'])]
    for column in L_sum:
        try:df[[column]]=df[[column]].astype('float')
        except:continue
    #df[L_sum]=df[L_sum].astype('float')
    df=df.groupby(L).sum()[L_sum].reset_index()
    df=df.sort_values('date').reset_index(drop=True)
    return df
#列重命名，防止返回未空，自动建立默认值为0的df
def col_rename(df,L):
    try:
        df.columns=L
    except:
        interval_days=int((date.today()-timedelta(days=1)-recent_day)/timedelta(days=1))
        df=pd.DataFrame({'date':pd.date_range(start=recent_day+timedelta(days=1),end=date.today()-timedelta(days=1))})
        df['date']=df.date.apply(lambda x:x.strftime("%Y-%m-%d"))
        for x in [x for x in L if x!='date']:
            df[x]=[0]*interval_days
    return df

## 广告&内容每日数据(行程内)

#建立写库连接
con0 = pymysql.connect(host='rm-bp1d08monqz6q4khipo.mysql.rds.aliyuncs.com',user='bi',
                      passwd='VS6w@6wiv',charset='utf8')
cur0 = con0.cursor()#获取光标
cur0.execute('use mring')#使用数据库

#日播放次数播放行程数
sql0='SELECT MAX(day) FROM video_sensor_daily'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,8,19)
sql6='''SELECT p_date `date`,contentId,tag,COUNT(DISTINCT traceId) play_trip_cnt,COUNT(1) play_cnt
FROM mring.mojo_ord_event 
WHERE (tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','video_banner_click','click_video_detail','video_banner_show')
OR (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%'))
AND env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date,contentId,tag'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#日播放设备数&时间
sql7='''SELECT p_date `date`,contentId,tag,COUNT(DISTINCT devicename) play_dev_cnt,SUM(playTime) play_time
FROM mring.mojo_ord_event 
WHERE env='prod'
AND tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','SCREEN_OTHER_RECOVER_END',
'video_banner_click','click_video_detail','video_banner_show')
AND p_date BETWEEN '{}' AND '{}'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND devicename NOT IN ({})
GROUP BY p_date,contentId,tag'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#日扫码次数
sql8='''SELECT p_date `date`,CASE WHEN utmcontent LIKE '%http://l.zuzuche.com/sBLPP%' THEN '3897f62797cc8926e2bfc0280949b28b' 
  WHEN utmcontent='https://h5.mobilemart.cn/screenMarketing/pengCar' THEN '9300b8665e10f25cc6c4201473d7c8a6'
 WHEN utmcontent='https://h5.mobilemart.cn/screenMarketing/NJBank' THEN '38c14707cf33c08f0e213310342b1c2c'
 WHEN utmcontent='http://t.cn/AiY3hTFc' THEN '1f194067f49862b087238381cad0fa07'
 WHEN utmcontent='http://t.cn/AiT9t99D' THEN 'b83f67867c4d0324e3f5cda04c7a7318' 
 WHEN utmcontent='http://t.cn/AiQYGkPG' THEN '13e9ceb5b606d87cf1ce266e1e694660'
 WHEN utmcontent='https://dwz.cn/fSLEoGZr' THEN '635ada9d3102a1a7ac7d5c63818d9dfc'
 WHEN utmcontent='https://dwz.cn/yCSk977F' THEN '643356f5876623b6d13a87b6916f0cbb'
 WHEN utmcontent='https://dwz.cn/ZFBaiNFH' THEN '881cd15c414ceb26506a857f9b4d0b0d'
 WHEN utmcontent IN ('https://dwz.cn/VFm7cWbK','http://h5.mobilemart.cn/screenMarketing/creditCard') THEN 'd24873d3f4f2aa352b71f151870c0d23'
 WHEN utmSource='splash_hz_zc' AND utmCampaign='hgyj'  THEN 'e83611ec7a0556351e2e9548cc741d2d'
 WHEN utmSource='splash_hz_kc' AND utmCampaign='hgyj'  THEN 'd4398ebad582e9ce7cc06c39f9fbcf17'
 WHEN utmSource='stream_hz_zc' AND utmCampaign='hgyj'  THEN '21bc96c9691539b5ea6fee66cf2919db'
 WHEN utmSource='stream_hz_kc' AND utmCampaign='hgyj'  THEN '400d6d636f3b66cd2c47e75641756bf5'
 WHEN utmSource='splash_sh_kc' AND utmCampaign='hgyj'  THEN '7af074ceb499fb6dd2074e43d7ae5770'
 WHEN utmSource='stream_sh_kc' AND utmCampaign='hgyj'  THEN 'e1bf5dfe25705c78e62dcef9a63a83bd'
 WHEN utmSource='splash_hz_zc' AND utmCampaign='ttjianshen'  THEN 'd308656e6b8d07338f46735e1f520184'
 WHEN utmSource='splash_hz_kc' AND utmCampaign='ttjianshen'  THEN '5d14aee1e31f9afa98fc0cabde7b3dcf'
 WHEN utmSource='stream_hz_zc' AND utmCampaign='ttjianshen'  THEN '1c89145b29e80e84aeb8876ea9c62c3c'
 WHEN utmSource='stream_hz_kc' AND utmCampaign='ttjianshen'  THEN 'd1815729522b865d324b11c1696213cd'
 WHEN utmSource='splash_sh_kc' AND utmCampaign='ttjianshen'  THEN '0652c2d252db49bf0d6e536d769b9a8f'
 WHEN utmSource='stream_sh_kc' AND utmCampaign='ttjianshen'  THEN 'e18bec5d6b895f7f668716f56c88f488'
 WHEN utmSource='ad_soft' AND utmCampaign='shangde'  THEN '27eb5c84dd90c081209a0b571c604904'
 WHEN utmSource='stream_hz' AND utmCampaign='shangde'  THEN 'eaeae415463d5228ffe023fb268c217b'
 WHEN utmSource='stream_sh' AND utmCampaign='shangde'  THEN '9764e79195d7b8b6e880cb63941f3366'
 WHEN utmSource='screen' AND utmCampaign='didi'  THEN 'a38fd735bdfd261d01cb438abbc8d702'
 WHEN utmSource='splash' AND utmCampaign='zhongyuyt'  THEN '54ff97b673b5c88a96973347972b9e72'
 WHEN utmSource='stream' AND utmCampaign='zhongyuyt'  THEN 'bc0efff4dcecd4f86823fae1ea32aba3'
 WHEN utmSource='content' AND utmCampaign='zhongyuyt'  THEN '9ef228575085a99e2d4446103145ab5a'
 WHEN utmSource='detail' AND utmCampaign='yuezi-center'  THEN '2daa6c001402f6214c8d888944bf6508'
 WHEN utmSource='banner' AND utmCampaign='yuezi-center'  THEN '835a52faee86f9743d582f3dc86eb4a2'
 WHEN utmSource='banner' AND utmCampaign='hmxs'  THEN '27f2133c51a82c9822433d7e809625ff'
 WHEN utmSource='screen' AND utmCampaign='vipkid'  THEN 'fc76ec31d3fe1a3c6856e6bf90d00d17'
 WHEN utmSource='banner' AND utmCampaign='vipkid'  THEN '3431b25f85003ad88c4363dbcb3461ec'
 WHEN utmSource='screen' AND utmCampaign='hongqiaopinhui'  THEN 'e3244510b4db03b0480cdf5f21e706ee'
 WHEN utmSource='screen' AND utmCampaign='pluryal'  THEN 'b2bb8a1f8b0716c45df22a4ed26f4d71'
 ELSE 'else ' END video,COUNT(1) scan_cnt FROM mring.mojo_ord_event
WHERE event='SCAN'
AND p_date BETWEEN '{}' AND '{}'
AND env='prod'
-- AND versioncode>=179
AND devicename NOT IN ({})
GROUP BY 
p_date,CASE WHEN utmcontent LIKE '%http://l.zuzuche.com/sBLPP%' THEN '3897f62797cc8926e2bfc0280949b28b' 
  WHEN utmcontent='https://h5.mobilemart.cn/screenMarketing/pengCar' THEN '9300b8665e10f25cc6c4201473d7c8a6'
 WHEN utmcontent='https://h5.mobilemart.cn/screenMarketing/NJBank' THEN '38c14707cf33c08f0e213310342b1c2c'
 WHEN utmcontent='http://t.cn/AiY3hTFc' THEN '1f194067f49862b087238381cad0fa07'
 WHEN utmcontent='http://t.cn/AiT9t99D' THEN 'b83f67867c4d0324e3f5cda04c7a7318' 
 WHEN utmcontent='http://t.cn/AiQYGkPG' THEN '13e9ceb5b606d87cf1ce266e1e694660'
 WHEN utmcontent='https://dwz.cn/fSLEoGZr' THEN '635ada9d3102a1a7ac7d5c63818d9dfc'
 WHEN utmcontent='https://dwz.cn/yCSk977F' THEN '643356f5876623b6d13a87b6916f0cbb'
 WHEN utmcontent='https://dwz.cn/ZFBaiNFH' THEN '881cd15c414ceb26506a857f9b4d0b0d'
 WHEN utmcontent IN ('https://dwz.cn/VFm7cWbK','http://h5.mobilemart.cn/screenMarketing/creditCard') THEN 'd24873d3f4f2aa352b71f151870c0d23'
 WHEN utmSource='splash_hz_zc' AND utmCampaign='hgyj'  THEN 'e83611ec7a0556351e2e9548cc741d2d'
 WHEN utmSource='splash_hz_kc' AND utmCampaign='hgyj'  THEN 'd4398ebad582e9ce7cc06c39f9fbcf17'
 WHEN utmSource='stream_hz_zc' AND utmCampaign='hgyj'  THEN '21bc96c9691539b5ea6fee66cf2919db'
 WHEN utmSource='stream_hz_kc' AND utmCampaign='hgyj'  THEN '400d6d636f3b66cd2c47e75641756bf5'
 WHEN utmSource='splash_sh_kc' AND utmCampaign='hgyj'  THEN '7af074ceb499fb6dd2074e43d7ae5770'
 WHEN utmSource='stream_sh_kc' AND utmCampaign='hgyj'  THEN 'e1bf5dfe25705c78e62dcef9a63a83bd'
 WHEN utmSource='splash_hz_zc' AND utmCampaign='ttjianshen'  THEN 'd308656e6b8d07338f46735e1f520184'
 WHEN utmSource='splash_hz_kc' AND utmCampaign='ttjianshen'  THEN '5d14aee1e31f9afa98fc0cabde7b3dcf'
 WHEN utmSource='stream_hz_zc' AND utmCampaign='ttjianshen'  THEN '1c89145b29e80e84aeb8876ea9c62c3c'
 WHEN utmSource='stream_hz_kc' AND utmCampaign='ttjianshen'  THEN 'd1815729522b865d324b11c1696213cd'
 WHEN utmSource='splash_sh_kc' AND utmCampaign='ttjianshen'  THEN '0652c2d252db49bf0d6e536d769b9a8f'
 WHEN utmSource='stream_sh_kc' AND utmCampaign='ttjianshen'  THEN 'e18bec5d6b895f7f668716f56c88f488'
 WHEN utmSource='ad_soft' AND utmCampaign='shangde'  THEN '27eb5c84dd90c081209a0b571c604904'
 WHEN utmSource='stream_hz' AND utmCampaign='shangde'  THEN 'eaeae415463d5228ffe023fb268c217b'
 WHEN utmSource='stream_sh' AND utmCampaign='shangde'  THEN '9764e79195d7b8b6e880cb63941f3366'
 WHEN utmSource='screen' AND utmCampaign='didi'  THEN 'a38fd735bdfd261d01cb438abbc8d702'
 WHEN utmSource='splash' AND utmCampaign='zhongyuyt'  THEN '54ff97b673b5c88a96973347972b9e72'
 WHEN utmSource='stream' AND utmCampaign='zhongyuyt'  THEN 'bc0efff4dcecd4f86823fae1ea32aba3'
 WHEN utmSource='content' AND utmCampaign='zhongyuyt'  THEN '9ef228575085a99e2d4446103145ab5a'
 WHEN utmSource='detail' AND utmCampaign='yuezi-center'  THEN '2daa6c001402f6214c8d888944bf6508'
 WHEN utmSource='banner' AND utmCampaign='yuezi-center'  THEN '835a52faee86f9743d582f3dc86eb4a2'
 WHEN utmSource='banner' AND utmCampaign='hmxs'  THEN '27f2133c51a82c9822433d7e809625ff'
 WHEN utmSource='screen' AND utmCampaign='vipkid'  THEN 'fc76ec31d3fe1a3c6856e6bf90d00d17'
 WHEN utmSource='banner' AND utmCampaign='vipkid'  THEN '3431b25f85003ad88c4363dbcb3461ec'
 WHEN utmSource='screen' AND utmCampaign='hongqiaopinhui'  THEN 'e3244510b4db03b0480cdf5f21e706ee'
 WHEN utmSource='screen' AND utmCampaign='pluryal'  THEN 'b2bb8a1f8b0716c45df22a4ed26f4d71'
 ELSE 'else ' END'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#累计播放次数点位数
sql9='''SELECT '{day}' `date`,contentId,tag,COUNT(DISTINCT devicename) total_play_dev_cnt,COUNT(1) total_play_cnt,
SUM(playTime) total_play_time FROM mring.mojo_ord_event
WHERE env='prod'
AND (tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','video_banner_click','click_video_detail')
OR (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%'))
AND p_date BETWEEN '2019-05-11' AND '{day}'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND devicename NOT IN ({test})
GROUP BY contentId,tag'''
#累计扫码次数
sql10='''SELECT '{day}' `date`,CASE WHEN utmcontent LIKE '%http://l.zuzuche.com/sBLPP%' THEN '3897f62797cc8926e2bfc0280949b28b' 
  WHEN utmcontent='https://h5.mobilemart.cn/screenMarketing/pengCar' THEN '9300b8665e10f25cc6c4201473d7c8a6'
 WHEN utmcontent='https://h5.mobilemart.cn/screenMarketing/NJBank' THEN '38c14707cf33c08f0e213310342b1c2c'
 WHEN utmcontent='http://t.cn/AiY3hTFc' THEN '1f194067f49862b087238381cad0fa07'
 WHEN utmcontent='http://t.cn/AiT9t99D' THEN 'b83f67867c4d0324e3f5cda04c7a7318' 
 WHEN utmcontent='https://dwz.cn/fSLEoGZr' THEN '635ada9d3102a1a7ac7d5c63818d9dfc'
 WHEN utmcontent='https://dwz.cn/yCSk977F' THEN '643356f5876623b6d13a87b6916f0cbb'
 WHEN utmcontent='https://dwz.cn/ZFBaiNFH' THEN '881cd15c414ceb26506a857f9b4d0b0d'
 WHEN utmcontent IN ('https://dwz.cn/VFm7cWbK','http://h5.mobilemart.cn/screenMarketing/creditCard') THEN 'd24873d3f4f2aa352b71f151870c0d23'
 WHEN utmSource='splash_hz_zc' AND utmCampaign='hgyj'  THEN 'e83611ec7a0556351e2e9548cc741d2d'
 WHEN utmSource='splash_hz_kc' AND utmCampaign='hgyj'  THEN 'd4398ebad582e9ce7cc06c39f9fbcf17'
 WHEN utmSource='stream_hz_zc' AND utmCampaign='hgyj'  THEN '21bc96c9691539b5ea6fee66cf2919db'
 WHEN utmSource='stream_hz_kc' AND utmCampaign='hgyj'  THEN '400d6d636f3b66cd2c47e75641756bf5'
 WHEN utmSource='splash_sh_kc' AND utmCampaign='hgyj'  THEN '7af074ceb499fb6dd2074e43d7ae5770'
 WHEN utmSource='stream_sh_kc' AND utmCampaign='hgyj'  THEN 'e1bf5dfe25705c78e62dcef9a63a83bd'
 WHEN utmSource='splash_hz_zc' AND utmCampaign='ttjianshen'  THEN 'd308656e6b8d07338f46735e1f520184'
 WHEN utmSource='splash_hz_kc' AND utmCampaign='ttjianshen'  THEN '5d14aee1e31f9afa98fc0cabde7b3dcf'
 WHEN utmSource='stream_hz_zc' AND utmCampaign='ttjianshen'  THEN '1c89145b29e80e84aeb8876ea9c62c3c'
 WHEN utmSource='stream_hz_kc' AND utmCampaign='ttjianshen'  THEN 'd1815729522b865d324b11c1696213cd'
 WHEN utmSource='splash_sh_kc' AND utmCampaign='ttjianshen'  THEN '0652c2d252db49bf0d6e536d769b9a8f'
 WHEN utmSource='stream_sh_kc' AND utmCampaign='ttjianshen'  THEN 'e18bec5d6b895f7f668716f56c88f488'
 WHEN utmSource='ad_soft' AND utmCampaign='shangde'  THEN '27eb5c84dd90c081209a0b571c604904'
 WHEN utmSource='stream_hz' AND utmCampaign='shangde'  THEN 'eaeae415463d5228ffe023fb268c217b'
 WHEN utmSource='stream_sh' AND utmCampaign='shangde'  THEN '9764e79195d7b8b6e880cb63941f3366'
 WHEN utmSource='screen' AND utmCampaign='didi'  THEN 'a38fd735bdfd261d01cb438abbc8d702'
 WHEN utmSource='splash' AND utmCampaign='zhongyuyt'  THEN '54ff97b673b5c88a96973347972b9e72'
 WHEN utmSource='stream' AND utmCampaign='zhongyuyt'  THEN 'bc0efff4dcecd4f86823fae1ea32aba3'
 WHEN utmSource='content' AND utmCampaign='zhongyuyt'  THEN '9ef228575085a99e2d4446103145ab5a'
 WHEN utmSource='detail' AND utmCampaign='yuezi-center'  THEN '2daa6c001402f6214c8d888944bf6508'
 WHEN utmSource='banner' AND utmCampaign='yuezi-center'  THEN '835a52faee86f9743d582f3dc86eb4a2'
 WHEN utmSource='banner' AND utmCampaign='hmxs'  THEN '27f2133c51a82c9822433d7e809625ff'
 WHEN utmSource='screen' AND utmCampaign='vipkid'  THEN 'fc76ec31d3fe1a3c6856e6bf90d00d17'
 WHEN utmSource='banner' AND utmCampaign='vipkid'  THEN '3431b25f85003ad88c4363dbcb3461ec'
 WHEN utmSource='screen' AND utmCampaign='hongqiaopinhui'  THEN 'e3244510b4db03b0480cdf5f21e706ee'
 WHEN utmSource='screen' AND utmCampaign='pluryal'  THEN 'b2bb8a1f8b0716c45df22a4ed26f4d71'
 ELSE 'else ' END video,COUNT(1) total_scan_cnt FROM mring.mojo_ord_event 
WHERE event='SCAN'
AND p_date BETWEEN '2019-05-11' AND '{day}'
AND env='prod'
-- AND versioncode>=179
AND devicename NOT IN ({test})
GROUP BY 
video'''
sql01='''insert into video_sensor_daily values('{}','{}','{}',{},{},{},{},0,0,0,{},0)'''
df6=get_sensor_info(sql6)
df6.columns=['date','contentid','tag','play_trip_cnt','play_cnt']
df7=get_sensor_info(sql7)
df7.columns=['date','contentid','tag','play_dev_cnt','play_time']
df8=get_sensor_info(sql8)
df8.columns=['date','video','scan_cnt']
#df9=get_total_sensor(sql9,recent_day+timedelta(days=1),date.today()-timedelta(days=1))
#df10=get_total_sensor(sql10,recent_day+timedelta(days=1),date.today()-timedelta(days=1))
df_video=df6.merge(df7,how='left',on=['date','contentid','tag']).merge(df8,how='left',left_on=['date','contentid'],right_on=['date','video']).\
drop('video',axis=1).fillna(0)
df_video=df_video[['date','contentid','tag','play_dev_cnt','play_trip_cnt','play_cnt','scan_cnt','play_time']]

for i in range(0,len(df_video)):
    L=[]
    for j in range(0,len(df_video.iloc[0])):
        L.append(df_video.iloc[i][j])
    cur0.execute(sql01.format(*L))
    if i/100==0:
        con0.commit()
    else:continue
con0.commit()

## 单广告每日数据(行程内，分城市车型)
#日播放次数播放行程数
sql0='SELECT MAX(day) FROM video_ad_sensor_daily'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,11,10)
sql6='''SELECT p_date `date`,devicename,contentId,tag,COUNT(DISTINCT traceId) play_trip_cnt,COUNT(1) play_cnt
FROM mring.mojo_ord_event 
WHERE (tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','video_banner_click','click_video_detail','video_banner_show')
OR (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%'))
AND env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND adSn!=''
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date,devicename,contentId,tag'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#日播放设备数&时间
sql7='''SELECT p_date `date`,devicename,contentId,tag,COUNT(DISTINCT devicename) play_dev_cnt,SUM(playTime) play_time
FROM mring.mojo_ord_event
WHERE env='prod'
AND tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','SCREEN_OTHER_RECOVER_END',
'video_banner_click','click_video_detail','video_banner_show')
AND p_date BETWEEN '{}' AND '{}'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND adSn!=''
AND devicename NOT IN ({})
GROUP BY p_date,devicename,contentId,tag'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#累计播放行程数、播放次数点位数
sql8='''SELECT '{day}' `date`,devicename,contentId,tag,COUNT(DISTINCT traceId) total_play_trip_cnt,
COUNT(DISTINCT devicename) total_play_dev_cnt,COUNT(1) total_play_cnt,SUM(playTime) total_play_time
FROM mring.mojo_ord_event 
WHERE env='prod'
AND (tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','video_banner_click','click_video_detail')
OR (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%'))
AND p_date BETWEEN '2019-05-11' AND '{day}'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND adSn!=''
AND devicename NOT IN ({test})
GROUP BY devicename,contentId,tag'''
df6=get_sensor_info_mgo_group_sum(sql6,L=['date','city_sn','car_type','contentid','tag'],
                                 cnames=['date','devicename','contentid','tag','play_trip_cnt','play_cnt'])
df7=get_sensor_info_mgo_group_sum(sql7,L=['date','city_sn','car_type','contentid','tag'],
                                 cnames=['date','devicename','contentid','tag','play_dev_cnt','play_time'])
#df8=get_total_sensor_mgo_group_sum(sql8,['date','city_sn','car_type','contentid','tag'],recent_day+timedelta(days=1),date.today()-timedelta(days=1))
df_video_ad=df6.merge(df7,how='left',on=['date','city_sn','car_type','contentid','tag'])
df_video_ad=df_video_ad[['date','city_sn','car_type','contentid','tag','play_dev_cnt','play_trip_cnt','play_cnt','play_time']]
sql01='''insert into video_ad_sensor_daily values('{}','{}','{}','{}','{}',{},{},{},0,0,0,{},0)'''
for i in range(0,len(df_video_ad)):
    L=[]
    for j in range(0,len(df_video_ad.iloc[0])):
        L.append(df_video_ad.iloc[i][j])
    cur0.execute(sql01.format(*L))
    if i/100==0:
        con0.commit()
    else:continue
con0.commit()

## 内容&广告每日数据(不加行程条件)
#日播放次数播放行程数
sql0='SELECT MAX(day) FROM video_ad_daily_fulltime'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,12,12)
sql6='''SELECT p_date `date`,devicename,contentId,tag,COUNT(1) play_cnt
FROM mring.mojo_ord_event 
WHERE (tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','video_banner_click','click_video_detail',
'video_banner_show','screensaver_ad_begin','click_show_screensaver_detail','click_invalid_screensaver_detail')
OR (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%'))
AND env='prod'
AND adSn!=''
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date,devicename,contentId,tag'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#日播放设备数&时间
sql7='''SELECT p_date `date`,devicename,contentId,tag,COUNT(DISTINCT devicename) play_dev_cnt,SUM(playTime) play_time
FROM mring.mojo_ord_event 
WHERE env='prod'
AND adSn!=''
AND tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','SCREEN_OTHER_RECOVER_END',
'video_banner_click','click_video_detail','video_banner_show','screensaver_ad_begin',
'click_show_screensaver_detail','click_invalid_screensaver_detail')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date,devicename,contentId,tag'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#累计播放次数点位数
sql9='''SELECT '{day}' `date`,devicename,contentId,tag,COUNT(DISTINCT devicename) total_play_dev_cnt,
COUNT(1) total_play_cnt,SUM(playTime) total_play_time FROM mring.mojo_ord_event 
WHERE env='prod'
AND adSn!=''
AND (tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','video_banner_click','click_video_detail')
OR (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%'))
AND p_date BETWEEN '2019-05-11' AND '{day}'
AND devicename NOT IN ({test})
GROUP BY devicename,contentId,tag'''
df6=get_sensor_info_mgo_group_sum(sql6,['date','city_sn','car_type','contentid','tag'],
                                  ['date','devicename','contentid','tag','play_cnt'])
df7=get_sensor_info_mgo_group_sum(sql7,['date','city_sn','car_type','contentid','tag'],
                                 ['date','devicename','contentid','tag','play_dev_cnt','play_time'])
#df9=get_total_sensor_mgo_group_sum(sql9,['date','city_sn','car_type','contentid','tag'],recent_day+timedelta(days=1),date.today()-timedelta(days=1))
df_video_ad=df6.merge(df7,how='left',on=['date','city_sn','car_type','contentid','tag'])
df_video_ad=df_video_ad[['date','city_sn','car_type','contentid','tag','play_dev_cnt','play_cnt','play_time']].fillna(0)
sql01='''insert into video_ad_daily_fulltime values('{}','{}','{}','{}','{}',{},{},0,0,{},0)'''
for i in range(0,len(df_video_ad)):
    L=[]
    for j in range(0,len(df_video_ad.iloc[0])):
        L.append(df_video_ad.iloc[i][j])
    cur0.execute(sql01.format(*L))
    if i/100==0:
        con0.commit()
    else:continue
con0.commit()

#广告计划日播放次数播放行程数设备数(不区分是否行程内)
sql0='SELECT MAX(day) FROM video_sensor_adplan_daily'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,12,31)
sql6='''SELECT p_date `date`,adsn,tag,COUNT(DISTINCT devicename) play_dev_cnt,COUNT(DISTINCT traceId) play_trip_cnt,COUNT(1) play_cnt
FROM mring.mojo_ord_event 
WHERE (tag IN ('SCREEN_OTHER_CONTENT_BEGIN','splash_show_begin','video_banner_click','click_video_detail',
'video_banner_show','screensaver_ad_begin','click_screensaver_dialog','click_show_screensaver_detail','click_invalid_screensaver_detail')
OR (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%'))
AND env='prod'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date,adsn,tag'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
df_adplan=get_sensor_info2(sql6).fillna('0')
df_adplan.columns=['date','adsn','tag','play_dev_cnt','play_trip_cnt','play_cnt']
sql01='''insert into video_sensor_adplan_daily values('{}','{}','{}',{},{},{})'''
for i in range(0,len(df_adplan)):
    L=[]
    for j in range(0,len(df_adplan.iloc[0])):
        L.append(df_adplan.iloc[i][j])
    cur0.execute(sql01.format(*L))
    if i/100==0:
        con0.commit()
    else:continue
con0.commit()

cur0.close()
con0.close()

## 总广告日报
## mring_sensor_trip_day_v3
#建立写库连接
con0 = pymysql.connect(host='rm-bp1d08monqz6q4khipo.mysql.rds.aliyuncs.com',user='bi',
                      passwd='VS6w@6wiv',charset='utf8')
cur0 = con0.cursor()#获取光标
cur0.execute('use mring')#使用数据库
#导入神策每日行程数据
sql0='SELECT MAX(day) FROM mring_sensor_trip_day_v3'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,8,19)
#每日灰度设备行程及内容广告播放情况,1011安装adSn不为空和视频播放正常结束判断贴片
sql7='''
SELECT devicename,p_date `date`,COUNT(1) cnt,SUM(inter) total,SUM(ad_cnt) ad_cnt,
SUM(show_video_start) show_video_start,SUM(video_time) video_time FROM 
(SELECT devicename,traceId,p_date,(MAX(time_stamp)-MIN(time_stamp))/1000 inter,
SUM(CASE WHEN (tag='SCREEN_OTHER_CONTENT_BEGIN' AND adSn!='') 
OR tag='splash_show_begin' THEN 1 ELSE 0 END) ad_cnt,
SUM(CASE WHEN tag='splash_show_begin' THEN 1 ELSE 0 END) show_video_start,
SUM(CASE WHEN tag='SCREEN_OTHER_RECOVER_END' THEN playTime ELSE 0 END) video_time
FROM mring.mojo_ord_event WHERE traceId IS NOT NULL AND traceId!=''
AND env='prod' AND versioncode>=179
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY devicename,traceId,p_date) a
GROUP BY devicename,p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#当日出车数
sql8='''
SELECT p_date `date`,devicename
FROM mring.mojo_ord_event WHERE traceId IS NOT NULL AND traceId!=''
AND env='prod' AND versioncode>=179
AND p_date BETWEEN '{}' AND '{}' 
AND devicename NOT IN ({})
GROUP BY devicename,p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#累计点位数(根据是否播放开屏定义点位)
sql9='''select DISTINCT '{day}' `date`,devicename
FROM mring.mojo_ord_event where env='prod' AND tag='splash_show_begin'
AND p_date BETWEEN '2019-05-11' AND '{day}'
AND devicename NOT IN ({test})
'''
#累计出车情况
sql10='''SELECT DISTINCT '{day}' `date`,devicename FROM mring.mojo_ord_event
WHERE env='prod' AND versioncode>=179 AND traceId!='' 
AND traceId IS NOT NULL AND p_date BETWEEN '2019-05-11' AND '{day}'
AND devicename NOT IN ({test})
'''
#当日点位数
sql11='''SELECT DISTINCT a.p_date `date`,a.devicename FROM mring.mojo_ord_event a
    JOIN mring.mojo_screen_info b ON a.devicename=b.devicename
    WHERE a.env='prod' AND ((a.tag IN ('splash_show_begin','SCREEN_OTHER_CONTENT_BEGIN') AND a.traceId!='' AND a.traceId IS NOT NULL) 
    OR (a.tag IN ('screensaver_ad_begin','SCREEN_OTHER_CONTENT_BEGIN') 
    AND b.city_sn=310100))
    AND a.p_date BETWEEN '{}' AND '{}'
    AND b.p_date BETWEEN '{}' AND '{}'
    AND a.devicename NOT IN ({})'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),
                                            recent_day+timedelta(days=1),date.today()-timedelta(days=1),
                                            test_dev_list)
#当日心跳数
sql12='''SELECT a.day,b.city_sn,b.car_type,COUNT(DISTINCT a.sn) daily_heartbeat_dev_num
FROM (SELECT DISTINCT sn,day FROM mring.mring_heartbeat_day WHERE day BETWEEN '{}' AND '{}') a
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' 
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND b.desc NOT LIKE '%%测试%%'
    ) b ON a.sn=b.third_code
GROUP BY a.day,b.city_sn,b.car_type'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1))
#当日全时流广告播放情况
sql13='''
SELECT devicename,p_date `date`,
SUM(CASE WHEN tag='SCREEN_OTHER_CONTENT_BEGIN' AND adSn!='' THEN 1 ELSE 0 END) stream_ad_cnt_fulltime,
SUM(CASE WHEN tag='screensaver_ad_begin' THEN 1 ELSE 0 END) screensaver_ad_cnt_fulltime,
SUM(CASE WHEN tag='SCREEN_OTHER_RECOVER_END' THEN playTime ELSE 0 END) video_time_fulltime
FROM  mring.mojo_ord_event
WHERE env='prod'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY devicename,p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
df7=get_sensor_info_mgo_sum(sql7,['devicename','date','cnt','total','ad_cnt','show_video_start','video_time'])
df8=get_sensor_info_mgo_count(sql8,['date','devicename'])
#df9=get_total_sensor_mgo_count(sql9,recent_day+timedelta(days=1),date.today()-timedelta(days=1))
#df10=get_total_sensor_mgo_count(sql10,recent_day+timedelta(days=1),date.today()-timedelta(days=1))
df11=get_sensor_info_mgo_count(sql11,['date','devicename'])
df12=reader(sql12)
df12['day']=df12['day'].apply(lambda x:x.strftime('%Y-%m-%d'))
df12.rename(columns={'day':'date'},inplace=True)
df13=get_sensor_info_mgo_sum(sql13,['devicename','date','stream_ad_cnt_fulltime','screensaver_ad_cnt_fulltime','video_time_fulltime'])
sql101='''insert into mring_sensor_trip_day_v3 values('{}','{}','{}',{},{},0,0,{},{},{},{},{},{},{},{},{})'''
df_ad=df12.merge(df13,on=['date','city_sn','car_type'],how='left').merge(df11,on=['date','city_sn','car_type'],how='left').\
merge(df7,on=['date','city_sn','car_type'],how='left').merge(df8,on=['date','city_sn','car_type'],how='left')
df_ad.columns=['date', 'city_sn', 'car_type','daily_heartbeat_dev_num',
              'stream_ad_cnt_fulltime','screensaver_ad_cnt_fulltime','video_time_fulltime','daily_play_dev_cnt','trip_cnt',
               'trip_time_sum','ad_cnt','show_video_start','video_time', 'daily_trip_dev_cnt']
df_ad=df_ad[['date','city_sn', 'car_type','daily_trip_dev_cnt','daily_play_dev_cnt','ad_cnt','show_video_start',
             'trip_cnt','trip_time_sum','video_time',
            'daily_heartbeat_dev_num','stream_ad_cnt_fulltime','screensaver_ad_cnt_fulltime','video_time_fulltime']].fillna(0).sort_values(['date','city_sn','car_type'])
for i in range(0,len(df_ad)):
    L=[]
    for j in range(0,len(df_ad.iloc[0])):
        L.append(df_ad.iloc[i][j])
    cur0.execute(sql101.format(*L))
con0.commit()
sql='''UPDATE mring_sensor_trip_day_v3 SET ad_cnt=(stream_ad_cnt_fulltime+screensaver_ad_cnt_fulltime) WHERE day>'{}' AND city_sn=310100 '''.format(recent_day)
cur0.execute(sql)
con0.commit()
cur0.close()
con0.close()

### tab点击明细
#建立写库连接
con0 = pymysql.connect(host='rm-bp1d08monqz6q4khipo.mysql.rds.aliyuncs.com',user='bi',
                      passwd='VS6w@6wiv',charset='utf8')
cur0 = con0.cursor()#获取光标
cur0.execute('use mring')#使用数据库
sql0='SELECT MAX(day) FROM mring_tab_click'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,8,31)
sql33='''SELECT p_date `date`,devicename,targetValue,COUNT(1) click_times,
COUNT(DISTINCT traceId) click_trip_cnt,COUNT(DISTINCT devicename) click_dev_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND tag='screen_tab_click' 
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date,devicename,targetValue'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql14='''insert into mring_tab_click values('{}','{}','{}','{}',{},{},{})'''
df33=get_sensor_info_mgo_group_sum(sql33,['date','city_sn','car_type','targetvalue'],
                                   ['date','devicename','targetvalue','click_times','click_trip_cnt','click_dev_cnt'])
df33=df33[['date','city_sn','car_type','targetvalue','click_times','click_trip_cnt','click_dev_cnt']]
for i in range(0,len(df33)):
    L=[]
    for j in range(0,len(df33.iloc[0])):
        L.append(df33.iloc[i][j])
    cur0.execute(sql14.format(*L))
con0.commit()

### video&banner点击数据
sql0='SELECT MAX(day) FROM mring_video_click'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,11,30)
sql='''SELECT p_date `date`,tag,page,currentValue,COUNT(DISTINCT traceId) click_trip_cnt,COUNT(1) click_cnt FROM  mring.mojo_ord_event WHERE 
env='prod' 
AND p_date BETWEEN '{}' AND '{}'
AND traceId!=''
AND versionCode>=204
AND tag IN ('click_video_detail','video_banner_click')
AND devicename NOT IN ({})
GROUP BY p_date,tag,page,currentValue'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
df_banner=get_sensor_info(sql)
sql01='''insert into mring_video_click values('{}','{}','{}','{}',{},{})'''
for i in range(0,len(df_banner)):
    L=[]
    for j in range(0,len(df_banner.iloc[0])):
        L.append(df_banner.iloc[i][j])
    cur0.execute(sql01.format(*L))
con0.commit()

## 扫码数据
sql0='SELECT MAX(day) FROM mring_sensor_scan'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,9,30)
sql='''SELECT utmSource,utmCampaign,utmContent,p_date `date`  FROM mring.mojo_ord_event 
WHERE event='SCAN'
AND env='prod' 
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql0='''INSERT INTO mring_sensor_scan values('{}','{}','{}','{}')'''
try:
    df=get_sensor_info(sql)
    df.columns=['utmsource','utmcampaign','utmcontent','date']
    df=df[['utmsource','utmcampaign','utmcontent','date']]
    for i in range(0,len(df)):
        L=[]
        for j in range(0,len(df.iloc[0])):
            L.append(df.iloc[i][j])
        cur0.execute(sql0.format(*L))
    con0.commit()
except:print('no scan')

## 交互数据
#当日交互行程内容数据
sql0='SELECT MAX(day) FROM mring_inter_day'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,8,31)
sql34='''SELECT p_date `date`,COUNT(DISTINCT traceId) inter_trip_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','video_recommend_swith',
'sub_tab_auto_change','list_auto_swith_time')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
ORDER BY `date` '''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
#当日内容交互
sql35='''SELECT a.p_date `date`,a.daily_play_trip_content_cnt,b.inter_trip_content_cnt FROM 
(SELECT p_date,COUNT(DISTINCT CONCAT(traceId,contentId)) daily_play_trip_content_cnt
FROM  mring.mojo_ord_event WHERE env='prod' AND traceId IS NOT NULL AND traceId!=''
AND tag='SCREEN_OTHER_CONTENT_BEGIN'
AND versioncode>=179
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
AND contentId IS NOT NULL
AND contentId!=''
AND (adSn='' OR adSn IS NULL)
GROUP BY p_date) a
LEFT JOIN 
(SELECT p_date,COUNT(DISTINCT CONCAT(traceId,contentId)) inter_trip_content_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND event IN ('CLICK','SLIDE')
AND contentId IS NOT NULL
 AND contentId!=''
AND (adSn='' OR adSn IS NULL)
AND tag NOT IN ('video_start','SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH',
'video_recommend_swith','sub_tab_auto_change','list_auto_swith_time')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date) b ON a.p_date=b.p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list,
                          recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql36='''SELECT p_date `date`,COUNT(DISTINCT traceId) close_trip_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' AND tag='screen_close'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql37='''SELECT p_date `date`,COUNT(DISTINCT traceId) recommend_click_trip_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' AND tag='video_recommend_click'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND page IN ('2','video','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql38='''SELECT p_date `date`,COUNT(DISTINCT traceId) eat_inter_trip_cnt
FROM  mring.mojo_ord_event
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','sub_tab_auto_change','list_auto_swith_time')
AND page IN ('eat','3','page:main, subPage:eat','28')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql39='''SELECT p_date `date`,COUNT(DISTINCT traceId) play_inter_trip_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','sub_tab_auto_change','list_auto_swith_time')
AND page IN ('play','4','page:main, subPage:play')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql40='''SELECT p_date `date`,COUNT(DISTINCT traceId) video_trip_cnt FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND traceId!=''
AND p_date BETWEEN '{}' AND '{}'
AND tag IN ('tab_auto_switch','screen_tab_click')
AND targetValue IN ('targetPage: main ,targetSubPage: video','video')
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql41='''SELECT p_date `date`,COUNT(DISTINCT traceId) play_trip_cnt FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND traceId!=''
AND p_date BETWEEN '{}' AND '{}'
AND tag IN ('tab_auto_switch','screen_tab_click')
AND targetValue IN ('targetPage: main ,targetSubPage: play','play')
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql42='''SELECT p_date `date`,COUNT(DISTINCT traceId) eat_trip_cnt FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND traceId!=''
AND p_date BETWEEN '{}' AND '{}'
AND tag IN ('tab_auto_switch','screen_tab_click')
AND targetValue IN ('targetPage: main ,targetSubPage: eat','eat')
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql43='''SELECT p_date `date`,COUNT(DISTINCT traceId) trip_inter_trip_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
 AND versioncode>=179
AND event IN ('CLICK','SLIDE','ZOOM')
AND page IN ('trip','1')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','sub_tab_auto_change','list_auto_swith_time')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql44='''SELECT p_date `date`,COUNT(DISTINCT traceId) video_inter_trip_cnt
FROM  mring.mojo_ord_event
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND event IN ('CLICK','SLIDE','ZOOM')
AND page IN ('video','page:main, subPage:video','2')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH',
'video_recommend_swith','sub_tab_auto_change','list_auto_swith_time')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql45='''SELECT p_date `date`,COUNT(DISTINCT traceId) eat_effect_inter_trip_cnt
FROM  mring.mojo_ord_event
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','screen_click',
'sub_tab_auto_change','list_auto_swith_time')
AND page IN ('eat','3','page:main, subPage:eat','28')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql46='''SELECT p_date `date`,COUNT(DISTINCT traceId) play_effect_inter_trip_cnt
FROM  mring.mojo_ord_event
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','screen_click',
'sub_tab_auto_change','list_auto_swith_time')
AND page IN ('play','4','page:main, subPage:play')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql47='''SELECT p_date `date`,COUNT(DISTINCT traceId) video_effect_inter_trip_cnt
FROM  mring.mojo_ord_event
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','screen_click',
'SCREEN_CLICK_SCREEN','video_recommend_swith','sub_tab_auto_change','list_auto_swith_time')
AND page IN ('video','2','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql48='''SELECT p_date `date`,COUNT(DISTINCT traceId) video_progress_change_trip_cnt FROM mring.mojo_ord_event
WHERE env='prod' AND tag='video_progress_change'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND page IN ('2','video','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql49='''SELECT p_date `date`,COUNT(DISTINCT traceId) hide_detail_trip_cnt FROM  mring.mojo_ord_event
WHERE env='prod' AND tag='hide_detail'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND page IN ('2','video','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql50='''SELECT p_date `date`,COUNT(DISTINCT traceId) click_video_detail_trip_cnt FROM mring.mojo_ord_event
WHERE env='prod' AND tag='click_video_detail'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND page IN ('2','video','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql51='''SELECT p_date `date`,COUNT(DISTINCT traceId) channel_click_trip_cnt FROM mring.mojo_ord_event
WHERE env='prod' AND tag='channel_click'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND page IN ('2','video','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql52='''SELECT p_date `date`,COUNT(DISTINCT traceId) video_zoomin_trip_cnt FROM mring.mojo_ord_event
WHERE env='prod' AND tag='video_zoomin'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND page IN ('2','video','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql53='''SELECT p_date `date`,COUNT(DISTINCT traceId) video_zoomout_trip_cnt FROM mring.mojo_ord_event
WHERE env='prod' AND tag='video_zoomout'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND page IN ('2','video','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql54='''SELECT p_date `date`,COUNT(DISTINCT traceId) video_banner_click_trip_cnt FROM mring.mojo_ord_event
WHERE env='prod' AND tag='video_banner_click'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND page IN ('2','video','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql55='''SELECT p_date `date`,COUNT(DISTINCT traceId) video_recommend_swith_trip_cnt FROM  mring.mojo_ord_event
WHERE env='prod' AND tag='video_recommend_swith'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND page IN ('2','video','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql56='''SELECT p_date `date`,COUNT(DISTINCT traceId) video_recommend_change_trip_cnt FROM mring.mojo_ord_event
WHERE env='prod' AND tag='video_recommend_change'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND page IN ('2','video','page:main, subPage:video')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql57='''SELECT p_date `date`,COUNT(DISTINCT traceId) activity_trip_cnt FROM mring.mojo_ord_event
WHERE 
env='prod'
AND traceId!=''
AND p_date BETWEEN '{}' AND '{}'
AND tag IN ('tab_auto_switch','screen_tab_click')
AND targetValue IN ('activity_video','targetPage: main ,targetSubPage: activity_video')
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql58='''SELECT p_date `date`,COUNT(DISTINCT traceId) activity_inter_trip_cnt
FROM mring.mojo_ord_event
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND versioncode>=179
AND event IN ('CLICK','SLIDE','ZOOM')
AND page IN ('21','33','34','35','36','37','41')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','sub_tab_auto_change','list_auto_swith_time')
AND p_date BETWEEN '{}' AND '{}'
AND  devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql59='''SELECT p_date `date`,COUNT(DISTINCT traceId) activity_effect_inter_trip_cnt
FROM mring.mojo_ord_event
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','screen_click','sub_tab_auto_change','list_auto_swith_time')
AND page IN ('21','33','34','35','36','37','41')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
sql60='''SELECT p_date `date`,COUNT(DISTINCT traceId) trip_effect_inter_trip_cnt
FROM mring.mojo_ord_event
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH','screen_click','sub_tab_auto_change','list_auto_swith_time')
AND page IN ('trip','1')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
for i in range (34,61):
    vars()['df'+str(i)]=get_sensor_info(vars()['sql'+str(i)])
df34=col_rename(df34,['date','inter_trip_cnt'])
df35=col_rename(df35,['date','daily_play_trip_content_cnt','inter_trip_content_cnt'])
df36=col_rename(df36,['date','close_trip_cnt'])
df37=col_rename(df37,['date','recommend_click_trip_cnt'])
df38=col_rename(df38,['date','eat_inter_trip_cnt'])
df39=col_rename(df39,['date','play_inter_trip_cnt'])
df40=col_rename(df40,['date','video_trip_cnt'])
df41=col_rename(df41,['date','play_trip_cnt'])
df42=col_rename(df42,['date','eat_trip_cnt'])
df43=col_rename(df43,['date','trip_inter_trip_cnt'])
df44=col_rename(df44,['date','video_inter_trip_cnt'])
df45=col_rename(df45,['date','eat_effect_inter_trip_cnt'])
df46=col_rename(df46,['date','play_effect_inter_trip_cnt'])
df47=col_rename(df47,['date','video_effect_inter_trip_cnt'])
df48=col_rename(df48,['date','video_progress_change_trip_cnt'])
df49=col_rename(df49,['date','hide_detail_trip_cnt'])
df50=col_rename(df50,['date','click_video_detail_trip_cnt'])
df51=col_rename(df51,['date','channel_click_trip_cnt'])
df52=col_rename(df52,['date','video_zoomin_trip_cnt'])
df53=col_rename(df53,['date','video_zoomout_trip_cnt'])
df54=col_rename(df54,['date','video_banner_click_trip_cnt'])
df55=col_rename(df55,['date','video_recommend_swith_trip_cnt'])
df56=col_rename(df56,['date','video_recommend_change_trip_cnt'])
df57=col_rename(df57,['date','activity_trip_cnt'])
df58=col_rename(df58,['date','activity_inter_trip_cnt'])
df59=col_rename(df59,['date','activity_effect_inter_trip_cnt'])
df60=col_rename(df60,['date','trip_effect_inter_trip_cnt'])
for i in range (34,61):
    vars()['df'+str(i)]=vars()['df'+str(i)].set_index('date')
L=[]
for i in range (34,61):
    L.insert(i-1,vars()['df'+str(i)])
df_inter=pd.concat(L,axis=1,sort=True)
df_inter.index.name='date'
df_inter=df_inter.reset_index()
df_inter=df_inter[['date','inter_trip_cnt','daily_play_trip_content_cnt','inter_trip_content_cnt','close_trip_cnt','recommend_click_trip_cnt',
                   'trip_inter_trip_cnt','video_inter_trip_cnt','eat_inter_trip_cnt','play_inter_trip_cnt','activity_inter_trip_cnt',
                   'video_trip_cnt','eat_trip_cnt','play_trip_cnt','activity_trip_cnt','trip_effect_inter_trip_cnt','video_effect_inter_trip_cnt',
                   'eat_effect_inter_trip_cnt','play_effect_inter_trip_cnt','activity_effect_inter_trip_cnt',
                  'video_progress_change_trip_cnt','hide_detail_trip_cnt','click_video_detail_trip_cnt','channel_click_trip_cnt',
                  'video_zoomin_trip_cnt','video_zoomout_trip_cnt','video_banner_click_trip_cnt','video_recommend_swith_trip_cnt',
                  'video_recommend_change_trip_cnt']].fillna(0)
sql102='''insert into mring_inter_day values('{}',{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{})'''
for i in range(0,len(df_inter)):
    L=[]
    for j in range(0,len(df_inter.iloc[0])):
        L.append(df_inter.iloc[i][j])
    cur0.execute(sql102.format(*L))
con0.commit()
#出租车版交互日报
sql0='SELECT MAX(day) FROM mring_inter_day_taxi'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2020,1,19)
sql1='''SELECT a.day `date`,COUNT(a.sn) online_dev_cnt FROM mring_heartbeat_day a
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' 
    AND  b.main_app='com.mgo.walle' AND b.desc NOT LIKE '%%测试%%') b ON a.sn=b.third_code
WHERE day BETWEEN '{}' AND '{}'
GROUP BY a.day'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1))
sql2='''SELECT p_date `date`,COUNT(1) inter_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND category='WALLE'
AND event IN ('CLICK','SLIDE','ZOOM')
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH',
'sub_tab_auto_change','list_auto_swith_time')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
ORDER BY `date` '''
sql3='''SELECT p_date `date`,COUNT(1) close_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='screen_close'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date
'''
sql4='''SELECT p_date `date`,COUNT(1) screensaver_play_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='screensaver_ad_begin'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql5='''SELECT p_date `date`,COUNT(1) screensaver_click_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag IN ('click_show_screensaver_detail','click_invalid_screensaver_detail')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql6='''SELECT p_date `date`,COUNT(1) screensaver_next_click_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='show_next_screensaver_click'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql7='''SELECT p_date `date`,COUNT(1) voice_click_screensaver_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND page='0'
AND tag='SCREEN_CLICK_SET_BUTTON'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql8='''SELECT p_date `date`,COUNT(1) tab_click_around_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND category='WALLE'
AND tag='screen_tab_click' 
AND targetValue='around'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql9='''SELECT p_date `date`,COUNT(1) map_zoomout_around_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='map_zoomout'
AND page='38'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql10='''SELECT p_date `date`,COUNT(1) business_skip_popular_click_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='click_business_skip_popular'
AND page='38'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql11='''SELECT p_date `date`,COUNT(1) list_switch_around_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='list_switch'
AND page='38'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql12='''SELECT p_date `date`,COUNT(1) map_point_around_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='map_point'
AND page='38'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql13='''SELECT p_date `date`,COUNT(1) detail_show_around_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='detail_show'
AND page='38'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql14='''SELECT p_date `date`,COUNT(1) tab_click_popular_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND category='WALLE'
AND tag='screen_tab_click' 
AND targetValue='popular'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql15='''SELECT p_date `date`,COUNT(1) list_switch_popular_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='list_switch'
AND page IN ('5','6')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql16='''SELECT p_date `date`,COUNT(1) list_load_more_popular_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='list_load_more'
AND page IN ('5','6')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql17='''SELECT p_date `date`,COUNT(1) detail_show_popular_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='detail_show'
AND page IN ('5','6')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql18='''SELECT p_date `date`,COUNT(1) image_slide_popular_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='image_slide'
AND page IN ('5','6')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql19='''SELECT p_date `date`,COUNT(1) preview_show_popular_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='preview_show'
AND page IN ('5','6')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql20='''SELECT p_date `date`,COUNT(1) detail_close_popular_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND tag='detail_close'
AND page IN ('5','6')
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql21='''SELECT p_date `date`,COUNT(1) tab_click_video_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND category='WALLE'
AND tag='screen_tab_click' 
AND targetValue='video'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql22='''SELECT p_date `date`,COUNT(1) channel_click_video_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND category='WALLE'
AND page='40'
AND tag='channel_click' 
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql23='''SELECT p_date `date`,COUNT(1) video_recommend_swith_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND category='WALLE'
AND page='40'
AND tag='video_recommend_swith' 
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql24='''SELECT p_date `date`,COUNT(1) video_recommend_change_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND category='WALLE'
AND page='40'
AND tag='video_recommend_change' 
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql25='''SELECT p_date `date`,COUNT(1) video_zoomout_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND category='WALLE'
AND page='40'
AND tag='video_zoomout' 
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql26='''SELECT p_date `date`,COUNT(1) video_zoomin_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND page='40'
AND tag='video_zoomin'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql27='''SELECT p_date `date`,COUNT(1) voice_click_video_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND page='40'
AND tag='SCREEN_CLICK_SET_BUTTON'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
sql28='''SELECT p_date `date`,COUNT(1) video_banner_click_cnt FROM  mring.mojo_ord_event 
WHERE env='prod' 
AND category='WALLE'
AND page='40'
AND tag='video_banner_click'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY p_date'''
df1=reader_mring(sql1).fillna(0)
df1['date']=df1['date'].apply(lambda x:x.strftime('%Y-%m-%d'))
for i in range (2,29):
    vars()['df'+str(i)]=get_sensor_info(vars()['sql'+str(i)].format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list))
df2=col_rename(df2,['date','inter_cnt'])
df3=col_rename(df3,['date','close_cnt'])
df4=col_rename(df4,['date','screensaver_play_cnt'])
df5=col_rename(df5,['date','screensaver_click_cnt'])
df6=col_rename(df6,['date','screensaver_next_click_cnt'])
df7=col_rename(df7,['date','voice_click_screensaver_cnt'])
df8=col_rename(df8,['date','tab_click_around_cnt'])
df9=col_rename(df9,['date','map_zoomout_around_cnt'])
df10=col_rename(df10,['date','business_skip_popular_click_cnt'])
df11=col_rename(df11,['date','list_switch_around_cnt'])
df12=col_rename(df12,['date','map_point_around_cnt'])
df13=col_rename(df13,['date','detail_show_around_cnt'])
df14=col_rename(df14,['date','tab_click_popular_cnt'])
df15=col_rename(df15,['date','list_switch_popular_cnt'])
df16=col_rename(df16,['date','list_load_more_popular_cnt'])
df17=col_rename(df17,['date','detail_show_popular_cnt'])
df18=col_rename(df18,['date','image_slide_popular_cnt'])
df19=col_rename(df19,['date','preview_show_popular_cnt'])
df20=col_rename(df20,['date','detail_close_popular_cnt'])
df21=col_rename(df21,['date','tab_click_video_cnt'])
df22=col_rename(df22,['date','channel_click_video_cnt'])
df23=col_rename(df23,['date','video_recommend_swith_cnt'])
df24=col_rename(df24,['date','video_recommend_change_cnt'])
df25=col_rename(df25,['date','video_zoomout_cnt'])
df26=col_rename(df26,['date','video_zoomin_cnt'])
df27=col_rename(df27,['date','voice_click_video_cnt'])
df28=col_rename(df28,['date','video_banner_click_cnt'])
for i in range (1,29):
    vars()['df'+str(i)]=vars()['df'+str(i)].set_index('date')
L=[]
for i in range (1,29):
    L.insert(i-1,vars()['df'+str(i)])
df_taxi=pd.concat(L,axis=1,sort=True)
df_taxi.index.name='date'
df_taxi=df_taxi.reset_index()
df_taxi=df_taxi[['date','online_dev_cnt','inter_cnt','close_cnt','screensaver_play_cnt','screensaver_click_cnt',
                   'screensaver_next_click_cnt','voice_click_screensaver_cnt','tab_click_around_cnt','map_zoomout_around_cnt',
                   'business_skip_popular_click_cnt','list_switch_around_cnt','map_point_around_cnt','detail_show_around_cnt',
                   'tab_click_popular_cnt','list_switch_popular_cnt','list_load_more_popular_cnt','detail_show_popular_cnt','image_slide_popular_cnt',
                   'preview_show_popular_cnt','detail_close_popular_cnt','tab_click_video_cnt','channel_click_video_cnt','video_recommend_swith_cnt',
                   'video_recommend_change_cnt','video_zoomout_cnt','video_zoomin_cnt','voice_click_video_cnt','video_banner_click_cnt']].fillna(0)
sql102='''insert into mring_inter_day_taxi values('{}',{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},
{},{},{},{},{},{},{},{})'''
for i in range(0,len(df_taxi)):
    L=[]
    for j in range(0,len(df_taxi.iloc[0])):
        L.append(df_taxi.iloc[i][j])
    cur0.execute(sql102.format(*L))
con0.commit()

## 过去7天内容交互数据
sql0='DELETE FROM mring_content_inter'
cur0.execute(sql0)
sql='''SELECT a.contentId,MIN(a.play_trip_cnt) play_trip_cnt,MIN(b.total_inter_trip_cnt) total_inter_trip_cnt,
SUM(CASE WHEN c.tag IN ('SCREEN_CLICK_SCREEN','screen_click') THEN tag_inter_trip_cnt ELSE 0 END) screen_click_trip_cnt,
SUM(CASE WHEN c.tag='video_recommend_swith' THEN tag_inter_trip_cnt ELSE 0 END) recommend_swith_trip_cnt,
SUM(CASE WHEN c.tag='SCREEN_CLICK_SPEAKER_BUTTON' THEN tag_inter_trip_cnt ELSE 0 END) click_speaker_button_trip_cnt,
SUM(CASE WHEN c.tag='SCREEN_OTHER_VOLUME_CHANGE' THEN tag_inter_trip_cnt ELSE 0 END) volume_change_trip_cnt,
SUM(CASE WHEN c.tag='video_recommend_click' THEN tag_inter_trip_cnt ELSE 0 END) recommend_click_trip_cnt,
SUM(CASE WHEN c.tag='video_recommend_change' THEN tag_inter_trip_cnt ELSE 0 END) recommend_change_trip_cnt,
SUM(CASE WHEN c.tag='video_progress_change' THEN tag_inter_trip_cnt ELSE 0 END) progress_change_trip_cnt,
SUM(CASE WHEN c.tag='video_pause_click' THEN tag_inter_trip_cnt ELSE 0 END) pause_click_trip_cnt,
SUM(CASE WHEN c.tag='video_start_click' THEN tag_inter_trip_cnt ELSE 0 END) start_click_trip_cnt
FROM 
(SELECT contentId,COUNT(DISTINCT traceId) play_trip_cnt
FROM  mring.mojo_ord_event WHERE env='prod' AND traceId IS NOT NULL AND traceId!=''
AND tag='SCREEN_OTHER_CONTENT_BEGIN'
-- AND versioncode>=179
AND p_date BETWEEN date_sub(current_date(),7) AND date_sub(current_date(),1)
AND contentId IS NOT NULL
AND contentId!=''
AND adSn=''
AND devicename NOT IN ({})
GROUP BY contentId) a
LEFT JOIN 
(SELECT contentId,COUNT(DISTINCT traceId) total_inter_trip_cnt
FROM  mring.mojo_ord_event
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE')
AND contentId IS NOT NULL
 AND contentId!=''
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH')
AND adSn=''
AND p_date BETWEEN date_sub(current_date(),7) AND date_sub(current_date(),1)
AND devicename NOT IN ({})
GROUP BY contentId) b ON a.contentId=b.contentId
LEFT JOIN 
(SELECT contentId,tag,COUNT(DISTINCT traceId) tag_inter_trip_cnt
FROM  mring.mojo_ord_event 
WHERE 
env='prod'
AND traceId IS NOT NULL AND traceId!=''
AND event IN ('CLICK','SLIDE')
AND contentId IS NOT NULL
AND contentId!=''
AND tag NOT IN ('video_start','video_pause','video_resume',
'SCREEN_CLICK_STOP_PLAY','SCREEN_CLICK_RECOVER_PLAY','SCREEN_CLICK_CONSOLE_AUTO_VANISH')
AND adSn=''
AND p_date BETWEEN date_sub(current_date(),7) AND date_sub(current_date(),1)
AND devicename NOT IN ({})
GROUP BY contentId,tag) c ON a.contentId=c.contentId
GROUP BY a.contentId'''
df=get_sensor_info(sql.format(test_dev_list,test_dev_list,test_dev_list))
df.columns=['contentid','play_trip_cnt','total_inter_trip_cnt','screen_click_trip_cnt','recommend_swith_trip_cnt',
'click_speaker_button_trip_cnt','volume_change_trip_cnt','recommend_click_trip_cnt','recommend_change_trip_cnt',
'progress_change_trip_cnt','pause_click_trip_cnt','start_click_trip_cnt']
df=df.fillna(0)
sql0='''insert into mring_content_inter values('{}',{},{},{},{},{},{},{},{},{},{},{})'''
for i in range(0,len(df)):
    L=[]
    for j in range(0,len(df.iloc[0])):
        L.append(df.iloc[i][j])
    cur0.execute(sql0.format(*L))
con0.commit()



cur0.close()
con0.close()
## 设备规模日报，mring_sensor_daily
#建立写库连接
con0 = pymysql.connect(host='rm-bp1d08monqz6q4khipo.mysql.rds.aliyuncs.com',user='bi',
                      passwd='VS6w@6wiv',charset='utf8')
cur0 = con0.cursor()#获取光标
cur0.execute('use mring')#使用数据库
### 更新mring_day表日期
sql0='SELECT MAX(day) FROM mring_day'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,8,1)
sql='''insert into mring_day values('{}')'''
for day in  pd.date_range(start=recent_day+timedelta(days=1),end=datetime.now().date()-timedelta(days=1),freq='1D'):
    day=day.date()
    cur0.execute(sql.format(day)) 
con0.commit()

#更新魔晶业务日报中间表加入trip_v3
sql0='SELECT MAX(day) FROM mring_sensor_daily'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,7,31)
sql8='''SELECT a.day,a.city_sn,a.car_type,total_dev_num,total_trip_dev_num,g.total_play_dev_cnt,
g.trip_cnt trip_cnt,ROUND(g.trip_time_sum/60,0) trip_time FROM 
(SELECT a.day,b.city_sn,b.car_type,COUNT(1) total_dev_num FROM
(SELECT day FROM mring.mring_day) a 
JOIN
(SELECT  a.third_code,DATE(a.add_time) day,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND b.desc NOT LIKE '%%测试%%') b
ON a.day>=b.day
WHERE a.day>='2019-08-12' AND a.day BETWEEN '{}' AND '{}'
GROUP BY a.day,b.city_sn,b.car_type) a 
LEFT JOIN 
(SELECT a.day,b.city_sn,b.car_type,COUNT(DISTINCT b.device_name) total_trip_dev_num FROM
(SELECT day FROM mring.mring_day) a 
JOIN 
(SELECT `day`,device_name,b.city_sn,b.car_type FROM (SELECT DISTINCT device_name,DATE(add_time) `day` FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3_201911
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3_201912
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3_202001
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201908
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201909
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201910
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201911
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201912
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_202001
) A) a
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE  a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND b.desc NOT LIKE '%%测试%%'
) b ON a.device_name=b.third_code
GROUP BY a.day,device_name,b.city_sn,b.car_type) b ON a.day>=b.day
GROUP BY a.day,b.city_sn,b.car_type) f ON a.day=f.day AND a.city_sn=f.city_sn AND a.car_type=f.car_type
LEFT JOIN 
mring_sensor_trip_day_v3 g ON a.day=g.day AND a.city_sn=g.city_sn AND a.car_type=g.car_type
order by a.day DESC'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1))
sql9='''SELECT '{}' day,city_sn,car_type,COUNT(1) normal_dev_num FROM
(SELECT  a.third_code,c.city_sn,c.car_type,DATE(a.add_time) day
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND b.desc NOT LIKE '%%测试%%'
    AND a.third_code NOT IN (SELECT device_name FROM abnormal_device_list WHERE day='{}') ) a
WHERE a.day<='{}' 
GROUP BY city_sn,car_type'''
sql10='''SELECT '{}' day,b.city_sn,b.car_type,COUNT(DISTINCT device_name) daily_trip_dev_num FROM (SELECT DISTINCT device_name,DATE(add_time) `day` FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
WHERE DATE(add_time)='{}'
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
WHERE DATE(add_time)='{}') A) a
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND b.desc NOT LIKE '%%测试%%'
) b ON a.device_name=b.third_code
GROUP BY b.city_sn,car_type'''
sql11='''SELECT '{}' day,b.city_sn,b.car_type,COUNT(DISTINCT sn) daily_online_dev_num FROM mring_heartbeat_day a
JOIN 
(
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') 
    AND b.desc NOT LIKE '%%测试%%'
) b ON a.sn=b.third_code
WHERE a.day='{}'
AND a.sn IN
(SELECT DISTINCT device_name FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
WHERE DATE(add_time)='{}'
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
WHERE DATE(add_time)='{}') A)
GROUP BY b.city_sn,b.car_type'''
sql12='''SELECT '{}' day,b.city_sn,b.car_type,COUNT(DISTINCT a.sn) daily_play_dev_num
from (SELECT * FROM mring.mring_sensor_summary where tag='splash_show_begin' AND day='{}') a
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' 
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND b.desc NOT LIKE '%%测试%%'
) b ON a.sn=b.third_code
GROUP BY b.city_sn,b.car_type'''
sql13='''SELECT a.day,c.city_sn,c.car_type,COUNT(DISTINCT a.device_name) daily_online_dev_num_server
from (SELECT DISTINCT device_name,DATE(add_time) day FROM mgo_screen_trip WHERE DATE(add_time) BETWEEN '{}' AND '{}') a
JOIN (SELECT DISTINCT sn,day FROM mring.mring_heartbeat_day WHERE day BETWEEN '{}' AND '{}') b ON a.device_name=b.sn AND a.day=b.day
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' 
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND b.desc NOT LIKE '%%测试%%'
    ) c ON a.device_name=c.third_code
GROUP BY a.day,c.city_sn,c.car_type'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),
                                             recent_day+timedelta(days=1),date.today()-timedelta(days=1))
sql14='''SELECT a.day,c.city_sn,c.car_type,COUNT(DISTINCT a.device_name) daily_online_dev_num_client
from (SELECT DISTINCT device_name,DATE(add_time) day FROM mgo_screen_trip_v3 WHERE DATE(add_time) BETWEEN '{}' AND '{}') a
JOIN (SELECT DISTINCT sn,day FROM mring.mring_heartbeat_day WHERE day BETWEEN '{}' AND '{}') b ON a.device_name=b.sn AND a.day=b.day
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' 
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND b.desc NOT LIKE '%%测试%%'
    ) c ON a.device_name=c.third_code
GROUP BY a.day,c.city_sn,c.car_type'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),
                                             recent_day+timedelta(days=1),date.today()-timedelta(days=1))
sql15='''SELECT '{}' day,b.city_sn,b.car_type,COUNT(DISTINCT a.sn) total_heartbeat_dev_num
FROM (SELECT DISTINCT sn,day FROM mring.mring_heartbeat_day WHERE day<='{}') a
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND  DATE(a.add_time)<='{}'
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND b.desc NOT LIKE '%%测试%%'
    ) b ON a.sn=b.third_code
GROUP BY b.city_sn,b.car_type'''
sql16='''SELECT a.day,b.city_sn,b.car_type,COUNT(DISTINCT a.sn) daily_heartbeat_dev_num
FROM (SELECT DISTINCT sn,day FROM mring.mring_heartbeat_day WHERE day BETWEEN '{}' AND '{}') a
JOIN (
    SELECT  a.third_code,c.city_sn,c.car_type
    FROM      mgo.mgo_driver_third_service a
    JOIN mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' 
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND b.desc NOT LIKE '%%测试%%'
    ) b ON a.sn=b.third_code
GROUP BY a.day,b.city_sn,b.car_type'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1))
df8=reader_mring(sql8).fillna(0)
df8['day']=df8['day'].apply(lambda x:x.strftime('%Y-%m-%d'))
df9=get_mring_total_info(sql9,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df10=get_mring_total_info(sql10,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df11=get_mring_total_info(sql11,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df12=get_mring_total_info(sql12,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df13=reader(sql13)
df13['day']=df13['day'].apply(lambda x:x.strftime('%Y-%m-%d'))
df14=reader(sql14)
df14['day']=df14['day'].apply(lambda x:x.strftime('%Y-%m-%d'))
df15=get_mring_total_info(sql15,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df16=reader(sql16)
df16['day']=df16['day'].apply(lambda x:x.strftime('%Y-%m-%d'))
df_daily=df8.merge(df9,how='left',on=['day','city_sn','car_type']).merge(df10,how='left',on=['day','city_sn','car_type']).\
merge(df11,how='left',on=['day','city_sn','car_type']).merge(df12,how='left',on=['day','city_sn','car_type']).\
merge(df13,how='left',on=['day','city_sn','car_type']).merge(df14,how='left',on=['day','city_sn','car_type']).\
merge(df15,how='left',on=['day','city_sn','car_type']).merge(df16,how='left',on=['day','city_sn','car_type'])   
df_daily=df_daily[['day','city_sn','car_type','total_dev_num','total_heartbeat_dev_num','total_trip_dev_num','total_play_dev_cnt','normal_dev_num',
                   'daily_trip_dev_num','daily_online_dev_num','daily_play_dev_num','daily_heartbeat_dev_num','daily_online_dev_num_server',
                   'daily_online_dev_num_client','trip_cnt','trip_time']].\
sort_values(['day','city_sn','car_type']).fillna(0).reset_index(drop=True)
sql='''insert into mring_sensor_daily values('{}','{}','{}',{},{},{},{},{},{},{},{},{},{},{},{},{})'''
for i in range(0,len(df_daily)):
    L=[]
    for j in range(0,len(df_daily.iloc[0])):
        L.append(df_daily.iloc[i][j])
    cur0.execute(sql.format(*L))
con0.commit()
print('sensor_daily',datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M"))
cur0.close()
con0.close()

#建立写库连接
con0 = pymysql.connect(host='rm-bp1d08monqz6q4khipo.mysql.rds.aliyuncs.com',user='bi',
                      passwd='VS6w@6wiv',charset='utf8')
cur0 = con0.cursor()#获取光标
cur0.execute('use mring')#使用数据库
## 代理商日报
sql0='SELECT MAX(day) FROM mring_sensor_daily_agent'
cur0.execute(sql0)
recent_day=cur0.fetchone()[0]
if recent_day==None:
    recent_day=date(2019,12,10)
sql9='''SELECT a.day,a.parent_id,total_dev_num,total_trip_dev_num FROM 
(SELECT a.day,b.parent_id,COUNT(1) total_dev_num FROM
(SELECT day FROM mring.mring_day) a 
JOIN
(SELECT  a.third_code,DATE(a.add_time) day,b.owner_sn parent_id
    FROM      mgo.mgo_driver_third_service a
    join mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND b.desc NOT LIKE '%%测试%%' AND  b.main_app IN ('com.mgo.gad','com.mgo.walle')  ) b ON a.day>=b.day
WHERE a.day>='2019-08-12' AND a.day BETWEEN '{}' AND '{}'
GROUP BY a.day,b.parent_id) a 
LEFT JOIN 
(SELECT a.day,b.parent_id,COUNT(DISTINCT b.device_name) total_trip_dev_num FROM
(SELECT day FROM mring.mring_day) a 
JOIN 
(SELECT `day`,device_name,parent_id FROM (SELECT DISTINCT device_name,DATE(add_time) `day` FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3_201911
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3_201912
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3_202001
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201908
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201909
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201910
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201911
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_201912
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_202001
) A) a
JOIN (
    SELECT  a.third_code,b.owner_sn parent_id
    FROM      mgo.mgo_driver_third_service a
    join mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND b.desc NOT LIKE '%%测试%%' AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') 
) b ON a.device_name=b.third_code
GROUP BY a.day,device_name) b ON a.day>=b.day
GROUP BY a.day,b.parent_id) f ON a.day=f.day AND a.parent_id=f.parent_id
order by a.day'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1))
sql10='''SELECT '{}' day,parent_id,COUNT(DISTINCT device_name) daily_trip_dev_num 
FROM (SELECT DISTINCT device_name,DATE(add_time) `day` FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
WHERE DATE(add_time)='{}'
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
WHERE DATE(add_time)='{}') A) a
JOIN (
    SELECT  a.third_code,b.owner_sn parent_id
    FROM      mgo.mgo_driver_third_service a
    join mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009'
    AND b.desc NOT LIKE '%%测试%%' AND  b.main_app IN ('com.mgo.gad','com.mgo.walle')
) b ON a.device_name=b.third_code
GROUP BY parent_id '''
sql11='''SELECT '{}' day,parent_id,COUNT(DISTINCT sn) daily_online_dev_num FROM mring_heartbeat_day a
JOIN 
(
    SELECT  a.third_code,b.owner_sn parent_id
    FROM      mgo.mgo_driver_third_service a
    join mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND b.desc NOT LIKE '%%测试%%' 
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle')
) b ON a.sn=b.third_code
WHERE a.day='{}'
AND a.sn IN
(SELECT DISTINCT device_name FROM
(SELECT device_name,add_time FROM mgo.mgo_screen_trip
WHERE DATE(add_time)='{}'
UNION
SELECT device_name,add_time FROM mgo.mgo_screen_trip_v3
WHERE DATE(add_time)='{}') A)
GROUP BY parent_id '''
sql12='''SELECT '{}' day,parent_id,COUNT(DISTINCT a.sn) daily_play_dev_num
from (SELECT * FROM mring.mring_sensor_summary where tag='splash_show_begin' AND day='{}') a
JOIN (
    SELECT  a.third_code,b.owner_sn parent_id
    FROM      mgo.mgo_driver_third_service a
    join mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND b.desc NOT LIKE '%%测试%%' 
    AND b.main_app IN ('com.mgo.gad','com.mgo.walle')
) b ON a.sn=b.third_code
GROUP BY parent_id '''
sql13='''SELECT devicename,'{day}' `date`,COUNT(1) trip_cnt,SUM(inter) trip_time_total,SUM(ad_cnt) ad_cnt,
SUM(show_video_start) show_video_start,SUM(video_time) video_time FROM 
(SELECT devicename,traceId,(MAX(time_stamp)-MIN(time_stamp))/1000 inter,
SUM(CASE WHEN (tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%'
AND adSn!='') 
OR tag='splash_show_begin' THEN 1 ELSE 0 END) ad_cnt,
SUM(CASE WHEN tag='splash_show_begin' THEN 1 ELSE 0 END) show_video_start,
SUM(CASE WHEN tag='SCREEN_OTHER_RECOVER_END' THEN playTime ELSE 0 END) video_time
FROM  mring.mojo_ord_event WHERE traceId IS NOT NULL AND traceId!=''
AND env='prod' AND versioncode>=179
AND p_date='{day}' 
AND devicename NOT IN ({test})
GROUP BY devicename,traceId) a
GROUP BY devicename '''
sql14='''SELECT '{}' day,parent_id,COUNT(DISTINCT sn) total_heartbeat_dev_num FROM mring_heartbeat_day a
JOIN 
(
    SELECT  a.third_code,b.owner_sn parent_id
    FROM      mgo.mgo_driver_third_service a
    join mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND b.desc NOT LIKE '%%测试%%' 
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle') AND DATE(a.add_time)<='{}'
) b ON a.sn=b.third_code
WHERE a.day<='{}'
GROUP BY parent_id '''
sql15='''SELECT a.day,parent_id,COUNT(DISTINCT sn) daily_heartbeat_dev_num FROM mring_heartbeat_day a
JOIN 
(
    SELECT  a.third_code,b.owner_sn parent_id
    FROM      mgo.mgo_driver_third_service a
    join mgo.mgo_screen_info b on a.third_code=b.device_name
    JOIN mgo.mgo_screen_car c ON a.car_id=c.id
    WHERE     a.status=0 and a.is_delete=0 and a.tag='TS-0009' AND b.desc NOT LIKE '%%测试%%' 
    AND  b.main_app IN ('com.mgo.gad','com.mgo.walle')
) b ON a.sn=b.third_code
WHERE a.day BETWEEN '{}' AND '{}'
GROUP BY a.day,parent_id '''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1))
#当日全时流广告播放情况
sql16='''
SELECT devicename,p_date `date`,
SUM(CASE WHEN tag='SCREEN_OTHER_RECOVER_END' AND message like '内容正常结束%' AND adSn!='' THEN 1 ELSE 0 END) stream_ad_cnt_fulltime,
SUM(CASE WHEN tag='SCREEN_OTHER_RECOVER_END' THEN playTime ELSE 0 END) video_time_fulltime
FROM  mring.mojo_ord_event 
WHERE env='prod'
AND p_date BETWEEN '{}' AND '{}'
AND devicename NOT IN ({})
GROUP BY devicename,p_date
'''.format(recent_day+timedelta(days=1),date.today()-timedelta(days=1),test_dev_list)
df9=reader_mring(sql9).fillna(0)
df9['day']=df9['day'].apply(lambda x:x.strftime('%Y-%m-%d'))
df10=get_mring_total_info(sql10,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df11=get_mring_total_info(sql11,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df12=get_mring_total_info(sql12,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df13=get_total_sensor_agent_sum_info(sql13,recent_day+timedelta(days=1),date.today()-timedelta(days=1),
                                    ['devicename','date','trip_cnt','trip_time_total','ad_cnt','show_video_start','video_time']).fillna(0)
df13.rename(columns={"date": "day"},inplace=True)
df14=get_mring_total_info(sql14,recent_day+timedelta(days=1),date.today()-timedelta(days=1)).fillna(0)
df15=reader_mring(sql15).fillna(0)
df15['day']=df15['day'].apply(lambda x:x.strftime('%Y-%m-%d'))
df16=get_mring_agent_sum_info (sql16,['devicename','date','stream_ad_cnt_fulltime','video_time_fulltime'])
df16.rename(columns={"date": "day"},inplace=True)
for i in range (9,17):
    vars()['df'+str(i)]=vars()['df'+str(i)].set_index(['day','parent_id'])
L=[]
for i in range (9,17):
    L.append(vars()['df'+str(i)])
df_agent=pd.concat(L,axis=1).reset_index().fillna(0)
sql15='''insert into mring_sensor_daily_agent values('{}','{}',{},{},NULL,NULL,{},{},{},{},{},{},{},{},{},{},{},{})'''
for i in range(0,len(df_agent)):
    L=[]
    for j in range(0,len(df_agent.iloc[0])):
        L.append(df_agent.iloc[i][j])
    cur0.execute(sql15.format(*L))
print('agent_daily',datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M"))
con0.commit()
cur0.close()
con0.close()