# -*- coding:utf-8 -*-

import time
import numpy as np
import pandas as pd
import datetime
import pytz
from dateutil.parser import parse
from OpsManage.models import *
from django.db import connection
from django.db.models import Sum
import MySQLdb
import json
from dateutil.relativedelta import relativedelta
from OpsManage.views import link

def report_activate_data(beginDate, endData, platform):
    apps_filter_day = str(datetime.datetime.strptime(beginDate, "%Y-%m-%d") + datetime.timedelta(days=-7))[:10]
    # 获取某日的client新增激活数据
    sql_new_client_android = 'SELECT device_id, language, app_id, source FROM opsmanage_push_client_activate WHERE platform = "%s" and db_time >= "%s" and db_time <= "%s"' % (
        platform, beginDate, endData)
    df_new_client_android = pd.read_sql(sql_new_client_android, connection)
    # 获取在某时间段的client新增激活数据但不在这时间段有appsflyer记录的设备
    sql_no_client_android = 'SELECT a.device_id, a.language,a.app_id, a.source FROM opsmanage_push_client_activate as a, opsmanage_push_appsflyer as b WHERE a.device_id=b.device_id and a.platform = "%s" and a.db_time >= "%s" and a.db_time <= "%s" and b.db_time < "%s" and b.db_time >= "%s"' % (
        platform, beginDate, endData, beginDate, apps_filter_day)
    df_no_client_android = pd.read_sql(sql_no_client_android, connection)

    # no_device_list = df_no_client_android['device_id'].tolist()
    # df_outer_android = df_new_client_android.drop(df_new_client_android.loc[df_new_client_android['device_id'].isin(no_device_list)].index)

    df_outer_android = pd.concat([df_new_client_android, df_no_client_android], ignore_index=True)
    df_outer_android = df_outer_android.drop_duplicates('device_id', keep=False)
    df_new_client_android = df_outer_android
    # 获取某日的appsflyer新增激活数据
    sql_new_appsf_android = 'SELECT device_id, language as back_language, app_id as back_app_id, channeltype, source as back_source FROM opsmanage_push_appsflyer WHERE platform = "%s" and db_time >= "%s" and db_time < "%s"' % (
        platform, beginDate, endData)
    df_new_appsf_android = pd.read_sql(sql_new_appsf_android, connection)
    # appsflyer内全都属于广告数据
    df_new_appsf_android['channeltype'] = 'Ad'
    # 取2者合集
    df_inner_android = pd.merge(df_new_client_android, df_new_appsf_android, on=['device_id'])
    # 取合集和客户端上报数据的合集，去重
    df_new_client_android['channeltype'] = 'Organic'
    df_concat_android = pd.concat([df_inner_android, df_new_client_android], ignore_index=True)
    df_concat_android = df_concat_android.drop_duplicates('device_id')
    # df_concat_android.fillna("None", inplace=True)
    # 取前者和appsflyer的并集，去重
    df_final_report = pd.concat([df_concat_android, df_new_appsf_android], ignore_index=True)
    df_final_report = df_final_report.drop_duplicates('device_id')
    df_final_report.replace('', np.nan, inplace=True)
    df_final_report.replace('None', np.nan, inplace=True)
    df_final_report.fillna({"source": df_final_report["back_source"]}, inplace=True)
    df_final_report.fillna({"language": df_final_report["back_language"]}, inplace=True)
    df_final_report.fillna({"app_id": df_final_report["back_app_id"]}, inplace=True)
    if platform == 'android':
        df_final_report.fillna({"source": 'googleplay'}, inplace=True)
    else:
        df_final_report.fillna({"source": 'ios'}, inplace=True)
    df_final_report.fillna("None", inplace=True)
    df_final_report['new_size'] = 1

    return df_final_report


def report_data(beginDate, endDate, countEndDate, show_end_date, reportDB):
    ios_appid = []
    android_appid = []
    gameversionls = Game_Version_Config.objects.all()
    for gamecfg in gameversionls:
        android_appid.append(str(gamecfg.android_appid))
        ios_appid.append(str(gamecfg.ios_appid))

    ios_appid = tuple(ios_appid)
    android_appid = tuple(android_appid)
    # 充值IOS
    sql_char_ios = 'SELECT a.language, a.source, b.app_id, b.recharge FROM opsmanage_push_client_activate a RIGHT JOIN(SELECT * FROM opsmanage_push_client_recharge WHERE db_time >= "%s" and db_time < "%s" and app_id in %s) b ON a.device_id = b.device_id;' % (
        beginDate, endDate, ios_appid)
    # sql_char_ios = 'SELECT b.app_id, a.language, b.recharge FROM opsmanage_push_client_activate as a, opsmanage_push_client_recharge as b WHERE (a.platform = "ios" or a.platform = "IPhonePlayer") and a.device_id = b.device_id and b.db_time >= "%s" and b.db_time < "%s"' % (
    #     beginDate, endDate)
    df_recharge_ios = pd.read_sql(sql_char_ios, connection)
    df_recharge_ios['platform'] = 'IOS'
    df_recharge_ios.replace('', np.nan, inplace=True)
    df_recharge_ios.replace('None', np.nan, inplace=True)
    df_recharge_ios.fillna({"language": "Unkown"}, inplace=True)
    df_recharge_ios.fillna({"source": "ios"}, inplace=True)
    # 充值Android
    sql_char_android = 'SELECT a.language, a.source, b.app_id, b.recharge FROM opsmanage_push_client_activate a RIGHT JOIN(SELECT * FROM opsmanage_push_client_recharge WHERE db_time >= "%s" and db_time < "%s" and app_id in %s) b ON a.device_id = b.device_id;' % (
        beginDate, endDate, android_appid)
    # sql_char_android = 'SELECT b.app_id, a.language, b.recharge FROM opsmanage_push_client_activate as a, opsmanage_push_client_recharge as b WHERE a.platform = "android" and a.device_id = b.device_id and b.db_time >= "%s" and b.db_time < "%s"' % (
    #     beginDate, endDate)
    df_recharge_android = pd.read_sql(sql_char_android, connection)
    df_recharge_android['platform'] = 'Android'
    df_recharge_android.replace('', np.nan, inplace=True)
    df_recharge_android.replace('None', np.nan, inplace=True)
    df_recharge_android.fillna({"language": "Unkown"}, inplace=True)
    df_recharge_android.fillna({"source": "googleplay"}, inplace=True)
    # 活跃设备
    sql_activate_device = 'SELECT app_id, language, platform, source FROM opsmanage_push_client_activate_all WHERE db_time >= "%s" and db_time < "%s" GROUP BY device_id' % (
        beginDate, endDate)
    df_activate_device = pd.read_sql(sql_activate_device, connection)
    df_activate_device['cnt'] = 1
    df_activate_device.loc[df_activate_device['platform']=='android', 'platform'] = 'Android'
    df_activate_device.loc[df_activate_device['platform'] == 'ios', 'platform'] = 'IOS'
    df_group_activate_device = df_activate_device.groupby(['app_id', 'platform', 'language', 'source'], as_index=False).sum()
    # 活跃角色
    sql_activate_player = 'SELECT app_id, language, platform, source FROM opsmanage_push_client_online WHERE db_time >= "%s" and db_time < "%s" GROUP BY player_id' % (
        beginDate, endDate)
    df_activate_player = pd.read_sql(sql_activate_player, connection)
    df_activate_player['cnt'] = 1
    df_activate_player.loc[df_activate_player['platform'] == 'android', 'platform'] = 'Android'
    df_activate_player.loc[df_activate_player['platform'] == 'ios', 'platform'] = 'IOS'
    df_group_activate_player = df_activate_player.groupby(['app_id', 'platform', 'language', 'source'], as_index=False).sum()

    df_concat_recharge_all = pd.concat([df_recharge_ios, df_recharge_android], ignore_index=True)
    df_group_recharge = df_concat_recharge_all.groupby(['app_id', 'platform', 'language', 'source'], as_index=False).sum()

    # 新增激活数Android
    df_new_android = report_activate_data(beginDate, endDate, "android")
    # df_new_android.loc[df_new_android['channeltype'] != 'Organic', 'channeltype'] = 'Ad'
    df_new_group_android = df_new_android.groupby(['app_id', 'channeltype', 'language', 'source'], as_index=False).sum()
    df_new_group_android['platform'] = 'Android'

    # 新增激活数IOS
    df_new_ios = report_activate_data(beginDate, endDate, "ios")
    # df_new_ios.loc[df_new_ios['channeltype'] != 'Organic', 'channeltype'] = 'Ad'
    df_new_group_ios = df_new_ios.groupby(['app_id', 'channeltype', 'language', 'source'], as_index=False).sum()
    df_new_group_ios['platform'] = 'IOS'

    # 充值设备
    sql_recharge = 'SELECT distinct a.device_id as rechargeDevice, a.app_id, b.language, b.platform, b.source FROM opsmanage_push_client_recharge as a, opsmanage_push_client_activate as b WHERE a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s"' % (
        beginDate, endDate)
    df_rechargeDevice = pd.read_sql(sql_recharge, connection)
    print(sql_recharge)
    print(df_rechargeDevice)
    df_rechargeDevice.replace('', 'None', inplace=True)
    df_rechargeDevice.replace(np.nan, 'None', inplace=True)
    df_rechargeDevice.loc[(df_rechargeDevice['platform'] == 'android') & (df_rechargeDevice['source'] == 'None'), 'source'] = 'googleplay'
    df_rechargeDevice.loc[(df_rechargeDevice['platform'] == 'ios') & (df_rechargeDevice['source'] == 'None'), 'source'] = 'ios'
    df_rechargeDevice['platform'] = df_rechargeDevice['platform'].replace('ios', 'IOS').replace('android', 'Android')
    df_rechargeDevice_amount = df_rechargeDevice.groupby(['app_id', 'language', 'platform', 'source'], as_index=False).count()
    print(df_rechargeDevice_amount)

    # 自然新增充值设备
    # 指定时间段内新增自然激活设备数在该时间段内的充值设备数
    sql_downPayMent = 'SELECT distinct a.device_id as paymentDevice, a.app_id, b.language, b.platform, b.source FROM opsmanage_push_client_recharge as a, opsmanage_push_client_activate as b WHERE a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s" and b.db_time >= "%s" and b.db_time < "%s"' % (
        beginDate, endDate, beginDate, endDate)
    df_downPayMentDevice = pd.read_sql(sql_downPayMent, connection)
    df_downPayMentDevice.replace('', 'None', inplace=True)
    df_downPayMentDevice.replace(np.nan, 'None', inplace=True)
    df_downPayMentDevice.loc[(df_downPayMentDevice['platform'] == 'android') & (df_downPayMentDevice['source'] == 'None'), 'source'] = 'googleplay'
    df_downPayMentDevice.loc[(df_downPayMentDevice['platform'] == 'ios') & (df_downPayMentDevice['source'] == 'None'), 'source'] = 'ios'
    df_downPayMentDevice['platform'] = df_downPayMentDevice['platform'].replace('ios', 'IOS').replace('android', 'Android')
    df_downPayMentDevice_amount = df_downPayMentDevice.groupby(['app_id', 'language', 'platform', 'source'], as_index=False).count()

    # 广告新增充值设备
    # 指定时间段内新增广告激活设备数在该时间段内的充值设备数
    sql_AdPayMent = 'SELECT distinct a.device_id as AdDevice, a.app_id, b.language, b.platform, b.source FROM opsmanage_push_client_recharge as a, opsmanage_push_appsflyer as b WHERE a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s" and b.db_time >= "%s" and b.db_time < "%s"' % (
        beginDate, endDate, beginDate, endDate)
    df_AdPayMentDevice = pd.read_sql(sql_AdPayMent, connection)
    df_AdPayMentDevice.replace('', 'None', inplace=True)
    df_AdPayMentDevice.replace(np.nan, 'None', inplace=True)
    df_AdPayMentDevice.loc[(df_AdPayMentDevice['platform'] == 'android') & (df_AdPayMentDevice['source'] == 'None'), 'source'] = 'googleplay'
    df_AdPayMentDevice.loc[(df_AdPayMentDevice['platform'] == 'ios') & (df_AdPayMentDevice['source'] == 'None'), 'source'] = 'ios'
    df_AdPayMentDevice['platform'] = df_AdPayMentDevice['platform'].replace('ios', 'IOS').replace('android','Android')
    df_AdPayMentDevice_amount = df_AdPayMentDevice.groupby(['app_id', 'language', 'platform', 'source'],
                                                               as_index=False).count()

    # 自然新增充值
    # 指定时间段内新增自然激活设备数在该时间段内的充值金额
    sql_downPayMentAmount = 'SELECT a.recharge as paymentAmount, a.app_id, b.language, b.platform, b.source FROM opsmanage_push_client_recharge as a, opsmanage_push_client_activate as b WHERE a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s" and b.db_time >= "%s" and b.db_time < "%s"' % (
        beginDate, endDate, beginDate, endDate)
    df_downPayMentAmount = pd.read_sql(sql_downPayMentAmount, connection)
    df_downPayMentAmount['platform'] = df_downPayMentAmount['platform'].replace('ios', 'IOS').replace('android', 'Android')
    df_downPayMentAmount_sum = df_downPayMentAmount.groupby(['app_id', 'language', 'platform', 'source'], as_index=False).sum()

    # 广告新增充值金额
    # 指定时间段内新增广告激活设备数在该时间段内的充值金额
    sql_AdPayMentAmount = 'SELECT a.recharge as AdAmount, a.app_id, b.language, b.platform, b.source FROM opsmanage_push_client_recharge as a, opsmanage_push_appsflyer as b WHERE a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s" and b.db_time >= "%s" and b.db_time < "%s"' % (
        beginDate, endDate, beginDate, endDate)
    df_AdPayMentAmount = pd.read_sql(sql_AdPayMentAmount, connection)
    df_AdPayMentAmount['platform'] = df_AdPayMentAmount['platform'].replace('ios', 'IOS').replace('android','Android')
    df_AdPayMentAmount_sum = df_AdPayMentAmount.groupby(['app_id', 'language', 'platform', 'source'],as_index=False).sum()

    # 充值角色
    # 指定时间段内有充值的角色数，player id排重； 类似充值设备的概念，只是统计的是角色数
    sql_PayMentRole = 'SELECT DISTINCT a.player_id AS RechargePlayer, a.app_id, b.language, b.platform,b.source FROM opsmanage_push_client_recharge a, opsmanage_push_appsflyer b WHERE a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s"' % (beginDate, endDate)
    df_PayMentRole = pd.read_sql(sql_PayMentRole, connection)
    df_PayMentRole.replace('', 'None', inplace=True)
    df_PayMentRole.replace(np.nan, 'None', inplace=True)
    df_PayMentRole.loc[(df_PayMentRole['platform'] == 'android') & (df_PayMentRole['source'] == 'None'), 'source'] = 'googleplay'
    df_PayMentRole.loc[(df_PayMentRole['platform'] == 'ios') & (df_PayMentRole['source'] == 'None'), 'source'] = 'ios'
    df_PayMentRole['platform'] = df_PayMentRole['platform'].replace('ios', 'IOS').replace('android','Android')
    df_PayMentRoleSum = df_PayMentRole.groupby(['app_id', 'language', 'platform', 'source'],
                                                            as_index=False).count()

    # 新增角色
    # 指定时间段内新增角色数，不限制激活时间，只要是指定时间段内新增的player id即可
    sql_AddPayMentRole = 'SELECT DISTINCT a.player_id AS AddPlayerNum, a.app_id, a.language, a.platform, a.source FROM opsmanage_push_client_online_new a, opsmanage_push_client_activate b WHERE a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s"' % (beginDate, endDate)
    df_AddPayMentRole = pd.read_sql(sql_AddPayMentRole, connection)
    df_AddPayMentRole.replace('', 'None', inplace=True)
    df_AddPayMentRole.replace(np.nan, 'None', inplace=True)
    df_AddPayMentRole.loc[
        (df_AddPayMentRole['platform'] == 'android') & (df_AddPayMentRole['source'] == 'None'), 'source'] = 'googleplay'
    df_AddPayMentRole.loc[(df_AddPayMentRole['platform'] == 'ios') & (df_AddPayMentRole['source'] == 'None'), 'source'] = 'ios'
    df_AddPayMentRole['platform'] = df_AddPayMentRole['platform'].replace('ios', 'IOS').replace('android', 'Android')
    df_AddPayMentRoleSum = df_AddPayMentRole.groupby(['app_id', 'language', 'platform', 'source'],
                                                            as_index=False).count()

    # 新增充值角色
    # 指定时间段内新增角色数在该时间段内的充值角色数
    sql_AddRechargeRole = 'select DISTINCT n.player_id as AddRechargePlayer, n.app_id, n.language,n.platform, n.source FROM (SELECT DISTINCT a.player_id, a.app_id, a.LANGUAGE, a.platform, a.source FROM opsmanage_push_client_online_new a, opsmanage_push_client_activate b WHERE a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s") n, opsmanage_push_client_recharge t WHERE t.player_id = n.player_id and t.db_time >="%s" and t.db_time < "%s"' % (beginDate, endDate, beginDate, endDate)
    df_AddRechargeRole = pd.read_sql(sql_AddRechargeRole, connection)
    df_AddRechargeRole.replace('', 'None', inplace=True)
    df_AddRechargeRole.replace(np.nan, 'None', inplace=True)
    df_AddRechargeRole.loc[
        (df_AddRechargeRole['platform'] == 'android') & (df_AddRechargeRole['source'] == 'None'), 'source'] = 'googleplay'
    df_AddRechargeRole.loc[
        (df_AddRechargeRole['platform'] == 'ios') & (df_AddRechargeRole['source'] == 'None'), 'source'] = 'ios'
    df_AddRechargeRole['platform'] = df_AddRechargeRole['platform'].replace('ios', 'IOS').replace('android', 'Android')
    df_AddRechargeRoleSum = df_AddRechargeRole.groupby(['app_id', 'language', 'platform', 'source'],
                                                            as_index=False).count()

    # 新增充值角色充值金额
    # 指定时间段内新增角色数在该时间段内的充值金额
    sql_AddRechargeNum = 'select t.recharge as AddRechargeNum, n.app_id, n.language,n.platform, n.source FROM (SELECT DISTINCT a.player_id, a.app_id, a.LANGUAGE, a.platform, a.source FROM opsmanage_push_client_online_new a, opsmanage_push_client_activate b WHERE a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s") n, opsmanage_push_client_recharge t WHERE t.player_id = n.player_id and t.db_time >="%s" and t.db_time < "%s"' % (
    beginDate, endDate, beginDate, endDate)
    df_AddRechargeNum = pd.read_sql(sql_AddRechargeNum, connection)
    df_AddRechargeNum.replace('', 'None', inplace=True)
    df_AddRechargeNum.replace(np.nan, 'None', inplace=True)
    df_AddRechargeNum.loc[
        (df_AddRechargeNum['platform'] == 'android') & (
                    df_AddRechargeNum['source'] == 'None'), 'source'] = 'googleplay'
    df_AddRechargeNum.loc[
        (df_AddRechargeNum['platform'] == 'ios') & (
                    df_AddRechargeNum['source'] == 'None'), 'source'] = 'ios'
    df_AddRechargeNum['platform'] = df_AddRechargeNum['platform'].replace('ios', 'IOS').replace(
        'android', 'Android')
    df_AddRechargeNumSum = df_AddRechargeNum.groupby(['app_id', 'language', 'platform', 'source'],
                                                                     as_index=False).sum()

    # 新增充值旧角色
    # 指定时间段之前的角色数在之前的时间内未充值，在该指定时间段内充值的角色数
    sql_AddRechargeOldRole = 'select DISTINCT k.player_id as RechargeOldRole, k.app_id, r.language, r.platform, r.source FROM opsmanage_push_client_recharge k, opsmanage_push_appsflyer r WHERE k.player_id NOT IN (SELECT DISTINCT t.player_id FROM opsmanage_push_client_recharge t WHERE EXISTS (SELECT DISTINCT a.player_id FROM opsmanage_push_client_recharge a, opsmanage_push_client_activate b WHERE a.db_time >= "%s" AND a.db_time < "%s" AND b.db_time < "%s" AND a.device_id = b.device_id) AND t.db_time < "%s") AND k.db_time >= "%s" and k.db_time < "%s" AND k.device_id = r.device_id' % (
        beginDate, endDate, beginDate, beginDate, beginDate, endDate)
    df_AddRechargeOldRole = pd.read_sql(sql_AddRechargeOldRole, connection)
    df_AddRechargeOldRole.replace('', 'None', inplace=True)
    df_AddRechargeOldRole.replace(np.nan, 'None', inplace=True)
    df_AddRechargeOldRole.loc[
        (df_AddRechargeOldRole['platform'] == 'android') & (
                df_AddRechargeOldRole['source'] == 'None'), 'source'] = 'googleplay'
    df_AddRechargeOldRole.loc[
        (df_AddRechargeOldRole['platform'] == 'ios') & (
                df_AddRechargeOldRole['source'] == 'None'), 'source'] = 'ios'
    df_AddRechargeOldRole['platform'] = df_AddRechargeOldRole['platform'].replace('ios', 'IOS').replace(
        'android', 'Android')
    df_AddRechargeOldRoleSum = df_AddRechargeOldRole.groupby(['app_id', 'language', 'platform', 'source'],
                                                     as_index=False).count()

    # 新增充值就角色充值金额
    # 指定时间段之前的角色数在之前的时间内未充值，在该指定时间段内的充值金额
    sql_AddRechargeOldNum = 'select k.recharge as RechargeOldNum, k.app_id, r.language, r.platform, r.source FROM opsmanage_push_client_recharge k, opsmanage_push_appsflyer r WHERE k.player_id NOT IN (SELECT DISTINCT t.player_id FROM opsmanage_push_client_recharge t WHERE EXISTS (SELECT DISTINCT a.player_id FROM opsmanage_push_client_recharge a, opsmanage_push_client_activate b WHERE a.db_time >= "%s" AND a.db_time < "%s" AND b.db_time < "%s" AND a.device_id = b.device_id) AND t.db_time < "%s") AND k.db_time >= "%s" and k.db_time < "%s" AND k.device_id = r.device_id' % (
        beginDate, endDate, beginDate, beginDate, beginDate, endDate)
    df_AddRechargeOldNum = pd.read_sql(sql_AddRechargeOldNum, connection)
    df_AddRechargeOldNum.replace('', 'None', inplace=True)
    df_AddRechargeOldNum.replace(np.nan, 'None', inplace=True)
    df_AddRechargeOldNum.loc[
        (df_AddRechargeOldNum['platform'] == 'android') & (
                df_AddRechargeOldNum['source'] == 'None'), 'source'] = 'googleplay'
    df_AddRechargeOldNum.loc[
        (df_AddRechargeOldNum['platform'] == 'ios') & (
                df_AddRechargeOldNum['source'] == 'None'), 'source'] = 'ios'
    df_AddRechargeOldNum['platform'] = df_AddRechargeOldNum['platform'].replace('ios', 'IOS').replace(
        'android', 'Android')
    df_AddRechargeOldNumSum = df_AddRechargeOldNum.groupby(['app_id', 'language', 'platform', 'source'],
                                                             as_index=False).sum()

    df_concat_new_all = pd.concat([df_new_group_android, df_new_group_ios], ignore_index=True)

    df_language_group_all = df_concat_new_all
    df_language_recharge = df_concat_recharge_all

    df_language_appid_all = pd.concat([df_language_group_all, df_language_recharge], ignore_index=True)
    df_unique_language_appid = df_language_appid_all.drop_duplicates(['app_id','language','platform', 'source'])


    for idx in df_unique_language_appid.index:
        language = df_unique_language_appid.loc[idx]['language']
        app_id = df_unique_language_appid.loc[idx]['app_id']
        platform = df_unique_language_appid.loc[idx]['platform']
        source = df_unique_language_appid.loc[idx]['source']
        data = {}
        data['language'] = language
        data['app_id'] = app_id
        data['platform'] = platform
        data['source'] = source
        data['date'] = beginDate
        data['end_date'] = show_end_date
        # 新增激活
        new_activate = df_concat_new_all.loc[
            (df_concat_new_all['platform'] == platform) & (df_concat_new_all['language'] == language) & (
                    df_concat_new_all['app_id'] == app_id) & (df_concat_new_all['source'] == source)]
        if new_activate.empty:
            data['new_activate'] = 0
        else:
            new_activate = new_activate['new_size'].sum()
            data['new_activate'] = 0 if np.isnan(new_activate) else new_activate
        # 广告激活
        ad_activate = df_concat_new_all.loc[
            (df_concat_new_all['platform'] == platform) & (df_concat_new_all['language'] == language) & (
                    df_concat_new_all['app_id'] == app_id) & (df_concat_new_all['channeltype'] == 'Ad') & (
                        df_concat_new_all['source'] == source)]
        if ad_activate.empty:
            data['ad_activate'] = 0
        else:
            ad_activate = ad_activate['new_size'].sum()
            data['ad_activate'] = 0 if np.isnan(ad_activate) else ad_activate
        # 自然激活
        na_activate = df_concat_new_all.loc[
            (df_concat_new_all['platform'] == platform) & (df_concat_new_all['language'] == language) & (
                    df_concat_new_all['app_id'] == app_id) & (df_concat_new_all['channeltype'] == 'Organic') & (
                    df_concat_new_all['source'] == source)]
        if na_activate.empty:
            data['na_activate'] = 0
        else:
            na_activate = na_activate['new_size'].sum()
            data['na_activate'] = 0 if np.isnan(na_activate) else na_activate
        # 充值
        data['recharge'] = 0
        if not df_group_recharge.empty:
            recharge = df_group_recharge.loc[
                (df_group_recharge['platform'] == platform) & (df_group_recharge['language'] == language) & (
                        df_group_recharge['app_id'] == app_id) & (df_group_recharge['source'] == source)]
            if not recharge.empty:
                recharge = recharge['recharge'].sum()
                data['recharge'] = 0 if np.isnan(recharge) else recharge
        # 活跃设备
        data['active_device'] = 0
        if not df_group_activate_device.empty:
            count = df_group_activate_device.loc[
                (df_group_activate_device['platform'] == platform) & (df_group_activate_device['language'] == language) & (
                        df_group_activate_device['app_id'] == app_id) & (df_group_activate_device['source'] == source)]
            if not count.empty:
                device_cnt = count['cnt'].sum()
                data['active_device'] = 0 if np.isnan(device_cnt) else device_cnt
        # 活跃角色
        data['active_player'] = 0
        if not df_group_activate_player.empty:
            count = df_group_activate_player.loc[
                (df_group_activate_player['platform'] == platform) & (df_group_activate_player['language'] == language) & (
                        df_group_activate_player['app_id'] == app_id) & (df_group_activate_player['source'] == source)]
            if not count.empty:
                player_cnt = count['cnt'].sum()
                data['active_player'] = 0 if np.isnan(player_cnt) else player_cnt
        # 充值设备
        data['rechargeDevice'] = 0
        if not df_rechargeDevice_amount.empty:
            count = df_rechargeDevice_amount.loc[
                (df_rechargeDevice_amount['language'] == language) & (df_rechargeDevice_amount['app_id'] == app_id) & (
                            df_rechargeDevice_amount['platform'] == platform) & (
                            df_rechargeDevice_amount['source'] == source)
                ]
            print(count)
            if not count.empty:
                device_cnt = count['rechargeDevice'].sum()
                data['rechargeDevice'] = 0 if np.isnan(device_cnt) else device_cnt

        # 自然新增充值设备
        data['paymentDevice'] = 0
        if not df_downPayMentDevice_amount.empty:
            count = df_downPayMentDevice_amount.loc[
                (df_downPayMentDevice_amount['language'] == language) & (
                        df_downPayMentDevice_amount['app_id'] == app_id) & (
                        df_downPayMentDevice_amount['platform'] == platform) & (
                        df_downPayMentDevice_amount['source'] == source)
                ]
            if not count.empty:
                device_cnt = count['paymentDevice'].sum()
                data['paymentDevice'] = 0 if np.isnan(device_cnt) else device_cnt

        # 广告新增充值设备
        data['AdDevice'] = 0
        if not df_AdPayMentDevice_amount.empty:
            count = df_AdPayMentDevice_amount.loc[
                (df_AdPayMentDevice_amount['language'] == language) & (
                        df_AdPayMentDevice_amount['app_id'] == app_id) & (
                        df_AdPayMentDevice_amount['platform'] == platform) & (
                        df_AdPayMentDevice_amount['source'] == source)
                ]
            if not count.empty:
                device_cnt = count['AdDevice'].sum()
                data['AdDevice'] = 0 if np.isnan(device_cnt) else device_cnt

        # 自然新增充值
        data['paymentAmount'] = 0
        if not df_downPayMentAmount_sum.empty:
            count = df_downPayMentAmount_sum.loc[
                (df_downPayMentAmount_sum['language'] == language) & (
                        df_downPayMentAmount_sum['app_id'] == app_id) & (
                        df_downPayMentAmount_sum['platform'] == platform) & (
                        df_downPayMentAmount_sum['source'] == source)
                ]
            if not count.empty:
                paymentSum = count['paymentAmount'].sum()
                data['paymentAmount'] = 0 if np.isnan(paymentSum) else paymentSum

        # 广告新增充值
        data['AdAmount'] = 0
        if not df_AdPayMentAmount_sum.empty:
            count = df_AdPayMentAmount_sum.loc[
                (df_AdPayMentAmount_sum['language'] == language) & (
                        df_AdPayMentAmount_sum['app_id'] == app_id) & (
                        df_AdPayMentAmount_sum['platform'] == platform) & (
                        df_AdPayMentAmount_sum['source'] == source)
                ]
            if not count.empty:
                paymentSum = count['AdAmount'].sum()
                data['AdAmount'] = 0 if np.isnan(paymentSum) else paymentSum

        # 充值角色
        data['RechargePlayer'] = 0
        if not df_PayMentRoleSum.empty:
            count = df_PayMentRoleSum.loc[
                (df_PayMentRoleSum['language'] == language) & (
                        df_PayMentRoleSum['app_id'] == app_id) & (
                        df_PayMentRoleSum['platform'] == platform) & (
                        df_PayMentRoleSum['source'] == source)
                ]
            if not count.empty:
                Sum = count['RechargePlayer'].sum()
                data['RechargePlayer'] = 0 if np.isnan(Sum) else Sum

        # 新增角色
        data['AddPlayerNum'] = 0
        if not df_AddPayMentRoleSum.empty:
            count = df_AddPayMentRoleSum.loc[
                (df_AddPayMentRoleSum['language'] == language) & (
                        df_AddPayMentRoleSum['app_id'] == app_id) & (
                        df_AddPayMentRoleSum['platform'] == platform) & (
                        df_AddPayMentRoleSum['source'] == source)
                ]
            if not count.empty:
                Sum = count['AddPlayerNum'].sum()
                data['AddPlayerNum'] = 0 if np.isnan(Sum) else Sum

        # 新增充值角色
        data['AddRechargePlayer'] = 0
        if not df_AddRechargeRoleSum.empty:
            count = df_AddRechargeRoleSum.loc[
                (df_AddRechargeRoleSum['language'] == language) & (
                        df_AddRechargeRoleSum['app_id'] == app_id) & (
                        df_AddRechargeRoleSum['platform'] == platform) & (
                        df_AddRechargeRoleSum['source'] == source)
                ]
            if not count.empty:
                Sum = count['AddRechargePlayer'].sum()
                data['AddRechargePlayer'] = 0 if np.isnan(Sum) else Sum

        # 新增充值角色充值金额
        data['AddRechargeNum'] = 0
        if not df_AddRechargeNumSum.empty:
            count = df_AddRechargeNumSum.loc[
                (df_AddRechargeNumSum['language'] == language) & (
                        df_AddRechargeNumSum['app_id'] == app_id) & (
                        df_AddRechargeNumSum['platform'] == platform) & (
                        df_AddRechargeNumSum['source'] == source)
                ]
            if not count.empty:
                Sum = count['AddRechargeNum'].sum()
                data['AddRechargeNum'] = 0 if np.isnan(Sum) else Sum

        # 新增充值旧角色
        data['RechargeOldRole'] = 0
        if not df_AddRechargeOldRoleSum.empty:
            count = df_AddRechargeOldRoleSum.loc[
                (df_AddRechargeOldRoleSum['language'] == language) & (
                        df_AddRechargeOldRoleSum['app_id'] == app_id) & (
                        df_AddRechargeOldRoleSum['platform'] == platform) & (
                        df_AddRechargeOldRoleSum['source'] == source)
                ]
            if not count.empty:
                Sum = count['RechargeOldRole'].sum()
                data['RechargeOldRole'] = 0 if np.isnan(Sum) else Sum

        # 新增充值就角色充值金额
        data['RechargeOldNum'] = 0
        if not df_AddRechargeOldNumSum.empty:
            count = df_AddRechargeOldNumSum.loc[
                (df_AddRechargeOldNumSum['language'] == language) & (
                        df_AddRechargeOldNumSum['app_id'] == app_id) & (
                        df_AddRechargeOldNumSum['platform'] == platform) & (
                        df_AddRechargeOldNumSum['source'] == source)
                ]
            if not count.empty:
                Sum = count['RechargeOldNum'].sum()
                data['RechargeOldNum'] = 0 if np.isnan(Sum) else Sum

        # 上线
        data['new_online'] = report_new_online(platform, app_id, language, source, beginDate, endDate, countEndDate)
        # 广告花费
        sql_ad_cost = 'SELECT ad_cost from opsmanage_ad_cost_conf WHERE platform = "%s" and app_id = "%s" and language = "%s" and source = "%s" and date = "%s"'%(platform, app_id, language, source, beginDate)
        df_cost = pd.read_sql(sql_ad_cost, connection)
        if df_cost.empty:
            data['ad_cost'] = 0
        else:
            ad_cost = df_cost['ad_cost'].sum()
            data['ad_cost'] = 0 if np.isnan(ad_cost) else ad_cost

        # 留存
        data['remain'] = report_remain(platform, app_id, language, source, beginDate, endDate)
        print(data)
        print((str(datetime.date.today())))

        # if app_id == 'id1296714932':
        #     new_activate_cp = df_new_ios.loc[
        #         (df_new_ios['language'] == language) & (df_new_ios['app_id'] == app_id)]
        #     device_list = new_activate_cp['device_id'].tolist()
        #     reg_file = open(r'F:\ios_month_activate.txt', 'a+')
        #     for device in device_list:
        #         reg_file.writelines(device + "\n")
        #     reg_file.close()
        # elif app_id == 'com.wingjoy.rise':
        #     new_activate_cp = df_new_android.loc[
        #         (df_new_android['language'] == language) & (df_new_android['app_id'] == app_id)]
        #     device_list = new_activate_cp['device_id'].tolist()
        #     reg_file = open(r'F:\android_month_activate.txt', 'a+')
        #     for device in device_list:
        #         reg_file.writelines(device + "\n")
        #     reg_file.close()
        print('---------------------------------------------------------------------------------------------------------')
        reportDB.objects.update_or_create(**data)


def update_day_report():
    try:
        before_yesterday = str(datetime.date.today() + datetime.timedelta(days=(-2)))
        yesterday = str(datetime.date.today() + datetime.timedelta(days=(-1)))
        today = str(datetime.date.today())
        be_yesterday_report_data = Report_Day.objects.filter(date=before_yesterday)
        for data in be_yesterday_report_data:
            platform = data.platform
            app_id = data.app_id
            language = data.language
            beginDate = before_yesterday
            source = data.source
            # 注册
            data.new_reg = report_new_reg(platform, app_id, language, source, beginDate, yesterday, today)
            # 上线
            data.new_online = report_new_online(platform, app_id, language, source, beginDate, yesterday, today)
            # 留存
            sql_new_device_id = 'select distinct device_id from opsmanage_push_client_activate where db_time >= "%s" and db_time < "%s" and language = "%s" and app_id = "%s" union select distinct device_id from opsmanage_push_appsflyer where db_time >= "%s" and db_time < "%s" and language = "%s" and app_id = "%s"' % (
                before_yesterday, yesterday, language, app_id, before_yesterday, yesterday, language, app_id)
            df_new_device_id = pd.read_sql(sql_new_device_id, connection)
            sql_activate = 'select distinct device_id from opsmanage_push_client_activate_all where db_time >= "%s" and db_time < "%s"' % (yesterday, today)
            df_activate = pd.read_sql(sql_activate, connection)
            df_remain = pd.merge(df_new_device_id, df_activate, on=['device_id'])
            new_cnt = df_new_device_id.count()['device_id']
            remain_cnt = df_remain.count()['device_id']
            data.remain = remain_cnt
            # 保存
            data.save(update_fields=['new_reg','new_online','remain'])
    except Exception as e:
        print(e)


def data_week_info_up():
    try:
        yesterday = str(datetime.date.today() + datetime.timedelta(days=-1))
        today = str(datetime.date.today())
        week_start_day = str(datetime.date.today() + datetime.timedelta(days=-8))
        show_end_date = str(datetime.date.today() + datetime.timedelta(days=-1, seconds=-1))

        report_data(week_start_day, yesterday, today, show_end_date, Report_Week)
    except Exception as e:
        print(e)


def data_month_info_up():
    roi_month_up()
    # 月存留定时统计入口
    remain_month_up()
    try:
        before_yesterday = str(datetime.date.today() + datetime.timedelta(days=(-2)))
        yesterday = str(datetime.date.today() + datetime.timedelta(days=-1))
        today = str(datetime.date.today())
        month = before_yesterday[0:7]
        month_start_day = month + '-01'

        report_data(month_start_day, yesterday, today, before_yesterday, Report_Month)
    except Exception as e:
        print(e)


def data_real_up():
    try:
        print("data_real_up")
        now_time = datetime.datetime.now().strftime("%Y-%m-%d %H")
        last_time = (datetime.datetime.now()+datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H")
        print(now_time)
        print(last_time)
        #Android
        df_final_android = report_real_data_handle(last_time, now_time, "Android")
        #IOS
        df_final_ios = report_real_data_handle(last_time, now_time, "ios")

        df_new_android = df_final_android
        df_new_android['new_size'] = 1
        df_new_group_android = df_new_android.groupby(['country', 'language', 'channel', 'app_id'], as_index=False).sum()
        df_new_group_android['platform'] = 'Android'

        df_new_ios = df_final_ios
        df_new_ios['new_size'] = 1
        df_new_group_ios = df_new_ios.groupby(['country', 'language', 'channel', 'app_id'], as_index=False).sum()
        df_new_group_ios['platform'] = 'IOS'
        df_concat_new_all = pd.concat([df_new_group_android, df_new_group_ios], ignore_index=True)
        df_concat_new_all['date'] = now_time
        print(df_concat_new_all)
        if df_concat_new_all.index.tolist():
            df_list = np.array(df_concat_new_all).tolist()
            querysetlist = []
            for i in df_list:
                querysetlist.append(
                    Report_Realtime(country=i[0], language=i[1], channel=i[2], app_id= i[3], new_activate=i[4], platform=i[5], date=i[6]))
            Report_Realtime.objects.bulk_create(querysetlist)
        print("data_real_up_end")
    except Exception as e:
        print(e)

def roi_day_up():
    today_datetime = datetime.date.today()
    print ("roi_day_up_start")
    print((str(today_datetime)))
    cal_roi_day_up(today_datetime)

def cal_roi_day_up(today_datetime):
    try:
        today = str(today_datetime)
        first_charge_day_tmp = today
        last_charge_day_tmp = str(today_datetime + datetime.timedelta(days=- 1))
        gameversionls = Game_Version_Config.objects.all()
        querysetlist = []
        for gamecfg in gameversionls:
            for i in range(30):
                organic_day_tmp = str(today_datetime + datetime.timedelta(days=-i - 2))
                organic_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(organic_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                first_day_tmp = str(today_datetime + datetime.timedelta(days=-i - 1))
                first_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(first_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                last_day_tmp = str(today_datetime + datetime.timedelta(days=-i))
                last_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(last_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                date_tmp = first_day_tmp[0:10]
                print(organic_day_tmp +"_"+first_day_tmp+"_"+last_day_tmp+ "_"+ date_tmp)
                # recharge count
                sql_recharge_android = 'SELECT DISTINCT(device_id) FROM opsmanage_push_client_recharge WHERE (app_id="%s" or app_id="%s") and createtime >= "%s" and createtime < "%s"' % (
                    gamecfg.ios_appid, gamecfg.android_appid,last_charge_day_tmp, first_charge_day_tmp)
                df_recharge_android = pd.read_sql(sql_recharge_android, connection)
                df_recharge_android['num_' + str(i + 1)] = 1

                # recharge account
                sql_recharge_account_android = 'SELECT device_id,SUM(recharge) FROM opsmanage_push_client_recharge WHERE (app_id="%s" or app_id="%s") and createtime >= "%s" and createtime < "%s" GROUP BY (device_id)' % (
                    gamecfg.ios_appid, gamecfg.android_appid,last_charge_day_tmp, first_charge_day_tmp)
                df_recharge_account_android = pd.read_sql(sql_recharge_account_android, connection)
                sql_all_android = 'SELECT device_id,country,language, platform, app_id FROM opsmanage_push_client_activate WHERE (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s" and device_id not in (SELECT device_id FROM  opsmanage_push_appsflyer WHERE  db_time_t >= "%s" and db_time_t < "%s")' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_day_tmp_t, last_day_tmp_t, organic_day_tmp_t, last_day_tmp_t)
                df_all_android = pd.read_sql(sql_all_android, connection)
                df_all_android['channel'] = 'Organic'
                df_all_android['sub1_channel'] = 'None'
                df_all_android['sub2_channel'] = 'None'
                df_all_android['sub3_channel'] = 'None'
                sql_appsf_android = 'SELECT device_id,country_code as country,channel,sub1_channel, sub2_channel, sub3_channel, language, platform, app_id FROM opsmanage_push_appsflyer WHERE (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s"' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_day_tmp_t, last_day_tmp_t)
                df_appsf_android = pd.read_sql(sql_appsf_android, connection)
                df_concat_android = pd.concat([df_appsf_android, df_all_android], ignore_index=True)
                df_concat_android = df_concat_android.drop_duplicates('device_id')
                df_concat_android_num = df_concat_android.join(df_recharge_android.set_index('device_id'), on='device_id')
                df_concat_android_account = df_concat_android.join(df_recharge_account_android.set_index('device_id'), on='device_id')
                df_concat_android['size'] = 1
                df_grouped_android = df_concat_android.groupby(['country', 'channel', "sub1_channel", "sub2_channel", "sub3_channel", "language", "platform", "app_id"], as_index=False).sum()
                df_concat_android_num = df_concat_android_num.drop('device_id', axis=1)
                df_grouped_android_num = df_concat_android_num.groupby(['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", "platform", "app_id"],as_index=False).sum()
                df_concat_android_account = df_concat_android_account.drop('device_id', axis=1)
                df_grouped_android_account = df_concat_android_account.groupby(['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", "platform", "app_id"], as_index=False).sum()

                if i == 0:
                    df_concat_all = df_grouped_android.fillna(0)
                    df_concat_all['date'] = date_tmp
                    if df_concat_all.index.tolist():
                        df_list = np.array(df_concat_all).tolist()

                        for li in df_list:
                            keyargs = dict(
                                list(zip(['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", 'platform',"app_id", 'new_activate', 'date', ], li)))
                            querysetlist.append(Roi_Day(**keyargs))
                        # Roi_Day.objects.bulk_create(querysetlist)

                df_concat_all = df_grouped_android_num.fillna(0)
                df_concat_all['date'] = date_tmp
                if df_concat_all.index.tolist():
                    df_list = np.array(df_concat_all).tolist()
                    # querysetlist = []
                    for li in df_list:
                        keyargs = dict(
                            list(zip(['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", 'platform',"app_id", 'num_' + str(i + 1), 'date'],
                                li)))
                        querysetlist.append(Roi_Day(**keyargs))
                    # Roi_Day.objects.bulk_create(querysetlist)
                df_concat_all = df_grouped_android_account.fillna(0)
                df_concat_all['date'] = date_tmp
                if df_concat_all.index.tolist():
                    df_list = np.array(df_concat_all).tolist()
                    # querysetlist = []
                    for li in df_list:
                        keyargs = dict(
                            list(zip(['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", 'platform',"app_id", 'day_' + str(i + 1), 'date'],
                                li)))
                        querysetlist.append(Roi_Day(**keyargs))
        Roi_Day.objects.bulk_create(querysetlist)
    except Exception as e:
        print ("roi_day_error")
        print(e)


def roi_week_up():
    today_datetime = datetime.date.today()
    print ("roi_week_up_start")
    print((str(today_datetime)))
    cal_roi_week_up(today_datetime)
    charge_week_lost_cal(today_datetime)

def charge_month_lost_cal(today_datetime):
    # 付费流失
    try:

        today = str(today_datetime)
        last_charge_day_tmp = today[0:7] + '-01'

        first_day_tmp = str(today_datetime - relativedelta(months=1))
        first_charge_day_tmp = first_day_tmp[0:7] + '-01'
        querysetlist = []
        gameversionls = Game_Version_Config.objects.all()
        for gamecfg in gameversionls:

            sql_recharge = 'SELECT DISTINCT(device_id) FROM opsmanage_push_client_recharge WHERE (app_id="%s" or app_id="%s") and createtime >= "%s" and createtime < "%s"' % (
                gamecfg.ios_appid, gamecfg.android_appid,first_charge_day_tmp, last_charge_day_tmp)
            df_recharge = pd.read_sql(sql_recharge, connection)

            for i in range(24):
                first_day_tmp = str(today_datetime - relativedelta(months=i + 1))
                organic_day_tmp = str(today_datetime - relativedelta(months=i + 1) + datetime.timedelta(days=- 7))
                organic_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(organic_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())
                last_day_tmp = str(today_datetime - relativedelta(months=i))

                month = first_day_tmp[0:7]
                first_day_tmp = month + "-01"
                first_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(first_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())
                end_month = last_day_tmp[0:7]
                last_day_tmp = end_month + "-01"
                last_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(last_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())
                date_tmp = month


                # organic_day_tmp = str(today_datetime + datetime.timedelta(days=-(i + 2) * 7 - 1))
                # first_day_tmp = str(today_datetime + datetime.timedelta(days=-(i + 1) * 7 - 1))
                # last_day_tmp = str(today_datetime + datetime.timedelta(days=-i * 7 - 1))
                # date_tmp = (first_day_tmp[0:10] if len(first_day_tmp) > 10 else first_day_tmp) + '~' + str(datetime.date(*tuple(parse(last_day_tmp).timetuple())[:3]) + datetime.timedelta(days=-1))

                sql_all_android = 'SELECT device_id,country,language, platform, app_id FROM opsmanage_push_client_activate WHERE (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s" and device_id not in (SELECT device_id FROM  opsmanage_push_appsflyer WHERE  db_time_t >= "%s" and db_time_t < "%s")' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_day_tmp_t, last_day_tmp_t, organic_day_tmp_t, last_day_tmp_t)
                df_all_android = pd.read_sql(sql_all_android, connection)
                df_all_android['channel'] = 'Organic'
                df_all_android['sub1_channel'] = 'None'
                df_all_android['sub2_channel'] = 'None'
                df_all_android['sub3_channel'] = 'None'
                sql_appsf_android = 'SELECT device_id,country_code as country,channel,sub1_channel, sub2_channel, sub3_channel, language, platform, app_id FROM opsmanage_push_appsflyer WHERE  (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s"' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_day_tmp_t, last_day_tmp_t)
                df_appsf_android = pd.read_sql(sql_appsf_android, connection)
                df_concat_android = pd.concat([df_appsf_android, df_all_android], ignore_index=True)
                df_concat_android = df_concat_android.drop_duplicates('device_id')
                df_grouped = pd.merge(df_recharge, df_concat_android, how='inner', on=['device_id'])
                df_grouped = df_grouped.groupby(['device_id','country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", "platform","app_id"], as_index=False)

                df_concat_all = df_grouped.fillna(0)
                df_concat_all['date'] = date_tmp
                df_concat_all['month_index'] = i + 1
                if df_concat_all.index.tolist():
                    df_list = np.array(df_concat_all).tolist()

                    for li in df_list:
                        keyargs = dict(
                            list(zip(['device_id', "app_id", 'channel','country',   "language",'platform','sub1_channel', "sub2_channel", "sub3_channel",
                                   'date', 'month_index'], li)))
                        querysetlist.append(Month_Charge_Analysis(**keyargs))
        Month_Charge_Analysis.objects.bulk_create(querysetlist)

    except Exception as e:
        print(e)

def charge_week_lost_cal(today_datetime):
    # 付费流失
    try:
        first_charge_day_tmp = str(today_datetime + datetime.timedelta(days=-7))
        last_charge_day_tmp = str(today_datetime)
        gameversionls = Game_Version_Config.objects.all()
        querysetlist = []
        for gamecfg in gameversionls:
            sql_recharge = 'SELECT DISTINCT(device_id) FROM opsmanage_push_client_recharge WHERE (app_id="%s" or app_id="%s") and createtime >= "%s" and createtime < "%s"' % (
                gamecfg.ios_appid, gamecfg.android_appid, first_charge_day_tmp, last_charge_day_tmp)
            df_recharge = pd.read_sql(sql_recharge, connection)

            for i in range(26):
                organic_day_tmp = str(today_datetime + datetime.timedelta(days=-(i + 2) * 7))
                organic_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(organic_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                first_day_tmp = str(today_datetime + datetime.timedelta(days=-(i + 1) * 7))
                first_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(first_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                last_day_tmp = str(today_datetime + datetime.timedelta(days=-i * 7))
                last_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(last_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                date_tmp = (first_day_tmp[0:10] if len(first_day_tmp) > 10 else first_day_tmp) + '~' + str(datetime.date(*tuple(parse(last_day_tmp).timetuple())[:3]) + datetime.timedelta(days=-1))

                sql_all_android = 'SELECT device_id,country,language, platform, app_id FROM opsmanage_push_client_activate WHERE (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s" and device_id not in (SELECT device_id FROM  opsmanage_push_appsflyer WHERE  db_time_t >= "%s" and db_time_t < "%s")' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_day_tmp_t, last_day_tmp_t, organic_day_tmp_t, last_day_tmp_t)
                df_all_android = pd.read_sql(sql_all_android, connection)
                df_all_android['channel'] = 'Organic'
                df_all_android['sub1_channel'] = 'None'
                df_all_android['sub2_channel'] = 'None'
                df_all_android['sub3_channel'] = 'None'
                sql_appsf_android = 'SELECT device_id,country_code as country,channel,sub1_channel, sub2_channel, sub3_channel, language, platform, app_id FROM opsmanage_push_appsflyer WHERE  (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s"' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_day_tmp_t, last_day_tmp_t)
                df_appsf_android = pd.read_sql(sql_appsf_android, connection)
                df_concat_android = pd.concat([df_appsf_android, df_all_android], ignore_index=True)
                df_concat_android = df_concat_android.drop_duplicates('device_id')
                df_grouped = pd.merge(df_recharge, df_concat_android, how='inner', on=['device_id'])
                df_grouped = df_grouped.groupby(['device_id','country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", "platform","app_id"], as_index=False)

                df_concat_all = df_grouped.fillna(0)
                df_concat_all['date'] = date_tmp
                df_concat_all['week_index'] = i + 1
                if df_concat_all.index.tolist():
                    df_list = np.array(df_concat_all).tolist()

                    for li in df_list:
                        keyargs = dict(
                            list(zip(['device_id', "app_id", 'channel','country',   "language",'platform','sub1_channel', "sub2_channel", "sub3_channel",
                                   'date', 'week_index'], li)))
                        querysetlist.append(Week_Charge_Analysis(**keyargs))
        Week_Charge_Analysis.objects.bulk_create(querysetlist)

    except Exception as e:
        print(e)

def cal_roi_week_up(today_datetime):
    try:
        first_charge_day_tmp = str(today_datetime + datetime.timedelta(days=-7))
        last_charge_day_tmp = str(today_datetime)

        # recharge account
        gameversionls = Game_Version_Config.objects.all()
        querysetlist = []
        for gamecfg in gameversionls:
            sql_recharge_account_android = 'SELECT device_id,SUM(recharge) FROM opsmanage_push_client_recharge WHERE  (app_id="%s" or app_id="%s") and createtime >= "%s" and createtime < "%s" GROUP BY (device_id)' % (
                gamecfg.ios_appid, gamecfg.android_appid,first_charge_day_tmp, last_charge_day_tmp)
            df_recharge_account_android = pd.read_sql(sql_recharge_account_android, connection)

            for i in range(26):
                organic_day_tmp = str(today_datetime + datetime.timedelta(days=-(i + 2) * 7 ))
                organic_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(organic_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                first_day_tmp = str(today_datetime + datetime.timedelta(days=-(i + 1) * 7 ))
                first_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(first_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                last_day_tmp = str(today_datetime + datetime.timedelta(days=-i * 7))
                last_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(last_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                date_tmp = (first_day_tmp[0:10] if len(first_day_tmp) > 10 else first_day_tmp) + '~' + str(datetime.date(*tuple(parse(last_day_tmp).timetuple())[:3]) + datetime.timedelta(days=-1))

                sql_recharge_android = 'SELECT DISTINCT(device_id) FROM opsmanage_push_client_recharge WHERE (app_id="%s" or app_id="%s") and createtime >= "%s" and createtime < "%s"' % (
                    gamecfg.ios_appid, gamecfg.android_appid, first_charge_day_tmp, last_charge_day_tmp)
                df_recharge_android = pd.read_sql(sql_recharge_android, connection)
                # recharge count
                df_recharge_android['num_' + str(i + 1)] = 1

                sql_all_android = 'SELECT device_id,country,language, platform, app_id FROM opsmanage_push_client_activate WHERE (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s" and device_id not in (SELECT device_id FROM  opsmanage_push_appsflyer WHERE  db_time_t >= "%s" and db_time_t < "%s")' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_day_tmp_t, last_day_tmp_t, organic_day_tmp_t, last_day_tmp_t)
                df_all_android = pd.read_sql(sql_all_android, connection)
                df_all_android['channel'] = 'Organic'
                df_all_android['sub1_channel'] = 'None'
                df_all_android['sub2_channel'] = 'None'
                df_all_android['sub3_channel'] = 'None'
                sql_appsf_android = 'SELECT device_id,country_code as country,channel,sub1_channel, sub2_channel, sub3_channel, language, platform, app_id FROM opsmanage_push_appsflyer WHERE  (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s"' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_day_tmp_t, last_day_tmp_t)
                df_appsf_android = pd.read_sql(sql_appsf_android, connection)
                df_concat_android = pd.concat([df_appsf_android, df_all_android], ignore_index=True)
                df_concat_android = df_concat_android.drop_duplicates('device_id')
                df_concat_android_num = df_concat_android.join(df_recharge_android.set_index('device_id'), on='device_id')
                df_concat_android_account = df_concat_android.join(df_recharge_account_android.set_index('device_id'), on='device_id')
                df_concat_android['size'] = 1
                df_grouped_android = df_concat_android.groupby(['country', 'channel', "sub1_channel", "sub2_channel", "sub3_channel", "language", "platform", "app_id"], as_index=False).sum()
                df_concat_android_num = df_concat_android_num.drop('device_id', axis=1)
                df_grouped_android_num = df_concat_android_num.groupby(['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", "platform", "app_id"],as_index=False).sum()
                df_concat_android_account = df_concat_android_account.drop('device_id', axis=1)
                df_grouped_android_account = df_concat_android_account.groupby(['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", "platform", "app_id"], as_index=False).sum()

                if i == 0:
                    df_concat_all = df_grouped_android.fillna(0)
                    df_concat_all['date'] = date_tmp
                    if df_concat_all.index.tolist():
                        df_list = np.array(df_concat_all).tolist()
                        # querysetlist = []
                        for li in df_list:
                            keyargs = dict(
                                list(zip(['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", 'platform',"app_id", 'new_activate', 'date', ], li)))
                            querysetlist.append(Roi_Week(**keyargs))
                        # Roi_Week.objects.bulk_create(querysetlist)

                df_concat_all = df_grouped_android_num.fillna(0)
                df_concat_all['date'] = date_tmp
                if df_concat_all.index.tolist():
                    df_list = np.array(df_concat_all).tolist()
                    # querysetlist = []
                    for li in df_list:
                        keyargs = dict(
                            list(zip(['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", 'platform',"app_id", 'num_' + str(i + 1), 'date'],
                                li)))
                        querysetlist.append(Roi_Week(**keyargs))
                    # Roi_Week.objects.bulk_create(querysetlist)

                df_concat_all = df_grouped_android_account.fillna(0)
                df_concat_all['date'] = date_tmp
                if df_concat_all.index.tolist():
                    df_list = np.array(df_concat_all).tolist()
                    # querysetlist = []
                    for li in df_list:
                        keyargs = dict(
                            list(zip(['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", 'platform',"app_id", 'week_' + str(i + 1), 'date'],
                                li)))
                        querysetlist.append(Roi_Week(**keyargs))
        Roi_Week.objects.bulk_create(querysetlist)

    except Exception as e:
        print(e)

def roi_month_up():
    todaydate = datetime.date.today()
    cal_roi_month_up(todaydate + datetime.timedelta(days=-1))
    charge_month_lost_cal(todaydate + datetime.timedelta(days=-1))

def cal_roi_month_up(todaydate):
    try:
        today = str(todaydate)
        now_month = today[0:7] + '-01'

        first_day_tmp = str(todaydate - relativedelta(months=1))
        first_month = first_day_tmp[0:7] + '-01'
        gameversionls = Game_Version_Config.objects.all()
        querysetlist = []
        for gamecfg in gameversionls:
            for i in range(24):
                first_day_tmp = str(todaydate - relativedelta(months=i + 1))
                organic_day_tmp = str(todaydate - relativedelta(months=i + 1) + datetime.timedelta(days=- 7))
                organic_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(organic_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                last_day_tmp = str(todaydate - relativedelta(months= i))


                month = first_day_tmp[0:7]
                first_day_tmp = month + "-01"
                first_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(first_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                end_month = last_day_tmp[0:7]
                last_day_tmp = end_month + "-01"
                last_day_tmp_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(last_day_tmp).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                date_tmp = month

                # recharge count
                sql_recharge_android = 'SELECT DISTINCT(device_id) FROM opsmanage_push_client_recharge WHERE (app_id="%s" or app_id="%s") and createtime >= "%s" and createtime <= "%s"' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_month, now_month)
                df_recharge_android = pd.read_sql(sql_recharge_android, connection)
                df_recharge_android['num_' + str(i + 1)] = 1

                # recharge account
                sql_recharge_account_android = 'SELECT device_id,SUM(recharge) FROM opsmanage_push_client_recharge WHERE (app_id="%s" or app_id="%s") and createtime >= "%s" and createtime < "%s" GROUP BY (device_id)' % (
                    gamecfg.ios_appid, gamecfg.android_appid, first_month, now_month)
                df_recharge_account_android = pd.read_sql(sql_recharge_account_android, connection)

                # sql_all_android = 'SELECT device_id,country,language, platform, app_id FROM opsmanage_push_client_activate WHERE db_time >= "%s" and db_time < "%s"' % (
                #     first_day_tmp, last_day_tmp)
                sql_all_android = 'SELECT device_id,country,language, platform, app_id FROM opsmanage_push_client_activate WHERE (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s" and device_id not in (SELECT device_id FROM  opsmanage_push_appsflyer WHERE  db_time_t >= "%s" and db_time_t < "%s")' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_day_tmp_t, last_day_tmp_t, organic_day_tmp_t, last_day_tmp_t)
                df_all_android = pd.read_sql(sql_all_android, connection)
                df_all_android['channel'] = 'Organic'
                df_all_android['sub1_channel'] = 'None'
                df_all_android['sub2_channel'] = 'None'
                df_all_android['sub3_channel'] = 'None'
                sql_appsf_android = 'SELECT device_id,country_code as country,channel,sub1_channel, sub2_channel, sub3_channel, language, platform, app_id FROM opsmanage_push_appsflyer WHERE (app_id="%s" or app_id="%s") and  db_time_t >= "%s" and db_time_t < "%s"' % (
                    gamecfg.ios_appid, gamecfg.android_appid,first_day_tmp_t, last_day_tmp_t)
                df_appsf_android = pd.read_sql(sql_appsf_android, connection)
                df_concat_android = pd.concat([df_appsf_android, df_all_android], ignore_index=True)
                df_concat_android = df_concat_android.drop_duplicates('device_id')
                df_concat_android_num = df_concat_android.join(df_recharge_android.set_index('device_id'), on='device_id')
                df_concat_android_account = df_concat_android.join(df_recharge_account_android.set_index('device_id'),
                                                                   on='device_id')
                df_concat_android['size'] = 1
                df_grouped_android = df_concat_android.groupby(
                    ['country', 'channel','sub1_channel', "sub2_channel", "sub3_channel", "language", "platform", "app_id"], as_index=False).sum()
                df_concat_android_num = df_concat_android_num.drop('device_id', axis=1)
                df_grouped_android_num = df_concat_android_num.groupby(
                    ['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", "platform", "app_id"], as_index=False).sum()
                df_concat_android_account = df_concat_android_account.drop('device_id', axis=1)
                df_grouped_android_account = df_concat_android_account.groupby(
                    ['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", "platform", "app_id"], as_index=False).sum()

                if i == 0:
                    df_concat_all = df_grouped_android.fillna(0)
                    df_concat_all['date'] = date_tmp
                    if df_concat_all.index.tolist():
                        df_list = np.array(df_concat_all).tolist()
                        # querysetlist = []
                        for li in df_list:
                            keyargs = dict(list(zip(
                                ['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", 'platform', "app_id", 'new_activate', 'date'],
                                li)))
                            querysetlist.append(Roi_Month(**keyargs))

                df_concat_all = df_grouped_android_num.fillna(0)
                df_concat_all['date'] = date_tmp
                if df_concat_all.index.tolist():
                    df_list = np.array(df_concat_all).tolist()
                    # querysetlist = []
                    for li in df_list:
                        keyargs = dict(list(zip(
                            ['country', 'channel','sub1_channel', "sub2_channel", "sub3_channel", "language", 'platform', "app_id", 'num_' + str(i + 1),
                             'date'], li)))
                        querysetlist.append(Roi_Month(**keyargs))

                df_concat_all = df_grouped_android_account.fillna(0)
                df_concat_all['date'] = date_tmp
                if df_concat_all.index.tolist():
                    df_list = np.array(df_concat_all).tolist()
                    # querysetlist = []
                    for li in df_list:
                        keyargs = dict(list(zip(
                            ['country', 'channel', 'sub1_channel', "sub2_channel", "sub3_channel", "language", 'platform', "app_id", 'month_' + str(i + 1),
                             'date'], li)))
                        querysetlist.append(Roi_Month(**keyargs))
        Roi_Month.objects.bulk_create(querysetlist)

    except Exception as e:
        print(e)


def remain_activate_data(beginDate, endData, platform):
    apps_filter_day = str(datetime.datetime.strptime(beginDate, "%Y-%m-%d") + datetime.timedelta(days=-7))[:10]
    # 获取某日的client新增激活数据
    sql_new_client_android = 'SELECT device_id,country as country_code,language, app_id, source FROM opsmanage_push_client_activate WHERE platform = "%s" and db_time >= "%s" and db_time <= "%s"' % (
        platform, beginDate, endData)
    df_new_client_android = pd.read_sql(sql_new_client_android, connection)
    # 获取在某时间段的client新增激活数据但不在这时间段有appsflyer记录的设备
    sql_no_client_android = 'SELECT a.device_id,a.country as country_code,a.language,a.app_id,a.source FROM opsmanage_push_client_activate as a, opsmanage_push_appsflyer as b WHERE a.platform = "%s" and a.device_id=b.device_id and a.db_time >= "%s" and a.db_time <= "%s" and b.db_time < "%s" and b.db_time >= "%s"' % (
        platform, beginDate, endData, beginDate, apps_filter_day)
    df_no_client_android = pd.read_sql(sql_no_client_android, connection)
    df_outer_android = pd.concat([df_new_client_android, df_no_client_android], ignore_index=True)
    df_outer_android = df_outer_android.drop_duplicates('device_id', keep=False)
    df_new_client_android = df_outer_android
    # 获取某日的appsflyer新增激活数据
    sql_new_appsf_android = 'SELECT device_id, language as back_language, source as back_source, app_id as back_app_id, country_code as country, channeltype, channel, sub1_channel, sub2_channel, sub3_channel FROM opsmanage_push_appsflyer WHERE platform = "%s" and db_time >= "%s" and db_time < "%s"' % (
        platform, beginDate, endData)
    df_new_appsf_android = pd.read_sql(sql_new_appsf_android, connection)
    # 取2者合集
    df_inner_android = pd.merge(df_new_client_android, df_new_appsf_android, on=['device_id'])
    # 取合集和客户端上报数据的合集，去重
    df_new_client_android['channel'] = 'Organic'
    df_new_client_android['channeltype'] = 'Organic'
    df_concat_android = pd.concat([df_inner_android, df_new_client_android], ignore_index=True)
    df_concat_android = df_concat_android.drop_duplicates('device_id')
    # df_concat_android.fillna("None", inplace=True)
    # 取前者和appsflyer的并集，去重
    df_final_android = pd.concat([df_concat_android, df_new_appsf_android], ignore_index=True)
    df_final_android = df_final_android.drop_duplicates('device_id')
    df_final_android.fillna({"country": df_final_android["country_code"]}, inplace=True)
    df_final_android.fillna({"language": df_final_android["back_language"]}, inplace=True)
    df_final_android.fillna({"app_id": df_final_android["back_app_id"]}, inplace=True)
    df_final_android.fillna({"source": df_final_android["back_source"]}, inplace=True)

    df_final_android.fillna("None", inplace=True)
    return df_final_android


def remain_day_data(date_today):
    date_yesterday = date_today + datetime.timedelta(days=(-1))
    str_today = str(date_today)[:10]
    str_yesterday = str(date_yesterday)[:10]
    # 获取昨日的激活数据
    sql_activate_android = 'SELECT distinct(device_id) FROM opsmanage_push_client_activate_all WHERE db_time >= "%s" and db_time < "%s" AND platform="Android"' % (
        str_yesterday, str_today)
    df_activate_android = pd.read_sql(sql_activate_android, connection)
    sql_activate_ios = 'SELECT distinct(device_id) FROM opsmanage_push_client_activate_all WHERE db_time >= "%s" and db_time < "%s" AND platform ="ios"' % (
        str_yesterday, str_today)
    df_activate_ios = pd.read_sql(sql_activate_ios, connection)
    # 计算前30日的留存
    print("for remain")
    rangelst = [i for i in range(30)]
    rangelst.extend([35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90])
    for i in rangelst:
        r_yesterday = str(date_today + datetime.timedelta(days=-(i + 1)))[:10]
        r_today = str(date_today + datetime.timedelta(days=-(i + 0)))[:10]

        df_final_android = remain_activate_data(r_yesterday, r_today, "Android")
        # 取今日留存
        df_remain_android = pd.merge(df_final_android, df_activate_android, on=['device_id'])

        df_remain_android['new_size'] = 1
        df_grouped_android_new_tmp = df_remain_android.groupby(
            ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel', 'sub3_channel'],
            as_index=False).sum()
        df_grouped_android_new_tmp['platform'] = 'Android'
        # IOS
        df_final_ios = remain_activate_data(r_yesterday, r_today, "ios")
        # 取今日留存
        df_remain_ios = pd.merge(df_final_ios, df_activate_ios, on=['device_id'])

        df_remain_ios['new_size'] = 1
        df_grouped_ios_new_tmp = df_remain_ios.groupby(
            ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel', 'sub3_channel'],
            as_index=False).sum()
        df_grouped_ios_new_tmp['platform'] = 'IOS'

        df_concat_all = pd.concat([df_grouped_android_new_tmp, df_grouped_ios_new_tmp], ignore_index=True)
        df_concat_all['date'] = r_yesterday

        if i == 0:
            print("for remain 0")
            df_new_android = df_final_android
            df_new_android['new_size'] = 1
            df_new_group_android = df_new_android.groupby(
                ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                 'sub3_channel'],
                as_index=False).sum()
            df_new_group_android['platform'] = 'Android'
            df_new_ios = df_final_ios
            df_new_ios['new_size'] = 1
            df_new_group_ios = df_new_ios.groupby(
                ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                 'sub3_channel'],
                as_index=False).sum()
            df_new_group_ios['platform'] = 'IOS'
            df_concat_new_all = pd.concat([df_new_group_android, df_new_group_ios], ignore_index=True)
            df_concat_new_all['date'] = r_yesterday
            # 量级统计
            report_day_quantity(r_yesterday, df_new_android, df_new_ios)

            if df_concat_new_all.index.tolist():
                df_list = np.array(df_concat_new_all).tolist()
                for li in df_list:
                    try:
                        keyargs = dict(list(zip(
                            ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                             'sub3_channel', 'new_activate', 'platform', 'date'], li)))
                        Remain_Day.objects.update_or_create(**keyargs)
                    except Exception as e:
                        print(e)
                        continue
        else:
            print(("for remain %r" % i))
            if df_concat_all.index.tolist():
                df_list = np.array(df_concat_all).tolist()
                for li in df_list:
                    try:
                        remainStr = ''
                        if i < 30:
                            remainStr = 'remain_' + str(1 + i)
                        if i > 30:
                            remainStr = 'remain_' + str(i)
                        keyargs = dict(list(zip(
                            ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                             'sub3_channel', remainStr, 'platform', 'date'], li)))
                        remainCnt = keyargs.get(remainStr)
                        del keyargs[remainStr]
                        obj = Remain_Day.objects.filter(**keyargs)
                        updateData = {remainStr: remainCnt}
                        obj.update(**updateData)
                    except Exception as e:
                        print(e)
                        continue

def remain_day_up():
    # 计时
    t0 = time.clock()
    # 每日定时任务入口
    try:
        # 渠道分析
        print("channel_analysis_day_timer")
        channel_analysis_day_timer()
        # 前日日报更新
        print("update_day_report")
        update_day_report()
        # 计算昨日广告花费
        print("calc_ad_cost")
        calc_ad_cost()
        try:
            up_online_remain_up(datetime.date.today())
        except Exception as e:
            print(e)
            print('error: up_online_remain_up')

        yesterday = str(datetime.date.today() + datetime.timedelta(days=(-1)))
        today = str(datetime.date.today())
        # 日报统计
        print("report_data")
        report_data(yesterday, today, today, yesterday, Report_Day)
        # 获取昨日的激活数据
        remain_day_data(datetime.date.today())

        print("roi_day")
        roi_day_up()
    except Exception as e:
        print(e)
    print(time.clock()- t0 ,'seconds process time: remain_day_up')

def channel_analysis_day_timer():
    now = datetime.date.today()
    data_channel_analysis_day_up(now)


def data_channel_analysis_day_up(now):
    try:
        cursor = connection.cursor()

        today = str(now)
        yesterday = str(now + datetime.timedelta(days=-1))
        beforeyesterday = str(now + datetime.timedelta(days=-2))
        lastweekday = str(now + datetime.timedelta(days=-8))
        endDate = str(now)

        querysetlist = []
        gameversionls = Game_Version_Config.objects.all()
        for gamecfg in gameversionls:
            for i in range(2):
                # 重新计算前天数据
                if i == 1:
                    # 删除前天的表数据
                    sql_del = 'DELETE FROM opsmanage_channel_analysis_day WHERE date="%s" and (app_id="%s" or app_id="%s")'%(beforeyesterday, gamecfg.ios_appid, gamecfg.android_appid)
                    cursor.execute(sql_del)

                    today = yesterday
                    yesterday = beforeyesterday
                    # lastweekday = str(now +  - relativedelta(months=+1))
                    lastweekday = str(now + datetime.timedelta(days=-9))

                yesterday_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(yesterday).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())
                yesterday_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(yesterday_t))

                today_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(today).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                lastweekday_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(lastweekday).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                endDate_t = time.mktime(pytz.timezone(gamecfg.zone).localize(
                    datetime.datetime(*tuple(parse(endDate).timetuple())[:-2])).astimezone(
                    pytz.timezone('Europe/Moscow')).timetuple())

                sql_all = 'SELECT device_id,country,language, platform, app_id FROM opsmanage_push_client_activate WHERE  (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s" AND device_id not in (SELECT device_id FROM opsmanage_push_appsflyer WHERE db_time_t >= "%s" AND db_time_t < "%s")' % (
                gamecfg.ios_appid, gamecfg.android_appid,yesterday_t, today_t, lastweekday_t, today_t)
                df_all = pd.read_sql(sql_all, connection)
                df_all['channel'] = 'Organic'
                df_all['sub1_channel'] = 'None'
                df_all['sub2_channel'] = 'None'
                df_all['sub3_channel'] = 'None'

                # if Channel_Analysis_Day.objects.filter(date=yesterday).count() > 0:
                #     continue

                if Channel_Analysis_Day.objects.filter(date=yesterday,app_id=gamecfg.ios_appid).count() > 0 and Channel_Analysis_Day.objects.filter(date=yesterday,app_id=gamecfg.android_appid).count() > 0:
                    continue
                # Facebook.instagram
                # andriod
                sql_appsf = 'SELECT device_id as device_id,language ,country_code as country, channel,platform, sub1_channel, sub2_channel, sub3_channel, app_id FROM opsmanage_push_appsflyer WHERE  (app_id="%s" or app_id="%s") and db_time_t >= "%s" and db_time_t < "%s"' % (
                    gamecfg.ios_appid, gamecfg.android_appid,yesterday_t, today_t)
                df_appsf = pd.read_sql(sql_appsf, connection)
                df_appsf = df_appsf.drop_duplicates('device_id')

                #充值
                sql_char_android = 'SELECT b.device_id as device_id,c.language as language, b.recharge as recharge, c.country_code as country,c.channel as channel,c.platform as platform, c.sub1_channel as sub1_channel, c.sub2_channel as sub2_channel, c.sub3_channel as sub3_channel  FROM opsmanage_push_client_recharge as b, opsmanage_push_appsflyer as c ' \
                                   'WHERE  (b.app_id="%s" or b.app_id="%s") and (c.app_id="%s" or c.app_id="%s") and c.device_id = b.device_id' % (gamecfg.ios_appid, gamecfg.android_appid, gamecfg.ios_appid, gamecfg.android_appid)
                df_char_android = pd.read_sql(sql_char_android, connection)

                sql_char_android_organic = 'SELECT a.device_id as device_id, a.language as language,b.recharge as recharge, a.country as country, a.platform as platform  FROM opsmanage_push_client_activate as a, opsmanage_push_client_recharge as b WHERE (a.app_id="%s" or a.app_id="%s") and (b.app_id="%s" or b.app_id="%s") ' \
                                           'and a.device_id = b.device_id and b.device_id not in (select device_id from opsmanage_push_appsflyer )' % (gamecfg.ios_appid, gamecfg.android_appid, gamecfg.ios_appid, gamecfg.android_appid)

                df_char_android_organic = pd.read_sql(sql_char_android_organic, connection)
                df_char_android_organic['channel'] = 'Organic'
                # df_char_android_organic['platform'] = 'android'
                df_char_android_organic['sub1_channel'] = 'None'
                df_char_android_organic['sub2_channel'] = 'None'
                df_char_android_organic['sub3_channel'] = 'None'

                sql_char_android = pd.concat([df_char_android, df_char_android_organic], ignore_index=True)
                df_grouped_char_all = sql_char_android.groupby(['country', 'channel', "sub1_channel", "sub2_channel", "sub3_channel", "platform", "language"],as_index=False).sum()

                sql_re_android = 'SELECT b.device_id as device_id, c.language as language,c.country_code as country,c.channel as channel,c.platform as platform, c.sub1_channel as sub1_channel, c.sub2_channel as sub2_channel, c.sub3_channel as sub3_channel, c.app_id as app_id ' \
                                 'FROM opsmanage_push_client_reg as b, opsmanage_push_appsflyer as c WHERE c.device_id = b.device_id and b.db_time_t >= "%s" and b.db_time_t < "%s" and c.db_time_t >= "%s" and c.db_time_t < "%s" and  (c.app_id="%s" or c.app_id="%s") and (b.app_id="%s" or b.app_id="%s")' % (
                    yesterday_t, endDate_t,yesterday_t, today_t, gamecfg.ios_appid, gamecfg.android_appid, gamecfg.ios_appid, gamecfg.android_appid)
                df_re_android = pd.read_sql(sql_re_android, connection)
                df_re_android = df_re_android.drop_duplicates('device_id')

                sql_re_android_organic = 'SELECT b.device_id as device_id, a.language as language,a.country as country, a.platform as platform, a.app_id as app_id FROM opsmanage_push_client_activate as a,opsmanage_push_client_reg as b ' \
                                         'WHERE a.device_id = b.device_id and b.device_id not in (select device_id from opsmanage_push_appsflyer WHERE  db_time_t >= "%s" and db_time_t < "%s") and b.db_time_t >= "%s" and b.db_time_t < "%s" ' \
                                         'and a.db_time_t >= "%s" and a.db_time_t < "%s" and  (a.app_id="%s" or a.app_id="%s") and (b.app_id="%s" or b.app_id="%s")' % (
                                                lastweekday_t, endDate_t,yesterday_t, endDate_t, yesterday_t, today_t, gamecfg.ios_appid, gamecfg.android_appid, gamecfg.ios_appid, gamecfg.android_appid)
                df_re_android_organic = pd.read_sql(sql_re_android_organic, connection)
                df_re_android_organic = df_re_android_organic.drop_duplicates('device_id')
                df_re_android_organic['channel'] = 'Organic'
                # df_re_android_organic['platform'] = 'android'
                df_re_android_organic['sub1_channel'] = 'None'
                df_re_android_organic['sub2_channel'] = 'None'
                df_re_android_organic['sub3_channel'] = 'None'

                df_re_android = pd.concat([df_re_android,df_re_android_organic], ignore_index=True)
                df_re_android = df_re_android.drop_duplicates('device_id')
                df_re_android["re_size"] = 1
                df_grouped_re_all = df_re_android.groupby(['country', 'channel', "sub1_channel", "sub2_channel", "sub3_channel", 'platform', "language", "app_id"],as_index=False).sum()

                sql_onl_android= 'SELECT b.device_id as device_id, c.language as language,c.country_code as country,c.channel as channel, c.platform as platform,c.sub1_channel as sub1_channel, c.sub2_channel as sub2_channel, c.sub3_channel as sub3_channel, c.app_id as app_id FROM opsmanage_push_client_online as b, opsmanage_push_appsflyer as c' \
                                 ' WHERE c.device_id = b.device_id and b.db_time_t >= "%s" and b.db_time_t < "%s" and c.db_time_t >= "%s" and c.db_time_t < "%s" and  (c.app_id="%s" or c.app_id="%s") and (b.app_id="%s" or b.app_id="%s")' % (
                    yesterday_t, endDate_t,yesterday_t, today_t, gamecfg.ios_appid, gamecfg.android_appid, gamecfg.ios_appid, gamecfg.android_appid)
                df_re_onl_android = pd.read_sql(sql_onl_android, connection)
                df_re_onl_android = df_re_onl_android.drop_duplicates('device_id')

                sql_onl_android_organic = 'SELECT a.device_id as device_id,a.language as language, a.country as country, a.platform as platform, a.app_id as app_id FROM opsmanage_push_client_activate as a,opsmanage_push_client_online as b' \
                                          ' WHERE a.device_id = b.device_id and  (a.app_id="%s" or a.app_id="%s") and (b.app_id="%s" or b.app_id="%s") and b.device_id not in (select device_id from opsmanage_push_appsflyer ' \
                                          'WHERE  db_time_t >= "%s" and db_time_t < "%s") and b.db_time_t >= "%s" and b.db_time_t < "%s" and a.db_time_t >= "%s" and a.db_time_t < "%s"' % (
                    gamecfg.ios_appid, gamecfg.android_appid, gamecfg.ios_appid, gamecfg.android_appid,lastweekday_t, endDate_t, yesterday_t, endDate_t, yesterday_t, today_t)
                df_re_onl_android_organic = pd.read_sql(sql_onl_android_organic, connection)
                df_re_onl_android_organic = df_re_onl_android_organic.drop_duplicates('device_id')
                df_re_onl_android_organic['channel'] = 'Organic'
                # df_re_onl_android_organic['platform'] = 'android'
                df_re_onl_android_organic['sub1_channel'] = 'None'
                df_re_onl_android_organic['sub2_channel'] = 'None'
                df_re_onl_android_organic['sub3_channel'] = 'None'

                df_re_onl_android = pd.concat([df_re_onl_android, df_re_onl_android_organic],ignore_index=True)
                df_re_onl_android = df_re_onl_android.drop_duplicates('device_id')
                df_re_onl_android["re_onl_size"] = 1
                df_grouped_re_onl_all = df_re_onl_android.groupby(['country', 'channel', "sub1_channel", "sub2_channel", "sub3_channel", 'platform', "language","app_id"],as_index=False).sum()

                df_group_onlre_all = pd.merge(df_grouped_re_onl_all, df_grouped_re_all, how='outer',
                                              on=['country', 'channel', "sub1_channel", "sub2_channel", "sub3_channel",
                                                  'platform', "language", "app_id"])
                # df_group_onlre_all = df_grouped_re_all.join(df_grouped_re_onl_all, on=['country', 'channel', "sub1_channel", "sub2_channel", "sub3_channel",'platform', "language"])
                # if df_grouped_char_all.empty:
                df_group_onl_re_char_all = df_group_onlre_all
                df_group_onl_re_char_all["recharge"] = 0
                # else:
                #     df_group_onl_re_char_all = pd.merge(df_grouped_char_all, df_group_onlre_all, how='outer',on=['country', 'channel', "sub1_channel", "sub2_channel","sub3_channel", 'platform', "language"])

                df_concat_android = pd.concat([df_appsf, df_all], ignore_index=True)
                df_concat_android = df_concat_android.drop_duplicates('device_id')
                df_concat_android['size'] = 1
                df_concat_android.fillna("None")
                df_grouped_android = df_concat_android.groupby(
                    ['country', 'channel', "sub1_channel", "sub2_channel", "sub3_channel", 'platform', "language","app_id"],
                    as_index=False).sum()
                if not df_grouped_android.empty:
                    df_group_analy_andriod_all = pd.merge(df_grouped_android, df_group_onl_re_char_all, how='outer',
                                                      on=['country', 'channel', "sub1_channel", "sub2_channel", "sub3_channel",
                                                          'platform', "language","app_id"])

                    df_group_analy_andriod_all['date'] = yesterday

                    df_group_analy_andriod_all = df_group_analy_andriod_all.fillna(0)
                    if df_group_analy_andriod_all.index.tolist():
                        df_list = np.array(df_group_analy_andriod_all).tolist()

                        for i in df_list:
                            # language = i[6]
                            # url = "https://translate.google.com/m?hl=en&sl=auto&tl=en&ie=UTF-8&prev=_m&q={}".format(language)
                            # ret = requests.post(url)
                            # findstart = ret.text.find('<div dir="ltr" class="t0">', 0, len(ret.text))
                            # if findstart == -1:
                            #     findstart = ret.text.find('<div dir="ltr" class="t0">', 0, len(ret.text))
                            # endindex = ret.text.find("</div>", findstart, len(ret.text))
                            # languageinEnglish = ret.text[findstart + len('<div dir="ltr" class="t0">'):endindex]
                            if Ad_Channel.objects.filter(channel=i[1]).count() == 0:
                                AddNewChannel(i[1])
                            if Language_Version_Config.objects.filter(language=i[6]).count() == 0:
                                Language_Version_Config.objects.create(
                                    language=i[6],
                                    name=""
                            )
                            querysetlist.append(
                                Channel_Analysis_Day(country=i[0], channel=i[1], sub1_channel=i[2], sub2_channel=i[3],
                                                     sub3_channel=i[4], language=i[6], app_id=i[7], new_activate=i[8], new_device_onl=i[9],
                                                     new_device_reg=i[10], recharge_amount=i[11], platform=i[5],  date=i[12]))
        Channel_Analysis_Day.objects.bulk_create(querysetlist)
    except Exception as e:
        import traceback
        errormsg = traceback.format_exc()
        print(errormsg)
        print(e)


def AddNewChannel(media_source):
    '''
    渠道增加
    :param media_source:
    :return:
    '''
    if media_source.startswith("Facebook") or media_source.startswith("google") or media_source.startswith("Twitter") \
    or media_source.startswith("Instagram") or media_source.startswith("Snapchat"):
        Ad_Channel.objects.create(
            channel=media_source,
            type="Non-Organic"
        )
    else:
        if media_source.endswith("_nonint"):
            channleType = str("Non-Organic")
        elif media_source.endswith("_int"):
            channleType = str("Non-Organic")
        elif media_source.endswith("_share"):
            channleType = str("Non-Organic")
        else:
            channleType = str("Organic")
        Ad_Channel.objects.create(
            channel=media_source,
            type=channleType
        )


def report_day_quantity(date, df_final_android, df_final_ios):
    GCC6_Countrys = []
    try:
        adconf = Area_Config.objects.select_related().get(name='GCC6')
        gcc6 = adconf.countrys
        GCC6_Countrys = gcc6.split('/')
    except Exception as e:
        print(e)

    df_concat_new_all = pd.concat([df_final_android, df_final_ios], ignore_index=True)
    df_unique_appid = df_concat_new_all.drop_duplicates(['app_id'])
    for idx in df_unique_appid.index:
        app_id = df_unique_appid.loc[idx]['app_id']
        try:
            df_android = df_final_android.loc[df_final_android['app_id'] == app_id]
            if not df_android.empty:
                quantity(app_id, "Android", df_android, GCC6_Countrys, date)
            df_ios = df_final_ios.loc[df_final_ios['app_id'] == app_id]
            if not df_ios.empty:
                quantity(app_id, "IOS", df_ios, GCC6_Countrys, date)
        except Exception as e:
            print(e)


def calc_ad_cost():
    try:
        yesterday = str(datetime.date.today() + datetime.timedelta(days=(-1)))[:10]
        today = str(datetime.date.today())[:10]
        ad_cost_cfgs = Ad_Cost_Conf.objects.filter(date=yesterday)

        sql = 'select distinct device_id, app_id, country_code as country, language, channel from opsmanage_push_appsflyer where db_time >="%s" and db_time < "%s"' % (
            yesterday, today)
        df = pd.read_sql(sql, connection)
        df['new_size'] = 1
        df_group = df.groupby(['app_id', 'channel', 'language', 'country'], as_index=False).sum()


        for ad_cost_cfg in ad_cost_cfgs:
            if ad_cost_cfg.ad_cost != 0 or ad_cost_cfg.cpi == 0:
                continue

            # sql = 'select device_id as cnt from opsmanage_push_appsflyer where app_id = "%s" and country_code = "%s" and language = "%s" and channel = "%s" and db_time >="%s" and db_time < "%s"' % (
            # ad_cost_cfg.app_id, ad_cost_cfg.country, ad_cost_cfg.language, ad_cost_cfg.channel, yesterday, today)
            # df = pd.read_sql(sql, connection)
            # cnt = df.count()['cnt']
            count = df_group.loc[
                (df_group['app_id'] == ad_cost_cfg.app_id) & (df_group['channel'] == ad_cost_cfg.channel) & (
                            df_group['language'] == ad_cost_cfg.language) & (df_group['country'] == ad_cost_cfg.country)]['new_size'].sum()
            cnt = 0 if np.isnan(count) else count
            cost = cnt * ad_cost_cfg.cpi
            ad_cost_cfg.ad_cost = cost
            ad_cost_cfg.save()
    except Exception as e:
        print(e)


def quantity(app_id, platform, df_report, GCC6_Countrys, date):
    # 新增激活数
    db_dict = {}
    db_dict['app_id'] = app_id
    db_dict['platform'] = platform
    new_activate_cnt = df_report['new_size'].sum()
    db_dict['new_activate'] = 0 if np.isnan(new_activate_cnt) else new_activate_cnt
    na_activate_cnt = df_report[df_report['channeltype'] == 'Organic']['new_size'].sum()
    db_dict['na_activate'] = 0 if np.isnan(na_activate_cnt) else na_activate_cnt
    ad_activate_cnt = db_dict['new_activate'] - db_dict['na_activate']
    db_dict['ad_activate'] = 0 if np.isnan(ad_activate_cnt) else ad_activate_cnt

    incent_cnt = df_report[df_report['channeltype'] == 'Incent']['new_size'].sum()
    db_dict['incent'] = 0 if np.isnan(incent_cnt) else incent_cnt
    non_incent_cnt = df_report[df_report['channeltype'] == 'Non-incent']['new_size'].sum()
    db_dict['non_incent'] = 0 if np.isnan(non_incent_cnt) else non_incent_cnt
    share_cnt = df_report[df_report['channeltype'] == 'Share']['new_size'].sum()
    db_dict['share'] = 0 if np.isnan(share_cnt) else share_cnt

    df_gcc_android = df_report[df_report['country'].isin(GCC6_Countrys)]
    df_not_gcc_android = df_report[-df_report['country'].isin(GCC6_Countrys)]

    gcc_incent_cnt = df_gcc_android[df_gcc_android['channeltype'] == 'Incent']['new_size'].sum()
    db_dict['gcc_incent'] = 0 if np.isnan(gcc_incent_cnt) else gcc_incent_cnt
    gcc_non_incent_cnt = df_gcc_android[df_gcc_android['channeltype'] == 'Non-incent']['new_size'].sum()
    db_dict['gcc_non_incent'] = 0 if np.isnan(gcc_non_incent_cnt) else gcc_non_incent_cnt
    gcc_organic_cnt = df_gcc_android[df_gcc_android['channeltype'] == 'Organic']['new_size'].sum()
    db_dict['gcc_organic'] = 0 if np.isnan(gcc_organic_cnt) else gcc_organic_cnt
    gcc_share_cnt = df_gcc_android[df_gcc_android['channeltype'] == 'Share']['new_size'].sum()
    db_dict['gcc_share'] = 0 if np.isnan(gcc_share_cnt) else gcc_share_cnt

    no_gcc_incent_cnt = df_not_gcc_android[df_not_gcc_android['channeltype'] == 'Incent']['new_size'].sum()
    db_dict['no_gcc_incent'] = 0 if np.isnan(no_gcc_incent_cnt) else no_gcc_incent_cnt
    no_gcc_non_incent_cnt = df_not_gcc_android[df_not_gcc_android['channeltype'] == 'Non-incent']['new_size'].sum()
    db_dict['no_gcc_non_incent'] = 0 if np.isnan(no_gcc_non_incent_cnt) else no_gcc_non_incent_cnt
    no_gcc_organic_cnt = df_not_gcc_android[df_not_gcc_android['channeltype'] == 'Organic']['new_size'].sum()
    db_dict['no_gcc_organic'] = 0 if np.isnan(no_gcc_organic_cnt) else no_gcc_organic_cnt
    no_gcc_share_cnt = df_not_gcc_android[df_not_gcc_android['channeltype'] == 'Share']['new_size'].sum()
    db_dict['no_gcc_share'] = 0 if np.isnan(no_gcc_share_cnt) else no_gcc_share_cnt
    db_dict['date'] = date

    Report_Quantity.objects.update_or_create(**db_dict)


def report_real_data_handle(begin_time, end_time, platform):
    # 获取某日的client新增激活数据
    sql_new_client = 'SELECT device_id,country as country_code,language,app_id FROM opsmanage_push_client_activate WHERE platform = "%s" and db_time >= "%s" and db_time <= "%s"' % (
        platform, begin_time, end_time)
    print(sql_new_client)
    df_new_client = pd.read_sql(sql_new_client, connection)
    print(df_new_client)
    # 获取某日的appsflyer新增激活数据
    sql_new_appsf = 'SELECT device_id, language as back_language, country_code as country, channel, app_id as back_app_id FROM opsmanage_push_appsflyer WHERE platform = "%s" and db_time >= "%s" and db_time < "%s"' % (
        platform, begin_time, end_time)
    print(sql_new_appsf)
    df_new_appsf = pd.read_sql(sql_new_appsf, connection)
    print(df_new_appsf)
    # 取2者合集
    df_inner = pd.merge(df_new_client, df_new_appsf, on=['device_id'])
    # 取合集和客户端上报数据的合集，去重
    df_new_client['channel'] = 'Organic'
    df_concat = pd.concat([df_inner, df_new_client], ignore_index=True)
    df_concat = df_concat.drop_duplicates('device_id')
    df_concat.fillna("None", inplace=True)
    # 取前者和appsflyer的并集，去重
    df_final = pd.concat([df_concat, df_new_appsf], ignore_index=True)
    df_final = df_final.drop_duplicates('device_id')
    df_final.fillna({"country": df_final["country_code"]}, inplace=True)
    df_final.fillna({"language": df_final["back_language"]}, inplace=True)
    df_final.fillna({"app_id": df_final["back_app_id"]}, inplace=True)
    print(df_final)
    return df_final


def report_new_reg(platform, app_id, language, source, beginDate, endDate, countEndDate):
    apps_filter_day = str(datetime.datetime.strptime(beginDate, "%Y-%m-%d") + datetime.timedelta(days=-7))[:10]

    sql_reg_activate = 'SELECT distinct a.device_id FROM opsmanage_push_client_activate as a, opsmanage_push_client_reg as b WHERE (a.platform = "%s") and (a.app_id = "%s") and (a.language = "%s") and (a.source = "%s") and a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s" and b.db_time >= "%s" and b.db_time < "%s"' % (
        platform, app_id, language, source, beginDate, endDate, beginDate, countEndDate)
    df_reg_activate = pd.read_sql(sql_reg_activate, connection)

    # 获取在某时间段的client新增激活数据但不在这时间段有appsflyer记录的设备
    sql_no_client = 'SELECT a.device_id FROM opsmanage_push_client_activate as a, opsmanage_push_appsflyer as b WHERE a.device_id=b.device_id and (a.platform = "%s") and (a.app_id = "%s") and (a.language = "%s") and (a.source = "%s") and a.db_time >= "%s" and a.db_time <= "%s" and b.db_time < "%s" and b.db_time >= "%s"' % (
        platform, app_id, language, source, beginDate, endDate, beginDate, apps_filter_day)
    df_no_client = pd.read_sql(sql_no_client, connection)
    no_device_list = df_no_client['device_id'].tolist()
    df_outer = df_reg_activate.drop(df_reg_activate.loc[df_reg_activate['device_id'].isin(no_device_list)].index)
    # df_outer = pd.concat([df_reg_activate, df_no_client], ignore_index=True)
    # df_outer = df_outer.drop_duplicates('device_id', keep=False)
    df_reg_activate = df_outer

    df_reg_activate['size'] = 1
    sql_reg_appsflyer = 'SELECT distinct a.device_id FROM opsmanage_push_appsflyer as a, opsmanage_push_client_reg as b WHERE (a.platform = "%s") and (a.app_id = "%s") and (a.language = "%s") and (a.source = "%s") and a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s" and b.db_time >= "%s" and b.db_time < "%s"' % (
        platform, app_id, language, source, beginDate, endDate, beginDate, countEndDate)
    df_reg_appsflyer = pd.read_sql(sql_reg_appsflyer, connection)
    df_reg_appsflyer['size'] = 1
    df_reg = pd.concat([df_reg_activate, df_reg_appsflyer], ignore_index=True)
    df_reg = df_reg.drop_duplicates('device_id')
    reg_cnt = df_reg['size'].sum()
    # if beginDate == '2018-12-23' and countEndDate == '2018-12-25':
    #     device_list = df_reg['device_id'].tolist()
    #     reg_file = open(r'F:\reg.txt', 'a+')
    #     for device in device_list:
    #         reg_file.writelines(device + "\n")
    #     reg_file.close()
    return 0 if np.isnan(reg_cnt) else reg_cnt


def report_new_online(platform, app_id, language, source, beginDate, endDate, countEndDate):
    apps_filter_day = str(datetime.datetime.strptime(beginDate, "%Y-%m-%d") + datetime.timedelta(days=-7))[:10]

    sql_online_activate = 'SELECT distinct a.device_id FROM opsmanage_push_client_activate as a, opsmanage_push_client_online as b WHERE (a.platform = "%s") and (a.app_id = "%s") and (a.language = "%s") and (a.source = "%s") and a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s" and b.db_time >= "%s" and b.db_time < "%s"' % (
        platform, app_id, language, source, beginDate, endDate, beginDate, countEndDate)
    df_online_activate = pd.read_sql(sql_online_activate, connection)

    # 获取在某时间段的client新增激活数据但不在这时间段有appsflyer记录的设备
    sql_no_client = 'SELECT a.device_id FROM opsmanage_push_client_activate as a, opsmanage_push_appsflyer as b WHERE a.device_id=b.device_id and (a.platform = "%s") and (a.app_id = "%s") and (a.language = "%s") and (a.source = "%s") and a.db_time >= "%s" and a.db_time <= "%s" and b.db_time < "%s" and b.db_time >= "%s"' % (
        platform, app_id, language, source, beginDate, endDate, beginDate, apps_filter_day)
    df_no_client = pd.read_sql(sql_no_client, connection)
    no_device_list = df_no_client['device_id'].tolist()
    df_outer = df_online_activate.drop(df_online_activate.loc[df_online_activate['device_id'].isin(no_device_list)].index)
    # df_outer = pd.concat([df_online_activate, df_no_client], ignore_index=True)
    # df_outer = df_outer.drop_duplicates('device_id', keep=False)
    df_online_activate = df_outer

    df_online_activate['size'] = 1
    sql_online_appsflyer = 'SELECT distinct a.device_id FROM opsmanage_push_appsflyer as a, opsmanage_push_client_online as b WHERE (a.platform = "%s") and (a.app_id = "%s") and (a.language = "%s") and (a.source = "%s") and a.device_id = b.device_id and a.db_time >= "%s" and a.db_time < "%s" and b.db_time >= "%s" and b.db_time < "%s"' % (
        platform, app_id, language, source, beginDate, endDate, beginDate, countEndDate)
    df_online_appsflyer = pd.read_sql(sql_online_appsflyer, connection)
    df_online_appsflyer['size'] = 1
    df_online = pd.concat([df_online_activate, df_online_appsflyer], ignore_index=True)
    df_online = df_online.drop_duplicates('device_id')
    online_cnt = df_online['size'].sum()
    # if beginDate == '2018-12-23' and countEndDate == '2018-12-25':
    #     device_list = df_online['device_id'].tolist()
    #     online_file = open(r'F:\online.txt', 'a+')
    #     for device in device_list:
    #         online_file.writelines(device + "\n")
    #     online_file.close()
    return 0 if np.isnan(online_cnt) else online_cnt


def report_remain(platform, app_id, language, source, beginDate, endDate):
    sql_remain = 'SELECT app_id, language, new_activate, remain_2 FROM opsmanage_remain_day WHERE (platform = "%s") and (app_id = "%s") and (language = "%s") and (source = "%s") and date >= "%s" and date < "%s" ' % (
        platform, app_id, language, source, beginDate, endDate)
    df_remain = pd.read_sql(sql_remain, connection)
    remain_cnt = df_remain['remain_2'].sum()
    return 0 if np.isnan(remain_cnt) else remain_cnt


def charge_from_game():
    today_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print (today_datetime)
    # 计算间隔
    OffSetStmap = 3600
    calChargeData(today_datetime[:-3], OffSetStmap)

def calChargeData(end_date, OffSetStmap):
    try:
        print ("calChargeData_start")
        import OpsManage.settings as settings
        local_db = MySQLdb.connect(host=settings.DATABASES['default']['HOST'], user=settings.DATABASES['default']['USER'], passwd=settings.DATABASES['default']['PASSWORD'],
                             db=settings.DATABASES['default']['NAME'], charset='utf8')
        gameversionls = Game_Version_Config.objects.all()
        for gamecfg in gameversionls:
            tmp_end_time = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M")
            if int(OffSetStmap) == 3600:
                tmp_last_time = (tmp_end_time + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H:%M:%S")
            else:
                tmp_last_time = (tmp_end_time + datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")

            tmp_end_time = str(tmp_end_time)
            end_time_timstamp = time.mktime(pytz.timezone('Europe/Moscow').localize(
                datetime.datetime(*tuple(parse(tmp_end_time).timetuple())[:-2])).astimezone(
                pytz.timezone(gamecfg.zone)).timetuple())
            end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time_timstamp))
            last_time_timestamp = time.mktime(pytz.timezone('Europe/Moscow').localize(
                datetime.datetime(*tuple(parse(tmp_last_time).timetuple())[:-2])).astimezone(
                pytz.timezone(gamecfg.zone)).timetuple())
            last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_time_timestamp))

            db = MySQLdb.connect(host=gamecfg.game_host, user=gamecfg.game_user, passwd=gamecfg.game_pwd,
                                 db=gamecfg.game_dbname, charset='utf8')
            cursor = db.cursor()
            sql = 'SELECT playerid,accountid, deviceid, type, goodId, id, param,currencyCode,price, total_dollar,createtime, purchasetime  From shop_order \
                      WHERE status = 1 AND createtime <= "%s" AND createtime > "%s"' % (end_time, last_time)
            print(sql)
            cursor.execute(sql)
            charge_data = cursor.fetchall()
            if charge_data:
                for data in charge_data:
                    player_id = data[0]
                    user_id = data[1]
                    device_id = data[2]
                    platformtype = data[3]
                    good_id = data[4]
                    order_id = data[5]
                    param= data[6]
                    localcurrency_type = data[7]
                    local_recharge = float(data[8])
                    recharge = data[9]
                    db_time = data[10]
                    buytime = data[11]/1000
                    currency_type = 'USD'
                    paramdic = json.loads(param)
                    if platformtype == 0 or platformtype == 5:
                        packageDic = json.loads(paramdic['json'])
                        app_id = packageDic['packageName']
                    else:
                        app_item_idDic = paramdic['receipt']
                        app_id = 'id' + str(app_item_idDic['app_item_id'])
                    insertSql = "INSERT opsmanage_push_client_recharge ( `time`,`device_id`,`user_id`,`player_id`,`order_id`,`good_id`,`recharge`,`currency_type`,`local_recharge`,`localcurrency_type`,`app_id`,`createtime`) \
                                 VALUES (%d, '%s', '%s', '%s', '%s', '%s', %f,'%s', %f, '%s', '%s', '%s') " % (buytime, device_id, user_id, player_id, order_id, good_id, recharge, currency_type, local_recharge, localcurrency_type, app_id, db_time)
                    local_cursor = local_db.cursor()
                    try:
                        local_cursor.execute(insertSql)
                        local_db.commit()
                    except Exception as e:
                        print("calChargeData_end_insert")
                        print(e)

                db.close()
        local_db.close()
        print ("calChargeData_end")
    except Exception as e:
        print("calChargeData_end_error")
        print(e)

# def up_online_data_record(date_today):
#     '''
#     暂时不用
#     统计当天详细的上线数据，并记录在opsmanage_push_client_online_record 中
#     当天的上线数据：appsflyer 的广告量（Facebook Ads）+ activate的自然量(扣掉广告量（Facebook Ads）和appid为none的数据)
#     '''
#     try:
#         date_yesterday = date_today + datetime.timedelta(days=(-1))
#         beginDate = str(date_yesterday)[:10]
#         endData = str(date_today)[:10]
#         # 获取某日的client上线数据
#         sql_client_online_platform = 'SELECT device_id,app_id,country as country_code,language,platform FROM opsmanage_push_client_online WHERE app_id <> "None"  and db_time >= "%s" and db_time <= "%s"' % (
#             beginDate, endData)
#         df_client_online_platform = pd.read_sql(sql_client_online_platform, connection)
#         df_distinct_client_platform = df_client_online_platform.drop_duplicates('device_id')
#
#         # 获取某日的client激活数据
#         sql_new_active_platform = 'SELECT device_id,app_id,country as country_code,language,platform FROM opsmanage_push_client_activate WHERE app_id <> "None"  and db_time >= "%s" and db_time <= "%s"' % (
#             beginDate, endData)
#         df_new_active_platform = pd.read_sql(sql_new_active_platform, connection)
#
#         # 某日的appsf数据
#         sql_new_appsf_platform = 'SELECT device_id, channeltype, channel, sub1_channel, sub2_channel, sub3_channel FROM opsmanage_push_appsflyer WHERE install_time_selected_timezone >= "%s" and install_time_selected_timezone < "%s"' % (
#             beginDate, endData)
#         df_new_appsf_platform = pd.read_sql(sql_new_appsf_platform, connection)
#
#         df_organic_platform = pd.merge(df_distinct_client_platform, df_new_active_platform, on=['device_id','app_id','country_code','language','platform'])
#         df_organic_platform['channel'] = 'Organic'
#         df_organic_platform['channeltype'] = 'Organic'
#
#         # activate的自然量(扣掉广告量（Facebook Ads）和appid为none的数据)
#         df_online_platform = pd.merge(df_new_appsf_platform,df_distinct_client_platform,on=['device_id'])
#         df_active_platform = pd.merge(df_new_appsf_platform,df_new_active_platform,on=['device_id'])
#         df_inner_platform = pd.merge(df_online_platform,df_active_platform, on=['device_id','app_id','country_code','language','platform','channeltype','channel','sub1_channel','sub2_channel','sub3_channel'])
#
#         # appsflyer 的广告量（Facebook Ads）
#         sql_Facebook_Ads_platform = 'SELECT device_id,language as back_language, app_id as back_app_id, country_code as country, channeltype, channel, sub1_channel, sub2_channel, sub3_channel FROM opsmanage_push_appsflyer WHERE channel = "Facebook Ads"  and install_time_selected_timezone >= "%s" and install_time_selected_timezone < "%s"' % (
#             beginDate, endData)
#         df_Facebook_Ads_platform = pd.read_sql(sql_Facebook_Ads_platform, connection)
#         df_new_ads_platform = pd.merge(df_Facebook_Ads_platform, df_client_online_platform, on=['device_id'])
#
#         # 某日的总上线数据
#         df_final_platform = pd.concat([df_inner_platform, df_new_ads_platform], ignore_index=True)
#         df_final_platform = pd.concat([df_final_platform, df_organic_platform], ignore_index=True)
#         df_final_platform = df_final_platform.drop_duplicates('device_id')
#         df_final_platform.fillna({"country": df_final_platform["country_code"]}, inplace=True)
#         df_final_platform.fillna({"language": df_final_platform["back_language"]}, inplace=True)
#         df_final_platform.fillna({"app_id": df_final_platform["back_app_id"]}, inplace=True)
#
#         df_final_platform.fillna("None", inplace=True)
#
#         # 写入数据库
#         df_final_platform.drop(['country_code','back_language','back_app_id'], axis=1 ,inplace=True)
#         df_final_platform['date'] = beginDate
#         df_final_platform = pd.DataFrame(df_final_platform,
#                                          columns=['date', 'device_id', 'country', 'platform', 'language', 'app_id',
#                                                   'channeltype', 'channel', 'sub1_channel', 'sub2_channel', 'sub3_channel'])
#         link.create_connection(df_final_platform, 'abmanage', 'opsmanage_push_client_online_record')
#     except Exception as e:
#         print e


def up_online_remain_data(beginDate, endData, platform):
    """
    当天的上线数据：appsflyer 的广告量（Facebook Ads）+ activate的自然量(扣掉广告量（Facebook Ads）和appid为none的数据)
    """
    try:
        # 获取某日的client上线数据
        sql_client_online_platform = 'SELECT device_id,app_id,country as country_code,language FROM opsmanage_push_client_online WHERE app_id <> "None" and platform = "%s" and db_time >= "%s" and db_time <= "%s"' % (
            platform,beginDate, endData)
        df_client_online_platform = pd.read_sql(sql_client_online_platform, connection)
        df_distinct_client_platform = df_client_online_platform.drop_duplicates('device_id')

        # 获取某日的client激活数据
        sql_new_active_platform = 'SELECT device_id,app_id,country as country_code,language FROM opsmanage_push_client_activate WHERE app_id <> "None" and platform = "%s" and db_time >= "%s" and db_time <= "%s"' % (
            platform,beginDate, endData)
        df_new_active_platform = pd.read_sql(sql_new_active_platform, connection)

        # 某日的appsf数据
        sql_new_appsf_platform = 'SELECT device_id, channeltype, channel, sub1_channel, sub2_channel, sub3_channel FROM opsmanage_push_appsflyer WHERE  platform = "%s" and db_time >= "%s" and db_time < "%s"' % (
            platform, beginDate, endData)
        df_new_appsf_platform = pd.read_sql(sql_new_appsf_platform, connection)

        # activate的自然量(扣掉广告量（Facebook Ads）和appid为none的数据) = Organic数据 + 非Organic数据
        # 统计 Organic数据
        df_organic_platform = pd.merge(df_distinct_client_platform, df_new_active_platform,
                                       on=['device_id', 'app_id', 'country_code', 'language'])
        df_organic_platform['channel'] = 'Organic'
        df_organic_platform['channeltype'] = 'Organic'

        # 统计 非Organic数据
        df_online_platform = pd.merge(df_new_appsf_platform, df_distinct_client_platform, on=['device_id'])
        df_active_platform = pd.merge(df_new_appsf_platform, df_new_active_platform, on=['device_id'])
        df_inner_platform = pd.merge(df_online_platform, df_active_platform,
                                     on=['device_id', 'app_id', 'country_code', 'language', 'channeltype',
                                         'channel', 'sub1_channel', 'sub2_channel', 'sub3_channel'])

        # appsflyer 的广告量（Facebook Ads）
        sql_Facebook_Ads_platform = 'SELECT device_id,language as back_language, app_id as back_app_id, country_code as country, channeltype, channel, sub1_channel, sub2_channel, sub3_channel FROM opsmanage_push_appsflyer WHERE channel = "Facebook Ads" and platform = "%s" and db_time >= "%s" and db_time < "%s"' % (
            platform, beginDate, endData)
        df_Facebook_Ads_platform = pd.read_sql(sql_Facebook_Ads_platform, connection)
        df_new_ads_platform = pd.merge(df_Facebook_Ads_platform, df_client_online_platform, on=['device_id'])

        # 某日的总上线数据
        df_final_platform = pd.concat([df_inner_platform, df_new_ads_platform], ignore_index=True)
        df_final_platform = pd.concat([df_final_platform, df_organic_platform], ignore_index=True)
        df_final_platform = df_final_platform.drop_duplicates('device_id')
        df_final_platform.fillna({"country": df_final_platform["country_code"]}, inplace=True)
        df_final_platform.fillna({"language": df_final_platform["back_language"]}, inplace=True)
        df_final_platform.fillna({"app_id": df_final_platform["back_app_id"]}, inplace=True)

        df_final_platform.fillna("None", inplace=True)
        return df_final_platform
    except Exception as e:
        print(e)


def up_online_remain_up(date_today):
    begin = time.clock()
    try:
        date_yesterday = date_today + datetime.timedelta(days=(-1))
        str_today = str(date_today)[:10]
        str_yesterday = str(date_yesterday)[:10]
        # android
        sql_online_android = 'SELECT distinct(device_id) FROM opsmanage_push_client_online WHERE db_time >= "%s" and db_time < "%s" AND platform="android"' % (
            str_yesterday, str_today)
        df_online_android = pd.read_sql(sql_online_android, connection)
        # ios
        sql_online_ios = 'SELECT distinct(device_id) FROM opsmanage_push_client_online WHERE db_time >= "%s" and db_time < "%s" AND platform ="ios"' % (
            str_yesterday, str_today)
        df_online_ios = pd.read_sql(sql_online_ios, connection)
        # 计算前30日的留存
        rangelst = [i for i in range(30)]
        rangelst.extend([35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90])
        for i in rangelst:
            r_yesterday = str(date_today + datetime.timedelta(days=-(i + 1)))
            r_today = str(date_today + datetime.timedelta(days=-(i + 0)))

            # android 当天上线数
            df_final_android = up_online_remain_data(r_yesterday, r_today, "android")
            # android 取今日留存
            df_remain_android = pd.merge(df_final_android, df_online_android, on=['device_id'])
            df_remain_android['new_size'] = 1
            df_grouped_android_new_tmp = df_remain_android.groupby(
                ['country', 'app_id', 'language', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                 'sub3_channel'],
                as_index=False).sum()
            df_grouped_android_new_tmp['platform'] = 'Android'

            # IOS 当天上线数
            df_final_ios = up_online_remain_data(r_yesterday, r_today, "ios")
            # IOS 今日留存
            df_remain_ios = pd.merge(df_final_ios, df_online_ios, on=['device_id'])
            df_remain_ios['new_size'] = 1
            df_grouped_ios_new_tmp = df_remain_ios.groupby(
                ['country', 'app_id', 'language', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                 'sub3_channel'],
                as_index=False).sum()
            df_grouped_ios_new_tmp['platform'] = 'IOS'

            df_concat_all = pd.concat([df_grouped_android_new_tmp, df_grouped_ios_new_tmp], ignore_index=True)
            # 计算前些天的数据
            df_concat_all['date'] = r_yesterday
            if i == 0:
                df_new_android = df_final_android
                df_new_android['new_size'] = 1
                df_new_group_android = df_new_android.groupby(
                    ['country', 'app_id', 'language', 'channeltype', 'channel', 'sub1_channel',
                     'sub2_channel', 'sub3_channel'],
                    as_index=False).sum()
                df_new_group_android['platform'] = 'Android'

                df_new_ios = df_final_ios
                df_new_ios['new_size'] = 1
                df_new_group_ios = df_new_ios.groupby(
                    ['country', 'app_id', 'language', 'channeltype', 'channel', 'sub1_channel',
                     'sub2_channel', 'sub3_channel'],
                    as_index=False).sum()
                df_new_group_ios['platform'] = 'IOS'

                df_concat_new_all = pd.concat([df_new_group_android, df_new_group_ios], ignore_index=True)
                df_concat_new_all['date'] = r_yesterday
                print(('Record the data of day %s'% r_yesterday))

                if df_concat_new_all.index.tolist():
                    df_list = np.array(df_concat_new_all).tolist()
                    for li in df_list:
                        try:
                            keyargs = dict(list(zip(
                                ['country', 'app_id', 'language', 'channeltype', 'channel', 'sub1_channel',
                                 'sub2_channel', 'sub3_channel', 'new_online', 'platform', 'date'], li)))
                            Up_Online_Remain.objects.update_or_create(**keyargs)
                        except Exception as e:
                            print(e)
                            continue
            else:
                if df_concat_all.index.tolist():
                    df_list = np.array(df_concat_all).tolist()
                    for li in df_list:
                        try:
                            remainStr = ''
                            if i < 30:
                                remainStr = 'remain_' + str(1 + i)
                            if i > 30:
                                remainStr = 'remain_' + str(i)
                            keyargs = dict(list(zip(
                                ['country', 'app_id', 'language', 'channeltype', 'channel', 'sub1_channel',
                                 'sub2_channel', 'sub3_channel', remainStr, 'platform', 'date'], li)))
                            remainCnt = keyargs.get(remainStr)
                            del keyargs[remainStr]
                            obj = Up_Online_Remain.objects.filter(**keyargs)
                            updateData = {remainStr: remainCnt}
                            obj.update(**updateData)
                        except Exception as e:
                            print(e)
                            continue

    except Exception as e:
        print(e)
    print(("%r seconds process time :up_online_remain_up"%(time.clock() - begin)))


def Channel_assortment():
    todaydate = str(datetime.date.today())
    channel_assortment_cal(todaydate)

def channel_assortment_cal(date):
    '''
    渠道分类
    :return:
    '''
    # pass
    try:
        todaydate = datetime.datetime.strptime(date, "%Y-%m-%d")
        yesterday = str(todaydate + datetime.timedelta(days=-1))
        # todaydate = str(todaydate) + ' 00:00:00'
        # yesterday += ' 00:00:00'
        sql_appsf = 'SELECT device_id as device_id,language ,country_code as country, channel,platform, sub1_channel, sub2_channel, sub3_channel, app_id, db_time as db_time FROM opsmanage_push_appsflyer WHERE  db_time >= "%s" and db_time < "%s"' % (
            yesterday, todaydate)
        df_appsf = pd.read_sql(sql_appsf, connection)
        #
        sql_active = 'SELECT device_id,country,language, platform, app_id, db_time FROM opsmanage_push_client_activate WHERE db_time >= "%s" and db_time < "%s" AND device_id not in (SELECT device_id FROM opsmanage_push_appsflyer WHERE db_time >= "%s" AND db_time < "%s")' % (
            yesterday, todaydate, yesterday, todaydate)
        df_active = pd.read_sql(sql_active, connection)
        df_active['channel'] = 'Organic'
        df_active['sub1_channel'] = 'None'
        df_active['sub2_channel'] = 'None'
        df_active['sub3_channel'] = 'None'
        #
        df_all = pd.concat([df_appsf, df_active], ignore_index=True)
        df_all = df_all.drop_duplicates('device_id')
        # andriod
        if df_all.index.tolist():
            df_list = np.array(df_all).tolist()
            querysetlist = []
            for i in df_list:
                try:
                    Channel_All.objects.create(app_id=i[0], channel=i[1],country=i[2], db_time=i[3], device_id=i[4], language=i[5], platform=i[6], sub1_channel=i[7], sub2_channel=i[8],
                                             sub3_channel=i[9])
                except Exception as e:
                    print ('channel_assortment_sql_insert_error')
                    print(e)
    except Exception as e:
        print ('channel_assortment_cal_error')
        print(e)


# 来自运营的分析需求($%&&*^*@&^*^@&#*&$)
def recharge_remain_activate_data(beginDate, endData, platform):
    # 获取充值设备ID
    sql_recharge_device = 'SELECT distinct(device_id) FROM opsmanage_push_client_recharge'
    df_recharge_dev = pd.read_sql(sql_recharge_device, connection)

    apps_filter_day = str(datetime.datetime.strptime(beginDate, "%Y-%m-%d") + datetime.timedelta(days=-7))[:10]
    # 获取某日的client新增激活数据
    sql_new_client_android = 'SELECT device_id,country as country_code,language, app_id FROM opsmanage_push_client_activate WHERE platform = "%s" and db_time >= "%s" and db_time <= "%s"' % (
        platform, beginDate, endData)
    df_new_client_android_tmp = pd.read_sql(sql_new_client_android, connection)
    df_new_client_android = pd.merge(df_new_client_android_tmp, df_recharge_dev, on=['device_id'])
    # 获取在某时间段的client新增激活数据但不在这时间段有appsflyer记录的设备
    sql_no_client_android = 'SELECT a.device_id,a.country as country_code,a.language,a.app_id FROM opsmanage_push_client_activate as a, opsmanage_push_appsflyer as b WHERE a.platform = "%s" and a.device_id=b.device_id and a.db_time >= "%s" and a.db_time <= "%s" and b.db_time < "%s" and b.db_time >= "%s"' % (
        platform, beginDate, endData, beginDate, apps_filter_day)
    df_no_client_android_tmp = pd.read_sql(sql_no_client_android, connection)
    df_no_client_android = pd.merge(df_no_client_android_tmp, df_recharge_dev, on=['device_id'])

    df_outer_android = pd.concat([df_new_client_android, df_no_client_android], ignore_index=True)
    df_outer_android = df_outer_android.drop_duplicates('device_id', keep=False)
    df_new_client_android = df_outer_android
    # 获取某日的appsflyer新增激活数据
    sql_new_appsf_android = 'SELECT device_id, language as back_language, app_id as back_app_id, country_code as country, channeltype, channel, sub1_channel, sub2_channel, sub3_channel FROM opsmanage_push_appsflyer WHERE platform = "%s" and db_time >= "%s" and db_time < "%s"' % (
        platform, beginDate, endData)
    df_new_appsf_android_tmp = pd.read_sql(sql_new_appsf_android, connection)
    df_new_appsf_android = pd.merge(df_new_appsf_android_tmp, df_recharge_dev, on=['device_id'])
    # 取2者合集
    df_inner_android = pd.merge(df_new_client_android, df_new_appsf_android, on=['device_id'])
    # 取合集和客户端上报数据的合集，去重
    df_new_client_android['channel'] = 'Organic'
    df_new_client_android['channeltype'] = 'Organic'
    df_concat_android = pd.concat([df_inner_android, df_new_client_android], ignore_index=True)
    df_concat_android = df_concat_android.drop_duplicates('device_id')
    # df_concat_android.fillna("None", inplace=True)
    # 取前者和appsflyer的并集，去重
    df_final_android = pd.concat([df_concat_android, df_new_appsf_android], ignore_index=True)
    df_final_android = df_final_android.drop_duplicates('device_id')
    df_final_android.fillna({"country": df_final_android["country_code"]}, inplace=True)
    df_final_android.fillna({"language": df_final_android["back_language"]}, inplace=True)
    df_final_android.fillna({"app_id": df_final_android["back_app_id"]}, inplace=True)

    df_final_android.fillna("None", inplace=True)
    return df_final_android


def recharge_remain_day_data(date_today):
    date_yesterday = date_today + datetime.timedelta(days=(-1))
    str_today = str(date_today)[:10]
    str_yesterday = str(date_yesterday)[:10]
    # 获取充值设备ID
    sql_recharge_device = 'SELECT distinct(device_id) FROM opsmanage_push_client_recharge'
    df_recharge_dev = pd.read_sql(sql_recharge_device, connection)
    # 获取昨日的激活数据
    sql_activate_android = 'SELECT distinct(device_id) FROM opsmanage_push_client_activate_all WHERE db_time >= "%s" and db_time < "%s" AND platform="Android"' % (
        str_yesterday, str_today)
    df_activate_android_tmp = pd.read_sql(sql_activate_android, connection)
    df_activate_android = pd.merge(df_activate_android_tmp, df_recharge_dev, on=['device_id'])
    sql_activate_ios = 'SELECT distinct(device_id) FROM opsmanage_push_client_activate_all WHERE db_time >= "%s" and db_time < "%s" AND platform ="ios"' % (
        str_yesterday, str_today)
    df_activate_ios_tmp = pd.read_sql(sql_activate_ios, connection)
    df_activate_ios = pd.merge(df_activate_ios_tmp, df_recharge_dev, on=['device_id'])
    # 计算前30日的留存
    print("for remain")
    for i in range(30):
        r_yesterday = str(date_today + datetime.timedelta(days=-(i + 1)))[:10]
        r_today = str(date_today + datetime.timedelta(days=-(i + 0)))[:10]

        df_final_android = recharge_remain_activate_data(r_yesterday, r_today, "Android")
        # 取今日留存
        df_remain_android = pd.merge(df_final_android, df_activate_android, on=['device_id'])

        df_remain_android['new_size'] = 1
        df_grouped_android_new_tmp = df_remain_android.groupby(
            ['app_id', 'country', 'language', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel', 'sub3_channel'],
            as_index=False).sum()
        df_grouped_android_new_tmp['platform'] = 'Android'
        # IOS
        df_final_ios = recharge_remain_activate_data(r_yesterday, r_today, "ios")
        # 取今日留存
        df_remain_ios = pd.merge(df_final_ios, df_activate_ios, on=['device_id'])

        df_remain_ios['new_size'] = 1
        df_grouped_ios_new_tmp = df_remain_ios.groupby(
            ['app_id', 'country', 'language', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel', 'sub3_channel'],
            as_index=False).sum()
        df_grouped_ios_new_tmp['platform'] = 'IOS'

        df_concat_all = pd.concat([df_grouped_android_new_tmp, df_grouped_ios_new_tmp], ignore_index=True)
        df_concat_all['date'] = r_yesterday

        if i == 0:
            print("for remain 0")
            df_new_android = df_final_android
            df_new_android['new_size'] = 1
            df_new_group_android = df_new_android.groupby(
                ['app_id', 'country', 'language', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                 'sub3_channel'],
                as_index=False).sum()
            df_new_group_android['platform'] = 'Android'
            df_new_ios = df_final_ios
            df_new_ios['new_size'] = 1
            df_new_group_ios = df_new_ios.groupby(
                ['app_id', 'country', 'language', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                 'sub3_channel'],
                as_index=False).sum()
            df_new_group_ios['platform'] = 'IOS'
            df_concat_new_all = pd.concat([df_new_group_android, df_new_group_ios], ignore_index=True)
            df_concat_new_all['date'] = r_yesterday

            if df_concat_new_all.index.tolist():
                df_list = np.array(df_concat_new_all).tolist()
                for li in df_list:
                    try:
                        keyargs = dict(list(zip(
                            ['app_id', 'country', 'language', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                             'sub3_channel', 'new_activate', 'platform', 'date'], li)))
                        Recharge_Remain_Day.objects.update_or_create(**keyargs)
                    except Exception as e:
                        print(e)
                        continue
        else:
            print(("for remain %r" % i))
            if df_concat_all.index.tolist():
                df_list = np.array(df_concat_all).tolist()
                for li in df_list:
                    try:
                        remainStr = 'remain_' + str(1 + i)
                        keyargs = dict(list(zip(
                            ['app_id', 'country', 'language', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                             'sub3_channel', remainStr, 'platform', 'date'], li)))
                        remainCnt = keyargs.get(remainStr)
                        del keyargs[remainStr]
                        obj = Recharge_Remain_Day.objects.filter(**keyargs)
                        updateData = {remainStr: remainCnt}
                        obj.update(**updateData)
                    except Exception as e:
                        print(e)
                        continue


def remain_month_up():
    todayDate = datetime.date.today()
    remain_month_data(todayDate + datetime.timedelta(days=-1))


def remain_month_data(date_today):
    ''' 月留存数据统计'''
    firMonth = str(date_today - relativedelta(months=1))[0:7] + '-01'
    nowMonth = str(date_today)[0:7] + '-01'
    # str_today = str(date_today)[:10]
    # str_yesterday = str(date_yesterday)[:10]
    # 获取上月的激活数据
    sql_activate_android = 'SELECT distinct(device_id) FROM opsmanage_push_client_activate_all WHERE db_time >= "%s" and db_time < "%s" AND platform="Android"' % (
        firMonth, nowMonth)
    df_activate_android = pd.read_sql(sql_activate_android, connection)
    sql_activate_ios = 'SELECT distinct(device_id) FROM opsmanage_push_client_activate_all WHERE db_time >= "%s" and db_time < "%s" AND platform ="ios"' % (
        firMonth, nowMonth)
    df_activate_ios = pd.read_sql(sql_activate_ios, connection)
    # 计算12月的留存
    print("for remain month")
    for i in range(12):
        # r_yesterday = str(date_today + datetime.timedelta(days=-(i + 1)))[:10]
        # r_today = str(date_today + datetime.timedelta(days=-(i + 0)))[:10]
        firtDayTmp = str(date_today - relativedelta(months=i + 1))
        lastDayTmp = str(date_today - relativedelta(months=i))
        dateTemp = firtDayTmp[0:7]
        r_firtMonth = firtDayTmp[0:7] + '-01'
        r_lastMonth = lastDayTmp[0:7] + '-01'

        # 取当月android留存
        df_final_android = remain_activate_data(r_firtMonth, r_lastMonth, "Android")
        df_remain_android = pd.merge(df_final_android, df_activate_android, on=['device_id'])

        df_remain_android['new_size'] = 1
        df_grouped_android_new_tmp = df_remain_android.groupby(
            ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel', 'sub3_channel'],
            as_index=False).sum()
        df_grouped_android_new_tmp['platform'] = 'Android'

        # 取当月ios留存
        df_final_ios = remain_activate_data(r_firtMonth, r_lastMonth, "ios")
        df_remain_ios = pd.merge(df_final_ios, df_activate_ios, on=['device_id'])

        df_remain_ios['new_size'] = 1
        df_grouped_ios_new_tmp = df_remain_ios.groupby(
            ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel', 'sub3_channel'],
            as_index=False).sum()
        df_grouped_ios_new_tmp['platform'] = 'IOS'

        df_concat_all = pd.concat([df_grouped_android_new_tmp, df_grouped_ios_new_tmp], ignore_index=True)
        df_concat_all['date'] = dateTemp

        if i == 0:
            print("for remain month 0")
            df_new_android = df_final_android
            df_new_android['new_size'] = 1
            df_new_group_android = df_new_android.groupby(
                ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                 'sub3_channel'],
                as_index=False).sum()
            df_new_group_android['platform'] = 'Android'
            df_new_ios = df_final_ios
            df_new_ios['new_size'] = 1
            df_new_group_ios = df_new_ios.groupby(
                ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                 'sub3_channel'],
                as_index=False).sum()
            df_new_group_ios['platform'] = 'IOS'
            df_concat_new_all = pd.concat([df_new_group_android, df_new_group_ios], ignore_index=True)
            df_concat_new_all['date'] = dateTemp
            # 量级统计
            report_day_quantity(r_firtMonth, df_new_android, df_new_ios)

            if df_concat_new_all.index.tolist():
                df_list = np.array(df_concat_new_all).tolist()
                for li in df_list:
                    try:
                        keyargs = dict(list(zip(
                            ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                             'sub3_channel', 'new_activate', 'platform', 'date'], li)))
                        Remain_Month.objects.update_or_create(**keyargs)
                    except Exception as e:
                        print(e)
                        continue
        else:
            print(("for remain month %r" % i))
            if df_concat_all.index.tolist():
                df_list = np.array(df_concat_all).tolist()
                for li in df_list:
                    try:
                        remainStr = 'remain_' + str(1 + i)
                        keyargs = dict(list(zip(
                            ['app_id', 'country', 'language', 'source', 'channeltype', 'channel', 'sub1_channel', 'sub2_channel',
                             'sub3_channel', remainStr, 'platform', 'date'], li)))
                        remainCnt = keyargs.get(remainStr)
                        del keyargs[remainStr]
                        obj = Remain_Month.objects.filter(**keyargs)
                        updateData = {remainStr: remainCnt}
                        obj.update(**updateData)
                    except Exception as e:
                        print(e)
                        continue
