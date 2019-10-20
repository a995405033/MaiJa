#!/usr/bin/env python
# -*- coding: utf-8 -*-


from OpsManage.models import *
from OpsManage.tasks import *
from OpsManage import cron
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from OpsManage.views.report import update_day_report
import datetime
from django.db import connection
import pandas as pd


@login_required()
def checking_computations(request):
    return render(request, 'aset/checking_computations.html', locals())


@login_required()
def report_day_data_check(request):
    if request.method == "POST":
        try:
            data = {}
            project = request.POST.get('project')
            date = request.POST.get('date')

            sg_conf = Game_Version_Config.objects.filter(name=project)
            if sg_conf.count() != 1:
                return
            ios_appid = sg_conf[0].ios_appid
            android_appid = sg_conf[0].android_appid
            year, mon, day = date.split('-')
            day = datetime.datetime(int(year), int(mon), int(day))
            next_day = day + datetime.timedelta(days=1)
            str_day = str(day)
            str_next_day = str(next_day)
            # activate激活数
            sql_activate_count = 'SELECT distinct device_id FROM opsmanage_push_client_activate WHERE (app_id = "%s" or app_id = "%s") and db_time >= "%s" and db_time <= "%s"' % (
                ios_appid, android_appid, str_day, str_next_day)
            df_activate_count = pd.read_sql(sql_activate_count, connection)
            activate_cnt = df_activate_count.count()["device_id"]
            # appsflyer激活数
            sql_appsflyer_count = 'SELECT distinct device_id FROM opsmanage_push_appsflyer WHERE (app_id = "%s" or app_id = "%s") and db_time >= "%s" and db_time <= "%s"' % (
                ios_appid, android_appid, str_day, str_next_day)
            df_appsflyer_count = pd.read_sql(sql_appsflyer_count, connection)
            appsflyer_cnt = df_appsflyer_count.count()["device_id"]
            # activate和appsflyer相同设备数
            df_inner_device = pd.merge(df_activate_count, df_appsflyer_count, on=['device_id'])
            same_deviceid_cnt = df_inner_device.count()["device_id"]
            # 日报激活数
            report_day_activate_cnt = activate_cnt + appsflyer_cnt - same_deviceid_cnt
            # appsflyer organic的数量
            sql_appsflyer_organic_count = 'SELECT distinct device_id FROM opsmanage_push_appsflyer WHERE (app_id = "%s" or app_id = "%s") and channeltype = "Organic" and db_time >= "%s" and db_time <= "%s"' % (
                ios_appid, android_appid, str_day, str_next_day)
            df_appsflyer_organic_count = pd.read_sql(sql_appsflyer_organic_count, connection)
            appsflyer_organic_cnt = df_appsflyer_organic_count.count()["device_id"]
            # 广告激活数
            report_day_ad_activate_cnt = appsflyer_cnt - appsflyer_organic_cnt
            # 上线人数
            sql_online_count = 'SELECT distinct device_id FROM opsmanage_push_client_online WHERE (app_id = "%s" or app_id = "%s") and db_time >= "%s" and db_time <= "%s"' % (
                ios_appid, android_appid, str_day, str_next_day)
            df_online_count = pd.read_sql(sql_online_count, connection)
            online_cnt = df_online_count.count()["device_id"]
            # 今日新设备上线数
            df_activate_all = pd.concat([df_activate_count, df_appsflyer_count], ignore_index=True)
            df_activate_all = df_activate_all.drop_duplicates('device_id')
            df_new_online = pd.merge(df_activate_all, df_online_count, on=['device_id'])
            new_online_cnt = df_new_online.count()["device_id"]
            # 注册人数
            sql_reg_count = 'SELECT distinct device_id FROM opsmanage_push_client_reg WHERE (app_id = "%s" or app_id = "%s") and db_time >= "%s" and db_time <= "%s"' % (
                ios_appid, android_appid, str_day, str_next_day)
            df_reg_count = pd.read_sql(sql_reg_count, connection)
            reg_cnt = df_reg_count.count()["device_id"]
            # 今日新设备注册数
            df_new_reg = pd.merge(df_activate_all, df_reg_count, on=['device_id'])
            new_reg_cnt = df_new_reg.count()["device_id"]
            # 充值设备
            sql_recharge_cnt = 'SELECT distinct device_id FROM opsmanage_push_client_recharge WHERE (app_id = "%s" or app_id = "%s") and db_time >= "%s" and db_time <= "%s"' % (
                ios_appid, android_appid, str_day, str_next_day)
            df_recharge_count = pd.read_sql(sql_recharge_cnt, connection)
            recharge_cnt = df_recharge_count.count()["device_id"]
            # 充值金额
            sql_recharge_sum = 'SELECT device_id, recharge FROM opsmanage_push_client_recharge WHERE (app_id = "%s" or app_id = "%s") and db_time >= "%s" and db_time <= "%s"' % (
                ios_appid, android_appid, str_day, str_next_day)
            df_recharge_sum = pd.read_sql(sql_recharge_sum, connection)
            recharge_sum = df_recharge_sum.sum()["recharge"]
            # 充值能分类统计的设备数
            sql_all_activate_device = 'SELECT distinct device_id FROM opsmanage_push_client_activate'
            df_all_activate_device = pd.read_sql(sql_all_activate_device, connection)
            df_detail_device = pd.merge(df_all_activate_device, df_recharge_count, on=['device_id'])
            detail_device_cnt = df_detail_device.count()["device_id"]
            # 当日付费设备
            df_today_recharge_cnt = pd.merge(df_activate_count, df_recharge_count, on=['device_id'])
            today_recharge_cnt = df_today_recharge_cnt.count()["device_id"]
            # 当日付费设备付费金额
            df_today_recharge_sum = pd.merge(df_activate_count, df_recharge_sum, on=['device_id'])
            today_recharge_sum = df_today_recharge_sum.sum()["recharge"]
            # 5.17-6.16特殊激活
            ## activate激活数
            sql_activate_count_sp = 'SELECT distinct device_id FROM opsmanage_push_client_activate WHERE (app_id = "%s" or app_id = "%s") and db_time >= "%s" and db_time <= "%s"' % (
                ios_appid, android_appid, "2019-05-17 00:00:00", "2019-06-17 00:00:00")
            df_activate_count_sp = pd.read_sql(sql_activate_count_sp, connection)
            ## appsflyer激活数
            sql_appsflyer_count_sp = 'SELECT distinct device_id FROM opsmanage_push_appsflyer WHERE (app_id = "%s" or app_id = "%s") and db_time >= "%s" and db_time <= "%s"' % (
                ios_appid, android_appid, "2019-05-17 00:00:00", "2019-06-17 00:00:00")
            df_appsflyer_count_sp = pd.read_sql(sql_appsflyer_count_sp, connection)
            df_all_base_activate_device = pd.concat([df_activate_count_sp, df_appsflyer_count_sp], ignore_index=True)
            df_all_base_activate_device = df_all_base_activate_device.drop_duplicates('device_id')
            ##ios
            sql_activate_all_count_ios = 'SELECT distinct device_id FROM opsmanage_push_client_activate_all WHERE app_id = "%s" and db_time >= "%s" and db_time <= "%s"' % (
                ios_appid, str_day, str_next_day)
            df_activate_all_count_ios = pd.read_sql(sql_activate_all_count_ios, connection)
            df_activate_all_device_ios = pd.merge(df_all_base_activate_device, df_activate_all_count_ios,
                                                  on=['device_id'])
            activate_all_device_ios_cnt = df_activate_all_device_ios.count()["device_id"]
            ##android
            sql_activate_all_count_android = 'SELECT distinct device_id FROM opsmanage_push_client_activate_all WHERE app_id = "%s" and db_time >= "%s" and db_time <= "%s"' % (
                android_appid, str_day, str_next_day)
            df_activate_all_count_android = pd.read_sql(sql_activate_all_count_android, connection)

            df_activate_all_device_android = pd.merge(df_all_base_activate_device, df_activate_all_count_android,
                                                  on=['device_id'])
            activate_all_device_android_cnt = df_activate_all_device_android.count()["device_id"]
            ##all
            activate_all_device_cnt = activate_all_device_ios_cnt + activate_all_device_android_cnt


        except Exception as e:
            print(e)

    return render(request, 'aset/checking_computations_result.html', locals())
