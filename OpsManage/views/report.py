#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import datetime
import numpy as np
import pandas as pd
import json
import pytz
from calendar import monthrange
from dateutil.parser import parse
from django.db import connection
from django.db.models import Sum
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse
from OpsManage.models import *
from OpsManage.tasks import *
from OpsManage import cron
from django.http import JsonResponse
from OpsManage.utils.common import SelectSqlStrFormat
import copy


def report_day_quantity():
    GCC6_Countrys = []
    try:
        adconf = Area_Config.objects.select_related().get(name='GCC6')
        gcc6 = adconf.countrys
        GCC6_Countrys = gcc6.split('/')
    except Exception as e:
        print(e)

    try:
        yday_be_time_date = datetime.datetime.replace(datetime.datetime.now() + datetime.timedelta(days=-1), hour=0,
                                                       minute=0, second=0)
        yday_af_time_date = datetime.datetime.replace(datetime.datetime.now(), hour=0, minute=0, second=0)

        yday_be_time_str = str(yday_be_time_date)[:10]
        yday_af_time_str = str(yday_af_time_date)[:10]

        # 获取某日的client新增激活数据
        sql_new_client_android = 'SELECT device_id,country FROM opsmanage_push_client_activate WHERE platform = "Android" and db_time >= "%s" and db_time <= "%s"' % (
            yday_be_time_str, yday_af_time_str)
        df_new_client_android = pd.read_sql(sql_new_client_android, connection)
        # 获取某日的appsflyer新增激活数据
        sql_new_appsf_android = 'SELECT device_id, country_code, channeltype FROM opsmanage_push_appsflyer WHERE platform = "android" and install_time_selected_timezone >= "%s" and install_time_selected_timezone < "%s"' % (
            yday_be_time_str, yday_af_time_str)
        df_new_appsf_android = pd.read_sql(sql_new_appsf_android, connection)
        # 取2者合集
        df_inner_android = pd.merge(df_new_client_android, df_new_appsf_android, on=['device_id'])
        # 取合集和客户端上报数据的合集，去重
        df_new_client_android['channeltype'] = 'Organic'
        df_concat_android = pd.concat([df_inner_android, df_new_client_android], ignore_index=True)
        df_concat_android = df_concat_android.drop_duplicates('device_id')
        df_concat_android.fillna("None", inplace=True)
        # 取前者和appsflyer的并集，去重
        df_final_android = pd.concat([df_concat_android, df_new_appsf_android], ignore_index=True)
        df_final_android = df_final_android.drop_duplicates('device_id')
        df_final_android.fillna({"country": df_final_android["country_code"]}, inplace=True)
        df_final_android['new_size'] = 1
        # 新增激活数
        android_dict = {}
        android_dict['platform'] = "Android"
        new_activate_cnt = df_final_android['new_size'].sum()
        android_dict['new_activate'] = 0 if np.isnan(new_activate_cnt) else new_activate_cnt
        na_activate_cnt = df_final_android[df_final_android['channeltype'] == 'Organic']['new_size'].sum()
        android_dict['na_activate'] = 0 if np.isnan(na_activate_cnt) else na_activate_cnt
        ad_activate_cnt = android_dict['new_activate'] - android_dict['na_activate']
        android_dict['ad_activate'] = 0 if np.isnan(ad_activate_cnt) else ad_activate_cnt

        incent_cnt = df_final_android[df_final_android['channeltype'] == 'Incent']['new_size'].sum()
        android_dict['incent'] = 0 if np.isnan(incent_cnt) else incent_cnt
        non_incent_cnt = df_final_android[df_final_android['channeltype'] == 'Non-incent']['new_size'].sum()
        android_dict['non_incent'] = 0 if np.isnan(non_incent_cnt) else non_incent_cnt
        share_cnt = df_final_android[df_final_android['channeltype'] == 'Share']['new_size'].sum()
        android_dict['share'] = 0 if np.isnan(share_cnt) else share_cnt

        df_gcc_android = df_final_android[df_final_android['country'].isin(GCC6_Countrys)]
        df_not_gcc_android = df_final_android[-df_final_android['country'].isin(GCC6_Countrys)]

        gcc_incent_cnt = df_gcc_android[df_gcc_android['channeltype'] == 'Incent']['new_size'].sum()
        android_dict['gcc_incent'] = 0 if np.isnan(gcc_incent_cnt) else gcc_incent_cnt
        gcc_non_incent_cnt = df_gcc_android[df_gcc_android['channeltype'] == 'Non-incent']['new_size'].sum()
        android_dict['gcc_non_incent'] = 0 if np.isnan(gcc_non_incent_cnt) else gcc_non_incent_cnt
        gcc_organic_cnt = df_gcc_android[df_gcc_android['channeltype'] == 'Organic']['new_size'].sum()
        android_dict['gcc_organic'] = 0 if np.isnan(gcc_organic_cnt) else gcc_organic_cnt
        gcc_share_cnt = df_gcc_android[df_gcc_android['channeltype'] == 'Share']['new_size'].sum()
        android_dict['gcc_share'] = 0 if np.isnan(gcc_share_cnt) else gcc_share_cnt

        no_gcc_incent_cnt = df_not_gcc_android[df_not_gcc_android['channeltype'] == 'Incent']['new_size'].sum()
        android_dict['no_gcc_incent'] = 0 if np.isnan(no_gcc_incent_cnt) else no_gcc_incent_cnt
        no_gcc_non_incent_cnt = df_not_gcc_android[df_not_gcc_android['channeltype'] == 'Non-incent']['new_size'].sum()
        android_dict['no_gcc_non_incent'] = 0 if np.isnan(no_gcc_non_incent_cnt) else no_gcc_non_incent_cnt
        no_gcc_organic_cnt = df_not_gcc_android[df_not_gcc_android['channeltype'] == 'Organic']['new_size'].sum()
        android_dict['no_gcc_organic'] = 0 if np.isnan(no_gcc_organic_cnt) else no_gcc_organic_cnt
        no_gcc_share_cnt = df_not_gcc_android[df_not_gcc_android['channeltype'] == 'Share']['new_size'].sum()
        android_dict['no_gcc_share'] = 0 if np.isnan(no_gcc_share_cnt) else no_gcc_share_cnt
        android_dict['date'] = yday_be_time_str[:10]

        # IOS
        # 获取某日的client新增激活数据
        sql_new_client_ios = 'SELECT device_id,country FROM opsmanage_push_client_activate WHERE platform = "ios" and db_time >= "%s" and db_time <= "%s"' % (
            yday_be_time_str, yday_af_time_str)
        df_new_client_ios = pd.read_sql(sql_new_client_ios, connection)
        # 获取某日的appsflyer新增激活数据
        sql_new_appsf_ios = 'SELECT device_id, country_code, channeltype FROM opsmanage_push_appsflyer WHERE platform = "ios" and install_time_selected_timezone >= "%s" and install_time_selected_timezone < "%s"' % (
            yday_be_time_str, yday_af_time_str)
        df_new_appsf_ios = pd.read_sql(sql_new_appsf_ios, connection)
        # 取2者合集
        df_inner_ios = pd.merge(df_new_client_ios, df_new_appsf_ios, on=['device_id'])
        # 取合集和客户端上报数据的合集，去重
        df_new_client_ios['channeltype'] = 'Organic'
        df_concat_ios = pd.concat([df_inner_ios, df_new_client_ios], ignore_index=True)
        df_concat_ios = df_concat_ios.drop_duplicates('device_id')
        df_concat_ios.fillna("None", inplace=True)
        # 取前者和appsflyer的并集，去重
        df_final_ios = pd.concat([df_concat_ios, df_new_appsf_ios], ignore_index=True)
        df_final_ios = df_final_ios.drop_duplicates('device_id')
        df_final_ios.fillna({"country": df_final_ios["country_code"]}, inplace=True)
        df_final_ios['new_size'] = 1
        # 新增激活数
        ios_dict = {}
        ios_dict['platform'] = "IOS"
        new_activate_cnt = df_final_ios['new_size'].sum()
        ios_dict['new_activate'] = 0 if np.isnan(new_activate_cnt) else new_activate_cnt
        na_activate_cnt = df_final_ios[df_final_ios['channeltype'] == 'Organic']['new_size'].sum()
        ios_dict['na_activate'] = 0 if np.isnan(na_activate_cnt) else na_activate_cnt
        ad_activate_cnt = ios_dict['new_activate'] - ios_dict['na_activate']
        ios_dict['ad_activate'] = 0 if np.isnan(ad_activate_cnt) else ad_activate_cnt

        incent_cnt = df_final_ios[df_final_ios['channeltype'] == 'Incent']['new_size'].sum()
        ios_dict['incent'] = 0 if np.isnan(incent_cnt) else incent_cnt
        non_incent_cnt = df_final_ios[df_final_ios['channeltype'] == 'Non-incent']['new_size'].sum()
        ios_dict['non_incent'] = 0 if np.isnan(non_incent_cnt) else non_incent_cnt
        share_cnt = df_final_ios[df_final_ios['channeltype'] == 'Share']['new_size'].sum()
        ios_dict['share'] = 0 if np.isnan(share_cnt) else share_cnt

        df_gcc_ios = df_final_ios[df_final_ios['country'].isin(GCC6_Countrys)]
        df_not_gcc_ios = df_final_ios[-df_final_ios['country'].isin(GCC6_Countrys)]

        gcc_incent_cnt = df_gcc_ios[df_gcc_ios['channeltype'] == 'Incent']['new_size'].sum()
        ios_dict['gcc_incent'] = 0 if np.isnan(gcc_incent_cnt) else gcc_incent_cnt
        gcc_non_incent_cnt = df_gcc_ios[df_gcc_ios['channeltype'] == 'Non-incent']['new_size'].sum()
        ios_dict['gcc_non_incent'] = 0 if np.isnan(gcc_non_incent_cnt) else gcc_non_incent_cnt
        gcc_organic_cnt = df_gcc_ios[df_gcc_ios['channeltype'] == 'Organic']['new_size'].sum()
        ios_dict['gcc_organic'] = 0 if np.isnan(gcc_organic_cnt) else gcc_organic_cnt
        gcc_share_cnt = df_gcc_ios[df_gcc_ios['channeltype'] == 'Share']['new_size'].sum()
        ios_dict['gcc_share'] = 0 if np.isnan(gcc_share_cnt) else gcc_share_cnt

        no_gcc_incent_cnt = df_not_gcc_ios[df_not_gcc_ios['channeltype'] == 'Incent']['new_size'].sum()
        ios_dict['no_gcc_incent'] = 0 if np.isnan(no_gcc_incent_cnt) else no_gcc_incent_cnt
        no_gcc_non_incent_cnt = df_not_gcc_ios[df_not_gcc_ios['channeltype'] == 'Non-incent']['new_size'].sum()
        ios_dict['no_gcc_non_incent'] = 0 if np.isnan(no_gcc_non_incent_cnt) else no_gcc_non_incent_cnt
        no_gcc_organic_cnt = df_not_gcc_ios[df_not_gcc_ios['channeltype'] == 'Organic']['new_size'].sum()
        ios_dict['no_gcc_organic'] = 0 if np.isnan(no_gcc_organic_cnt) else no_gcc_organic_cnt
        no_gcc_share_cnt = df_not_gcc_ios[df_not_gcc_ios['channeltype'] == 'Share']['new_size'].sum()
        ios_dict['no_gcc_share'] = 0 if np.isnan(no_gcc_share_cnt) else no_gcc_share_cnt
        ios_dict['date'] = yday_be_time_str[:10]

        Report_Quantity.objects.update_or_create(**android_dict)
        Report_Quantity.objects.update_or_create(**ios_dict)
    except Exception as e:
        print(e)

@login_required()
@permission_required('OpsManage.can_read_report_day_record', login_url='/noperm/')
def report_day(request):
    languageDBValues = Report_Day.objects.values_list('language').distinct()
    languageValues = []
    for language in languageDBValues:
        languageValues.append(language[0])
    projectDBValues = Game_Version_Config.objects.values_list('name').distinct()
    projectValues = []
    for project in projectDBValues:
        projectValues.append(project[0])
    # 来源
    sourceDBValues = Report_Day.objects.values_list('source').distinct()
    sourceValues = []
    for source in sourceDBValues:
        sourceValues.append(source[0])

    date = datetime.datetime.now()
    this_week_start_dt = str(date - datetime.timedelta(days=date.weekday())).split()[0]

    return render(request, 'report/report_day.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_week_record', login_url='/noperm/')
def report_week(request):
    languageDBValues = Report_Week.objects.values_list('language').distinct()
    languageValues = []
    for language in languageDBValues:
        languageValues.append(language[0])
    projectDBValues = Game_Version_Config.objects.values_list('name').distinct()
    projectValues = []
    for project in projectDBValues:
        projectValues.append(project[0])
    # 来源
    sourceDBValues = Report_Week.objects.values_list('source').distinct()
    sourceValues = []
    for source in sourceDBValues:
        sourceValues.append(source[0])
    return render(request, 'report/report_week.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_month_record', login_url='/noperm/')
def report_month(request):
    languageDBValues = Report_Month.objects.values_list('language').distinct()
    languageValues = []
    for language in languageDBValues:
        languageValues.append(language[0])
    projectDBValues = Game_Version_Config.objects.values_list('name').distinct()
    projectValues = []
    for project in projectDBValues:
        projectValues.append(project[0])
    # 来源
    sourceDBValues = Report_Month.objects.values_list('source').distinct()
    sourceValues = []
    for source in sourceDBValues:
        sourceValues.append(source[0])
    return render(request, 'report/report_month.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_quantity_record', login_url='/noperm/')
def report_quantity(request):
    projectDBValues = Game_Version_Config.objects.values_list('name').distinct()
    projectValues = []
    for project in projectDBValues:
        projectValues.append(project[0])
    return render(request, 'report/report_quantity.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_realtime_record', login_url='/noperm/')
def report_real(request):
    languageDBValues = Report_Realtime.objects.values_list('language').distinct()
    languageValues = []
    for language in languageDBValues:
        languageValues.append(language[0])
    countryDBValues = Report_Realtime.objects.values_list('country').distinct()
    countryValues = []
    for country in countryDBValues:
        countryValues.append(country[0])
    channelDBValues = Report_Realtime.objects.values_list('channel').distinct()
    channelValues = []
    for channel in channelDBValues:
        channelValues.append(channel[0])
    projectDBValues = Game_Version_Config.objects.values_list('name').distinct()
    projectValues = []
    for project in projectDBValues:
        projectValues.append(project[0])
    return render(request, 'report/report_real.html', locals())


@login_required()
@permission_required('OpsManage.can_read_subchannel_analysis_record', login_url='/noperm/')
def subchannel_analysis(request):
    args = {}
    if request.method == "POST":
        start_time = request.POST.get('start_time')
        if start_time:
            starttime = time.mktime(time.strptime(start_time, '%Y-%m-%d %H:%M'))
            args["starttime"] = starttime
        end_time = request.POST.get('end_time')
        if end_time:
            endtime = time.mktime(time.strptime(end_time, '%Y-%m-%d %H:%M'))
            args["endtime"] = endtime
    selectCondition = ""
    index = 0
    for key, arg in list(args.items()):
        if index == 0:
            addCondition += "where "
        if key == "starttime":
            addCondition = addCondition + " timestamp >" + str(arg) + " and "
        elif key == "endtime":
            addCondition = addCondition + " timestamp <" + str(arg) + " and "
        index += 1
    gameversion_list = []
    area_list = []
    ad_channel_list = []

    # ad_sub1Channel = {}
    # ad_sub2Channel = {}
    # ad_sub3Channel = {}
    #渠道
    channel_list = Ad_Channel.objects.all()
    for channel in channel_list:
        ad_channel_list.append(channel.channel)
        # sub1List = []
        # sub1sql = "SELECT distinct sub1_channel FROM opsmanage_channel_analysis_day where channel=" + "'" +str(channel.channel) + "'"
        # countrydf = pd.read_sql(sub1sql, connection)
        # for index, row in countrydf.iterrows():
        #     sub1List.append(str(row["sub1_channel"]))
        # ad_sub1Channel[str(channel.channel)] = sub1List
        #
        # sub1List = []
        # sub1sql = "SELECT distinct sub2_channel FROM opsmanage_channel_analysis_day where channel=" + "'" + str(
        #     channel.channel) + "'"
        # countrydf = pd.read_sql(sub1sql, connection)
        # for index, row in countrydf.iterrows():
        #     sub1List.append(str(row["sub2_channel"]))
        # ad_sub2Channel[str(channel.channel)] = sub1List
        #
        # sub1List = []
        # sub1sql = "SELECT distinct sub3_channel FROM opsmanage_channel_analysis_day where channel=" + "'" + str(
        #     channel.channel) + "'"
        # countrydf = pd.read_sql(sub1sql, connection)
        # for index, row in countrydf.iterrows():
        #     sub1List.append(str(row["sub3_channel"]))
        # ad_sub3Channel[str(channel.channel)] = sub1List

    countrysql = 'SELECT distinct country  FROM opsmanage_channel_analysis_day' + selectCondition
    countrydf = pd.read_sql(countrysql, connection)
    for index, row in countrydf.iterrows():
        area_list.append(row["country"])
    # 版本
    lan_list = Language_Version_Config.objects.all()
    for lan in lan_list:
        gameversion_list.append(lan.language)
    game_list = Game_Version_Config.objects.all()
    return render(request, 'report/subchannel_analysis.html', locals())

@login_required()
@permission_required('OpsManage.can_read_report_geography_record', login_url='/noperm/')
def report_geography(request):
    gameversion_list = []
    area_list = []
    language_list = []
    ad_channel_list = Ad_Channel.objects.filter()

    countrysql = 'SELECT distinct country  FROM opsmanage_channel_analysis_day'
    countrydf = pd.read_sql(countrysql, connection)
    for index, row in countrydf.iterrows():
        area_list.append(row["country"])
    # 版本
    lan_list = Language_Version_Config.objects.all()
    for lan in lan_list:
        gameversion_list.append(lan.language)
    game_list = Game_Version_Config.objects.all()
    return render(request, 'report/report_geography.html', locals())

@login_required()
@permission_required('OpsManage.can_read_channel_analysis_day_record', login_url='/noperm/')
def channel_analysis(request):
    args = {}
    if request.method == "POST":
        start_time = request.POST.get('start_time')
        if start_time:
            starttime = time.mktime(time.strptime(start_time, '%Y-%m-%d %H:%M'))
            args["starttime"] = starttime
        end_time = request.POST.get('end_time')
        if end_time:
            endtime = time.mktime(time.strptime(end_time, '%Y-%m-%d %H:%M'))
            args["endtime"] = endtime
    selectCondition = ""
    index = 0
    for key, arg in list(args.items()):
        if index == 0:
            addCondition += "where "
        if key == "starttime":
            addCondition = addCondition + " timestamp >" + str(arg) + " and "
        elif key == "endtime":
            addCondition = addCondition + " timestamp <" + str(arg) + " and "
        index += 1

    gameversion_list = []
    area_list = []
    language_list = []
    ad_channel_list = Ad_Channel.objects.filter()

    countrysql = 'SELECT distinct country  FROM opsmanage_channel_analysis_day' + selectCondition
    countrydf = pd.read_sql(countrysql, connection)
    for index, row in countrydf.iterrows():
        area_list.append(row["country"])
    # 版本
    lan_list = Language_Version_Config.objects.all()
    for lan in lan_list:
        gameversion_list.append(lan.language)

    game_list = Game_Version_Config.objects.all()
    return render(request, 'report/channel_analysis.html', locals())


@login_required()
@permission_required('OpsManage.can_read_roi_day_record', login_url='/noperm/')
def roi_day(request):
    area_list = []
    gameversion_list = []
    game_list = []
    # duration = "0"
    ad_subchannel_list = []
    ad_channel_list = Ad_Channel.objects.filter()
    countrysql = 'SELECT distinct country  FROM opsmanage_channel_analysis_day'
    countrydf = pd.read_sql(countrysql, connection)
    for index, row in countrydf.iterrows():
        area_list.append(row["country"])
    # 子渠道
    # sub1_channelsql = 'SELECT distinct sub1_channel  FROM opsmanage_channel_analysis_day'
    # sub1_channeldf = pd.read_sql(sub1_channelsql, connection)
    # for index, row in sub1_channeldf.iterrows():
    #     ad_subchannel_list.append(row["sub1_channel"])
    # 版本
    lan_list = Language_Version_Config.objects.all()
    for lan in lan_list:
        gameversion_list.append(lan.language)
    # 游戏
    game_list = Game_Version_Config.objects.all()

    # ad_sub1Channel = {}
    # ad_sub2Channel = {}
    # ad_sub3Channel = {}
    # channel_list = Ad_Channel.objects.all()
    # for channel in channel_list:
    #     subsql = "SELECT distinct sub1_channel, sub2_channel, sub3_channel FROM opsmanage_channel_analysis_day where channel=" + "'" + str(
    #         channel.channel) + "'" + "GROUP BY sub1_channel, sub2_channel, sub3_channel"
    #     countrydf = pd.read_sql(subsql, connection)
    #     sub1List = []
    #     sub2List = []
    #     sub3List = []
    #     for index, row in countrydf.iterrows():
    #         sub1List.append(str(row["sub1_channel"]))
    #         sub1List = list(set(sub1List))
    #         sub2List.append(str(row["sub2_channel"]))
    #         sub2List = list(set(sub2List))
    #         sub3List.append(str(row["sub3_channel"]))
    #         sub3List = list(set(sub3List))
    #     ad_sub1Channel[str(channel.channel)] = sub1List
    #     ad_sub2Channel[str(channel.channel)] = sub2List
    #     ad_sub3Channel[str(channel.channel)] = sub3List

    return render(request, 'report/roi_day.html', locals())

@login_required()
@permission_required('OpsManage.can_read_roi_week_record', login_url='/noperm/')
def roi_week(request):
    area_list = []
    gameversion_list = []
    game_list = []
    # duration = "0"
    ad_subchannel_list = []
    ad_channel_list = Ad_Channel.objects.filter()
    countrysql = 'SELECT distinct country  FROM opsmanage_channel_analysis_day'
    countrydf = pd.read_sql(countrysql, connection)
    for index, row in countrydf.iterrows():
        area_list.append(row["country"])
    # 版本
    lan_list = Language_Version_Config.objects.all()
    for lan in lan_list:
        gameversion_list.append(lan.language)
    # 游戏
    game_list = Game_Version_Config.objects.all()
    return render(request, 'report/roi_week.html', locals())

@login_required()
@permission_required('OpsManage.can_read_week_charge_analysis', login_url='/noperm/')
def week_charge_analysis(request):
    area_list = []
    gameversion_list = []
    game_list = []
    # duration = "0"
    ad_subchannel_list = []
    ad_channel_list = Ad_Channel.objects.filter()
    countrysql = 'SELECT distinct country  FROM opsmanage_channel_analysis_day'
    countrydf = pd.read_sql(countrysql, connection)
    for index, row in countrydf.iterrows():
        area_list.append(row["country"])
    # 子渠道
    sub1_channelsql = 'SELECT distinct sub1_channel  FROM opsmanage_channel_analysis_day'
    sub1_channeldf = pd.read_sql(sub1_channelsql, connection)
    for index, row in sub1_channeldf.iterrows():
        ad_subchannel_list.append(row["sub1_channel"])
    # 版本
    lan_list = Language_Version_Config.objects.all()
    for lan in lan_list:
        gameversion_list.append(lan.language)
    # 游戏
    game_list = Game_Version_Config.objects.all()

    ad_sub1Channel = {}
    ad_sub2Channel = {}
    ad_sub3Channel = {}
    #
    channel_list = Ad_Channel.objects.all()
    for channel in channel_list:
        subsql = "SELECT distinct sub1_channel, sub2_channel, sub3_channel FROM opsmanage_channel_analysis_day where channel=" + "'" + str(
            channel.channel) + "'" + "GROUP BY sub1_channel, sub2_channel, sub3_channel"
        countrydf = pd.read_sql(subsql, connection)
        sub1List = []
        sub2List = []
        sub3List = []
        for index, row in countrydf.iterrows():
            sub1List.append(str(row["sub1_channel"]))
            sub1List = list(set(sub1List))
            sub2List.append(str(row["sub2_channel"]))
            sub2List = list(set(sub2List))
            sub3List.append(str(row["sub3_channel"]))
            sub3List = list(set(sub3List))
        ad_sub1Channel[str(channel.channel)] = sub1List
        ad_sub2Channel[str(channel.channel)] = sub2List
        ad_sub3Channel[str(channel.channel)] = sub3List


    return render(request, 'report/week_charge_analysis.html', locals())

@login_required()
@permission_required('OpsManage.can_read_month_charge_analysis', login_url='/noperm/')
def month_charge_analysis(request):
    area_list = []
    gameversion_list = []
    game_list = []
    # duration = "0"
    ad_subchannel_list = []
    ad_channel_list = Ad_Channel.objects.filter()
    countrysql = 'SELECT distinct country  FROM opsmanage_channel_analysis_day'
    countrydf = pd.read_sql(countrysql, connection)
    for index, row in countrydf.iterrows():
        area_list.append(row["country"])
    # 子渠道
    sub1_channelsql = 'SELECT distinct sub1_channel  FROM opsmanage_channel_analysis_day'
    sub1_channeldf = pd.read_sql(sub1_channelsql, connection)
    for index, row in sub1_channeldf.iterrows():
        ad_subchannel_list.append(row["sub1_channel"])
    # 版本
    lan_list = Language_Version_Config.objects.all()
    for lan in lan_list:
        gameversion_list.append(lan.language)
    # 游戏
    game_list = Game_Version_Config.objects.all()

    ad_sub1Channel = {}
    ad_sub2Channel = {}
    ad_sub3Channel = {}
    #
    channel_list = Ad_Channel.objects.all()
    for channel in channel_list:
        subsql = "SELECT distinct sub1_channel, sub2_channel, sub3_channel FROM opsmanage_channel_analysis_day where channel=" + "'" + str(
            channel.channel) + "'" + "GROUP BY sub1_channel, sub2_channel, sub3_channel"
        countrydf = pd.read_sql(subsql, connection)
        sub1List = []
        sub2List = []
        sub3List = []
        for index, row in countrydf.iterrows():
            sub1List.append(str(row["sub1_channel"]))
            sub1List = list(set(sub1List))
            sub2List.append(str(row["sub2_channel"]))
            sub2List = list(set(sub2List))
            sub3List.append(str(row["sub3_channel"]))
            sub3List = list(set(sub3List))
        ad_sub1Channel[str(channel.channel)] = sub1List
        ad_sub2Channel[str(channel.channel)] = sub2List
        ad_sub3Channel[str(channel.channel)] = sub3List


    return render(request, 'report/month_charge_analysis.html', locals())

@login_required()
@permission_required('OpsManage.can_read_roi_momth_record', login_url='/noperm/')
def roi_month(request):
    area_list = []
    gameversion_list = []
    ad_subchannel_list = []
    ad_channel_list = Ad_Channel.objects.filter()
    countrysql = 'SELECT distinct country  FROM opsmanage_channel_analysis_day'
    countrydf = pd.read_sql(countrysql, connection)
    for index, row in countrydf.iterrows():
        area_list.append(row["country"])
    lan_list = Language_Version_Config.objects.all()
    for lan in lan_list:
        gameversion_list.append(lan.language)

    game_list = Game_Version_Config.objects.all()

    return render(request, 'report/roi_month.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_geography_record', login_url='/noperm/')
def report_geography_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        channels = request.POST.getlist('channels[]')
        countrys = request.POST.getlist('countrys[]')
        country_show = request.POST.get('country_show')
        channel_show = request.POST.get('channel_show')
        time_show = request.POST.get('time_show')
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        languages = request.POST.getlist('gameverisions[]')
        platforms = request.POST.getlist('platforms[]')
        checkbox = request.POST.get('checkbox', '')
        f_start_time = request.POST.get('f_start_time')
        f_end_time = request.POST.get('f_end_time')
        conditionList = []
        conditionList.append(request.POST.get('condition_1'))
        conditionList.append(request.POST.get('condition_2'))
        conditionList.append(request.POST.get('condition_3'))
        conditionList.append(request.POST.get('condition_4'))
        conditionList.append(request.POST.get('condition_5'))
        game_perms = request.POST.get('game_perms')

        gameinfo = Game_Version_Config.objects.filter(name=game_perms)
        if len(gameinfo) <= 0:
            return render(request, 'report/report_geography_detail.html.html', locals())
        appid_sql = "AND (app_id ='" + gameinfo[0].android_appid + "'or app_id='" + gameinfo[0].ios_appid + "')"

        bHaveOrganic = False
        if len(channels) == 0:
            bHaveOrganic = True
        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '2000-01-01 01:01'

        end_timestamp = time.mktime(tuple(
            parse(str(datetime.date(*list(map(int, end_time[:10].split('-')))) + datetime.timedelta(days=1))).timetuple()))
        start_timestamp = time.mktime(tuple(parse(start_time[:10]).timetuple()))

        if not f_end_time:
            f_end_time = now_time
        if not f_start_time :
            if f_end_time == now_time:
                f_start_date = datetime.datetime.strptime(now_time, "%Y-%m-%d %H:%M")
                f_start_time = (f_start_date + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H:%M")
            else:
                f_start_time = '2000-01-01 01:01'

        f_end_time = str(datetime.date(*list(map(int, f_end_time[:10].split('-')))) + datetime.timedelta(days=1))

        f_end_timestamp = time.mktime(tuple(parse(f_end_time).timetuple()))
        f_start_timestamp = time.mktime(tuple(parse(f_start_time[:10]).timetuple()))

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '0'
        sql_temp = ''
        appsflyer_sql_tmp = ""
        active_sql_tmp = ""
        if languages:
            sql_language = ''
            for lan in languages:
                sql_language += ' language = "{}" or '.format(lan)

            sql_temp += ' and {} '.format('(' + sql_language[:-3] + ')')
            active_sql_tmp += ' and {} '.format('(' + sql_language[:-3] + ')')
            appsflyer_sql_tmp += ' and {} '.format('(' + sql_language[:-3] + ')')
        if platforms:
            sql_platform = ""
            sql_active = ""
            for pl in platforms:
                sql_platform += ' platform = "{}" or '.format(pl)
                sql_active += ' platform = "{}" or '.format(pl)

            sql_temp += ' and {} '.format('(' + sql_platform[:-3] + ')')
            active_sql_tmp += ' and {} '.format('(' + sql_active[:-3] + ')')
            appsflyer_sql_tmp += ' and {} '.format('(' + sql_platform[:-3] + ')')

        # 是否需要子渠道的数据
        newChannel = {}
        if channels:
            sql_channel = ''
            appssql_channel = ""
            for ch in channels:
                if "Organic" == ch:
                    bHaveOrganic = True
                # 判断是否有渠道提取
                Isolate_Channel_Data = Isolate_Channel.objects.filter(newname=ch)
                if Isolate_Channel_Data.count() == 1:
                    prename = Isolate_Channel_Data[0].prename
                    rule = Isolate_Channel_Data[0].rule
                    newname = Isolate_Channel_Data[0].newname
                    newChannel[newname] = [prename, rule]
                    if prename not in channels:
                        ch = prename

                    else:
                        continue
                sql_channel += ' channel = "{}" or '.format(ch)
                appssql_channel += ' channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')
            appsflyer_sql_tmp += ' and {} '.format('(' + appssql_channel[:-3] + ')')
        if countrys:
            sql_country = ''
            appssql_country = ''
            for ch in countrys:
                sql_country += ' country = "{}" or '.format(ch)
                appssql_country += ' country_code = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_country[:-3] + ')')
            appsflyer_sql_tmp += ' and {} '.format('(' + appssql_country[:-3] + ')')
            active_sql_tmp += ' and {} '.format('(' + sql_country[:-3] + ')')

        sql_temp += appid_sql
        appsflyer_sql_tmp += appid_sql
        active_sql_tmp += appid_sql

        dime_list = []
        show_dict = {}
        showappsflyerls = []
        showactive = ["device_id"]

        for condition in conditionList:
            if condition == 'country':
                if '国家' in dime_list:
                    continue
                dime_list.append('国家')
                show_dict['国家'] = 'country,'
                showappsflyerls.append("country_code as country")
                showactive.append("country")
            elif condition == 'time':
                if '日期' in dime_list:
                    continue
                dime_list.append('日期')
                show_dict['日期'] = 'date,'
                showappsflyerls.append("install_time_selected_timezone as date")
                showactive.append("time as date")
            elif condition == 'channel':
                if '渠道' in dime_list:
                    continue
                dime_list.append('渠道')
                show_dict['渠道'] = 'channel,'
                showappsflyerls.append("channel")
            elif condition == 'language':
                if '版本' in dime_list:
                    continue
                dime_list.append('版本')
                show_dict['版本'] = 'language,'
                showappsflyerls.append("language")
                showactive.append("language")
            elif condition == 'platform':
                if '平台' in dime_list:
                    continue
                dime_list.append('平台')
                show_dict['平台'] = 'platform,'
                showappsflyerls.append("platform")
                showactive.append("platform")

        channel_head = []
        channel_head.extend(dime_list)
        channel_head.extend(
            ['新增激活数', '广告花费', '激活成本', '新增设备上线', '上线率'])
        dime_str = ''
        dime_group = []
        for di in dime_list:
            dime_str += show_dict.get(di, '')
            dime_group.append(show_dict.get(di, ' ')[:-1])

        if len(newChannel) > 0:
            dime_str += 'sub1_channel,'
            dime_group.append('sub1_channel')
            if channel_show == 'channel':
                dime_str+= 'channel,'

        sql = 'SELECT %s ad_cost,new_activate,new_device_onl FROM opsmanage_channel_analysis_day WHERE date >= "%s" and date <= "%s"' % (
            dime_str, start_time, end_time)
        if sql_temp:
            sql = sql + sql_temp
        df = pd.read_sql(sql, connection)
        if not df.index.tolist():
            return render(request, 'report/report_geography_detail.html', locals())
        total_act = Channel_Analysis_Day.objects.filter(date__gte=start_time, date__lte=end_time).aggregate(
            total_act=Sum('new_activate')).get('total_act')
        channel_colume = df.columns.tolist()[:-4]

        if len(newChannel) > 0:
            for newchannel, subChannel in list(newChannel.items()):
                df.loc[(df["channel"] == subChannel[0]) & (df["sub1_channel"].str.startswith(subChannel[1])), "channel"] = newchannel
                try:
                    if subChannel[0] not in channels:
                        df = df[~df['channel'].isin([subChannel[0]])]
                except Exception as e:
                    print(e)
            dime_group.remove("sub1_channel")
            df = df.drop('sub1_channel', axis=1)
            if channel_show == 'channel':
                df = df.drop('channel', axis=1)
                channel_colume.remove('channel')
            # df = df.groupby(dime_group).agg(['sum'])
            channel_colume.remove('sub1_channel')

        df = df.groupby(dime_group).agg(['sum']).reset_index()

        sum_list = df.sum().tolist()
        sum_list[0] = '总计'
        df.loc[df.shape[0]] = sum_list
        # 激活成本
        df['pernew_cost'] = df.ad_cost / df.new_activate
        # 注册率
        # df['reg_rate'] = df.new_device_reg / df.new_activate
        # 新增注册成本
        # df['perreg_cost'] = df.ad_cost / df.new_device_reg
        # 上线率
        df['onl_rate'] = df.new_device_onl / df.new_activate
        # 新增上线成本
        # df['peronl_cost'] = df.ad_cost / df.new_device_onl
        if dime_group:
            for i in range(len(dime_group) - 1):
                df.iloc[df.shape[0]-1, i+1] = ''
        df['pernew_cost'] = df['pernew_cost'].apply(lambda x: round(x, 2))
        # df['reg_rate'] = df['reg_rate'].apply(lambda x: str(round(x * 100, 2)) + '%')
        # df['perreg_cost'] = df['perreg_cost'].apply(lambda x: round(x, 2))
        df['onl_rate'] = df['onl_rate'].apply(lambda x: str(round(x * 100, 2)) + '%')
        # df['peronl_cost'] = df['peronl_cost'].apply(lambda x: round(x, 2))
        channel_colume.extend(
            ['country', 'new_activate', 'ad_cost', 'pernew_cost',
             'new_device_onl', 'onl_rate'])
        df = df[channel_colume]

        df = df.fillna(0)
        channel_day_list = np.array(df).tolist()

        country_list = df.country.tolist()[:-1]
        country_list = list(set(country_list))
        activate_data = []
        country_act_dict = dict(list(zip(country_list, [list() for i in range(len(country_list))])))
        country_amount_dict = dict(list(zip(country_list, [list() for i in range(len(country_list))])))
        activate_sum_data = []
        amount_sum_data = []
        amount_sum_data_comp = []
        activate_sum_data_comp = []
        for i, j in zip(df.country.tolist()[:-1], df.new_activate['sum'].tolist()[:-1]):
            country_act_dict[i].append(j)
        # for i, j in zip(df.country.tolist()[:-1], df.recharge_amount['sum'].tolist()[:-1]):
        #     country_amount_dict[i].append(j)
        for country in country_list:
            dict_temp = {}
            dict_temp['name'] = country
            dict_temp['type'] = 'line'
            dict_temp['data'] = country_act_dict.get(country, [])
            activate_data.append(dict_temp)
        for country in country_list:
            activate_sum_data.append(sum(country_act_dict.get(country, [])))
            # amount_sum_data.append(sum(country_amount_dict.get(country, [])))
        for k, v in zip(amount_sum_data, country_list):
            amount_sum_data_comp.append({'value': k, 'name': v})
        for k, v in zip(activate_sum_data, country_list):
            activate_sum_data_comp.append({'value': k, 'name': v})

        country_list = json.dumps(country_list)
        amount_sum_data_comp = json.dumps(amount_sum_data_comp)
        activate_sum_data_comp = json.dumps(activate_sum_data_comp)

        if checkbox == 'true':
            channel_head.extend(['累计付费设备数','累计付费充值金额','ROI','付费率', 'ARPPU','LTV'])
            matchls = [value[0:-1] for value in list(show_dict.values())]
            # 渠道，日期， 国家
            if len(newChannel) > 0:
                showappsflyerls.append("sub1_channel")
                if channel_show == 'channel':
                    showappsflyerls.append("channel")
            start_time_stamp = time.mktime(time.strptime(end_time, "%Y-%m-%d"))
            appsflyer_end = datetime.date.fromtimestamp(start_time_stamp) + datetime.timedelta(days=+ 1)
            sql_channel = 'SELECT device_id ,%s  FROM opsmanage_push_appsflyer  WHERE install_time_selected_timezone >= "%s" and install_time_selected_timezone < "%s"' % (
            ",".join(showappsflyerls), start_time, appsflyer_end)
            if appsflyer_sql_tmp:
                sql_channel = sql_channel + appsflyer_sql_tmp
            df_channel = pd.read_sql(sql_channel, connection)
            if "date" in matchls:
                df_channel['date'] = df_channel['date'].apply(lambda x: x[:10])

            # db_time_end = datetime.date(*map(int, end_time[:10].split('-'))) + datetime.timedelta(days=1)
            # 是否有自然用户
            if bHaveOrganic:
                sql_cb = 'SELECT %s FROM opsmanage_push_client_activate WHERE db_time >= "%s" and db_time < "%s"' % (
                ",".join(showactive),start_time, appsflyer_end)
                if active_sql_tmp:
                    sql_cb += active_sql_tmp
                df_cb = pd.read_sql(sql_cb, connection)
                if "日期" in list(show_dict.keys()):
                    df_cb['date'] = df_cb['date'].map(
                        lambda x: pytz.timezone("Asia/Shanghai").localize(
                            datetime.datetime.fromtimestamp(int(x))).astimezone(
                            pytz.timezone("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")[0:10])
                df_cb["channel"] = "Organic"
                df_channel = pd.concat([df_channel, df_cb], ignore_index=True)
            df_channel = df_channel.drop_duplicates('device_id')

            sql_cr_ios = 'SELECT device_id,SUM(recharge) as aggregate_recharge FROM opsmanage_push_client_recharge WHERE db_time >= "%s" and db_time < "%s" GROUP BY (device_id)' % (
                f_start_time, f_end_time)
            df_cb_ios = pd.read_sql(sql_cr_ios, connection)

            df_channel = pd.merge(df_cb_ios, df_channel, how="left", on=["device_id"])

            df_channel["aggregate_payid"] = 1
            if df_channel.empty:
                df_channel["aggregate_recharge"] = 0
            df_channel.fillna(0)
            if len(newChannel) > 0:
                for newchannel, subChannel in list(newChannel.items()):
                    df_channel.loc[(df_channel["channel"] == subChannel[0]) & (
                        df_channel["sub1_channel"].str.startswith(subChannel[1])), "channel"] = newchannel
                    try:
                        if subChannel[0] not in channels:
                            df_channel = df_channel[~df_channel['channel'].isin([subChannel[0]])]
                    except Exception as e:
                        print(e)
                df_channel = df_channel.drop('sub1_channel', axis=1)
                if channel_show == 'channel':
                    df_channel = df_channel.drop('channel', axis=1)
            df_cb_group = df_channel.groupby(matchls).sum().reset_index()
            paydata = pd.merge(df, df_cb_group, how='left', on=matchls)

            paydata.loc[paydata.shape[0] - 1, 'aggregate_payid'] = paydata['aggregate_payid'].sum()
            paydata.loc[paydata.shape[0] - 1, 'aggregate_recharge'] = paydata['aggregate_recharge'].sum()

            # 累计付费设备数
            df['aggregate_payid'] = paydata["aggregate_payid"]
            # 累计付费金额
            df['aggregate_recharge'] = paydata["aggregate_recharge"]
            # ROI
            df['roi'] = paydata["aggregate_recharge"] / df.ad_cost["sum"].tolist()
            # # 付费率
            df['pay_rate'] = paydata["aggregate_payid"] / df.new_activate["sum"].tolist()
            # # ARPPU
            df['arppu'] = paydata["aggregate_recharge"] / df['aggregate_payid']
            # # LTV
            df['ltv'] = paydata["aggregate_recharge"] / df.new_activate["sum"].tolist()


            df['roi'] = df['roi'].map(lambda x: round(x, 2))
            # df['pay_rate'] = df['pay_rate'].map(lambda x: round(x, 2))
            # df['arppu'] = df['arppu'].map(lambda x: round(x, 2))
            # df['ltv'] = df['ltv'].map(lambda x: round(x, 2))
            df = df.fillna(0)
            df['aggregate_payid'] = df['aggregate_payid'].apply(lambda x: int(x) if x  else 0)
            df['aggregate_recharge'] = df['aggregate_recharge'].apply(lambda x: '$' + str(round(x, 2)))
            df['arppu'] = df['arppu'].apply(lambda x: '$' + str(round(x, 2)))
            df['pay_rate'] = df['pay_rate'].apply(lambda x: str(round(x * 100, 2)) + '%')
            df['ltv'] = df['ltv'].apply(lambda x: '$' + str(round(x, 2)))

        df = df.fillna(0)
        df['pernew_cost'] = df['pernew_cost'].apply(lambda x: '$' + str(round(x, 2)))
        # df['perreg_cost'] = df['perreg_cost'].apply(lambda x: '$' + str(round(x, 2)))
        # df['peronl_cost'] = df['peronl_cost'].apply(lambda x: '$' + str(round(x, 2)))
        df[('ad_cost', 'sum')] = df[('ad_cost', 'sum')].map(lambda x: '$' + str(round(x, 2)))
        channel_day_list = np.array(df).tolist()

        return render(request, 'report/report_geography_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_day_record', login_url='/noperm/')
def report_day_check(request):
    yesterday = str(datetime.date.today() + datetime.timedelta(days=(-1)))
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))[:10]
    if request.method == "POST":
        try:
            sources = request.POST.getlist('sources[]')
            languages = request.POST.getlist('languages[]')
            project = request.POST.get('project')
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')
            f_start_time = request.POST.get('f_start_time')
            f_end_time = request.POST.get('f_end_time')
            checkbox = request.POST.get('checkbox', '')
            if not end_time:
                end_time = now_time
            if not start_time:
                start_time = yesterday

            df_android = ReportDayFrame(project, languages, sources, start_time, end_time, f_start_time, f_end_time,
                                        checkbox, "Android", "opsmanage_report_day")
            df_ios = ReportDayFrame(project, languages, sources, start_time, end_time, f_start_time, f_end_time,
                                    checkbox, "IOS", "opsmanage_report_day")
            df = pd.concat([df_android, df_ios], ignore_index=True)
            df = df.groupby(['date'], as_index=False).sum()
            # 图形横轴时间数据
            date = df['date'].tolist()
            date = json.dumps(date)
            # 图形数据
            chart_android = ReportChartHandle(copy.deepcopy(df_android))
            chart_ios = ReportChartHandle(copy.deepcopy(df_ios))
            chart = ReportChartHandle(copy.deepcopy(df))

            df_android_out = ReportDataFrameHandle(df_android, checkbox)
            df_ios_out = ReportDataFrameHandle(df_ios, checkbox)
            df_out = ReportDataFrameHandle(df, checkbox)

            thhead = ['日期', '新增激活数', '广告激活数', '自然激活数', '自然激活占比', '广告花费($)', '激活成本($)',
                      '广告激活成本($)','新增设备上线', '上线率', '总ROI', '广告ROI', '总新增充值', '广告新增充值',
                      '自然新增充值', '总新增充值设备','广告新增充值设备', '自然新增充值设备', '总付费率', '广告付费率',
                      '自然付费率', '总ARPPU($)', '广告ARPPU($)', '自然ARRPU($)', '总LTV($)','广告LTV($)','自然LTV($)',
                      '总次留', '充值设备', '充值角色', '充值金额($)','新增充值占比', '日付费率', '日ARPU($)', '日ARPPU($)',
                      '活跃设备', '活跃角色', '新增角色','新增充值角色', '新增充值旧角色', '新增充值角色充值金额($)','新增充值旧角色充值金额($)']
            if checkbox == 'true':
                # thhead = thhead[:-1]
                thhead.extend(['累计付费设备数','累计付费充值金额($)','ROI','付费率', 'ARPPU($)','LTV($)'])#,'备注'])

        except Exception as e:
            print(e)
        return render(request, 'report/report_day_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_week_record', login_url='/noperm/')
def report_week_check(request):
    yesterday = str(datetime.date.today() + datetime.timedelta(days=(-1)))
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))[:10]
    if request.method == "POST":
        try:
            sources = request.POST.getlist('sources[]')
            languages = request.POST.getlist('languages[]')
            project = request.POST.get('project')
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')
            f_start_time = request.POST.get('f_start_time')
            f_end_time = request.POST.get('f_end_time')
            checkbox = request.POST.get('checkbox', '')
            if not end_time:
                end_time = now_time
            if not start_time:
                start_time = yesterday

            df_android = ReportDayFrame(project, languages, sources, start_time, end_time, f_start_time, f_end_time, checkbox,
                                        "Android", "opsmanage_report_week")
            df_ios = ReportDayFrame(project, languages, sources, start_time, end_time, f_start_time, f_end_time, checkbox,
                                    "IOS", "opsmanage_report_week")
            df = pd.concat([df_android, df_ios], ignore_index=True)
            df = df.groupby(['date'], as_index=False).sum()
            # 图形横轴时间数据
            date = df['date'].tolist()
            date = json.dumps(date)
            # 图形数据
            chart_android = ReportChartHandle(copy.deepcopy(df_android))
            chart_ios = ReportChartHandle(copy.deepcopy(df_ios))
            chart = ReportChartHandle(copy.deepcopy(df))

            df_android_out = ReportDataFrameHandle(df_android, checkbox)
            df_ios_out = ReportDataFrameHandle(df_ios, checkbox)
            df_out = ReportDataFrameHandle(df, checkbox)

            thhead = ['日期', '新增激活数', '广告激活数', '自然激活数', '自然激活占比', '广告花费($)', '激活成本($)',
                      '广告激活成本($)', '新增设备上线', '上线率', '总ROI', '广告ROI', '总新增充值', '广告新增充值',
                      '自然新增充值', '总新增充值设备', '广告新增充值设备', '自然新增充值设备', '总付费率', '广告付费率',
                      '自然付费率', '总ARPPU($)', '广告ARPPU($)', '自然ARRPU($)', '总LTV($)', '广告LTV($)', '自然LTV($)',
                      '总次留', '充值设备', '充值角色', '充值金额($)', '新增充值占比', '日付费率', '日ARPU($)', '日ARPPU($)',
                      '活跃设备', '活跃角色', '新增角色', '新增充值角色', '新增充值旧角色', '新增充值角色充值金额($)', '新增充值旧角色充值金额($)']
            if checkbox == 'true':
                # thhead = thhead[:-1]
                thhead.extend(['累计付费设备数', '累计付费充值金额($)', 'ROI', '付费率', 'ARPPU($)', 'LTV($)'])#, '备注'])
        except Exception as e:
            print(e)
        return render(request, 'report/report_day_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_month_record', login_url='/noperm/')
def report_month_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    today = str(datetime.date.today())
    month = today[0:7]
    month_start_day = month + '-01'
    if request.method == "POST":
        try:
            sources = request.POST.getlist('sources[]')
            languages = request.POST.getlist('languages[]')
            project = request.POST.get('project')
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')
            f_start_time = request.POST.get('f_start_time')
            f_end_time = request.POST.get('f_end_time')
            checkbox = request.POST.get('checkbox', '')
            if start_time:
                start_time = start_time + '-01'
            if end_time:
                end_time = end_time + '-01'
            if not end_time:
                end_time = now_time
            if not start_time:
                start_time = month_start_day

            df_android = ReportDayFrame(project, languages, sources, start_time, end_time, f_start_time, f_end_time, checkbox,
                                        "Android", "opsmanage_report_month")
            df_ios = ReportDayFrame(project, languages, sources, start_time, end_time, f_start_time, f_end_time, checkbox,
                                    "IOS", "opsmanage_report_month")
            df = pd.concat([df_android, df_ios], ignore_index=True)
            df = df.groupby(['date'], as_index=False).sum()
            # 图形横轴时间数据
            date = df['date'].tolist()
            date = json.dumps(date)
            # 图形数据
            chart_android = ReportChartHandle(copy.deepcopy(df_android))
            chart_ios = ReportChartHandle(copy.deepcopy(df_ios))
            chart = ReportChartHandle(copy.deepcopy(df))

            df_android_out = ReportDataFrameHandle(df_android, checkbox)
            df_ios_out = ReportDataFrameHandle(df_ios, checkbox)
            df_out = ReportDataFrameHandle(df, checkbox)

            thhead = ['日期', '新增激活数', '广告激活数', '自然激活数', '自然激活占比', '广告花费($)', '激活成本($)',
                      '广告激活成本($)', '新增设备上线', '上线率', '总ROI', '广告ROI', '总新增充值', '广告新增充值',
                      '自然新增充值', '总新增充值设备', '广告新增充值设备', '自然新增充值设备', '总付费率', '广告付费率',
                      '自然付费率', '总ARPPU($)', '广告ARPPU($)', '自然ARRPU($)', '总LTV($)', '广告LTV($)', '自然LTV($)',
                      '总次留', '充值设备', '充值角色', '充值金额($)', '新增充值占比', '日付费率', '日ARPU($)', '日ARPPU($)',
                      '活跃设备', '活跃角色', '新增角色', '新增充值角色', '新增充值旧角色', '新增充值角色充值金额($)', '新增充值旧角色充值金额($)']
            if checkbox == 'true':
                # thhead = thhead[:-1]
                thhead.extend(['累计付费设备数', '累计付费充值金额($)', 'ROI', '付费率', 'ARPPU($)', 'LTV($)'])#, '备注'])
        except Exception as e:
            print(e)
        return render(request, 'report/report_day_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_quantity_record', login_url='/noperm/')
def report_quantity_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        platform = request.POST.get('platform')
        project = request.POST.get('project')
        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '0'

        sg_conf = Game_Version_Config.objects.filter(name=project)
        if sg_conf.count() != 1:
            return

        sql = 'SELECT date,new_activate,ad_activate,na_activate,incent,non_incent,share,gcc_incent,no_gcc_incent,gcc_non_incent,no_gcc_non_incent,gcc_organic,no_gcc_organic,gcc_share,no_gcc_share FROM opsmanage_report_quantity WHERE date >= "%s" and date <= "%s"' % (start_time, end_time)
        if platform:
            sql += ' and platform="{}"'.format(platform)
        if project:
            sql += ' and (app_id="{}" or app_id="{}")'.format(sg_conf[0].ios_appid, sg_conf[0].android_appid)
        df = pd.read_sql(sql, connection)
        if not platform:
            df = df.groupby('date').agg(['sum']).reset_index()

        ave_list = df.mean().tolist()
        ave_list = [round(ave,2) for ave in df.mean().tolist()]
        sum_list = df.sum().tolist()
        ave_list.insert(0,'平均')
        sum_list[0] = '总计'
        df.loc[df.shape[0] + 1] = ave_list
        df.loc[df.shape[0] + 1] = sum_list

        df = df.fillna(' ')
        report_quantity_list = np.array(df).tolist()

        return render(request, 'report/report_quantity_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_realtime_record', login_url='/noperm/')
def report_real_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":

        start_time = request.POST.get('start_time', '')
        end_time = request.POST.get('end_time', '')
        if start_time == '' and end_time == '':
            start_time = str(datetime.date.today())
            end_time = start_time
        elif start_time == '':
            start_time = end_time
        elif end_time == '':
            end_time = start_time

        channels = request.POST.getlist('channels[]')
        countrys = request.POST.getlist('countrys[]')
        languages= request.POST.getlist('languages[]')
        delta = datetime.datetime.strptime(start_time, '%Y-%m-%d') - datetime.datetime.strptime(end_time, '%Y-%m-%d')

        project = request.POST.get('project')
        sg_conf = Game_Version_Config.objects.filter(name=project)
        if sg_conf.count() != 1:
            return
        ios_app_id = sg_conf[0].ios_appid
        android_app_id = sg_conf[0].android_appid

        ser_temp_lst = []
        temp_time = start_time
        while temp_time <= end_time:
            ser_temp_lst.extend([temp_time + ' ' + hour for hour in
            ['00','01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
             '19', '20', '21', '22', '23']])
            temp_time = str(datetime.date(*list(map(int,temp_time.split('-'))))+ datetime.timedelta(days=1))
        ser_temp = pd.Series(ser_temp_lst)
        df_temp = ser_temp.to_frame(name='date')
        pre_end_time = str(datetime.date(*list(map(int, start_time.split('-')))) + datetime.timedelta(days=-1))
        pre_start_time = str(datetime.date(*list(map(int, pre_end_time.split('-')))) + datetime.timedelta(days=delta.days))
        sql_temp = ''
        if languages:
            sql_temp += SelectSqlStrFormat('language', languages)

        if countrys:
            sql_temp += SelectSqlStrFormat('country', countrys)

        if channels:
            sql_temp += SelectSqlStrFormat('channel', channels)

        sql_ios = 'SELECT date,new_activate FROM opsmanage_report_realtime WHERE platform = "IOS" and app_id = "%s" and date >= "%s" and date <= "%s"' % (ios_app_id, start_time, end_time+' 23')
        if sql_temp:
            sql_ios = sql_ios + sql_temp
        df_ios = pd.read_sql(sql_ios, connection)
        if not df_ios.index.tolist():
            df_ios.date = df_temp.date
            df_ios = df_ios.groupby('date').agg(['sum']).reset_index()
        else:
            df_ios = df_ios.groupby('date').agg(['sum']).reset_index()
            df_ios = pd.merge(df_ios, df_temp,on=['date'], how='outer').sort_values(by='date').reset_index(drop=True)
        df_ios = df_ios.fillna(0)

        sql_android = 'SELECT date,new_activate FROM opsmanage_report_realtime WHERE platform = "Android" and app_id = "%s" and date >= "%s" and date <= "%s"' % (
            android_app_id, start_time, end_time+' 23')
        if sql_temp:
            sql_android = sql_android + sql_temp
        df_android = pd.read_sql(sql_android, connection)

        if not df_android.index.tolist():
            df_android.date = df_temp.date
            df_android = df_android.groupby('date').agg(['sum']).reset_index()
        else:
            df_android = df_android.groupby('date').agg(['sum']).reset_index()
            df_android = pd.merge(df_android, df_temp, on=['date'], how='outer').sort_values(by='date').reset_index(drop=True)
        df_android = df_android.fillna(0)
        df = df_ios + df_android
        df['date'] = df['date'].map(lambda x: x[:13])

        df_show = df_ios.join(df_android.set_index('date'), on='date', lsuffix='_ios', rsuffix='_android')
        df_show = df_show.join(df.set_index('date'), on='date')

        df_show_cum = df_show.cumsum()
        df_show['cum_sum_ios'] = df_show_cum['new_activate_ios'].astype(int)
        df_show['cum_sum_android'] = df_show_cum['new_activate_android'].astype(int)
        df_show['cum_sum'] = df_show_cum['new_activate'].astype(int)
        df_show['new_activate_ios'] = df_show['new_activate_ios'].astype(int)
        df_show['new_activate_android'] = df_show['new_activate_android'].astype(int)
        df_show['new_activate'] = df_show['new_activate'].astype(int)


        df_show_list = np.array(df_show).tolist()

        pre_sql_ios = 'SELECT date,new_activate FROM opsmanage_report_realtime WHERE platform = "IOS" and date >= "%s" and date <= "%s"' % (
            pre_start_time, pre_end_time+' 23')
        if sql_temp:
            pre_sql_ios = pre_sql_ios + sql_temp

        pre_ser_temp_lst = []
        temp_time = pre_start_time
        while temp_time <= pre_end_time:
            pre_ser_temp_lst.extend([temp_time + ' ' + hour for hour in
                                 ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13',
                                  '14', '15', '16', '17', '18',
                                  '19', '20', '21', '22', '23']])
            temp_time = str(datetime.date(*list(map(int, temp_time.split('-')))) + datetime.timedelta(days=1))
        pre_ser_temp = pd.Series(pre_ser_temp_lst)
        pre_df_temp = pre_ser_temp.to_frame(name='date')
        pre_df_ios = pd.read_sql(pre_sql_ios, connection)

        if not pre_df_ios.index.tolist():
            pre_df_ios.date = pre_df_temp.date
            pre_df_ios = pre_df_ios.groupby('date').agg(['sum']).reset_index()
        else:
            pre_df_ios = pre_df_ios.groupby('date').agg(['sum']).reset_index()
            pre_df_ios = pd.merge(pre_df_ios, pre_df_temp, on=['date'], how='outer').sort_values(by='date').reset_index(
                drop=True)
        pre_df_ios = pre_df_ios.fillna(0)
        pre_sql_android = 'SELECT date,new_activate FROM opsmanage_report_realtime WHERE platform = "Android" and date >= "%s" and date <= "%s"' % (
            pre_start_time, pre_end_time+' 23')
        if sql_temp:
            pre_sql_android = pre_sql_android + sql_temp
        pre_df_android = pd.read_sql(pre_sql_android, connection)

        if not pre_df_android.index.tolist():
            pre_df_android.date = pre_df_temp.date
            pre_df_android = pre_df_android.groupby('date').agg(['sum']).reset_index()
        else:
            pre_df_android = pre_df_android.groupby('date').agg(['sum']).reset_index()
            pre_df_android = pd.merge(pre_df_android, pre_df_temp, on=['date'], how='outer').sort_values(by='date').reset_index(
                drop=True)
        pre_df_android = pre_df_android.fillna(0)
        pre_df = pre_df_ios + pre_df_android
        pre_df['date'] = pre_df['date'].map(lambda x: x[:13])
        pre_df_show = pre_df_ios.join(pre_df_android.set_index('date'), on='date', lsuffix='_ios', rsuffix='_android')
        pre_df_show = pre_df_show.join(pre_df.set_index('date'), on='date')

        date_total = df.date.tolist()
        date_total = json.dumps(date_total)

        new_activate = df.new_activate['sum'].tolist()
        new_activate_ios = df_ios.new_activate['sum'].tolist()
        new_activate_android = df_android.new_activate['sum'].tolist()
        pre_new_activate = pre_df.new_activate['sum'].tolist()
        pre_new_activate_ios = pre_df_ios.new_activate['sum'].tolist()
        pre_new_activate_android = pre_df_android.new_activate['sum'].tolist()

        return render(request, 'report/report_real_detail.html', locals())

def dataChange(rowchannel,rawsubchannel):
    if rowchannel == rawsubchannel:
        return rowchannel
    else:
        return rowchannel

@login_required()
@permission_required('OpsManage.can_read_channel_analysis_day_record', login_url='/noperm/')
def channel_analysis_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        channels = request.POST.getlist('channels[]')
        countrys = request.POST.getlist('countrys[]')
        country_show = request.POST.get('country_show')
        channel_show = request.POST.get('channel_show')
        time_show = request.POST.get('time_show')
        conditionList = []
        conditionList.append(request.POST.get('condition_1'))
        conditionList.append(request.POST.get('condition_2'))
        conditionList.append(request.POST.get('condition_3'))
        conditionList.append(request.POST.get('condition_4'))
        conditionList.append(request.POST.get('condition_5'))
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        languages = request.POST.getlist('gameverisions[]')
        platforms = request.POST.getlist('platforms[]')
        checkbox = request.POST.get('checkbox', '')
        f_start_time = request.POST.get('f_start_time')
        f_end_time = request.POST.get('f_end_time')
        game_perms = request.POST.get('game_perms')
        bHaveOrganic = False
        if len(channels) == 0:
            bHaveOrganic = True

        gameinfo = Game_Version_Config.objects.filter(name=game_perms)
        if len(gameinfo) <= 0:
            return render(request, 'report/channel_analysis_detail.html', locals())
        appid_sql = "AND (app_id ='" + gameinfo[0].android_appid + "'or app_id='" + gameinfo[0].ios_appid + "')"

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '2000-01-01 01:01'

        end_time_t = time.mktime(pytz.timezone(gameinfo[0].zone).localize(
            datetime.datetime(*tuple(parse(end_time).timetuple())[:-2])).astimezone(
            pytz.timezone('Europe/Moscow')).timetuple())
        start_time_t = time.mktime(pytz.timezone(gameinfo[0].zone).localize(
            datetime.datetime(*tuple(parse(start_time).timetuple())[:-2])).astimezone(
            pytz.timezone('Europe/Moscow')).timetuple())

        end_timestamp = time.mktime(tuple(parse(str(datetime.date(*list(map(int, end_time[:10].split('-')))) + datetime.timedelta(days=1))).timetuple()))
        start_timestamp = time.mktime(tuple(parse(start_time[:10]).timetuple()))

        if not f_end_time:
            f_end_time = now_time
        if not f_start_time :
            if f_end_time == now_time:
                f_start_date = datetime.datetime.strptime(now_time, "%Y-%m-%d %H:%M")
                f_start_time = (f_start_date + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H:%M")
            else:
                f_start_time = '2000-01-01 01:01'
        f_end_time = str(datetime.date(*list(map(int, f_end_time[:10].split('-')))) + datetime.timedelta(days=1))

        f_end_timestamp = time.mktime(tuple(parse(f_end_time).timetuple()))
        f_start_timestamp = time.mktime(tuple(parse(f_start_time[:10]).timetuple()))

        sql_temp = ''
        appsflyer_sql_tmp = ""
        active_sql_tmp = ""
        if languages:
            sql_language = ''
            for lan in languages:
                sql_language += ' language = "{}" or '.format(lan)

            sql_temp += ' and {} '.format('(' + sql_language[:-3] + ')')
            active_sql_tmp += ' and {} '.format('(' + sql_language[:-3] + ')')
            appsflyer_sql_tmp += ' and {} '.format('(' + sql_language[:-3] + ')')
        if platforms:
            sql_platform = ""
            sql_active = ""
            for pl in platforms:
                sql_platform += ' platform = "{}" or '.format(pl)
                sql_active += ' platform = "{}" or '.format(pl)

            sql_temp += ' and {} '.format('(' + sql_platform[:-3] + ')')
            active_sql_tmp  += ' and {} '.format('(' + sql_active[:-3] + ')')
            appsflyer_sql_tmp += ' and {} '.format('(' + sql_platform[:-3] + ')')
        #是否需要子渠道的数据
        newChannel = {}
        if channels:
            sql_channel = ''
            appssql_channel = ""
            for ch in channels:
                if "Organic" == ch:
                    bHaveOrganic = True
                # 判断是否有渠道提取
                Isolate_Channel_Data = Isolate_Channel.objects.filter(newname=ch)
                if Isolate_Channel_Data.count() == 1:
                    prename = Isolate_Channel_Data[0].prename
                    rule = Isolate_Channel_Data[0].rule
                    newname = Isolate_Channel_Data[0].newname
                    newChannel[newname] = [prename, rule]
                    if prename not in channels:
                        ch = prename

                    else:
                        continue
                sql_channel += ' channel = "{}" or '.format(ch)
                appssql_channel += ' channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')
            appsflyer_sql_tmp+= ' and {} '.format('(' + appssql_channel[:-3] + ')')
        else:
            bHaveOrganic = True
        if countrys:
            sql_country = ''
            appssql_country = ''
            for ch in countrys:
                sql_country += ' country = "{}" or '.format(ch)
                appssql_country+= ' country_code = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_country[:-3] + ')')
            appsflyer_sql_tmp += ' and {} '.format('(' + appssql_country[:-3] + ')')
            active_sql_tmp += ' and {} '.format('(' + sql_country[:-3] + ')')

        sql_temp += appid_sql
        appsflyer_sql_tmp += appid_sql
        active_sql_tmp += appid_sql

        dime_list = []
        show_dict = {}
        showappsflyerls = []
        showactive = ["device_id"]

        for condition in conditionList:
            if condition ==  'country':
                if '国家' in dime_list:
                    continue
                dime_list.append('国家')
                show_dict['国家'] = 'country,' 
                showappsflyerls.append("country_code as country")
                showactive.append("country")
            elif condition == 'time':
                if '日期' in dime_list:
                    continue
                dime_list.append('日期')
                show_dict['日期'] = 'date,'
                showappsflyerls.append("install_time_selected_timezone as date")
                showactive.append("time as date")
            elif condition == 'channel':
                if '渠道' in dime_list:
                    continue
                dime_list.append( '渠道')
                show_dict['渠道'] = 'channel,'
                showappsflyerls.append("channel")
            elif condition == 'language':
                if '版本' in dime_list:
                    continue
                dime_list.append('版本')
                show_dict['版本'] = 'language,'
                showappsflyerls.append("language")
                showactive.append("language")
            elif condition == 'platform':
                if '平台' in dime_list:
                    continue
                dime_list.append('平台')
                show_dict['平台'] = 'platform,'
                showappsflyerls.append("platform")
                showactive.append("platform")

        channel_head = []
        channel_head.extend(dime_list)
        channel_head.extend(['新增激活数', '激活数占比', '广告花费', '激活成本', '新增设备上线', '上线率'])

        dime_str = ''
        dime_group = []
        ad_str = ""
        ad_group = []
        for di in dime_list:
            dime_str += show_dict.get(di, '')
            ad_group.append(show_dict.get(di, ' ')[:-1])
            ad_str+= show_dict.get(di, '')
            dime_group.append(show_dict.get(di, ' ')[:-1])

        if len(newChannel) > 0:
            dime_str += 'sub1_channel,'
            dime_group.append('sub1_channel')

        sql = 'SELECT %s new_activate,new_device_onl FROM opsmanage_channel_analysis_day WHERE date >= "%s" and date <= "%s"' % (
            dime_str, start_time, end_time)
        if sql_temp:
            sql = sql + sql_temp

        df = pd.read_sql(sql, connection)
        if not df.index.tolist():
            return render(request, 'report/channel_analysis_detail.html', locals())

        total_act = Channel_Analysis_Day.objects.filter(date__gte=start_time, date__lte=end_time).aggregate(total_act=Sum('new_activate')).get('total_act')

        channel_colume = df.columns.tolist()[:-3]

        if len(newChannel) > 0:
            for newchannel, subChannel in list(newChannel.items()):
                df.loc[(df["channel"] == subChannel[0]) & (df["sub1_channel"].str.startswith(subChannel[1])) , "channel"] = newchannel
                try:
                    if subChannel[0] not in channels:
                        df = df[~df['channel'].isin([subChannel[0]])]
                except Exception as e:
                    print(e)
            dime_group.remove("sub1_channel")
            df = df.drop('sub1_channel', axis = 1)
            channel_colume.remove('sub1_channel')


        # df = df.groupby(dime_group, as_index=False).sum()
        # 广告
        adsql = 'SELECT %s ad_cost FROM opsmanage_ad_cost_conf WHERE date >= "%s" and date <= "%s" ' % (
            ad_str, start_time, end_time)
        ad_df = pd.read_sql(adsql, connection)

        if ad_df.empty:
            df["ad_cost"] = 0
        df = df.groupby(dime_group).agg(['sum']).reset_index()

        if not ad_df.empty:
            ad_df = ad_df.groupby(ad_group).agg(['sum']).reset_index()
            df = pd.merge(df, ad_df, how="left", on=ad_group)

        if "channel" in dime_group:
            dfCal = df.copy()
            for key in dime_group:
                if key != "channel":
                    del dfCal[key]
            del dfCal["ad_cost"]
            dfCal.loc[(dfCal["channel"].str.startswith("Facebook")) | (dfCal["channel"].str.startswith("google")) | (dfCal["channel"].str.startswith("youtube"))| (dfCal["channel"].str.startswith("Twitter")) | (dfCal["channel"].str.startswith("Instagram"))| (dfCal["channel"].str.startswith("Snapchat"))| (dfCal["channel"].str.endswith("_nonint")), "channel"] = "Non-Organic"
            dfCal.loc[(dfCal["channel"].str.endswith("_int")), "channel"] = "Non-Organic"
            dfCal.loc[(dfCal["channel"].str.endswith("share")), "channel"] = "Non-Organic"
            dfCal.loc[(dfCal["channel"] == "Organic") | (dfCal["channel"] == "None"), "channel"] = "Organic"
            # 特殊梳理
            dfCal.loc[(dfCal["channel"].str.startswith("SG-Android-GCC")) | (dfCal["channel"].str.startswith("SG-iOS-GCC"))| (dfCal["channel"].str.startswith("SG_GCC_Android")), "channel"] = "Non-Organic"
            dfCal.loc[(dfCal["channel"].str.startswith("Tapjoy_CPE")), "channel"] = "Non-Organic"
            cols = ["channel"]
            cols.extend(["new_activate","new_device_onl"])
            dfCal = dfCal.groupby("channel", as_index=False).sum()
            dfCal.columns = cols
            dfCal = dfCal.groupby("channel").agg(['sum']).reset_index()

        sum_list = df.sum().tolist()

        sum_list[0] = '总计'
        df.loc[df.shape[0]] = sum_list
        if dime_group:
            for i in range(len(dime_group)-1):
                df.iloc[df.shape[0]-1, i+1] = ''

        if "channel" in dime_group:
            df = pd.concat([df, dfCal], ignore_index=True)


        #激活成本
        df['pernew_cost'] = df.ad_cost / df.new_activate
        #注册率
        # df['reg_rate'] = df.new_device_reg / df.new_activate
        #新增注册成本
        # df['perreg_cost'] = df.ad_cost / df.new_device_reg
        # 上线率
        df['onl_rate'] = df.new_device_onl / df.new_activate
        #新增上线成本
        # df['peronl_cost'] = df.ad_cost / df.new_device_onl
        #激活数占比
        df['act_rate'] = df['new_activate'] / total_act


        #激活成本
        df['pernew_cost'] = df['pernew_cost'].apply(lambda x: round(x,2))
        #注册率
        # df['reg_rate'] = df['reg_rate'].apply(lambda x: str(round(x*100,2))+'%')
        #新增注册成本
        # df['perreg_cost'] = df['perreg_cost'].apply(lambda x: round(x,2))
        # 上线率
        df['onl_rate'] = df['onl_rate'].apply(lambda x: str(round(x*100,2))+'%')
        #新增上线成本
        # df['peronl_cost'] = df['peronl_cost'].apply(lambda x: round(x,2))
        #激活数占比
        df['act_rate'] = df['act_rate'].apply(lambda x: str(round(x*100,2))+'%')
        channel_colume.extend(
            ['channel','new_activate', 'act_rate', 'ad_cost', 'pernew_cost',
             'new_device_onl', 'onl_rate'])
        df = df[channel_colume]

        df = df.fillna(0)
        channel_day_list = np.array(df).tolist()

        calCnt = dfCal.shape[0] + 1
        channel_list = df.channel.tolist()[:-calCnt]
        channel_list = list(set(channel_list))

        if "date" not in dime_group:
            date_list = []
        else:
            date_list = df.date.tolist()[:-calCnt]
        date_list = list(set(date_list))

        activate_data = []
        cpi_data = []
        channel_act_dict = dict(list(zip(channel_list,[list() for i in range(len(channel_list))])))
        channel_cpi_dict = dict(list(zip(channel_list,[list() for i in range(len(channel_list))])))
        channel_amount_dict = dict(list(zip(channel_list,[list() for i in range(len(channel_list))])))
        channel_ad_cost_dict = dict(list(zip(channel_list,[list() for i in range(len(channel_list))])))
        activate_sum_data = []
        amount_sum_data = []
        amount_sum_data_comp = []
        activate_sum_data_comp = []
        channel_ad_cost_data = []

        for i,j in zip(df.channel.tolist()[:-calCnt], df.new_activate['sum'].tolist()[:-calCnt]):
            channel_act_dict[i].append(j)
        for i,j in zip(df.channel.tolist()[:-calCnt], df.pernew_cost.tolist()[:-calCnt]):
            channel_cpi_dict[i].append(j)
        # for i,j in zip(df.channel.tolist()[:-calCnt], df.recharge_amount['sum'].tolist()[:-calCnt]):
        #     channel_amount_dict[i].append(j)
        for i,j in zip(df.channel.tolist()[:-calCnt], df.ad_cost['sum'].tolist()[:-calCnt]):
            channel_ad_cost_dict[i].append(j)
        for channel in channel_list:
            dict_temp = {}
            dict_temp['name'] = channel
            dict_temp['type'] = 'line'
            dict_temp['data'] = channel_act_dict.get(channel,[])
            activate_data.append(dict_temp)
        for channel in channel_list:
            dict_temp = {}
            dict_temp['name'] = channel
            dict_temp['type'] = 'line'
            dict_temp['data'] = channel_cpi_dict.get(channel, [])
            cpi_data.append(dict_temp)
        for channel in channel_list:
            activate_sum_data.append(sum(channel_act_dict.get(channel,[])))
            # amount_sum_data.append(sum(channel_amount_dict.get(channel,[])))
            channel_ad_cost_data.append(round(sum(channel_ad_cost_dict.get(channel,[])) / sum(channel_act_dict.get(channel,[])),2))
        for k, v in zip(amount_sum_data, channel_list):
            amount_sum_data_comp.append({'value': k, 'name': v})
        for k, v in zip(activate_sum_data, channel_list):
            activate_sum_data_comp.append({'value': k, 'name': v})

        channel_list = json.dumps(channel_list)
        date_list = json.dumps(date_list)
        activate_data = json.dumps(activate_data)
        cpi_data = json.dumps(cpi_data)
        activate_sum_data = json.dumps(activate_sum_data)
        amount_sum_data = json.dumps(amount_sum_data)
        amount_sum_data_comp = json.dumps(amount_sum_data_comp)
        activate_sum_data_comp = json.dumps(activate_sum_data_comp)
        channel_ad_cost_data = json.dumps(channel_ad_cost_data)

        if checkbox == 'true':
            channel_head.extend(['累计付费设备数', '累计付费充值金额', 'ROI', '付费率', 'ARPPU', 'LTV'])
            matchls = [value[0:-1] for value in list(show_dict.values())]
            # 渠道，日期， 国家
            if len(newChannel) > 0:
                showappsflyerls.append("sub1_channel")
            # analysis 和appsflyer时间的表示不一样
            start_time_stamp = time.mktime(time.strptime(end_time, "%Y-%m-%d"))
            appsflyer_end = datetime.date.fromtimestamp(start_time_stamp) + datetime.timedelta(days= + 1)
            appsflyer_end_t = time.mktime(pytz.timezone(gameinfo[0].zone).localize(
                datetime.datetime(*tuple(parse(str(appsflyer_end)).timetuple())[:-2])).astimezone(
                pytz.timezone('Europe/Moscow')).timetuple())

            sql_channel = 'SELECT device_id ,%s  FROM opsmanage_push_appsflyer  WHERE install_time_selected_timezone >= "%s" and install_time_selected_timezone < "%s"' % (",".join(showappsflyerls), start_time, appsflyer_end)
            if appsflyer_sql_tmp:
                sql_channel = sql_channel + appsflyer_sql_tmp
            df_channel = pd.read_sql(sql_channel, connection)
            if "date" in matchls:
                df_channel['date'] = df_channel['date'].apply(lambda x: x[:10])

            # db_time_end = datetime.date(*map(int, end_time[:10].split('-'))) + datetime.timedelta(days=1)
            # 是否有自然用户
            if bHaveOrganic:
                sql_cb = 'SELECT %s FROM opsmanage_push_client_activate WHERE db_time_t >= "%s" and db_time_t < "%s" ' % (",".join(showactive), start_time_t, appsflyer_end_t)
                if active_sql_tmp:
                    sql_cb +=  active_sql_tmp
                df_cb = pd.read_sql(sql_cb, connection)
                if "日期" in list(show_dict.keys()):
                    df_cb['date'] = df_cb['date'].map(
                        lambda x: pytz.timezone("Asia/Shanghai").localize(
                            datetime.datetime.fromtimestamp(int(x))).astimezone(
                            pytz.timezone("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")[0:10])
                df_cb["channel"] = "Organic"
                df_channel = pd.concat([df_channel, df_cb], ignore_index=True)
            df_channel = df_channel.drop_duplicates('device_id')


            # sql_cr_ios = 'SELECT device_id,recharge as aggregate_recharge FROM opsmanage_push_client_recharge WHERE time >= %d and time <= %d' % (
            #     f_start_timestamp, f_end_timestamp)
            sql_cr_ios = 'SELECT device_id,SUM(recharge) as aggregate_recharge FROM opsmanage_push_client_recharge WHERE createtime >= "%s" and createtime < "%s"  GROUP BY (device_id)' % (
                f_start_time, f_end_time)
            df_cb_ios = pd.read_sql(sql_cr_ios, connection)

            df_channel = pd.merge(df_cb_ios, df_channel, how="left", on=["device_id"])

            df_channel["aggregate_payid"] = 1
            if df_channel.empty:
                df_channel["aggregate_recharge"] = 0
            df_channel.fillna(0)

            if len(newChannel) > 0:
                for newchannel, subChannel in list(newChannel.items()):
                    df_channel.loc[(df_channel["channel"] == subChannel[0]) & (
                        df_channel["sub1_channel"].str.startswith(subChannel[1])), "channel"] = newchannel
                    try:
                        if subChannel[0] not in channels:
                            df_channel = df_channel[~df_channel['channel'].isin([subChannel[0]])]
                    except Exception as e:
                        print(e)
                df_channel = df_channel.drop('sub1_channel', axis=1)

            df_cb_group = df_channel.groupby(matchls,as_index=False).sum()

            paydata = pd.merge(df, df_cb_group, how='left', on=matchls)

            paydata.loc[paydata.shape[0] - 1 - len(dfCal), 'aggregate_payid'] = paydata['aggregate_payid'].sum()
            paydata.loc[paydata.shape[0] - 1 - len(dfCal), 'aggregate_recharge'] = paydata['aggregate_recharge'].sum()
            removels = []
            for i in range(paydata.shape[0] - 1 - len(dfCal),paydata.shape[0]):
                removels.append(i)
            tmppaydata = paydata.drop(removels)

            aggregate_data = pd.DataFrame([tmppaydata['channel'], tmppaydata['aggregate_payid'], tmppaydata['aggregate_recharge']])
            aggregate_data = aggregate_data.T
            aggregate_data.loc[(aggregate_data["channel"].str.startswith("Facebook")) | (aggregate_data["channel"].str.startswith("google")) | (aggregate_data["channel"].str.startswith("Twitter")) | (aggregate_data["channel"].str.startswith("Instagram")) | (aggregate_data["channel"].str.startswith("Snapchat")) | (aggregate_data["channel"].str.endswith("_nonint")), "channel"] = "Non-Organic"
            aggregate_data.loc[(aggregate_data["channel"].str.endswith("_int")), "channel"] = "Non-Organic"
            aggregate_data.loc[(aggregate_data["channel"].str.endswith("share")), "channel"] = "Non-Organic"
            aggregate_data.loc[(aggregate_data["channel"] == "Organic") | (aggregate_data["channel"] == "None"), "channel"] = "Organic"
            # 特殊梳理
            aggregate_data.loc[(aggregate_data["channel"].str.startswith("SG-Android-GCC")) | (aggregate_data["channel"].str.startswith("SG-iOS-GCC")) | (aggregate_data["channel"].str.startswith("SG_GCC_Android")), "channel"] = "Non-Organic"
            aggregate_data.loc[(aggregate_data["channel"].str.startswith("Tapjoy_CPE")), "channel"] = "Non-Organic"
            aggregate_data = aggregate_data.groupby("channel", as_index=False).sum()
            startindex = paydata.shape[0] - len(dfCal)
            for i in range(startindex, paydata.shape[0]):
                paydata.loc[i, 'aggregate_payid'] = aggregate_data['aggregate_payid'][i-startindex]
                paydata.loc[i, 'aggregate_recharge'] = aggregate_data['aggregate_recharge'][i-startindex]


            # 累计付费设备数
            df['aggregate_payid'] = paydata["aggregate_payid"]
            # 累计付费金额
            df['aggregate_recharge'] =paydata["aggregate_recharge"]
            # ROI
            df['roi'] = paydata["aggregate_recharge"] / df.ad_cost["sum"].tolist()
            # # 付费率
            df['pay_rate'] = paydata["aggregate_payid"] / df.new_activate["sum"].tolist()
            # # ARPPU
            df['arppu'] = paydata["aggregate_recharge"] / df['aggregate_payid']
            # # LTV
            df['ltv'] = paydata["aggregate_recharge"] / df.new_activate["sum"].tolist()
            df['roi'] = df['roi'].map(lambda x: round(x, 2))
            # df['pay_rate'] = df['pay_rate'].map(lambda x: round(x, 2))
            # df['arppu'] = df['arppu'].map(lambda x: round(x, 2))
            # df['ltv'] = df['ltv'].map(lambda x: round(x, 2))
            df = df.fillna(0)
            df['aggregate_payid'] = df['aggregate_payid'].apply(lambda x: int(x) if x else 0)
            df['aggregate_recharge'] = df['aggregate_recharge'].apply(lambda x: '$' + str(round(x, 2)))
            df['arppu'] = df['arppu'].apply(lambda x: '$' + str(round(x, 2)))
            df['pay_rate'] = df['pay_rate'].apply(lambda x: str(round(x * 100, 2)) + '%')
            df['ltv'] = df['ltv'].apply(lambda x: '$' +  str(round(x, 2)))
        df = df.fillna(0)
        df['pernew_cost'] = df['pernew_cost'].apply(lambda x: '$' + str(round(x, 2)))
        # df['perreg_cost'] = df['perreg_cost'].apply(lambda x: '$' + str(round(x, 2)))
        # df['peronl_cost'] = df['peronl_cost'].apply(lambda x: '$' + str(round(x, 2)))
        df[('ad_cost','sum')] = df[('ad_cost','sum')].map(lambda x: '$' + str(round(x, 2)))
        channel_day_list = np.array(df).tolist()
        return render(request, 'report/channel_analysis_detail.html', locals())

@login_required(login_url='/login')
@permission_required('OpsManage.can_read_subchannel_analysis_record', login_url='/noperm/')
def subchannel_analysis_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        channels = request.POST.getlist('channels')
        countrys = request.POST.getlist('countrys[]')
        subchannels = request.POST.get('subchannels')
        sub1channels = request.POST.get('sub1channels')
        sub2channels = request.POST.get('sub2channels')
        country_show = request.POST.get('country_show')
        channel_show = request.POST.get('channel_show')
        time_show = request.POST.get('time_show')
        conditionList = []
        conditionList.append(request.POST.get('condition_1'))
        conditionList.append(request.POST.get('condition_2'))
        conditionList.append(request.POST.get('condition_3'))
        conditionList.append(request.POST.get('condition_4'))
        conditionList.append(request.POST.get('condition_5'))
        conditionList.append(request.POST.get('condition_6'))
        conditionList.append(request.POST.get('condition_7'))
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        languages = request.POST.getlist('gameverisions[]')
        platforms = request.POST.getlist('platforms[]')
        checkbox = request.POST.get('checkbox', '')
        f_start_time = request.POST.get('f_start_time')
        f_end_time = request.POST.get('f_end_time')
        bHaveOrganic = False
        game_perms = request.POST.get('game_perms')

        if len(channels) == 0:
            bHaveOrganic = True

        gameinfo = Game_Version_Config.objects.filter(name=game_perms)
        if len(gameinfo) <= 0:
            return render(request, 'report/subchannel_analysis_detail.html', locals())
        appid_sql = "AND (app_id ='" + gameinfo[0].android_appid + "'or app_id='" + gameinfo[0].ios_appid + "')"

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '2000-01-01 01:01'

        end_timestamp = time.mktime(tuple(parse(str(datetime.date(*list(map(int, end_time[:10].split('-')))) + datetime.timedelta(days=1))).timetuple()))
        start_timestamp = time.mktime(tuple(parse(start_time[:10]).timetuple()))

        if not f_end_time:
            f_end_time = now_time
        if not f_start_time :
            if f_end_time == now_time:
                f_start_date = datetime.datetime.strptime(now_time, "%Y-%m-%d %H:%M")
                f_start_time = (f_start_date + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H:%M")
            else:
                f_start_time = '2000-01-01 01:01'
        f_end_time = str(datetime.date(*list(map(int, f_end_time[:10].split('-')))) + datetime.timedelta(days=1))

        f_end_timestamp = time.mktime(tuple(parse(f_end_time).timetuple()))
        f_start_timestamp = time.mktime(tuple(parse(f_start_time[:10]).timetuple()))

        sql_temp = ''
        appsflyer_sql_tmp = ""
        active_sql_tmp = ""
        if languages:
            sql_language = ''
            for lan in languages:
                sql_language += ' language = "{}" or '.format(lan)

            sql_temp += ' and {} '.format('(' + sql_language[:-3] + ')')
            active_sql_tmp += ' and {} '.format('(' + sql_language[:-3] + ')')
            appsflyer_sql_tmp += ' and {} '.format('(' + sql_language[:-3] + ')')
        if platforms:
            sql_platform = ""
            sql_active = ""
            for pl in platforms:
                sql_platform += ' platform = "{}" or '.format(pl)
                sql_active += ' platform = "{}" or '.format(pl)

            sql_temp += ' and {} '.format('(' + sql_platform[:-3] + ')')
            active_sql_tmp += ' and {} '.format('(' + sql_active[:-3] + ')')
            appsflyer_sql_tmp += ' and {} '.format('(' + sql_platform[:-3] + ')')
        if channels:
            sql_channel = ''
            appssql_channel = ""
            for ch in channels:
                if "Organic" == ch:
                    bHaveOrganic = True
                sql_channel += ' channel = "{}" or '.format(ch)
                appssql_channel += ' channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')
            appsflyer_sql_tmp+= ' and {} '.format('(' + appssql_channel[:-3] + ')')

        if subchannels:
            # sql_channel = ''
            # appssql_channel = ""
            # for ch in subchannels:
            sql_channel = ' sub1_channel like "%{}%" or '.format(subchannels)
            appssql_channel = ' sub1_channel like "%{}%" or '.format(subchannels)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')
            appsflyer_sql_tmp+= ' and {} '.format('(' + appssql_channel[:-3] + ')')

        if sub1channels:
            # sql_channel = ''
            # appssql_channel = ""
            # for ch in sub1channels:
            sql_channel = ' sub2_channel like "%{}%" or '.format(sub1channels)
            appssql_channel = ' sub2_channel like "%{}%" or '.format(sub1channels)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')
            appsflyer_sql_tmp+= ' and {} '.format('(' + appssql_channel[:-3] + ')')

        if sub2channels:
            # sql_channel = ''
            # appssql_channel = ""
            # for ch in sub2channels:
            sql_channel = ' sub3_channel like "%{}%" or '.format(sub2channels)
            appssql_channel = ' sub3_channel like "%{}%" or '.format(sub2channels)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')
            appsflyer_sql_tmp+= ' and {} '.format('(' + appssql_channel[:-3] + ')')

        if countrys:
            sql_country = ''
            appssql_country = ''
            for ch in countrys:
                sql_country += ' country = "{}" or '.format(ch)
                appssql_country+= ' country_code = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_country[:-3] + ')')
            appsflyer_sql_tmp += ' and {} '.format('(' + appssql_country[:-3] + ')')
            active_sql_tmp += ' and {} '.format('(' + sql_country[:-3] + ')')

        sql_temp += appid_sql
        appsflyer_sql_tmp += appid_sql
        active_sql_tmp += appid_sql

        # dime_list = ['子渠道', '子子渠道', '子子子渠道']
        # show_dict = {'子渠道': 'sub1_channel,','子子渠道': 'sub2_channel,','子子子渠道': 'sub3_channel,'}
        # showappsflyerls = ['sub1_channel', 'sub2_channel', 'sub3_channel']
        # showactive = ["device_id"]
        #
        # if country_show and country_show != 'country':
        #     dime_list.append('国家')
        #     show_dict['国家'] = 'country,'
        #     showappsflyerls.append("country_code as country")
        #     showactive.append("country")
        # if time_show and time_show != 'time':
        #     dime_list.insert(int(time_show) + 2, '日期')
        #     show_dict['日期'] = 'date,'
        #     showappsflyerls.append("install_time_selected_timezone as date")
        #     showactive.append("time as date")

        dime_list = []
        show_dict = {}
        showappsflyerls = []
        showactive = ["device_id"]

        for condition in conditionList:
            if condition == 'country':
                if '国家' in dime_list:
                    continue
                dime_list.append('国家')
                show_dict['国家'] = 'country,'
                showappsflyerls.append("country_code as country")
                showactive.append("country")
            elif condition == 'time':
                if '日期' in dime_list:
                    continue
                dime_list.append('日期')
                show_dict['日期'] = 'date,'
                showappsflyerls.append("install_time_selected_timezone as date")
                showactive.append("time as date")
            elif condition == 'channel_1':
                if '子渠道' in dime_list:
                    continue
                dime_list.append('子渠道')
                show_dict['子渠道'] = 'sub1_channel,'
                showappsflyerls.append("sub1_channel")
            elif condition == 'channel_2':
                if '子子渠道' in dime_list:
                    continue
                dime_list.append('子子渠道')
                show_dict['子子渠道'] = 'sub2_channel,'
                showappsflyerls.append("sub2_channel")
            elif condition == 'channel_3':
                if '子子子渠道' in dime_list:
                    continue
                dime_list.append('子子子渠道')
                show_dict['子子子渠道'] = 'sub3_channel,'
                showappsflyerls.append("sub3_channel")
            elif condition == 'language':
                if '版本' in dime_list:
                    continue
                dime_list.append('版本')
                show_dict['版本'] = 'language,'
                showappsflyerls.append("language")
                showactive.append("language")
            elif condition == 'platform':
                if '平台' in dime_list:
                    continue
                dime_list.append('平台')
                show_dict['平台'] = 'platform,'
                showappsflyerls.append("platform")
                showactive.append("platform")

        channel_head = []
        channel_head.extend(dime_list)
        channel_head.extend(['新增激活数', '激活数占比', '广告花费', '激活成本', '新增设备上线', '上线率'])

        dime_str = ''
        dime_group = []
        for di in dime_list:
            dime_str += show_dict.get(di, '')
            dime_group.append(show_dict.get(di, ' ')[:-1])

        sql = 'SELECT %s ad_cost,new_activate,new_device_onl FROM opsmanage_channel_analysis_day WHERE date >= "%s" and date <= "%s"' % (
            dime_str, start_time, end_time)
        if sql_temp:
            sql = sql + sql_temp

        df = pd.read_sql(sql, connection)
        if not df.index.tolist():
            return render(request, 'report/channel_analysis_detail.html', locals())

        total_act = Channel_Analysis_Day.objects.filter(date__gte=start_time, date__lte=end_time).aggregate(total_act=Sum('new_activate')).get('total_act')
        channel_colume = df.columns.tolist()[:-4]

        df = df.groupby(dime_group).agg(['sum']).reset_index()

        sum_list = df.sum().tolist()
        sum_list[0] = '总计'
        df.loc[df.shape[0]] = sum_list

        #激活成本
        df['pernew_cost'] = df.ad_cost / df.new_activate
        #注册率
        # df['reg_rate'] = df.new_device_reg / df.new_activate
        #新增注册成本
        # df['perreg_cost'] = df.ad_cost / df.new_device_reg
        # 上线率
        df['onl_rate'] = df.new_device_onl / df.new_activate
        #新增上线成本
        # df['peronl_cost'] = df.ad_cost / df.new_device_onl
        #激活数占比
        df['act_rate'] = df['new_activate'] / total_act

        if dime_group:
            for i in range(len(dime_group)-1):
                df.iloc[df.shape[0]-1, i+1] = ''

        #激活成本
        df['pernew_cost'] = df['pernew_cost'].apply(lambda x: round(x,2))
        #注册率
        # df['reg_rate'] = df['reg_rate'].apply(lambda x: str(round(x*100,2))+'%')
        #新增注册成本
        # df['perreg_cost'] = df['perreg_cost'].apply(lambda x: round(x,2))
        # 上线率
        df['onl_rate'] = df['onl_rate'].apply(lambda x: str(round(x*100,2))+'%')
        #新增上线成本
        # df['peronl_cost'] = df['peronl_cost'].apply(lambda x: round(x,2))
        #激活数占比
        df['act_rate'] = df['act_rate'].apply(lambda x: str(round(x*100,2))+'%')
        channel_colume.extend(
            ['sub1_channel','new_activate', 'act_rate', 'ad_cost', 'pernew_cost',
             'new_device_onl', 'onl_rate'])
        df = df[channel_colume]

        df = df.fillna(0)
        channel_day_list = np.array(df).tolist()

        channel_list = df.sub1_channel.tolist()[:-1]
        channel_list = list(set(channel_list))

        if "date" not in dime_group:
            date_list = []
        else:
            date_list = df.date.tolist()[:-1]
        date_list = list(set(date_list))

        activate_data = []
        cpi_data = []
        channel_act_dict = dict(list(zip(channel_list,[list() for i in range(len(channel_list))])))
        channel_cpi_dict = dict(list(zip(channel_list,[list() for i in range(len(channel_list))])))
        channel_amount_dict = dict(list(zip(channel_list,[list() for i in range(len(channel_list))])))
        channel_ad_cost_dict = dict(list(zip(channel_list,[list() for i in range(len(channel_list))])))
        activate_sum_data = []
        amount_sum_data = []
        amount_sum_data_comp = []
        activate_sum_data_comp = []
        channel_ad_cost_data = []

        for i,j in zip(df.sub1_channel.tolist()[:-1], df.new_activate['sum'].tolist()[:-1]):
            channel_act_dict[i].append(j)
        for i,j in zip(df.sub1_channel.tolist()[:-1], df.pernew_cost.tolist()[:-1]):
            channel_cpi_dict[i].append(j)
        # for i,j in zip(df.sub1_channel.tolist()[:-1], df.recharge_amount['sum'].tolist()[:-1]):
        #     channel_amount_dict[i].append(j)
        for i,j in zip(df.sub1_channel.tolist()[:-1], df.ad_cost['sum'].tolist()[:-1]):
            channel_ad_cost_dict[i].append(j)
        for channel in channel_list:
            dict_temp = {}
            dict_temp['name'] = channel
            dict_temp['type'] = 'line'
            dict_temp['data'] = channel_act_dict.get(channel,[])
            activate_data.append(dict_temp)
        for channel in channel_list:
            dict_temp = {}
            dict_temp['name'] = channel
            dict_temp['type'] = 'line'
            dict_temp['data'] = channel_cpi_dict.get(channel, [])
            cpi_data.append(dict_temp)
        for channel in channel_list:
            activate_sum_data.append(sum(channel_act_dict.get(channel,[])))
            # amount_sum_data.append(sum(channel_amount_dict.get(channel,[])))
            channel_ad_cost_data.append(round(sum(channel_ad_cost_dict.get(channel,[])) / sum(channel_act_dict.get(channel,[])),2))
        for k, v in zip(amount_sum_data, channel_list):
            amount_sum_data_comp.append({'value': k, 'name': v})
        for k, v in zip(activate_sum_data, channel_list):
            activate_sum_data_comp.append({'value': k, 'name': v})

        channel_list = json.dumps(channel_list)
        date_list = json.dumps(date_list)
        activate_data = json.dumps(activate_data)
        cpi_data = json.dumps(cpi_data)
        activate_sum_data = json.dumps(activate_sum_data)
        amount_sum_data = json.dumps(amount_sum_data)
        amount_sum_data_comp = json.dumps(amount_sum_data_comp)
        activate_sum_data_comp = json.dumps(activate_sum_data_comp)
        channel_ad_cost_data = json.dumps(channel_ad_cost_data)

        if checkbox == 'true':
            channel_head.extend(['累计付费设备数', '累计付费充值金额', 'ROI', '付费率', 'ARPPU', 'LTV'])
            matchls = [value[0:-1] for value in list(show_dict.values())]
            # 渠道，日期， 国家
            start_time_stamp = time.mktime(time.strptime(end_time, "%Y-%m-%d"))
            appsflyer_end = datetime.date.fromtimestamp(start_time_stamp) + datetime.timedelta(days=+ 1)
            sql_channel = 'SELECT device_id ,%s  FROM opsmanage_push_appsflyer  WHERE install_time_selected_timezone >= "%s" and install_time_selected_timezone < "%s"' % (",".join(showappsflyerls), start_time, appsflyer_end)
            if appsflyer_sql_tmp:
                sql_channel = sql_channel + appsflyer_sql_tmp
            df_channel = pd.read_sql(sql_channel, connection)
            if "date" in matchls:
                df_channel['date'] = df_channel['date'].apply(lambda x: x[:10])

            # db_time_end = datetime.date(*map(int, end_time[:10].split('-'))) + datetime.timedelta(days=1)
            # 是否有自然用户
            if bHaveOrganic:
                sql_cb = 'SELECT %s FROM opsmanage_push_client_activate WHERE db_time >= "%s" and db_time < "%s" ' % (",".join(showactive),
                start_time, appsflyer_end)
                if active_sql_tmp:
                    sql_cb +=  active_sql_tmp
                df_cb = pd.read_sql(sql_cb, connection)
                if "日期" in list(show_dict.keys()):
                    df_cb['date'] = df_cb['date'].map(
                        lambda x: pytz.timezone("Asia/Shanghai").localize(
                            datetime.datetime.fromtimestamp(int(x))).astimezone(
                            pytz.timezone("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")[0:10])
                df_cb["channel"] = "Organic"
                df_channel = pd.concat([df_channel, df_cb], ignore_index=True)
            df_channel = df_channel.drop_duplicates('device_id')


            # sql_cr_ios = 'SELECT device_id,recharge as aggregate_recharge FROM opsmanage_push_client_recharge WHERE time >= %d and time <= %d' % (
            #     f_start_timestamp, f_end_timestamp)
            sql_cr_ios = 'SELECT device_id,SUM(recharge) as aggregate_recharge FROM opsmanage_push_client_recharge WHERE db_time >= "%s" and db_time < "%s"  GROUP BY (device_id)' % (
                f_start_time, f_end_time)
            df_cb_ios = pd.read_sql(sql_cr_ios, connection)

            df_channel = pd.merge(df_cb_ios, df_channel, how="left", on=["device_id"])
            df_channel["aggregate_payid"] = 1
            if df_channel.empty:
                df_channel["aggregate_recharge"] = 0

            df_channel["aggregate_payid"] = 1
            df_channel.fillna(0)
            df_cb_group = df_channel.groupby(matchls,as_index=False).sum()

            paydata = pd.merge(df, df_cb_group, how='left', on=matchls)

            paydata.loc[paydata.shape[0] - 1, 'aggregate_payid'] = paydata['aggregate_payid'].sum()
            paydata.loc[paydata.shape[0] - 1, 'aggregate_recharge'] = paydata['aggregate_recharge'].sum()

            # 累计付费设备数
            df['aggregate_payid'] = paydata["aggregate_payid"]
            # 累计付费金额
            df['aggregate_recharge'] = paydata["aggregate_recharge"]
            # ROI
            df['roi'] = paydata["aggregate_recharge"] / df.ad_cost["sum"].tolist()
            # # 付费率
            df['pay_rate'] = paydata["aggregate_payid"] / df.new_activate["sum"].tolist()
            # # ARPPU
            df['arppu'] = paydata["aggregate_recharge"] / df['aggregate_payid']
            # # LTV
            df['ltv'] = paydata["aggregate_recharge"] / df.new_activate["sum"].tolist()
            df['roi'] = df['roi'].map(lambda x: round(x, 2))
            # df['pay_rate'] = df['pay_rate'].map(lambda x: round(x, 2))
            # df['arppu'] = df['arppu'].map(lambda x: round(x, 2))
            # df['ltv'] = df['ltv'].map(lambda x: round(x, 2))
            df = df.fillna(0)
            df['aggregate_payid'] = df['aggregate_payid'].apply(lambda x: int(x) if x else 0)
            df['aggregate_recharge'] = df['aggregate_recharge'].apply(lambda x: '$' + str(round(x, 2)))
            df['arppu'] = df['arppu'].apply(lambda x: '$' + str(round(x, 2)))
            df['pay_rate'] = df['pay_rate'].apply(lambda x: str(round(x * 100, 2)) + '%')
            df['ltv'] = df['ltv'].apply(lambda x: '$' + str(round(x, 2)))

        df = df.fillna(0)
        df['pernew_cost'] = df['pernew_cost'].apply(lambda x: '$' + str(round(x, 2)))
        # df['perreg_cost'] = df['perreg_cost'].apply(lambda x: '$' + str(round(x, 2)))
        # df['peronl_cost'] = df['peronl_cost'].apply(lambda x: '$' + str(round(x, 2)))
        df[('ad_cost', 'sum')] = df[('ad_cost', 'sum')].map(lambda x: '$' + str(round(x, 2)))
        channel_day_list = np.array(df).tolist()
        return render(request, 'report/subchannel_analysis_detail.html', locals())

@login_required()
@permission_required('OpsManage.can_read_roi_day_record', login_url='/noperm/')
def roi_day_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        channels = request.POST.getlist('channels[]')
        # 子渠道
        subchannels = request.POST.get('subchannels')
        sub1ch_perms = request.POST.get('sub1ch_perms')
        sub2ch_perms = request.POST.get('sub2ch_perms')
        # 是否显示子渠道
        # checkbox = request.POST.getlist('checkbox')[0]
        countrys = request.POST.getlist('countrys[]')
        # country_show = request.POST.get('country_show')
        # channel_show = request.POST.get('channel_show')
        # time_show = request.POST.get('time_show')
        start_time = request.POST.get('start_time')
        duration = request.POST.get('duration')
        game_perms = request.POST.get('game_perms')
        end_time = request.POST.get('end_time')
        languages = request.POST.getlist('gameverisions[]')
        platforms = request.POST.getlist('platforms[]')
        conditionList = []
        conditionList.append(request.POST.get('condition_1'))
        conditionList.append(request.POST.get('condition_2'))
        conditionList.append(request.POST.get('condition_3'))
        conditionList.append(request.POST.get('condition_4'))
        conditionList.append(request.POST.get('condition_5'))
        conditionList.append(request.POST.get('condition_6'))
        # end_time = now_time
        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '0'

        gameinfo = Game_Version_Config.objects.filter(name=game_perms)
        if len(gameinfo) <= 0:
            return render(request, 'report/roi_day_detail.html', locals())
        appid_sql = "AND (app_id ='" + gameinfo[0].android_appid + "'or app_id='" + gameinfo[0].ios_appid + "')"

        sql_temp = ''
        if languages:
            sql_language = ''
            for lan in languages:
                sql_language += ' language = "{}" or '.format(lan)

            sql_temp += ' and {} '.format('(' + sql_language[:-3] + ')')
        if platforms:
            sql_platform = ""
            sql_active = ""
            for pl in platforms:
                sql_platform += ' platform = "{}" or '.format(pl)

            sql_temp += ' and {} '.format('(' + sql_platform[:-3] + ')')

        if countrys:
            sql_country = ''
            for ch in countrys:
                sql_country += ' country = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_country[:-3] + ')')

        if subchannels:
            # sql_subchannel = ''
            # for ch in subchannels:
            sql_subchannel = ' sub1_channel like "%{}%" or '.format(subchannels)
            sql_temp += ' and {} '.format('(' + sql_subchannel[:-3] + ')')

        if sub1ch_perms:
            # sql_sub1channel = ''
            # for ch in sub1ch_perms:
            sql_sub1channel = ' sub2_channel = "{}" or '.format(sub1ch_perms)
            sql_temp += ' and {} '.format('(' + sql_sub1channel[:-3] + ')')

        if sub2ch_perms:
            # sql_sub2channel = ''
            # for ch in sub2ch_perms:
            sql_sub2channel = ' sub3_channel = "{}" or '.format(sub2ch_perms)
            sql_temp += ' and {} '.format('(' + sql_sub2channel[:-3] + ')')

        # 是否需要子渠道的数据
        newChannel = {}
        if channels:
            sql_channel = ''
            for ch in channels:
                # 判断是否有渠道提取
                Isolate_Channel_Data = Isolate_Channel.objects.filter(newname=ch)
                if Isolate_Channel_Data.count() == 1:
                    prename = Isolate_Channel_Data[0].prename
                    rule = Isolate_Channel_Data[0].rule
                    newname = Isolate_Channel_Data[0].newname
                    newChannel[newname] = [prename, rule]
                    if prename not in channels:
                        ch = prename
                    else:
                        continue
                sql_channel += ' channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')
        sql_temp += appid_sql
        # if checkbox == "true" and subchannels:
        #     sql_subchannel = ''
        #     for ch in subchannels:
        #         sql_subchannel += ' sub_channel = "{}" or '.format(ch)
        #     sql_temp += ' and {} '.format('(' + sql_subchannel[:-3] + ')')



        dime_list = []
        show_dict = {}
        for condition in conditionList:
            if condition == 'country':
                if '国家' in dime_list:
                    continue
                dime_list.append('国家')
                show_dict['国家'] = 'country,'

            elif condition == 'time':
                if '时间' in dime_list:
                    continue
                dime_list.append('时间')
                show_dict['时间'] = 'date,'

            elif condition == 'channel':
                if '渠道' in dime_list:
                    continue
                dime_list.append('渠道')
                show_dict['渠道'] = 'channel,'
            elif condition == 'language':
                if '版本' in dime_list:
                    continue
                dime_list.append('版本')
                show_dict['版本'] = 'language,'
            elif condition == 'platform':
                if '平台' in dime_list:
                    continue
                dime_list.append('平台')
                show_dict['平台'] = 'platform,'
            elif condition == 'subchannel':
                if '子渠道' in dime_list:
                    continue
                dime_list.append('子渠道')
                show_dict['子渠道'] = 'sub1_channel,'

        # if checkbox == "true":
        #     dime_list.append('子渠道')
        #     show_dict['子渠道'] = 'sub_channel,'
        roi_head = []
        week_roi = ['广告花费']
        week_roi_list = ['第1天ROI', '第2天ROI', '第3天ROI', '第4天ROI', '第5天ROI', '第6天ROI', '第7天ROI'
            ,'第8天ROI', '第9天ROI', '第10天ROI', '第11天ROI', '第12天ROI', '第13天ROI', '第14天ROI'
            , '第15天ROI', '第16天ROI', '第17天ROI', '第18天ROI', '第19天ROI', '第20天ROI', '第21天ROI'
            , '第22天ROI', '第23天ROI', '第24天ROI', '第25天ROI', '第26天ROI', '第27天ROI','第28天ROI','第29天ROI','第30天ROI'
                    ]
        if duration == "0":
            week_roi.extend(week_roi_list)
        elif duration == "1":
            week_roi.extend(week_roi_list[0:8])
        elif duration == "2":
            week_roi.extend(week_roi_list[8:16])
        elif duration == "3":
            week_roi.extend(week_roi_list[16:24])
        elif duration == "4":
            week_roi.extend(week_roi_list[24:30])

        roi_head.extend(dime_list)
        roi_head.extend(week_roi)

        pay_head = []
        week_pay = ['广告花费']
        week_pay_list = ['第1天付费', '第2天付费', '第3天付费', '第4天付费', '第5天付费', '第6天付费', '第7天付费'
            , '第8天付费', '第9天付费', '第10天付费', '第11天付费', '第12天付费', '第13天付费', '第14天付费'
            , '第15天付费', '第16天付费', '第17天付费', '第18天付费', '第19天付费', '第20天付费', '第21天付费'
            , '第22天付费', '第23天付费', '第24天付费', '第25天付费', '第26天付费', '第27天付费', '第28天付费', '第29天付费', '第30天付费']

        if duration == "0":
            week_pay.extend(week_pay_list)
        elif duration == "1":
            week_pay.extend(week_pay_list[0:8])
        elif duration == "2":
            week_pay.extend(week_pay_list[8:16])
        elif duration == "3":
            week_pay.extend(week_pay_list[16:24])
        elif duration == "4":
            week_pay.extend(week_pay_list[24:30])
        pay_head.extend(dime_list)
        pay_head.extend(week_pay)

        arppu_head = []
        week_arppu_list = ['第1天', '第2天', '第3天', '第4天', '第5天', '第6天', '第7天'
            ,'第8天', '第9天', '第10天', '第11天', '第12天', '第13天', '第14天'
            , '第15天', '第16天', '第17天', '第18天', '第19天', '第20天', '第21天'
            , '第22天', '第23天', '第24天', '第25天', '第26天',  '第27天',  '第28天',  '第29天',  '第30天']

        if duration == "0":
            week_arppu = week_arppu_list
        elif duration == "1":
            week_arppu = week_arppu_list[0:8]
        elif duration == "2":
            week_arppu = week_arppu_list[8:16]
        elif duration == "3":
            week_arppu = week_arppu_list[16:24]
        elif duration == "4":
            week_arppu = week_arppu_list[24:30]

        arppu_head.extend(dime_list)
        arppu_head.extend(week_arppu)

        ltv_head = []
        week_ltv = ['新增激活数']
        week_ltv_list = ['第1天', '第2天', '第3天', '第4天', '第5天', '第6天', '第7天'
            ,'第8天', '第9天', '第10天', '第11天', '第12天', '第13天', '第14天'
            , '第15天', '第16天', '第17天', '第18天', '第19天', '第20天', '第21天'
            , '第22天', '第23天', '第24天', '第25天', '第26天',  '第27天',  '第28天',  '第29天',  '第30天']

        if duration == "0":
            week_ltv.extend(week_ltv_list)
        elif duration == "1":
            week_ltv.extend(week_ltv_list[0:8])
        elif duration == "2":
            week_ltv.extend(week_ltv_list[8:16])
        elif duration == "3":
            week_ltv.extend(week_ltv_list[16:24])
        elif duration == "4":
            week_ltv.extend(week_ltv_list[24:30])
        ltv_head.extend(dime_list)
        ltv_head.extend(week_ltv)

        dime_str = ''
        dime_group = []

        for di in dime_list:
            dime_str += show_dict.get(di, '')
            dime_group.append(show_dict.get(di, ' ')[:-1])
        # if len(newChannel) > 0 and checkbox != "true":
        #     dime_str += "sub_channel,"
        # , num_8, num_9, num_10, num_11, num_12, num_13, num_14
        # , week_8, week_9, week_10, week_11, week_12, week_13, week_14

        sql_week = ['day_1', 'day_2', 'day_3', 'day_4', 'day_5', 'day_6', 'day_7', 'day_8', 'day_9', 'day_10', 'day_11', 'day_12', 'day_13', 'day_14' , 'day_15', 'day_16', 'day_17', 'day_18', 'day_19', 'day_20', 'day_21', 'day_22', 'day_23', 'day_24', 'day_25', 'day_26', 'day_27', 'day_28', 'day_29', 'day_30']
        sql_num = ['num_1','num_2','num_3','num_4','num_5','num_6','num_7', 'num_8', 'num_9', 'num_10', 'num_11', 'num_12', 'num_13', 'num_14','num_15','num_16','num_17','num_18','num_19','num_20','num_21', 'num_22', 'num_23', 'num_24', 'num_25', 'num_26', 'num_27', 'num_28', 'num_29', 'num_30']
        sliceval = -61
        if duration == "0":
            week_list = sql_week
            sql_week_str = ','.join(week_list)
            sql_num_str = ','.join(sql_num)
        elif duration == "1":
            week_list = sql_week[0:8]
            sql_week_str = ','.join(week_list)
            sql_num_str = ','.join(sql_num[0:8])
            sliceval = -17
        elif duration == "2":
            week_list = sql_week[8:16]
            sql_week_str = ','.join(week_list)
            sql_num_str = ','.join(sql_num[8:16])
            sliceval = -17
        elif duration == "3":
            week_list = sql_week[14:21]
            sql_week_str = ','.join(week_list)
            sql_num_str = ','.join(sql_num[16:24])
            sliceval = -17
        elif duration == "4":
            week_list = sql_week[21:26]
            sql_week_str = ','.join(week_list)
            sql_num_str = ','.join(sql_num[24:30])
            sliceval = -13

        sql = 'SELECT %s ad_cost,new_activate ,%s, %s FROM opsmanage_roi_day WHERE date >= "%s" and date <= "%s"' % (
            dime_str, sql_week_str,sql_num_str, start_time, end_time)
        if sql_temp:
            sql = sql + sql_temp

        df = pd.read_sql(sql, connection)
        if not df.index.tolist():
            return render(request, 'report/roi_day_detail.html', locals())
        if '子渠道' in show_dict:
            df["sub1_channel"] = df["sub1_channel"].map(lambda  x: x if x else "")

        if len(newChannel) > 0:
            for newchannel, subChannel in list(newChannel.items()):
                df.loc[(df["channel"] == subChannel[0]) & (
                df["sub1_channel"].str.startswith(subChannel[1])), "channel"] = newchannel
                try:
                    if subChannel[0] not in channels:
                        df = df[~df['channel'].isin([subChannel[0]])]
                except Exception as e:
                    print(e)
            # dime_group.remove("sub_channel")
            if '子渠道' not in show_dict:
                df = df.drop('sub1_channel', axis=1)
            # if channel_show == 'channel':
            #     df = df.drop('channel', axis=1)
            # df = df.groupby(dime_group).agg(['sum'])

        df = df.groupby(dime_group).agg(['sum']).reset_index()

        pay_colume = df.columns.tolist()[:sliceval]
        df_pay = df[pay_colume]
        # , 'week_8', 'week_9', 'week_10', 'week_11', 'week_12', 'week_13', 'week_14'
        for item in week_list:
            df_pay.loc[:, (item, 'sum')] = df[(item, 'sum')].apply(lambda x: '$'+ str(round(x, 2))).astype('str').str.cat(
                df.loc[:, (item.replace('day', 'num'), 'sum')].apply(lambda x: int(x)).astype('str'), sep='<br>')
        df_pay[('ad_cost', 'sum')] = df_pay[('ad_cost', 'sum')].apply(lambda x: '$' + str(round(x, 2)))
            # df['aggregate_payid'] = df['aggregate_payid'].apply(lambda x: int(x) if x != None else 0)
        df_pay = df_pay.fillna(0)
        pay_week_list = []
        pay_week_tmp = np.array(df_pay).tolist()
        roi_colume = df.columns.tolist()[:sliceval]

        df_roi = df[roi_colume]
        df_roi_tran = df[week_list]
        df_roi_tran = df_roi_tran.T.cumsum().T
        for item in week_list:
            df_roi_rate = (df_roi_tran[item] / df.ad_cost)['sum'].apply(lambda x: str(round(x * 100, 2)) + '%')
            df_roi.loc[:, (item, 'sum')] = df_roi_rate.astype('str').str.cat(
                df_roi_tran[(item, 'sum')].apply(lambda x: '$'+ str(round(x, 2))).astype('str'), sep='<br>')
        df_roi[('ad_cost', 'sum')] = df_roi[('ad_cost', 'sum')].apply(lambda x: '$' + str(round(x, 2)))

        cur_time = time.time()

        df_roi = df_roi.fillna(0)
        roi_week_list = []
        roi_week_tmp = np.array(df_roi).tolist()


        sliceval += - 1
        arppu_colume = df.columns.tolist()[:sliceval]
        df_arppu = df[arppu_colume]
        for item in week_list:
            df_arppu.loc[:, (item, 'sum')] = (df[item] / df[item.replace('day', 'num')])['sum'].apply( lambda x: '$'+str(round(x, 2)))
        df_arppu = df_arppu.fillna(0)
        arppu_week_list = []
        arppu_week_tmp = np.array(df_arppu).tolist()

        ltv_colume = df.columns.tolist()[:sliceval]
        df_ltv = df[ltv_colume]
        df_ltv.loc[:, ('new_activate', 'sum')] = df.loc[:, ('new_activate', 'sum')]
        for item in week_list:
            df_ltv.loc[:, (item, 'sum')] = (df_roi_tran[item] / df.new_activate)['sum'].apply(lambda x: '$'+ str(round(x, 2)))
        df_ltv = df_ltv.fillna(0)
        df_ltv[('new_activate', 'sum')] = df_ltv[('new_activate', 'sum')].astype(int)
        ltv_week_list = []
        ltv_week_tmp = np.array(df_ltv).tolist()

        index = 0
        for roi_week in roi_week_tmp:
            data_time = datetime.datetime.strptime(roi_week[0], "%Y-%m-%d")
            strdataDate = str(data_time + datetime.timedelta(days= 1))[0:10]
            dataDate = time.mktime(time.strptime(strdataDate, '%Y-%m-%d'))
            weekCnt = int((cur_time - dataDate) / (3600 * 24))
            limitval = weekCnt + len(dime_list) + 2 - (int(duration if duration != "0" else "1") - 1)
            if limitval > 0:
                # tem_roi = []
                # for index, value in enumerate(roi_week):
                #     if index >= limitval:
                #         tem_roi.append("")
                #     else:
                #         tem_roi.append(value)
                # roi_week_list.append(tem_roi)
                roi_week_list.append(roi_week[0:limitval])
                ltv_week_list.append(ltv_week_tmp[index][0:limitval])
                pay_week_list.append(pay_week_tmp[index][0:limitval])
                arppu_week_list.append(arppu_week_tmp[index][0:limitval -1])
            else:
                roi_week_list.append([])
                ltv_week_list.append([])
                pay_week_list.append([])
                arppu_week_list.append([])
            index += 1


        return render(request, 'report/roi_day_detail.html', locals())

@login_required()
@permission_required('OpsManage.can_read_week_charge_analysis', login_url='/noperm/')
def week_charge_analysis_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        channels = request.POST.getlist('channels[]')
        # 子渠道
        subchannels = request.POST.getlist('subchannels[]')
        sub1ch_perms = request.POST.getlist('sub1ch_perms[]')
        sub2ch_perms = request.POST.getlist('sub2ch_perms[]')
        # 是否显示子渠道
        # checkbox = request.POST.getlist('checkbox')[0]
        countrys = request.POST.getlist('countrys[]')
        # country_show = request.POST.get('country_show')
        # channel_show = request.POST.get('channel_show')
        # time_show = request.POST.get('time_show')
        start_time = request.POST.get('start_time')
        game_perms = request.POST.get('game_perms')
        end_time = request.POST.get('end_time')
        languages = request.POST.getlist('gameverisions[]')
        platforms = request.POST.getlist('platforms[]')

        # end_time = now_time
        if not end_time:
            end_time = now_time
        else:
            end_time = str(datetime.date(*list(map(int, end_time.split('-')))) + datetime.timedelta(days=-6)) + '~' + end_time
        if not start_time:
            start_time = '0'

        gameinfo = Game_Version_Config.objects.filter(name=game_perms)
        if len(gameinfo) <= 0:
            return render(request, 'report/week_charge_analysis_detail.html', locals())
        appid_sql = "AND (app_id ='" + gameinfo[0].android_appid + "'or app_id='" + gameinfo[0].ios_appid + "')"

        sql_temp = ''
        if languages:
            sql_language = ''
            for lan in languages:
                sql_language += ' language = "{}" or '.format(lan)

            sql_temp += ' and {} '.format('(' + sql_language[:-3] + ')')
        if platforms:
            sql_platform = ""
            sql_active = ""
            for pl in platforms:
                sql_platform += ' platform = "{}" or '.format(pl)

            sql_temp += ' and {} '.format('(' + sql_platform[:-3] + ')')

        if countrys:
            sql_country = ''
            for ch in countrys:
                sql_country += ' country = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_country[:-3] + ')')

        if subchannels:
            sql_subchannel = ''
            for ch in subchannels:
                sql_subchannel += ' sub1_channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_subchannel[:-3] + ')')

        if sub1ch_perms:
            sql_sub1channel = ''
            for ch in sub1ch_perms:
                sql_sub1channel += ' sub2_channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_sub1channel[:-3] + ')')

        if sub2ch_perms:
            sql_sub2channel = ''
            for ch in sub2ch_perms:
                sql_sub2channel += ' sub3_channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_sub2channel[:-3] + ')')

        # 是否需要子渠道的数据
        newChannel = {}
        if channels:
            sql_channel = ''
            for ch in channels:
                # 判断是否有渠道提取
                Isolate_Channel_Data = Isolate_Channel.objects.filter(newname=ch)
                if Isolate_Channel_Data.count() == 1:
                    prename = Isolate_Channel_Data[0].prename
                    rule = Isolate_Channel_Data[0].rule
                    newname = Isolate_Channel_Data[0].newname
                    newChannel[newname] = [prename, rule]
                    if prename not in channels:
                        ch = prename
                    else:
                        continue
                sql_channel += ' channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')
        sql_temp += appid_sql


        roi_head = ['日期','第1周', '第2周', '第3周', '第4周', '第5周', '第6周', '第7周'
            , '第8周', '第9周', '第10周', '第11周', '第12周', '第13周', '第14周'
            , '第15周', '第16周', '第17周', '第18周', '第19周', '第20周', '第21周'
            , '第22周', '第23周', '第24周', '第25周', '第26周']


        sql = 'SELECT DISTINCT(date), max(week_index) FROM opsmanage_week_charge_analysis WHERE date >= "%s" and date <= "%s" ' % (start_time, end_time)
        if sql_temp:
            sql = sql + sql_temp
        sql += ' GROUP BY date'

        df = pd.read_sql(sql, connection)
        if not df.index.tolist():
            return render(request, 'report/week_charge_analysis_detail.html', locals())

        # 充值流失率
        charge_week_list = []
        cmp_week_list = []
        cmp_num_week_list = []
        # 充值玩家活跃
        activity_week_list = []
        activity_cmp_list = []

        cur_time = int(time.time())
        for index, row in df.iterrows():
            list_info = []
            cmp_info = []
            cmp_num_info = []
            activity_info = []
            activity_cmp_info = []
            sql_first = 'SELECT device_id FROM opsmanage_week_charge_analysis WHERE date = "%s" and week_index =  1 ' % (str(row["date"]))
            if sql_temp:
                sql_first += sql_temp
            str_date = str(row["date"].split("~")[0])
            datastamp = time.mktime(time.strptime(str_date, "%Y-%m-%d"))
            data_str = datetime.datetime.strptime(str_date, "%Y-%m-%d")

            df_first = pd.read_sql(sql_first, connection)
            df_last = df_first

            list_info.append(row["date"])
            list_info.append(len(df_first))
            cmp_info.append(row["date"])
            cmp_info.append(len(df_first))
            cmp_num_info.append(row["date"])
            cmp_num_info.append(len(df_first))
            activity_info.append(row["date"])
            activity_info.append(len(df_first))
            activity_cmp_info.append(row["date"])
            activity_cmp_info.append(len(df_first))
            for i in range(2,27):
                # 判断时间
                if datastamp + i * 3600*24*7 < cur_time:
                    sql_next = 'SELECT device_id FROM opsmanage_week_charge_analysis WHERE date = "%s" and week_index =  "%s" ' % (str(row["date"]), str(i))
                    if sql_temp:
                        sql_next += sql_temp
                    df_next = pd.read_sql(sql_next, connection)
                    df_merge = pd.merge(df_next, df_first,how='inner', on=['device_id'])

                    if len(df_first) > 0:
                        len_num = len(df_merge)
                        list_info.append(str(len_num) + '<br>' + str(round((len(df_first) - len_num) * 100.0/ len(df_first), 2)) + '%')
                    else:
                        list_info.append(0)

                    df_cmp_merge = pd.merge(df_next, df_last, how='inner', on=['device_id'])
                    if len(df_last) > 0:
                        len_df_cmp_merge = len(df_cmp_merge)
                        cmp_info.append(str(len_df_cmp_merge) + '<br>' + str(round((len(df_last) - len_df_cmp_merge) * 100.0/ len(df_last), 2)) + '%')
                    else:
                        cmp_info.append(0)

                    if len(df_last) > 0:
                        len_df_next = len(df_next)
                        cmp_num_info.append(str(len_df_next) + '<br>' + str(round((len(df_last) - len_df_next) * 100.0/ len(df_last), 2)) + '%')
                    else:
                        cmp_num_info.append(0)

                    #首周付费留存
                    today = datetime.date.today()
                    start_data = data_str + datetime.timedelta(days=(i - 1)*7)
                    end_data = data_str + datetime.timedelta(days= i * 7)
                    sql_activity = "SELECT Distinct(device_id) as device_id from opsmanage_push_client_activate_all " \
                                    "WHERE db_time >= '%s' and db_time <= '%s'" %(start_data, end_data)

                    df_activity = pd.read_sql(sql_activity, connection)
                    df_activity_merge = pd.merge(df_activity, df_first, how='inner', on=['device_id'])
                    if len(df_first) > 0:
                        len_df_activity_merge = len(df_activity_merge)
                        activity_info.append(str(len_df_activity_merge) + '<br>' + str(round(len_df_activity_merge * 100.0/ len(df_first), 2)) + '%')
                    else:
                        activity_info.append(0)
                    # 环比付费留存
                    df_activity_cmp_merge = pd.merge(df_activity, df_last, how='inner', on=['device_id'])
                    if len(df_last) > 0:
                        len_df_activity_cmp_merge = len(df_activity_cmp_merge)
                        activity_cmp_info.append(str(len_df_activity_cmp_merge) + '<br>' + str(round(len_df_activity_cmp_merge * 100.0 / len(df_last), 2)) + '%')
                    else:
                        activity_cmp_info.append(0)

                    df_last = df_next

            charge_week_list.append(list_info)
            cmp_week_list.append(cmp_info)
            cmp_num_week_list.append(cmp_num_info)
            activity_week_list.append(activity_info)
            activity_cmp_list.append(activity_cmp_info)

        return render(request, 'report/week_charge_analysis_detail.html', locals())

@login_required()
@permission_required('OpsManage.can_read_month_charge_analysis', login_url='/noperm/')
def month_charge_analysis_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        channels = request.POST.getlist('channels[]')
        # 子渠道
        subchannels = request.POST.getlist('subchannels[]')
        sub1ch_perms = request.POST.getlist('sub1ch_perms[]')
        sub2ch_perms = request.POST.getlist('sub2ch_perms[]')
        # 是否显示子渠道
        # checkbox = request.POST.getlist('checkbox')[0]
        countrys = request.POST.getlist('countrys[]')
        # country_show = request.POST.get('country_show')
        # channel_show = request.POST.get('channel_show')
        # time_show = request.POST.get('time_show')
        start_time = request.POST.get('start_time')
        game_perms = request.POST.get('game_perms')
        end_time = request.POST.get('end_time')
        languages = request.POST.getlist('gameverisions[]')
        platforms = request.POST.getlist('platforms[]')

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '0'

        gameinfo = Game_Version_Config.objects.filter(name=game_perms)
        if len(gameinfo) <= 0:
            return render(request, 'report/month_charge_analysis_detail.html', locals())
        appid_sql = "AND (app_id ='" + gameinfo[0].android_appid + "'or app_id='" + gameinfo[0].ios_appid + "')"

        sql_temp = ''
        if languages:
            sql_language = ''
            for lan in languages:
                sql_language += ' language = "{}" or '.format(lan)

            sql_temp += ' and {} '.format('(' + sql_language[:-3] + ')')
        if platforms:
            sql_platform = ""
            sql_active = ""
            for pl in platforms:
                sql_platform += ' platform = "{}" or '.format(pl)

            sql_temp += ' and {} '.format('(' + sql_platform[:-3] + ')')

        if countrys:
            sql_country = ''
            for ch in countrys:
                sql_country += ' country = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_country[:-3] + ')')

        if subchannels:
            sql_subchannel = ''
            for ch in subchannels:
                sql_subchannel += ' sub1_channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_subchannel[:-3] + ')')

        if sub1ch_perms:
            sql_sub1channel = ''
            for ch in sub1ch_perms:
                sql_sub1channel += ' sub2_channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_sub1channel[:-3] + ')')

        if sub2ch_perms:
            sql_sub2channel = ''
            for ch in sub2ch_perms:
                sql_sub2channel += ' sub3_channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_sub2channel[:-3] + ')')

        # 是否需要子渠道的数据
        newChannel = {}
        if channels:
            sql_channel = ''
            for ch in channels:
                # 判断是否有渠道提取
                Isolate_Channel_Data = Isolate_Channel.objects.filter(newname=ch)
                if Isolate_Channel_Data.count() == 1:
                    prename = Isolate_Channel_Data[0].prename
                    rule = Isolate_Channel_Data[0].rule
                    newname = Isolate_Channel_Data[0].newname
                    newChannel[newname] = [prename, rule]
                    if prename not in channels:
                        ch = prename
                    else:
                        continue
                sql_channel += ' channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')
        sql_temp += appid_sql


        roi_head = ['日期','第1月', '第2月', '第3月', '第4月', '第5月', '第6月', '第7月'
            , '第8月', '第9月', '第10月', '第11月', '第12月', '第13月', '第14月'
            , '第15月', '第16月', '第17月', '第18月', '第19月', '第20月', '第21月'
            , '第22月', '第23月', '第24月']


        sql = 'SELECT DISTINCT(date), max(month_index) FROM opsmanage_month_charge_analysis WHERE date >= "%s" and date <= "%s" ' % (start_time, end_time)
        if sql_temp:
            sql = sql + sql_temp
        sql += ' GROUP BY date'

        df = pd.read_sql(sql, connection)
        if not df.index.tolist():
            return render(request, 'report/month_charge_analysis_detail.html', locals())

        # 充值流失率
        charge_week_list = []
        cmp_week_list = []
        cmp_num_week_list = []
        # 充值玩家活跃
        activity_week_list = []
        activity_cmp_list = []

        cur_time = int(time.time())
        for index, row in df.iterrows():
            list_info = []
            cmp_info = []
            cmp_num_info = []
            activity_info = []
            activity_cmp_info = []
            sql_first = 'SELECT device_id FROM opsmanage_month_charge_analysis WHERE date = "%s" and month_index =  1 ' % (str(row["date"]))
            if sql_temp:
                sql_first += sql_temp
            str_date = str(row["date"].split("~")[0])
            datastamp = time.mktime(time.strptime(str_date + "-01", "%Y-%m-%d"))
            data_str = datetime.datetime.strptime(str_date+ "-01", "%Y-%m-%d")

            df_first = pd.read_sql(sql_first, connection)
            df_last = df_first

            list_info.append(row["date"])
            list_info.append(len(df_first))
            cmp_info.append(row["date"])
            cmp_info.append(len(df_first))
            cmp_num_info.append(row["date"])
            cmp_num_info.append(len(df_first))
            activity_info.append(row["date"])
            activity_info.append(len(df_first))
            activity_cmp_info.append(row["date"])
            activity_cmp_info.append(len(df_first))
            for i in range(2,27):
                # 判断时间
                if datastamp + i * 3600*24*7 < cur_time:
                    sql_next = 'SELECT device_id FROM opsmanage_month_charge_analysis WHERE date = "%s" and month_index =  "%s" ' % (str(row["date"]), str(i))
                    if sql_temp:
                        sql_next += sql_temp
                    df_next = pd.read_sql(sql_next, connection)
                    df_merge = pd.merge(df_next, df_first,how='inner', on=['device_id'])

                    if len(df_first) > 0:
                        len_num = len(df_merge)
                        list_info.append(str(len_num) + '<br>' + str(round((len(df_first) - len_num) * 100.0/ len(df_first), 2)) + '%')
                    else:
                        list_info.append(0)

                    df_cmp_merge = pd.merge(df_next, df_last, how='inner', on=['device_id'])
                    if len(df_last) > 0:
                        len_df_cmp_merge = len(df_cmp_merge)
                        cmp_info.append(str(len_df_cmp_merge) + '<br>' + str(round((len(df_last) - len_df_cmp_merge) * 100.0/ len(df_last), 2)) + '%')
                    else:
                        cmp_info.append(0)

                    if len(df_last) > 0:
                        len_df_next = len(df_next)
                        cmp_num_info.append(str(len_df_next) + '<br>' + str(round((len(df_last) - len_df_next) * 100.0/ len(df_last), 2)) + '%')
                    else:
                        cmp_num_info.append(0)

                    #首周付费留存
                    today = datetime.date.today()
                    start_data = data_str + datetime.timedelta(days=(i - 1)*7)
                    end_data = data_str + datetime.timedelta(days= i * 7)
                    sql_activity = "SELECT Distinct(device_id) as device_id from opsmanage_push_client_activate_all " \
                                    "WHERE db_time >= '%s' and db_time <= '%s'" %(start_data, end_data)

                    df_activity = pd.read_sql(sql_activity, connection)
                    df_activity_merge = pd.merge(df_activity, df_first, how='inner', on=['device_id'])
                    if len(df_first) > 0:
                        len_df_activity_merge = len(df_activity_merge)
                        activity_info.append(str(len_df_activity_merge) + '<br>' + str(round(len_df_activity_merge * 100.0/ len(df_first), 2)) + '%')
                    else:
                        activity_info.append(0)
                    # 环比付费留存
                    df_activity_cmp_merge = pd.merge(df_activity, df_last, how='inner', on=['device_id'])
                    if len(df_last) > 0:
                        len_df_activity_cmp_merge = len(df_activity_cmp_merge)
                        activity_cmp_info.append(str(len_df_activity_cmp_merge) + '<br>' + str(round(len_df_activity_cmp_merge * 100.0 / len(df_last), 2)) + '%')
                    else:
                        activity_cmp_info.append(0)

                    df_last = df_next

            charge_week_list.append(list_info)
            cmp_week_list.append(cmp_info)
            cmp_num_week_list.append(cmp_num_info)
            activity_week_list.append(activity_info)
            activity_cmp_list.append(activity_cmp_info)

        return render(request, 'report/month_charge_analysis_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_roi_week_record', login_url='/noperm/')
def roi_week_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        channels = request.POST.getlist('channels[]')
        # 子渠道
        subchannels = request.POST.get('subchannels')
        sub1ch_perms = request.POST.get('sub1ch_perms')
        sub2ch_perms = request.POST.get('sub2ch_perms')
        # 是否显示子渠道
        # checkbox = request.POST.getlist('checkbox')[0]
        countrys = request.POST.getlist('countrys[]')
        # country_show = request.POST.get('country_show')
        # channel_show = request.POST.get('channel_show')
        # time_show = request.POST.get('time_show')
        start_time = request.POST.get('start_time')
        duration = request.POST.get('duration')
        game_perms = request.POST.get('game_perms')
        end_time = request.POST.get('end_time')
        languages = request.POST.getlist('gameverisions[]')
        platforms = request.POST.getlist('platforms[]')
        conditionList = []
        conditionList.append(request.POST.get('condition_1'))
        conditionList.append(request.POST.get('condition_2'))
        conditionList.append(request.POST.get('condition_3'))
        conditionList.append(request.POST.get('condition_4'))
        conditionList.append(request.POST.get('condition_5'))
        conditionList.append(request.POST.get('condition_6'))
        # end_time = now_time
        if not end_time:
            end_time = now_time
        else:
            end_time = str(datetime.date(*list(map(int, end_time.split('-'))))+ datetime.timedelta(days=-6)) + '~' + end_time
        if not start_time:
            start_time = '0'

        gameinfo = Game_Version_Config.objects.filter(name=game_perms)
        if len(gameinfo) <= 0:
            return render(request, 'report/roi_week_detail.html', locals())
        appid_sql = "AND (app_id ='" + gameinfo[0].android_appid + "'or app_id='" + gameinfo[0].ios_appid + "')"

        sql_temp = ''
        if languages:
            sql_language = ''
            for lan in languages:
                sql_language += ' language = "{}" or '.format(lan)

            sql_temp += ' and {} '.format('(' + sql_language[:-3] + ')')
        if platforms:
            sql_platform = ""
            sql_active = ""
            for pl in platforms:
                sql_platform += ' platform = "{}" or '.format(pl)

            sql_temp += ' and {} '.format('(' + sql_platform[:-3] + ')')

        if countrys:
            sql_country = ''
            for ch in countrys:
                sql_country += ' country = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_country[:-3] + ')')

        if subchannels:
            # sql_subchannel = ''
            # for ch in subchannels:
            sql_subchannel = ' sub1_channel like "%{}%" or '.format(subchannels)
            sql_temp += ' and {} '.format('(' + sql_subchannel[:-3] + ')')

        if sub1ch_perms:
            # sql_sub1channel = ''
            # for ch in sub1ch_perms:
            sql_sub1channel = ' sub2_channel like "%{}%" or '.format(sub1ch_perms)
            sql_temp += ' and {} '.format('(' + sql_sub1channel[:-3] + ')')

        if sub2ch_perms:
            # sql_sub2channel = ''
            # for ch in sub2ch_perms:
            sql_sub2channel = ' sub3_channel like "%{}%" or '.format(sub2ch_perms)
            sql_temp += ' and {} '.format('(' + sql_sub2channel[:-3] + ')')

        # 是否需要子渠道的数据
        newChannel = {}
        if channels:
            sql_channel = ''
            for ch in channels:
                # 判断是否有渠道提取
                Isolate_Channel_Data = Isolate_Channel.objects.filter(newname=ch)
                if Isolate_Channel_Data.count() == 1:
                    prename = Isolate_Channel_Data[0].prename
                    rule = Isolate_Channel_Data[0].rule
                    newname = Isolate_Channel_Data[0].newname
                    newChannel[newname] = [prename, rule]
                    if prename not in channels:
                        ch = prename
                    else:
                        continue
                sql_channel += ' channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')
        sql_temp += appid_sql
        # if checkbox == "true" and subchannels:
        #     sql_subchannel = ''
        #     for ch in subchannels:
        #         sql_subchannel += ' sub_channel = "{}" or '.format(ch)
        #     sql_temp += ' and {} '.format('(' + sql_subchannel[:-3] + ')')



        dime_list = []
        show_dict = {}
        for condition in conditionList:
            if condition == 'country':
                if '国家' in dime_list:
                    continue
                dime_list.append('国家')
                show_dict['国家'] = 'country,'

            elif condition == 'time':
                if '时间' in dime_list:
                    continue
                dime_list.append('时间')
                show_dict['时间'] = 'date,'

            elif condition == 'channel':
                if '渠道' in dime_list:
                    continue
                dime_list.append('渠道')
                show_dict['渠道'] = 'channel,'
            elif condition == 'language':
                if '版本' in dime_list:
                    continue
                dime_list.append('版本')
                show_dict['版本'] = 'language,'
            elif condition == 'platform':
                if '平台' in dime_list:
                    continue
                dime_list.append('平台')
                show_dict['平台'] = 'platform,'
            elif condition == 'subchannel':
                if '子渠道' in dime_list:
                    continue
                dime_list.append('子渠道')
                show_dict['子渠道'] = 'sub1_channel,'

        # if checkbox == "true":
        #     dime_list.append('子渠道')
        #     show_dict['子渠道'] = 'sub_channel,'
        roi_head = []
        week_roi = ['广告花费']
        week_roi_list = ['第1周ROI', '第2周ROI', '第3周ROI', '第4周ROI', '第5周ROI', '第6周ROI', '第7周ROI'
            ,'第8周ROI', '第9周ROI', '第10周ROI', '第11周ROI', '第12周ROI', '第13周ROI', '第14周ROI'
            , '第15周ROI', '第16周ROI', '第17周ROI', '第18周ROI', '第19周ROI', '第20周ROI', '第21周ROI'
            , '第22周ROI', '第23周ROI', '第24周ROI', '第25周ROI', '第26周ROI'
                    ]
        if duration == "0":
            week_roi.extend(week_roi_list)
        elif duration == "1":
            week_roi.extend(week_roi_list[0:7])
        elif duration == "2":
            week_roi.extend(week_roi_list[7:14])
        elif duration == "3":
            week_roi.extend(week_roi_list[14:21])
        elif duration == "4":
            week_roi.extend(week_roi_list[21:26])

        roi_head.extend(dime_list)
        roi_head.extend(week_roi)

        pay_head = []
        week_pay = ['广告花费']
        week_pay_list = ['第1周付费', '第2周付费', '第3周付费', '第4周付费', '第5周付费', '第6周付费', '第7周付费'
            , '第8周付费', '第9周付费', '第10周付费', '第11周付费', '第12周付费', '第13周付费', '第14周付费'
            , '第15周付费', '第16周付费', '第17周付费', '第18周付费', '第19周付费', '第20周付费', '第21周付费'
            , '第22周付费', '第23周付费', '第24周付费', '第25周付费', '第26周付费']

        if duration == "0":
            week_pay.extend(week_pay_list)
        elif duration == "1":
            week_pay.extend(week_pay_list[0:7])
        elif duration == "2":
            week_pay.extend(week_pay_list[7:14])
        elif duration == "3":
            week_pay.extend(week_pay_list[14:21])
        elif duration == "4":
            week_pay.extend(week_pay_list[21:26])
        pay_head.extend(dime_list)
        pay_head.extend(week_pay)

        arppu_head = []
        week_arppu_list = ['第1周', '第2周', '第3周', '第4周', '第5周', '第6周', '第7周'
            ,'第8周', '第9周', '第10周', '第11周', '第12周', '第13周', '第14周'
            , '第15周', '第16周', '第17周', '第18周', '第19周', '第20周', '第21周'
            , '第22周', '第23周', '第24周', '第25周', '第26周']

        if duration == "0":
            week_arppu = week_arppu_list
        elif duration == "1":
            week_arppu = week_arppu_list[0:7]
        elif duration == "2":
            week_arppu = week_arppu_list[7:14]
        elif duration == "3":
            week_arppu = week_arppu_list[14:21]
        elif duration == "4":
            week_arppu = week_arppu_list[21:26]

        arppu_head.extend(dime_list)
        arppu_head.extend(week_arppu)

        ltv_head = []
        week_ltv = ['新增激活数']
        week_ltv_list = ['第1周', '第2周', '第3周', '第4周', '第5周', '第6周', '第7周'
            ,'第8周', '第9周', '第10周', '第11周', '第12周', '第13周', '第14周'
            , '第15周', '第16周', '第17周', '第18周', '第19周', '第20周', '第21周'
            , '第22周', '第23周', '第24周', '第25周', '第26周']

        if duration == "0":
            week_ltv.extend(week_ltv_list)
        elif duration == "1":
            week_ltv.extend(week_ltv_list[0:7])
        elif duration == "2":
            week_ltv.extend(week_ltv_list[7:14])
        elif duration == "3":
            week_ltv.extend(week_ltv_list[14:21])
        elif duration == "4":
            week_ltv.extend(week_ltv_list[21:26])
        ltv_head.extend(dime_list)
        ltv_head.extend(week_ltv)

        dime_str = ''
        dime_group = []

        for di in dime_list:
            dime_str += show_dict.get(di, '')
            dime_group.append(show_dict.get(di, ' ')[:-1])
        # if len(newChannel) > 0 and checkbox != "true":
        #     dime_str += "sub_channel,"
        # , num_8, num_9, num_10, num_11, num_12, num_13, num_14
        # , week_8, week_9, week_10, week_11, week_12, week_13, week_14

        sql_week = ['week_1', 'week_2', 'week_3', 'week_4', 'week_5', 'week_6', 'week_7', 'week_8', 'week_9', 'week_10', 'week_11', 'week_12', 'week_13', 'week_14' , 'week_15', 'week_16', 'week_17', 'week_18', 'week_19', 'week_20', 'week_21', 'week_22', 'week_23', 'week_24', 'week_25', 'week_26']
        sql_num = ['num_1','num_2','num_3','num_4','num_5','num_6','num_7', 'num_8', 'num_9', 'num_10', 'num_11', 'num_12', 'num_13', 'num_14','num_15','num_16','num_17','num_18','num_19','num_20','num_21', 'num_22', 'num_23', 'num_24', 'num_25', 'num_26']
        sliceval = -53
        if duration == "0":
            week_list = sql_week
            sql_week_str = ','.join(week_list)
            sql_num_str = ','.join(sql_num)
        elif duration == "1":
            week_list = sql_week[0:7]
            sql_week_str = ','.join(week_list)
            sql_num_str = ','.join(sql_num[0:7])
            sliceval = -15
        elif duration == "2":
            week_list = sql_week[7:14]
            sql_week_str = ','.join(week_list)
            sql_num_str = ','.join(sql_num[7:14])
            sliceval = -15
        elif duration == "3":
            week_list = sql_week[14:21]
            sql_week_str = ','.join(week_list)
            sql_num_str = ','.join(sql_num[14:21])
            sliceval = -15
        elif duration == "4":
            week_list = sql_week[21:26]
            sql_week_str = ','.join(week_list)
            sql_num_str = ','.join(sql_num[21:26])
            sliceval = -11

        sql = 'SELECT %s ad_cost,new_activate ,%s, %s FROM opsmanage_roi_week WHERE date >= "%s" and date <= "%s"' % (
            dime_str, sql_week_str,sql_num_str, start_time, end_time)
        if sql_temp:
            sql = sql + sql_temp
        df = pd.read_sql(sql, connection)

        adsql = 'SELECT channel, sum(ad_cost) FROM opsmanage_ad_cost_conf WHERE date >= "%s" and date <= "%s" GROUP BY channel' % (start_time, end_time)
        ad_df = pd.read_sql(adsql, connection)

        if not df.index.tolist():
            return render(request, 'report/roi_week_detail.html', locals())
        if '子渠道' in show_dict:
            df["sub1_channel"] = df["sub1_channel"].map(lambda  x: x if x else "")

        if len(newChannel) > 0:
            for newchannel, subChannel in list(newChannel.items()):
                df.loc[(df["channel"] == subChannel[0]) & (
                df["sub1_channel"].str.startswith(subChannel[1])), "channel"] = newchannel
                try:
                    if subChannel[0] not in channels:
                        df = df[~df['channel'].isin([subChannel[0]])]
                except Exception as e:
                    print(e)
            # dime_group.remove("sub_channel")
            if '子渠道' not in show_dict:
                df = df.drop('sub1_channel', axis=1)
            # if channel_show == 'channel':
            #     df = df.drop('channel', axis=1)
            # df = df.groupby(dime_group).agg(['sum'])

        df = df.groupby(dime_group).agg(['sum']).reset_index()

        pay_colume = df.columns.tolist()[:sliceval]
        df_pay = df[pay_colume]
        # , 'week_8', 'week_9', 'week_10', 'week_11', 'week_12', 'week_13', 'week_14'
        for item in week_list:
            df_pay.loc[:, (item, 'sum')] = df[(item, 'sum')].apply(lambda x: '$'+ str(round(x, 2))).astype('str').str.cat(
                df.loc[:, (item.replace('week', 'num'), 'sum')].apply(lambda x: int(x)).astype('str'), sep='<br>')

        df_pay[('ad_cost', 'sum')] = df_pay[('ad_cost', 'sum')].apply(lambda x: '$' + str(round(x, 2)))
            # df['aggregate_payid'] = df['aggregate_payid'].apply(lambda x: int(x) if x != None else 0)
        df_pay = df_pay.fillna(0)
        pay_week_list = []
        pay_week_tmp = np.array(df_pay).tolist()
        roi_colume = df.columns.tolist()[:sliceval]

        df_roi = df[roi_colume]
        df_roi_tran = df[week_list]
        df_roi_tran = df_roi_tran.T.cumsum().T
        for item in week_list:
            df_roi_rate = (df_roi_tran[item] / df.ad_cost)['sum'].apply(lambda x: str(round(x * 100, 2)) + '%')
            df_roi.loc[:, (item, 'sum')] = df_roi_rate.astype('str').str.cat(
                df_roi_tran[(item, 'sum')].apply(lambda x: '$'+ str(round(x, 2))).astype('str'), sep='<br>')
        df_roi[('ad_cost', 'sum')] = df_roi[('ad_cost', 'sum')].apply(lambda x: '$' + str(round(x, 2)))

        cur_time = time.time()

        df_roi = df_roi.fillna(0)
        roi_week_list = []
        roi_week_tmp = np.array(df_roi).tolist()


        sliceval += - 1
        arppu_colume = df.columns.tolist()[:sliceval]
        df_arppu = df[arppu_colume]
        for item in week_list:
            df_arppu.loc[:, (item, 'sum')] = (df[item] / df[item.replace('week', 'num')])['sum'].apply( lambda x: '$'+str(round(x, 2)))
        df_arppu = df_arppu.fillna(0)
        arppu_week_list = []
        arppu_week_tmp = np.array(df_arppu).tolist()

        ltv_colume = df.columns.tolist()[:sliceval]
        df_ltv = df[ltv_colume]
        df_ltv.loc[:, ('new_activate', 'sum')] = df.loc[:, ('new_activate', 'sum')]
        for item in week_list:
            df_ltv.loc[:, (item, 'sum')] = (df_roi_tran[item] / df.new_activate)['sum'].apply(lambda x: '$'+ str(round(x, 2)))
        df_ltv = df_ltv.fillna(0)
        df_ltv[('new_activate', 'sum')] = df_ltv[('new_activate', 'sum')].astype(int)
        ltv_week_list = []
        ltv_week_tmp = np.array(df_ltv).tolist()

        index = 0
        for roi_week in roi_week_tmp:
            data_time = datetime.datetime.strptime(roi_week[0].split("~")[1], "%Y-%m-%d")
            strdataDate = str(data_time + datetime.timedelta(days= 1))[0:10]
            dataDate = time.mktime(time.strptime(strdataDate, '%Y-%m-%d'))
            weekCnt = int((cur_time - dataDate) / (3600 * 24 * 7))
            limitval = weekCnt + len(dime_list) + 2 - (int(duration if duration != "0" else "1") - 1) * 7
            if limitval > 0:
                # tem_roi = []
                # for index, value in enumerate(roi_week):
                #     if index >= limitval:
                #         tem_roi.append("")
                #     else:
                #         tem_roi.append(value)
                # roi_week_list.append(tem_roi)
                roi_week_list.append(roi_week[0:limitval])
                ltv_week_list.append(ltv_week_tmp[index][0:limitval])
                pay_week_list.append(pay_week_tmp[index][0:limitval])
                arppu_week_list.append(arppu_week_tmp[index][0:limitval -1])
            else:
                roi_week_list.append([])
                ltv_week_list.append([])
                pay_week_list.append([])
                arppu_week_list.append([])
            index += 1


        return render(request, 'report/roi_week_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_roi_momth_record', login_url='/noperm/')
def roi_month_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        channels = request.POST.getlist('channels[]')
        # 子渠道
        subchannels = request.POST.get('subchannels')
        sub1ch_perms = request.POST.get('sub1ch_perms')
        sub2ch_perms = request.POST.get('sub2ch_perms')
        # 是否显示子渠道
        # checkbox = request.POST.getlist('checkbox')[0]
        countrys = request.POST.getlist('countrys[]')
        # country_show = request.POST.get('country_show')
        # channel_show = request.POST.get('channel_show')
        # time_show = request.POST.get('time_show')
        duration = request.POST.get('duration')
        game_perms = request.POST.get('game_perms')
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        languages = request.POST.getlist('gameverisions[]')
        platforms = request.POST.getlist('platforms[]')
        conditionList = []
        conditionList.append(request.POST.get('condition_1'))
        conditionList.append(request.POST.get('condition_2'))
        conditionList.append(request.POST.get('condition_3'))
        conditionList.append(request.POST.get('condition_4'))
        conditionList.append(request.POST.get('condition_5'))
        conditionList.append(request.POST.get('condition_6'))

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '0'

        gameinfo = Game_Version_Config.objects.filter(name=game_perms)
        if len(gameinfo) <= 0:
            return render(request, 'report/roi_week_detail.html', locals())
        appid_sql = "AND (app_id ='" + gameinfo[0].android_appid + "'or app_id='" + gameinfo[0].ios_appid + "')"

        sql_temp = ''
        if languages:
            sql_language = ''
            for lan in languages:
                sql_language += ' language = "{}" or '.format(lan)

            sql_temp += ' and {} '.format('(' + sql_language[:-3] + ')')
        if platforms:
            sql_platform = ""
            sql_active = ""
            for pl in platforms:
                sql_platform += ' platform = "{}" or '.format(pl)

            sql_temp += ' and {} '.format('(' + sql_platform[:-3] + ')')

        if countrys:
            sql_country = ''
            for ch in countrys:
                sql_country += ' country = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_country[:-3] + ')')

        if subchannels:
            # sql_subchannel = ''
            # for ch in subchannels:
            sql_subchannel = ' sub1_channel like "%{}%" or '.format(subchannels)
            sql_temp += ' and {} '.format('(' + sql_subchannel[:-3] + ')')

        if sub1ch_perms:
            # sql_sub1channel = ''
            # for ch in sub1ch_perms:
            sql_sub1channel = ' sub2_channel like "%{}%" or '.format(sub1ch_perms)
            sql_temp += ' and {} '.format('(' + sql_sub1channel[:-3] + ')')

        if sub2ch_perms:
            # sql_sub2channel = ''
            # for ch in sub2ch_perms:
            sql_sub2channel = ' sub3_channel like "%{}%" or '.format(sub2ch_perms)
            sql_temp += ' and {} '.format('(' + sql_sub2channel[:-3] + ')')

        # 是否需要子渠道的数据
        newChannel = {}
        if channels:
            sql_channel = ''
            for ch in channels:
                # 判断是否有渠道提取
                Isolate_Channel_Data = Isolate_Channel.objects.filter(newname=ch)
                if Isolate_Channel_Data.count() == 1:
                    prename = Isolate_Channel_Data[0].prename
                    rule = Isolate_Channel_Data[0].rule
                    newname = Isolate_Channel_Data[0].newname
                    newChannel[newname] = [prename, rule]
                    if prename not in channels:
                        ch = prename
                    else:
                        continue
                sql_channel += ' channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')

        sql_temp += appid_sql

        dime_list = []
        show_dict = {}
        for condition in conditionList:
            if condition == 'country':
                if '国家' in dime_list:
                    continue
                dime_list.append('国家')
                show_dict['国家'] = 'country,'

            elif condition == 'time':
                if '时间' in dime_list:
                    continue
                dime_list.append('时间')
                show_dict['时间'] = 'date,'

            elif condition == 'channel':
                if '渠道' in dime_list:
                    continue
                dime_list.append('渠道')
                show_dict['渠道'] = 'channel,'
            elif condition == 'language':
                if '版本' in dime_list:
                    continue
                dime_list.append('版本')
                show_dict['版本'] = 'language,'
            elif condition == 'platform':
                if '平台' in dime_list:
                    continue
                dime_list.append('平台')
                show_dict['平台'] = 'platform,'
            elif condition == 'subchannel':
                if '子渠道' in dime_list:
                    continue
                dime_list.append('子渠道')
                show_dict['子渠道'] = 'sub1_channel,'

        # if checkbox == "true":
        #     dime_list.append('子渠道')
        #     show_dict['子渠道'] = 'sub_channel,'

        roi_head = []
        month_roi = ['广告花费']
        month_roi_list = ['首月ROI', '次月ROI', '第3月ROI', '第4月ROI', '第5月ROI', '第6月ROI', '第7月ROI','第8月ROI', '第9月ROI', '第10月ROI', '第11月ROI', '第12月ROI'
            , '第13月ROI', '第14月ROI', '第15月ROI', '第16月ROI', '第17月ROI', '第18月ROI', '第19月ROI', '第20月ROI', '第21月ROI', '第22月ROI', '第23月ROI', '第24月ROI']
        if duration == "0":
            month_roi.extend(month_roi_list)
        else :
            month_roi.extend(month_roi_list[(int(duration) - 1) * 6:int(duration) * 6])

        roi_head.extend(dime_list)
        roi_head.extend(month_roi)

        pay_head = []
        month_pay = ['广告花费']
        month_pay_list = ['首月付费', '次月付费', '第3月付费', '第4月付费', '第5月付费', '第6月付费', '第7月付费', '第8月付费', '第9月付费', '第10月付费', '第11月付费', '第12月付费'
            , '第13月付费', '第14月付费', '第15月付费', '第16月付费', '第17月付费', '第18月付费', '第19月付费', '第20月付费', '第21月付费', '第22月付费', '第23月付费', '第24月付费']

        if duration == "0":
            month_pay.extend(month_pay_list)
        else:
            month_pay.extend(month_pay_list[(int(duration) - 1)*6:int(duration)*6])
        pay_head.extend(dime_list)
        pay_head.extend(month_pay)

        arppu_head = []
        month_arppu = []
        month_arppu_list = ['首月', '次月', '第3月', '第4月', '第5月', '第6月', '第7月', '第8月', '第9月', '第10月', '第11月', '第12月'
            , '第13月', '第14月', '第15月', '第16月', '第17月', '第18月', '第19月', '第20月', '第21月', '第22月', '第23月', '第24月']

        if duration == "0":
            month_arppu.extend(month_arppu_list)
        else:
            month_arppu.extend(month_arppu_list[(int(duration) - 1) * 6:int(duration) * 6])
        arppu_head.extend(dime_list)
        arppu_head.extend(month_arppu)

        ltv_head = []
        month_ltv = ['新增激活数']
        if duration == "0":
            month_ltv.extend(month_arppu_list)
        else:
            month_ltv.extend(month_arppu_list[(int(duration) - 1) * 6:int(duration) * 6])
        ltv_head.extend(dime_list)
        ltv_head.extend(month_ltv)

        dime_str = ''
        dime_group = []
        for di in dime_list:
            dime_str += show_dict.get(di, '')
            dime_group.append(show_dict.get(di, ' ')[:-1])
        # if len(newChannel) > 0 and checkbox != "true":
        #     dime_str += "sub_channel,"
        # if channel_show != 'channel':
        #     dime_str += "channel,"
        sql_month = ['month_1','month_2','month_3','month_4','month_5','month_6','month_7','month_8','month_9','month_10','month_11','month_12','month_13','month_14','month_15','month_16','month_17','month_18','month_19','month_20','month_21','month_22','month_23','month_24']
        sql_num = ['num_1','num_2','num_3','num_4','num_5','num_6','num_7','num_8','num_9','num_10','num_11','num_12','num_13','num_14','num_15','num_16','num_17','num_18','num_19','num_20','num_21','num_22','num_23','num_24']
        sliceval = -49
        if duration == "0":
            month_list = sql_month
            sql_month_str = ','.join(month_list)
            sql_num_str = ','.join(sql_num)
        else:
            month_list = sql_month[(int(duration) - 1) * 6:int(duration) * 6]
            sql_month_str = ','.join(month_list)
            sql_num_str = ','.join(sql_num[(int(duration) - 1) * 6:int(duration) * 6])
            sliceval = -13

        sql = 'SELECT %s ad_cost,new_activate,%s,%s FROM opsmanage_roi_month WHERE date >= "%s" and date <= "%s"' % (
        dime_str,sql_month_str, sql_num_str, start_time, end_time)
        if sql_temp:
            sql = sql + sql_temp

        df = pd.read_sql(sql, connection)
        if not df.index.tolist():
            return render(request, 'report/roi_month_detail.html', locals())

        if '子渠道' in show_dict:
            df["sub1_channel"] = df["sub1_channel"].map(lambda x: x if x else "")

        if len(newChannel) > 0:
            for newchannel, subChannel in list(newChannel.items()):
                df.loc[(df["channel"] == subChannel[0]) & (
                    df["sub1_channel"].str.startswith(subChannel[1])), "channel"] = newchannel
                try:
                    if subChannel[0] not in channels:
                        df = df[~df['channel'].isin([subChannel[0]])]
                except Exception as e:
                    print(e)
            # dime_group.remove("sub_channel")
            if '子渠道' not in show_dict:
                df = df.drop('sub1_channel', axis=1)
            # if channel_show == 'channel':
            #     df = df.drop('channel', axis=1)
                # df = df.groupby(dime_group).agg(['sum'])

        df = df.groupby(dime_group).agg(['sum']).reset_index()

        pay_colume = df.columns.tolist()[:sliceval]
        df_pay = df[pay_colume]
        for item in month_list:
            df_pay.loc[:, (item, 'sum')] = df.loc[:,(item,'sum')].apply(lambda x: '$'+ str(round(x, 2))).astype('str').str.cat(
                df.loc[:,(item.replace('month', 'num'),'sum')].apply(lambda x: int(x)).astype('str'), sep='<br>')
        df_pay[('ad_cost', 'sum')] = df_pay[('ad_cost', 'sum')].apply(lambda x: '$' + str(round(x, 2)))
        df_pay = df_pay.fillna(0)
        pay_month_list = []
        pay_month_tmp = np.array(df_pay).tolist()

        roi_colume = df.columns.tolist()[:sliceval]
        df_roi = df[roi_colume]
        df_roi_tran = df[month_list]
        df_roi_tran = df_roi_tran.T.cumsum().T
        for item in month_list:
            df_roi_rate = (df_roi_tran[item] / df.ad_cost)['sum'].apply(lambda x: str(round(x*100,2)) + '%')
            df_roi.loc[:, (item, 'sum')] = df_roi_rate.astype('str').str.cat(df_roi_tran.loc[:,(item,'sum')].apply(lambda x: '$'+ str(round(x, 2))).astype('str'), sep='<br>')
        df_roi[('ad_cost', 'sum')] = df_roi[('ad_cost', 'sum')].apply(lambda x: '$' + str(round(x, 2)))
        df_roi = df_roi.fillna(0)
        roi_month_list = []
        roi_month_tmp = np.array(df_roi).tolist()


        sliceval -= 1
        arppu_colume = df.columns.tolist()[:sliceval]
        df_arppu = df[arppu_colume]
        for item in month_list:
            df_arppu.loc[:, (item, 'sum')] = (df[item] / df[item.replace('month', 'num')])['sum'].apply(lambda x: '$'+str(round(x,2)))
        df_arppu = df_arppu.fillna(0)
        arppu_month_list = []
        arppu_month_tmp= np.array(df_arppu).tolist()

        ltv_colume = df.columns.tolist()[:sliceval]
        df_ltv = df[ltv_colume]
        df_ltv.loc[:, ('new_activate', 'sum')] = df.loc[:, ('new_activate', 'sum')]
        for item in month_list:
            df_ltv.loc[:, (item, 'sum')] = (df_roi_tran[item] / df.new_activate)['sum'].apply(lambda x: '$'+ str(round(x,2)))
        df_ltv = df_ltv.fillna(0)
        df_ltv[('new_activate', 'sum')] = df_ltv[('new_activate', 'sum')].astype(int)
        ltv_month_list = []
        ltv_month_tmp = np.array(df_ltv).tolist()


        index = 0
        cur_time = time.time()
        for roi_month in roi_month_tmp:
            dataDate = time.mktime(time.strptime(roi_month[0] + "-01", '%Y-%m-%d'))
            weekCnt = int((cur_time - dataDate) / (3600 * 24 * 30))
            limitval = weekCnt + len(dime_list) + 2 - (int(duration if duration != "0" else "1") - 1) * 6 - 1
            if limitval > 0:
                roi_month_list.append(roi_month[0:limitval])
                ltv_month_list.append(ltv_month_tmp[index][0:limitval])
                pay_month_list.append(pay_month_tmp[index][0:limitval])
                arppu_month_list.append(arppu_month_tmp[index][0:limitval - 1])
            else:
                roi_month_list.append([])
                ltv_month_list.append([])
                pay_month_list.append([])
                arppu_month_list.append([])
            index += 1

        return render(request, 'report/roi_month_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_change_report_comment_record', login_url='/noperm/')
def report_comment_edit(request):
    if request.method == "POST":
        date = request.POST.get('date')
        value = request.POST.get('value')
        platform = request.POST.get('platform')
        try:
            report, created = Report_Comment.objects.get_or_create(date=date,platform=platform)
        except Exception as e:
            print(e)
            return HttpResponse(' ')
        report.content = value
        report.save()
        return HttpResponse(value)


@login_required()
@permission_required('OpsManage.can_change_report_realtime_record', login_url='/noperm/')
def report_real_collect(request):
    cron.data_real_up()
    return JsonResponse({'msg': '删除成功', "code": 200, 'data': ""})


def ReportRecharge(app_id, beginDate, endDate, rechargeBeginDate, rechargeEndDate):
    sql_new_device_id = 'select distinct device_id from opsmanage_push_client_activate where db_time >= "%s" and db_time < "%s" union select distinct device_id from opsmanage_push_appsflyer where db_time >= "%s" and db_time < "%s"' % (
        beginDate, endDate, beginDate, endDate)
    df_new_device_id = pd.read_sql(sql_new_device_id, connection)
    sql_recharge = 'SELECT device_id,recharge FROM opsmanage_push_client_recharge WHERE app_id = "%s" and db_time >= "%s" and db_time <= "%s"' % (
        app_id, rechargeBeginDate, rechargeEndDate)
    df_recharge = pd.read_sql(sql_recharge, connection)
    df_grouped_recharge = df_recharge.groupby(['device_id'], as_index=False).sum()
    df_inner_recharge = pd.merge(df_new_device_id, df_grouped_recharge, on=['device_id'])
    return df_inner_recharge


def ReportDayFrame(project, languages, sources, startDate, endDate, rStartDate, rEndDate, checkbox, platform, dbName):
    yesterday = str(datetime.date.today() + datetime.timedelta(days=(-1)))
    now_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))[:10]
    sg_conf = Game_Version_Config.objects.filter(name=project)
    if sg_conf.count() != 1:
        return
    app_id = sg_conf[0].ios_appid if platform == "IOS" else sg_conf[0].android_appid

    if not rEndDate:
        rEndDate = now_time
    if not rStartDate:
        rStartDate = yesterday

    sql_temp = ''
    if languages:
        sql_temp += SelectSqlStrFormat('language', languages)
    if sources:
        sql_temp += SelectSqlStrFormat('source', sources)

    sql_report = 'SELECT date,end_date,new_activate,ad_activate,na_activate,new_online,remain,recharge,ad_cost,active_device,active_player, rechargeDevice, paymentDevice, paymentAmount, AdDevice, AdAmount, RechargePlayer, AddPlayerNum, AddRechargePlayer, AddRechargeNum, RechargeOldRole, RechargeOldNum FROM %s WHERE date >= "%s" and date <= "%s" and app_id = "%s"' % (
        dbName, startDate, endDate, app_id)
    if sql_temp:
        sql_report = sql_report + sql_temp
    df_report = pd.read_sql(sql_report, connection)
    df_report = df_report.groupby(['date', 'end_date'], as_index=False).sum()
    # 保留做日后取时间优化参考
    # date_report_lst = df_report['date'].tolist()
    # date_report = json.dumps(date_report_lst)
    if dbName != 'opsmanage_report_day':
        df_report['date'] = df_report['date'] + '~' + df_report['end_date']
    df_report = pd.DataFrame(df_report,
                          columns=['date', 'new_activate', 'ad_activate', 'na_activate', 'nature_rate', 'ad_cost',
                                   'ad_pernew_cost','ad_perad_cost','new_online','new_online_rate', 'TotalROI', 'AdROI',
                                   'TotalRecharge', 'AdAmount', 'paymentAmount','TotalRechargeNum', 'AdDevice', 'paymentDevice',
                                   'TotalPayRate', 'AdPayRate', 'NaturePayRate', 'TotalARPPU', 'AdARPPU', 'NatureARPPU',
                                   'TotalLTV', 'AdLTV', 'NatureLTV','remain', 'rechargeDevice', 'RechargePlayer', 'recharge',
                                   'paymentProportion', 'dayPayRate', 'dayARP', 'dayARPPU', 'active_device', 'active_player',
                                   'AddPlayerNum', 'AddRechargePlayer', 'RechargeOldRole', 'AddRechargeNum',  'RechargeOldNum'])
    if checkbox == 'true':
        start_date = datetime.datetime.strptime(startDate, "%Y-%m-%d")
        over_date = datetime.datetime.strptime(endDate, "%Y-%m-%d")
        re_start_date = datetime.datetime.strptime(rStartDate, "%Y-%m-%d")
        re_end_date = datetime.datetime.strptime(rEndDate, "%Y-%m-%d") + datetime.timedelta(days=1)

        dates = df_report['date'].tolist()
        df_report['recharge_device'] = 0
        df_report['recharge_amount'] = 0
        df_report['roi'] = 0
        df_report['pay_rate'] = 0
        df_report['arppu'] = 0
        df_report['ltv'] = 0

        for date in dates:
            cBeginDate = start_date
            cEndDate = over_date
            if dbName != 'opsmanage_report_day':
                cBeginDate,cEndDate = date.split('~')
                cBeginDate = datetime.datetime.strptime(cBeginDate, "%Y-%m-%d")
                cEndDate = datetime.datetime.strptime(cEndDate, "%Y-%m-%d") + datetime.timedelta(days=1)
            else:
                cBeginDate = datetime.datetime.strptime(date, "%Y-%m-%d")
                cEndDate = cBeginDate + datetime.timedelta(days=1)
            df_inner_recharge = ReportRecharge(app_id, cBeginDate, cEndDate, re_start_date, re_end_date)
            if not df_report.empty:
                count = df_inner_recharge['recharge'].count().astype(int)
                df_report.loc[df_report['date'] == str(date), 'recharge_device'] = df_inner_recharge[
                    'recharge'].count().astype(int)
                recharge_sum = df_inner_recharge['recharge'].sum()
                df_report.loc[df_report['date'] == str(date), 'recharge_amount'] = 0 if np.isnan(
                    recharge_sum) else recharge_sum
        df_report['recharge_device'] = df_report['recharge_device'].astype(int)

    return df_report


def ReportChartHandle(df_report):
    try:
        # 激活成本
        df_report['ad_pernew_cost'] = df_report.ad_cost / df_report.new_activate
        # 广告激活成本
        df_report['ad_perad_cost'] = df_report.ad_cost / df_report.ad_activate
        df_report['ad_pernew_cost'] = df_report['ad_pernew_cost'].map(lambda x: str(round(x, 2)))
        df_report['ad_perad_cost'] = df_report['ad_perad_cost'].map(lambda x: str(round(x, 2)))

        # 图形数据
        chart = {}
        # 激活
        chart["new_act"] = json.dumps(df_report['new_activate'].tolist())
        chart["ad_act"] = json.dumps(df_report['ad_activate'].tolist())
        chart["nature_act"] = json.dumps(df_report['na_activate'].tolist())
        # 成本
        chart["ad_pernew_cost"] = json.dumps(df_report['ad_pernew_cost'].tolist())
        chart["ad_perad_cost"] = json.dumps(df_report['ad_perad_cost'].tolist())
        # 充值
        chart["recharge_amount"] = json.dumps(df_report['recharge'].tolist())

        return chart

    except Exception as e:
        print(e)


def ReportDataFrameHandle(df_report, checkbox):
    ave_list = [round(ave, 2) for ave in df_report.mean().tolist()]
    sum_list = df_report.sum().tolist()
    ave_list.insert(0, '平均')
    sum_list[0] = '总计'
    if df_report.index.tolist():
        df_report.loc[df_report.shape[0] + 1] = ave_list
        df_report.loc[df_report.shape[0] + 1] = sum_list
    df_report = df_report.fillna(0)
    df_report['new_activate'] = df_report['new_activate'].astype(int)
    df_report['ad_activate'] = df_report['ad_activate'].astype(int)
    df_report['na_activate'] = df_report['na_activate'].astype(int)
    df_report['new_online'] = df_report['new_online'].astype(int)
    df_report['active_device'] = df_report['active_device'].astype(int)
    df_report['active_player'] = df_report['active_player'].astype(int)
    df_report['TotalRechargeNum'] = df_report['TotalRechargeNum'].astype(int)
    df_report['AdDevice'] = df_report['AdDevice'].astype(int)
    df_report['RechargeOldRole'] = df_report['RechargeOldRole'].astype(int)
    df_report['AddRechargePlayer'] = df_report['AddRechargePlayer'].astype(int)
    df_report['AddPlayerNum'] = df_report['AddPlayerNum'].astype(int)
    df_report['RechargePlayer'] = df_report['RechargePlayer'].astype(int)
    df_report['rechargeDevice'] = df_report['rechargeDevice'].apply(lambda x: int(x))
    df_report['paymentDevice'] = df_report['paymentDevice'].apply(lambda x: int(x))
    # 自然激活占比
    df_report['nature_rate'] = df_report.na_activate / df_report.new_activate
    # 激活成本
    df_report['ad_pernew_cost'] = df_report.ad_cost / df_report.new_activate
    # 广告激活成本
    df_report['ad_perad_cost'] = df_report.ad_cost / df_report.ad_activate
    # 上线率
    df_report['new_online_rate'] = df_report.new_online / df_report.new_activate
    # 次日留存/总次留
    df_report['remain'] = df_report.remain / df_report.new_activate
    # 日ARP
    df_report['dayARP'] = df_report.recharge/df_report.active_device
    # 日ARPPU
    df_report['dayARPPU'] = df_report.recharge/df_report.rechargeDevice
    # 日付费率
    df_report['dayPayRate'] = df_report.rechargeDevice/df_report.active_device
    # 首日付费率/自然付费率
    df_report['NaturePayRate'] = df_report.paymentDevice/df_report.na_activate
    # 总新增充值
    df_report['TotalRecharge'] = df_report.AdAmount + df_report.paymentAmount
    # 首日付费占比/新增充值占比
    df_report['paymentProportion'] = df_report.TotalRecharge / df_report.recharge
    # 总新增充值设备
    df_report['TotalRechargeNum'] = df_report.AdDevice + df_report.paymentDevice
    # 总ROI
    df_report['TotalROI'] = df_report.TotalRecharge/df_report.ad_cost
    # 广告ROI
    df_report['AdROI'] = df_report.AdAmount/df_report.ad_cost
    # 总付费率
    df_report['TotalPayRate'] = df_report.TotalRechargeNum/df_report.new_activate
    # 广告付费率
    df_report['AdPayRate'] = df_report.AdDevice/df_report.ad_activate
    # 总ARPPU
    df_report['TotalARPPU'] = df_report.TotalRecharge/df_report.TotalRechargeNum
    # 广告ARPPU
    df_report['AdARPPU'] = df_report.AdAmount/df_report.AdDevice
    # 自然ARPPU
    df_report['NatureARPPU'] = df_report.paymentAmount/df_report.paymentDevice
    # 总LTV
    df_report['TotalLTV'] = df_report.TotalRecharge/df_report.new_activate
    # 广告LTV
    df_report['AdLTV'] = df_report.AdAmount/df_report.ad_activate
    # 自然LTV
    df_report['NatureLTV'] = df_report.paymentAmount/df_report.na_activate
    if checkbox == 'true' and not df_report.empty:
        # ROI
        df_report['roi'] = df_report.recharge_amount / df_report.ad_cost
        # 付费率
        df_report['pay_rate'] = df_report.recharge_device / df_report.new_activate
        # ARPPU
        df_report['arppu'] = df_report.recharge_amount / df_report.recharge_device
        # LTV
        df_report['ltv'] = df_report.recharge_amount / df_report.new_activate

    df_report['nature_rate'] = df_report['nature_rate'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['new_online_rate'] = df_report['new_online_rate'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['remain'] = df_report['remain'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['dayARP'] = df_report['dayARP'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['dayARPPU'] = df_report['dayARPPU'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['dayPayRate'] = df_report['dayPayRate'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['paymentProportion'] = df_report['paymentProportion'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['NaturePayRate'] = df_report['NaturePayRate'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['TotalROI'] = df_report['TotalROI'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['AdROI'] = df_report['AdROI'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['TotalPayRate'] = df_report['TotalPayRate'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['AdPayRate'] = df_report['AdPayRate'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['TotalARPPU'] = df_report['TotalARPPU'].map(lambda x: str(round(x, 2)))
    df_report['AdARPPU'] = df_report['AdARPPU'].map(lambda x: str(round(x, 2)))
    df_report['NatureARPPU'] = df_report['NatureARPPU'].map(lambda x: str(round(x, 2)))
    df_report['TotalLTV'] = df_report['TotalLTV'].map(lambda x: str(round(x, 2)))
    df_report['AdLTV'] = df_report['AdLTV'].map(lambda x: str(round(x, 2)))
    df_report['NatureLTV'] = df_report['NatureLTV'].map(lambda x: str(round(x, 2)))
    df_report['AdAmount'] = df_report['AdAmount'].map(lambda x: str(round(x, 2)))
    df_report['paymentAmount'] = df_report['paymentAmount'].map(lambda x: str(round(x, 2)))
    df_report['TotalRecharge'] = df_report['TotalRecharge'].map(lambda x: str(round(x, 2)))
    df_report['AddRechargeNum'] = df_report['AddRechargeNum'].map(lambda x: str(round(x, 2)))
    df_report['RechargeOldNum'] = df_report['RechargeOldNum'].map(lambda x: str(round(x, 2)))
    if checkbox == 'true':
        df_report['recharge_device'] = df_report['recharge_device'].astype(int)
        df_report['roi'] = df_report['roi'].map(lambda x: str(round(x * 100, 2)) + '%')
        df_report['pay_rate'] = df_report['pay_rate'].map(lambda x: str(round(x * 100, 2)) + '%')
        df_report['arppu'] = df_report['arppu'].apply(lambda x: "%.2f" % (x)).astype('str')
        df_report['ltv'] = df_report['ltv'].apply(lambda x: "%.2f" % (x)).astype('str')

    # 最后将广告花费和成本类的单位为美金，2个小数点
    df_report['ad_cost'] = df_report['ad_cost'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['ad_pernew_cost'] = df_report['ad_pernew_cost'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['ad_perad_cost'] = df_report['ad_perad_cost'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['recharge'] = df_report['recharge'].apply(lambda x: "%.2f" % (x)).astype('str')

    result = df_report.fillna(' ')
    report_day_list = np.array(result).tolist()
    return report_day_list


@login_required()
@permission_required('OpsManage.can_change_report_week_record', login_url='/noperm/')
def report_week_collect(request):
    try:
        cron.data_week_info_up()
        return JsonResponse({'msg': '成功', "code": 200, 'data': ""})
    except Exception as e:
        print(e)


def update_day_report(datetime_today):
    try:
        before_yesterday = str(datetime_today + datetime.timedelta(days=(-2)))[:10]
        yesterday = str(datetime_today + datetime.timedelta(days=(-1)))[:10]
        today = str(datetime_today)[:10]
        be_yesterday_report_data = Report_Day.objects.filter(date=before_yesterday)
        for data in be_yesterday_report_data:
            platform = data.platform
            app_id = data.app_id
            language = data.language
            source = data.source
            beginDate = before_yesterday
            # 注册
            data.new_reg = cron.report_new_reg(platform, app_id, language, source, beginDate, yesterday, today)
            # 上线
            data.new_online = cron.report_new_online(platform, app_id, language, source, beginDate, yesterday, today)
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


@login_required()
@permission_required('OpsManage.can_change_report_month_record', login_url='/noperm/')
def report_month_collect(request):
    try:
        #day
        day_s_date = datetime.datetime(2019, 4, 26)
        day_e_date = datetime.datetime(2019, 4, 29)
        while day_s_date <= day_e_date:
            today = day_s_date
            yesterday = str(today + datetime.timedelta(days=-1))[:10]
            strToday = str(today)[:10]
            try:
                update_day_report(today)
                cron.report_data(yesterday, strToday, strToday, yesterday, Report_Day)
                cron.remain_day_data(day_s_date)
                day_s_date = day_s_date + datetime.timedelta(days=1)
            except Exception as e:
                print(strToday)
                print(e)
    except Exception as e:
        print(e)


    # try:
    #     # week
    #     week_s_date = datetime.datetime(2018,11,9)
    #     week_e_date = datetime.datetime(2019,1,4)
    #     while week_s_date <= week_e_date:
    #         today = week_s_date
    #         yesterday = str(today + datetime.timedelta(days=-1))[:10]
    #         week_start_day = str(today + datetime.timedelta(days=-8))[:10]
    #         show_end_date = str(today + datetime.timedelta(days=-1, seconds=-1))[:10]
    #         today = str(today)[:10]
    #         try:
    #             cron.report_data(week_start_day, yesterday, today, show_end_date, Report_Week)
    #             week_s_date = week_s_date + datetime.timedelta(days=7)
    #         except Exception as e:
    #             print(week_start_day)
    #             print(e)
    # except Exception as e:
    #     print(e)
    #
    # try:
    #     #month
    #     month_s_date = datetime.datetime(2019, 1, 2)
    #     before_yesterday = str(month_s_date + datetime.timedelta(days=(-2)))[:10]
    #     yesterday = str(month_s_date + datetime.timedelta(days=-1))
    #     today = str(month_s_date)[:10]
    #     month = before_yesterday[0:7]
    #     month_start_day = month + '-01'
    #     cron.report_data(month_start_day, yesterday, today, before_yesterday, Report_Month)
    # except Exception as e:
    #     print(e)

    # try:
    #     cron.data_month_info_up()
    #     return JsonResponse({'msg': '成功', "code": 200, 'data': ""})
    # except Exception as e:
    #     print(e)

@login_required()
@permission_required('OpsManage.can_read_language_form_record', login_url='/noperm/')
def language_form(request):
    projectDBValues = Game_Version_Config.objects.values_list('name').distinct()
    projectValues = []
    for project in projectDBValues:
        projectValues.append(project[0])
    # 来源
    sourceDBValues = Report_Day.objects.values_list('source').distinct()
    sourceValues = []
    for source in sourceDBValues:
        sourceValues.append(source[0])

    date = datetime.datetime.now()
    this_week_start_dt = str(date - datetime.timedelta(days=date.weekday())).split()[0]

    return render(request, 'report/language_form.html', locals())

@login_required()
@permission_required('OpsManage.can_read_language_form_record', login_url='/noperm/')
def language_form_check(request):
    yesterday = str(datetime.date.today() + datetime.timedelta(days=(-1)))
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))[:10]
    if request.method == "POST":
        try:
            sources = request.POST.getlist('sources[]')
            project = request.POST.get('project')
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')
            f_start_time = request.POST.get('f_start_time')
            f_end_time = request.POST.get('f_end_time')
            checkbox = request.POST.get('checkbox', '')
            if not end_time:
                end_time = now_time
            if not start_time:
                start_time = yesterday

            df_android = LanguageFrame(project, sources, start_time, end_time, f_start_time, f_end_time,
                                        checkbox, "Android", "opsmanage_report_day")
            df_ios = LanguageFrame(project, sources, start_time, end_time, f_start_time, f_end_time,
                                    checkbox, "IOS", "opsmanage_report_day")
            df = pd.concat([df_android, df_ios], ignore_index=True)
            df = df.groupby(['language'], as_index=False).sum()

            df_android_out = LanguageFrameHandle(df_android, checkbox)
            df_ios_out = LanguageFrameHandle(df_ios, checkbox)
            df_out = LanguageFrameHandle(df, checkbox)

            thhead = ['版本', '新增激活数', '广告激活数', '自然激活数', '版本激活占比', '广告花费($)', '激活成本($)',
                      '广告激活成本($)', '新增设备上线', '上线率',
                      '新增上线成本($)', '次日留存', '活跃设备', '活跃玩家', '充值设备', '充值金额($)', '日付费率', '日ARPU($)', '日ARPPU($)']

        except Exception as e:
            print(e)
        return render(request, 'report/language_form_detail.html', locals())


def LanguageFrame(project, sources, startDate, endDate, rStartDate, rEndDate, checkbox, platform, dbName):
    yesterday = str(datetime.date.today() + datetime.timedelta(days=(-1)))
    now_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))[:10]
    sg_conf = Game_Version_Config.objects.filter(name=project)
    if sg_conf.count() != 1:
        return
    app_id = sg_conf[0].ios_appid if platform == "IOS" else sg_conf[0].android_appid

    # if not rEndDate:
    #     rEndDate = now_time
    # if not rStartDate:
    #     rStartDate = yesterday

    sql_temp = ''
    if sources:
        sql_temp += SelectSqlStrFormat('source', sources)

    sql_report = 'SELECT language, new_activate,ad_activate,na_activate,new_online,remain,recharge,ad_cost,active_device,active_player, rechargeDevice FROM %s WHERE date >= "%s" and date <= "%s" and app_id = "%s"' % (
        dbName, startDate, endDate, app_id)
    if sql_temp:
        sql_report = sql_report + sql_temp
    df_report = pd.read_sql(sql_report, connection)
    df_report = df_report.groupby(['language'], as_index=False).sum()

    df_report = pd.DataFrame(df_report,
                          columns=['language', 'new_activate', 'ad_activate', 'na_activate', 'nature_rate', 'ad_cost',
                                   'ad_pernew_cost',
                                   'ad_perad_cost', 'new_online',
                                   'new_online_rate',
                                   'new_online_cost','remain', 'active_device', 'active_player', 'rechargeDevice', 'recharge', 'dayPayRate', 'dayARP', 'dayARPPU'])

    return df_report


def LanguageFrameHandle(df_report, checkbox):
    sum_list = df_report.sum().tolist()
    sum_list[0] = '总计'
    if df_report.index.tolist():
        df_report.loc[df_report.shape[0] + 1] = sum_list
    df_report = df_report.fillna(0)
    df_report['new_activate'] = df_report['new_activate'].astype(int)
    df_report['ad_activate'] = df_report['ad_activate'].astype(int)
    df_report['na_activate'] = df_report['na_activate'].astype(int)
    df_report['new_online'] = df_report['new_online'].astype(int)
    df_report['active_device'] = df_report['active_device'].astype(int)
    df_report['active_player'] = df_report['active_player'].astype(int)
    df_report['rechargeDevice'] = df_report['rechargeDevice'].apply(lambda x: int(x))
    # 自然激活占比
    df_report['nature_rate'] = df_report.na_activate / df_report.new_activate
    # 激活成本
    df_report['ad_pernew_cost'] = df_report.ad_cost / df_report.new_activate
    # 广告激活成本
    df_report['ad_perad_cost'] = df_report.ad_cost / df_report.ad_activate
    # 上线率
    df_report['new_online_rate'] = df_report.new_online / df_report.new_activate
    # 新增上线成本
    df_report['new_online_cost'] = df_report.ad_cost / df_report.new_online
    # 次日留存/总次留
    df_report['remain'] = df_report.remain / df_report.new_activate
    # 日ARP
    df_report['dayARP'] = df_report.recharge/df_report.active_device
    # 日ARPPU
    df_report['dayARPPU'] = df_report.recharge/df_report.rechargeDevice
    # 日付费率
    df_report['dayPayRate'] = df_report.rechargeDevice/df_report.active_device

    df_report['nature_rate'] = df_report['nature_rate'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['new_online_rate'] = df_report['new_online_rate'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['remain'] = df_report['remain'].map(lambda x: str(round(x * 100, 2)) + '%')
    df_report['dayARP'] = df_report['dayARP'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['dayARPPU'] = df_report['dayARPPU'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['dayPayRate'] = df_report['dayPayRate'].map(lambda x: str(round(x * 100, 2)) + '%')

    # 最后将广告花费和成本类的单位为美金，2个小数点
    df_report['ad_cost'] = df_report['ad_cost'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['ad_pernew_cost'] = df_report['ad_pernew_cost'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['ad_perad_cost'] = df_report['ad_perad_cost'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['new_online_cost'] = df_report['new_online_cost'].apply(lambda x: "%.2f" % (x)).astype('str')
    df_report['recharge'] = df_report['recharge'].apply(lambda x: "%.2f" % (x)).astype('str')

    result = df_report.fillna(' ')
    report_day_list = np.array(result).tolist()
    return report_day_list
