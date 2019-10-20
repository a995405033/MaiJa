#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import json
from django.db import connection
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.contrib.auth.decorators import permission_required
from OpsManage.models import *
from OpsManage.tasks import *
from django.http import JsonResponse
from OpsManage import cron
from OpsManage.utils.common import SelectSqlStrFormat
import time
import datetime
import copy
from dateutil.relativedelta import relativedelta


@login_required()
def report_day(request):
    return render(request, 'report/report_day.html', locals())


@login_required()
def report_quantity(request):
    return render(request, 'report/report_quantity.html', locals())


@login_required()
@permission_required('OpsManage.can_read_remain_day_record', login_url='/noperm/')
def retain_day(request):
    recharge = False
    languageDBValues = Remain_Day.objects.values_list('language').distinct()
    languageValues = []
    for language in languageDBValues:
        languageValues.append(language[0])
    countryDBValues = Remain_Day.objects.values_list('country').distinct()
    countryValues = []
    for country in countryDBValues:
        countryValues.append(country[0])
    channelDBValues = Remain_Day.objects.values_list('channel').distinct()
    channelValues = []
    for channel in channelDBValues:
        channelValues.append(channel[0])
    GameNameDBValues = Game_Version_Config.objects.values_list('name').distinct()
    GameNameValues = []
    for GameName in GameNameDBValues:
        GameNameValues.append(GameName[0])
    # 来源
    sourceDBValues = Remain_Day.objects.values_list('source').distinct()
    sourceValues = []
    for source in sourceDBValues:
        sourceValues.append(source[0])
    return render(request, 'retain/retain_day.html', locals())


@login_required()
@permission_required('OpsManage.can_read_remain_day_record', login_url='/noperm/')
def recharge_retain_day(request):
    recharge = True
    languageDBValues = Remain_Day.objects.values_list('language').distinct()
    languageValues = []
    for language in languageDBValues:
        languageValues.append(language[0])
    countryDBValues = Remain_Day.objects.values_list('country').distinct()
    countryValues = []
    for country in countryDBValues:
        countryValues.append(country[0])
    channelDBValues = Remain_Day.objects.values_list('channel').distinct()
    channelValues = []
    for channel in channelDBValues:
        channelValues.append(channel[0])
    GameNameDBValues = Game_Version_Config.objects.values_list('name').distinct()
    GameNameValues = []
    for GameName in GameNameDBValues:
        GameNameValues.append(GameName[0])
    return render(request, 'retain/retain_day.html', locals())


@login_required()
@permission_required('OpsManage.can_read_remain_day_dime_record', login_url='/noperm/')
def retain_day_dime(request):
    languageDBValues = Remain_Day.objects.values_list('language').distinct()
    languageValues = []
    for language in languageDBValues:
        languageValues.append(language[0])
    countryDBValues = Remain_Day.objects.values_list('country').distinct()
    countryValues = []
    for country in countryDBValues:
        countryValues.append(country[0])
    channelDBValues = Remain_Day.objects.values_list('channel').distinct()
    channelValues = []
    for channel in channelDBValues:
        channelValues.append(channel[0])
    GameNameDBValues = Game_Version_Config.objects.values_list('name').distinct()
    GameNameValues = []
    for GameName in GameNameDBValues:
        GameNameValues.append(GameName[0])
    # 来源
    sourceDBValues = Remain_Day.objects.values_list('source').distinct()
    sourceValues = []
    for source in sourceDBValues:
        sourceValues.append(source[0])
    return render(request, 'retain/retain_day_dime.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_day_record', login_url='/noperm/')
def report_quantity_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '0'

        return render(request, 'report/report_quantity_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_remain_day_record', login_url='/noperm/')
def retain_day_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    now_time_date = datetime.datetime.now()
    if request.method == "POST":
        sources = request.POST.getlist('sources[]')
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        language = request.POST.getlist('languages[]')
        country = request.POST.getlist('countrys[]')
        platform = request.POST.getlist('platforms[]')
        channeltypes = request.POST.getlist('channeltypes[]')
        channels = request.POST.getlist('channels[]')
        sub1_channels = request.POST.get('sub1_channels')
        sub2_channels = request.POST.get('sub2_channels')
        sub3_channels = request.POST.get('sub3_channels')
        project = request.POST.get('items')
        recharge = request.POST.get('recharge') == 'True'
        sg_conf = Game_Version_Config.objects.filter(name=project)
        app_id = None
        if sg_conf.count() == 1:
            app_id = []
            app_id.append(sg_conf[0].ios_appid)
            app_id.append(sg_conf[0].android_appid)
        else:
            print("Get Project Return More Than 1")

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '1989-11-07'

        start_date = datetime.datetime.strptime(start_time, '%Y-%m-%d')
        lDayCnt = (now_time_date - start_date).days + 1

        sql_temp = ''
        if language:
            sql_temp += SelectSqlStrFormat('language', language)
        if country:
            sql_temp += SelectSqlStrFormat('country', country)
        if platform:
            sql_temp += SelectSqlStrFormat('platform', platform)
        if channeltypes:
            sql_temp += SelectSqlStrFormat('channeltype', channeltypes)
        if channels:
            sql_temp += SelectSqlStrFormat('channel', channels)
        if sub1_channels:
            # sql_temp += SelectSqlStrFormat('sub1_channel', sub1_channels)
            sql_temp += ' and sub1_channel like "%{}%" '.format(sub1_channels)
        if sub2_channels:
            # sql_temp += SelectSqlStrFormat('sub2_channel', sub2_channels)
            sql_temp += ' and sub2_channel like "%{}%" '.format(sub2_channels)
        if sub3_channels:
            # sql_temp += SelectSqlStrFormat('sub3_channel', sub3_channels)
            sql_temp += ' and sub3_channel like "%{}%" '.format(sub3_channels)
        if app_id:
            sql_temp += SelectSqlStrFormat('app_id', app_id)
        if sources:
            sql_temp += SelectSqlStrFormat('source', sources)

        selectValue = ['remain_2', 'remain_3', 'remain_4', 'remain_5', 'remain_6', 'remain_7', 'remain_8', 'remain_9', 'remain_10', 'remain_11', 'remain_12', 'remain_13', 'remain_14', 'remain_15', 'remain_16', 'remain_17', 'remain_18', 'remain_19', 'remain_20', 'remain_21', 'remain_22', 'remain_23', 'remain_24', 'remain_25', 'remain_26', 'remain_27', 'remain_28', 'remain_29', 'remain_30', 'remain_35', 'remain_40', 'remain_45', 'remain_50', 'remain_55', 'remain_60', 'remain_65', 'remain_70', 'remain_75', 'remain_80', 'remain_85', 'remain_90']
        sqlSelect = ','.join(selectValue)

        common = ['日期', '新增激活数', '次日', '3日', '4日', '5日', '6日', '7日', '8日', '9日', '10日', '11日', '12日', '13日', '14日',
                  '15日', '16日', '17日', '18日', '19日', '20日', '21日', '22日', '23日', '24日', '25日', '26日', '27日', '28日',
                  '29日', '30日', '35日', '40日', '45日', '50日', '55日', '60日', '65日', '70日', '75日', '80日', '85日', '90日']

        sql = 'SELECT date,new_activate, %s FROM opsmanage_remain_day WHERE date >= "%s" and date <= "%s"' % (sqlSelect, start_time, end_time)
        if recharge:
            common = common[:-12]
            sqlSelectRecharge = ','.join(selectValue[:-12])
            sql = 'SELECT date,new_activate, %s FROM opsmanage_recharge_remain_day WHERE date >= "%s" and date <= "%s"' % (sqlSelectRecharge, start_time, end_time)
        if sql_temp:
            sql += sql_temp
        df = pd.read_sql(sql, connection)

        df_sum = df.groupby('date').agg(['sum']).reset_index()
        df_sum = df_sum.fillna(0)
        if not df_sum.empty:
            i = 2
            while i <= 90:
            # for i in range(2, 31):
                df_sum['remain_' + str(i)] = df_sum['remain_' + str(i)].astype('int')
                df_sum['remain_' + str(i)] = (df_sum['remain_' + str(i)] / df_sum.new_activate).applymap(
                    lambda x: "%.2f" % (x * 100)).astype('str') + '%(' + df_sum['remain_' + str(i)].astype('str') + ')'
                i = i + 5 if i >= 30 else i + 1

        remain_day_tmp = np.array(df_sum).tolist()

        tb_head = []
        emptyVal = []
        for i in range(len(common)):
            emptyVal.append("E")
        tb_head.extend(common[:lDayCnt])

        remain_day_list = []
        dataMaxGrid = len(common)
        maxGrid = dataMaxGrid if lDayCnt > dataMaxGrid else lDayCnt
        for data in remain_day_tmp:
            ddate = datetime.datetime.strptime(data[0], '%Y-%m-%d')
            dayCnt = (now_time_date - ddate).days + 1
            if dayCnt < lDayCnt:
                data[dayCnt:] = emptyVal[dayCnt:]
            remain_day_list.append(data[:lDayCnt])

        return render(request, 'retain/retain_day_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_day_record', login_url='/noperm/')
def retain_sub_channel(request):
    sub1_values = []
    sub2_values = []
    sub3_values = []
    if request.method == "POST":
        channels = request.POST.get('channels[]')
        result = Remain_Day.objects.filter(channel=channels)
        sub1_channel = result.values_list('sub1_channel').distinct()
        for channel in sub1_channel:
            sub1_values.append(channel[0])
        sub2_channel = result.values_list('sub2_channel').distinct()
        for channel in sub2_channel:
            sub2_values.append(channel[0])
        sub3_channel = result.values_list('sub3_channel').distinct()
        for channel in sub3_channel:
            sub3_values.append(channel[0])
    # print(sub1_values)
    # print(sub2_values)
    # print(sub3_values)
    return JsonResponse({'msg': '删除成功', "code": 200, 'data': [sub1_values, sub2_values, sub3_values]})


@login_required()
@permission_required('OpsManage.can_read_remain_day_dime_record', login_url='/noperm/')
def retain_day_dime_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    now_time_date = datetime.datetime.now()
    if request.method == "POST":
        sources = request.POST.getlist('sources[]')
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        language = request.POST.getlist('languages[]')
        country = request.POST.getlist('countrys[]')
        platform = request.POST.getlist('platforms[]')
        channeltypes = request.POST.getlist('channeltypes[]')
        channels = request.POST.getlist('channels[]')
        sub1_channels = request.POST.get('sub1_channels')
        sub2_channels = request.POST.get('sub2_channels')
        sub3_channels = request.POST.get('sub3_channels')
        dimensions = set(request.POST.getlist('dimensions[]'))
        project = request.POST.get('items')
        sg_conf = Game_Version_Config.objects.filter(name=project)
        app_id = None
        if sg_conf.count() == 1:
            app_id = []
            app_id.append(sg_conf[0].ios_appid)
            app_id.append(sg_conf[0].android_appid)
        else:
            print("Get Project Return More Than 1")

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '1913-01-01'

        ddate = datetime.datetime.strptime(start_time, '%Y-%m-%d')
        dayCnt = (now_time_date - ddate).days

        sql_temp = ''
        if language:
            sql_temp += SelectSqlStrFormat('language', language)
        if country:
            sql_temp += SelectSqlStrFormat('country', country)
        if platform:
            sql_temp += SelectSqlStrFormat('platform', platform)
        if channeltypes:
            sql_temp += SelectSqlStrFormat('channeltype', channeltypes)
        if channels:
            sql_temp += SelectSqlStrFormat('channel', channels)
        if sub1_channels:
            # sql_temp += SelectSqlStrFormat('sub1_channel', sub1_channels)
            sql_temp += ' and sub1_channel like "%{}%" '.format(sub1_channels)
        if sub2_channels:
            # sql_temp += SelectSqlStrFormat('sub2_channel', sub2_channels)
            sql_temp += ' and sub2_channel like "%{}%" '.format(sub2_channels)
        if sub3_channels:
            # sql_temp += SelectSqlStrFormat('sub3_channel', sub3_channels)
            sql_temp += ' and sub3_channel like "%{}%" '.format(sub3_channels)
        if app_id:
            sql_temp += SelectSqlStrFormat('app_id', app_id)
        if sources:
            sql_temp += SelectSqlStrFormat('source', sources)

        sql = ''
        sql_base = 'SELECT '
        for di in dimensions:
            sqlStr = di + ','
            sql_base += sqlStr

        selectValue = ['remain_2', 'remain_3', 'remain_4', 'remain_5', 'remain_6', 'remain_7', 'remain_8', 'remain_9',
                       'remain_10', 'remain_11', 'remain_12', 'remain_13', 'remain_14', 'remain_15', 'remain_16',
                       'remain_17', 'remain_18', 'remain_19', 'remain_20', 'remain_21', 'remain_22', 'remain_23',
                       'remain_24', 'remain_25', 'remain_26', 'remain_27', 'remain_28', 'remain_29', 'remain_30',
                       'remain_35', 'remain_40', 'remain_45', 'remain_50', 'remain_55', 'remain_60', 'remain_65',
                       'remain_70', 'remain_75', 'remain_80', 'remain_85', 'remain_90']
        sqlSelect = ','.join(selectValue)

        sql_part = 'date,new_activate, %s FROM opsmanage_remain_day WHERE date >= "%s" and date <= "%s"' % (sqlSelect, start_time, end_time)
        sql_base += sql_part
        if sql_temp:
            sql_base = sql_base + sql_temp
        sql = sql_base
        # show_dict = {}
        # if country_show and country_show != 'country':
        #     dime_list.append('国家')
        #     show_dict['国家'] = 'country,'
        # if channel_show and channel_show != 'channel':
        #     dime_list.insert(int(channel_show)-1,'渠道')
        #     show_dict['渠道'] = 'channel,'
        dime_head = []
        dime_common = ['新增激活数', '次日', '3日', '4日', '5日', '6日', '7日', '8日', '9日', '10日', '11日', '12日', '13日', '14日',
                       '15日', '16日', '17日', '18日', '19日', '20日', '21日', '22日', '23日', '24日', '25日', '26日', '27日', '28日',
                       '29日', '30日', '35日', '40日', '45日', '50日', '55日', '60日', '65日', '70日', '75日', '80日', '85日', '90日']
        dime_head.extend(dimensions)
        dime_head.extend(dime_common[:dayCnt])

        # dime_str = ''
        # dime_group = []
        # for di in dimensions:
        #     dime_str += show_dict.get(di, '')
        #     dime_group.append(show_dict.get(di, ' ')[:-1])
        df = pd.read_sql(sql, connection)

        if not df.index.tolist():
            return render(request, 'retain/retain_day_dime_detail.html', locals())

        df = df.fillna(0)
        date_dimensions = copy.deepcopy(dimensions)
        date_dimensions.add('date')

        df_sum = df.groupby(list(dimensions), as_index=False).agg(['sum']).reset_index()

        if date_dimensions and len(date_dimensions) > 0:
            df_sum_tmp = df.groupby(list(date_dimensions), as_index=False).agg(['sum']).reset_index()
        else:
            df_sum_tmp = df.agg(['sum']).reset_index()

        if not df_sum_tmp.empty:
            # for i in range(2, 31):
            i = 2
            while i <= 90:
                dateSet = set(df_sum_tmp['date'].tolist())
                # activateList = df_sum_tmp[('new_activate','sum')].tolist()
                cur_date_list = []
                for strDate in dateSet:
                    ddate = datetime.datetime.strptime(strDate, '%Y-%m-%d')
                    dayCnt = (now_time_date - ddate).days
                    if i <= dayCnt:
                        cur_date_list.append(strDate)

                head_dimensions = copy.deepcopy(date_dimensions)
                head_dimensions.add('new_activate')
                calc_df = df_sum_tmp[df_sum_tmp['date'].isin(cur_date_list)][list(head_dimensions)]
                df_sum_calc_df = calc_df.groupby(list(dimensions), as_index=False).agg(['sum']).reset_index()
                del df_sum_calc_df['date']
                df_sum_calc_df.rename(columns={'new_activate':'activate_' + str(i)}, inplace = True)
                if not df_sum_calc_df.empty:
                    df_sum = pd.merge(df_sum, df_sum_calc_df, how="left", on=list(dimensions))
                else:
                    df_sum[('activate_' + str(i), 'sum')] = 0
                # print(df_sum)
                i = i + 5 if i >= 30 else i + 1

        del df_sum['date']
        sum_list = df_sum.sum().tolist()
        sum_list[0] = '平均留存'
        if dimensions:
            for i in range(len(dimensions) - 1):
                sum_list[i + 1] = ''
        df_sum.loc[df_sum.shape[0] + 1] = sum_list
        df_sum.fillna(0, inplace=True)
        if not df_sum.empty:
            i = 2
            # for i in range(2, 31):
            while i <= 90:
                df_sum['remain_' + str(i)] = df_sum['remain_' + str(i)].astype(int)
                df_sum['activate_' + str(i)] = df_sum['activate_' + str(i)].astype(int)

                def formatData(remain, activate):
                    remain = remain['sum']
                    activate = activate['sum']
                    pre = 0.0
                    if activate != 0:
                        pre = float(remain) / activate
                    sData = "%.2f" % (pre * 100) + '%(' + str(remain) + '/' + str(activate) + ')'
                    return sData

                df_sum['remain_' + str(i)] = df_sum.apply(lambda x: formatData(x['remain_' + str(i)], x['activate_' + str(i)]), axis = 1)

                i = i + 5 if i >= 30 else i + 1

        remain_day_dime_tmp = np.array(df_sum).tolist()
        remain_day_dime_list = []

        column = len(dime_head)
        for data in remain_day_dime_tmp:
            remain_day_dime_list.append(data[:column])

        return render(request, 'retain/retain_day_dime_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_change_remain_day_record', login_url='/noperm/')
def retain_day_collect(request):
    print("retain_day_collect start")
    cron.remain_day_up()
    print("retain_day_collect end")
    return JsonResponse({'msg': '删除成功', "code": 200, 'data': ""})

@login_required()
@permission_required('OpsManage.can_read_up_online_remain_record', login_url='/noperm/')
def up_online_remain(request):
    languageDBValues = Up_Online_Remain.objects.values_list('language').distinct()
    languageValues = []
    for language in languageDBValues:
        languageValues.append(language[0])
    countryDBValues = Up_Online_Remain.objects.values_list('country').distinct()
    countryValues = []
    for country in countryDBValues:
        countryValues.append(country[0])
    channelDBValues = Up_Online_Remain.objects.values_list('channel').distinct()
    channelValues = []
    for channel in channelDBValues:
        channelValues.append(channel[0])
    # area_idDBValues = Up_Online_Remain.objects.values_list('area_id').distinct()
    # area_idValues = []
    # for area_id in area_idDBValues:
    #     area_idValues.append(area_id[0])
    GameNameDBValues = Game_Version_Config.objects.values_list('name').distinct()
    GameNameValues = []
    for GameName in GameNameDBValues:
        GameNameValues.append(GameName[0])
    return render(request, 'retain/online_remain.html', locals())


@login_required()
@permission_required('OpsManage.can_read_up_online_remain_record', login_url='/noperm/')
def up_online_remain_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    now_time_date = datetime.datetime.now()
    if request.method == "POST":
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        language = request.POST.getlist('languages[]')
        country = request.POST.getlist('countrys[]')
        platform = request.POST.getlist('platforms[]')
        channeltypes = request.POST.getlist('channeltypes[]')
        channels = request.POST.getlist('channels[]')
        # area_id = request.POST.getlist('area_id[]')
        # items = request.POST.getlist('items[]')
        sub1_channels = request.POST.get('sub1_channels')
        sub2_channels = request.POST.get('sub2_channels')
        sub3_channels = request.POST.get('sub3_channels')
        project = request.POST.get('items')

        sg_conf = Game_Version_Config.objects.filter(name=project)
        app_id = None
        if sg_conf.count() == 1:
            app_id = []
            app_id.append(sg_conf[0].ios_appid)
            app_id.append(sg_conf[0].android_appid)
        else:
            print("Get Project Return More Than 1")

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '1989-11-07'

        start_date = datetime.datetime.strptime(start_time, '%Y-%m-%d')
        lDayCnt = (now_time_date - start_date).days + 1

        sql_temp = ''
        if language:
            sql_temp += SelectSqlStrFormat('language', language)
        if country:
            sql_temp += SelectSqlStrFormat('country', country)
        if platform:
            sql_temp += SelectSqlStrFormat('platform', platform)
        if channeltypes:
            sql_temp += SelectSqlStrFormat('channeltype', channeltypes)
        if channels:
            sql_temp += SelectSqlStrFormat('channel', channels)
        # if area_id:
        #     sql_temp += SelectSqlStrFormat('area_id', area_id)
        if sub1_channels:
            # sql_temp += SelectSqlStrFormat('sub1_channel', sub1_channels)
            sql_temp += ' and sub1_channel like "%{}%" '.format(sub1_channels)
        if sub2_channels:
            # sql_temp += SelectSqlStrFormat('sub2_channel', sub2_channels)
            sql_temp += ' and sub2_channel like "%{}%" '.format(sub2_channels)
        if sub3_channels:
            # sql_temp += SelectSqlStrFormat('sub3_channel', sub3_channels)
            sql_temp += ' and sub3_channel like "%{}%" '.format(sub3_channels)
        if app_id:
            sql_temp += SelectSqlStrFormat('app_id', app_id)

        sql = ''
        # if items:
        #     for item in items:
        #         sql_item = 'SELECT r.date,r.new_online,r.remain_2,r.remain_3,r.remain_4,r.remain_5,r.remain_6,r.remain_7,r.remain_8,r.remain_9,r.remain_10,r.remain_11,r.remain_12,r.remain_13,r.remain_14,r.remain_15,r.remain_16,r.remain_17,r.remain_18,r.remain_19,r.remain_20,r.remain_21,r.remain_22,r.remain_23,r.remain_24,r.remain_25,r.remain_26,r.remain_27,r.remain_28,r.remain_29,r.remain_30 FROM opsmanage_up_online_remain as r,opsmanage_game_version_config as g WHERE (r.app_id = g.android_appid or r.app_id = g.ios_appid) and g.app_name = "%s" and date >= "%s" and date <= "%s"' % (
        #             item, start_time, end_time)
        #         if sql_temp:
        #             sql_items = sql_item + sql_temp + ' union '
        #         else:
        #             sql_items = sql_item + ' union '
        #         sql += sql_items
        #     sql = sql[:-6]
        # else:
        selectValue = ['remain_2', 'remain_3', 'remain_4', 'remain_5', 'remain_6', 'remain_7', 'remain_8', 'remain_9',
                       'remain_10', 'remain_11', 'remain_12', 'remain_13', 'remain_14', 'remain_15', 'remain_16',
                       'remain_17', 'remain_18', 'remain_19', 'remain_20', 'remain_21', 'remain_22', 'remain_23',
                       'remain_24', 'remain_25', 'remain_26', 'remain_27', 'remain_28', 'remain_29', 'remain_30',
                       'remain_35', 'remain_40', 'remain_45', 'remain_50', 'remain_55', 'remain_60', 'remain_65',
                       'remain_70', 'remain_75', 'remain_80', 'remain_85', 'remain_90']
        sqlSelect = ','.join(selectValue)

        sql = 'SELECT date,new_online, %s FROM opsmanage_up_online_remain WHERE date >= "%s" and date <= "%s"' % (sqlSelect, start_time, end_time)
        if sql_temp:
            sql += sql_temp
        df = pd.read_sql(sql, connection)
        df_sum = df.groupby('date').agg(['sum']).reset_index()
        df_sum = df_sum.fillna(0)
        #计算留存率
        if not df_sum.empty:
            i = 2
            while i <= 90:
            # for i in range(2, 31):
                df_sum['remain_' + str(i)] = (df_sum['remain_' + str(i)] / df_sum.new_online).applymap(
                    lambda x: "%.2f" % (x * 100)).astype('str') + '%(' + df_sum['remain_' + str(i)].astype('str') + ')'
                i = i + 5 if i >= 30 else i + 1
        online_remain_list_tmp = np.array(df_sum).tolist()

        tb_head = []
        common = ['日期', '新增上线数', '次日', '3日', '4日', '5日', '6日', '7日', '8日', '9日', '10日', '11日', '12日', '13日', '14日',
                  '15日', '16日', '17日', '18日', '19日', '20日', '21日', '22日', '23日', '24日', '25日', '26日', '27日', '28日',
                  '29日', '30日', '35日', '40日', '45日', '50日', '55日', '60日', '65日', '70日', '75日', '80日', '85日', '90日']
        tb_head.extend(common[:lDayCnt])

        online_remain_list = []
        for data in online_remain_list_tmp:
            ddate = datetime.datetime.strptime(data[0], '%Y-%m-%d')
            dayCnt = (now_time_date - ddate).days + 1
            online_remain_list.append(data[:dayCnt])

        return render(request, 'retain/online_remain_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_up_online_remain_dime_record', login_url='/noperm/')
def up_online_remain_dime(request):
    languageDBValues = Up_Online_Remain.objects.values_list('language').distinct()
    languageValues = []
    for language in languageDBValues:
        languageValues.append(language[0])
    countryDBValues = Up_Online_Remain.objects.values_list('country').distinct()
    countryValues = []
    for country in countryDBValues:
        countryValues.append(country[0])
    channelDBValues = Up_Online_Remain.objects.values_list('channel').distinct()
    channelValues = []
    for channel in channelDBValues:
        channelValues.append(channel[0])
    # area_idDBValues = Up_Online_Remain.objects.values_list('area_id').distinct()
    # area_idValues = []
    # for area_id in area_idDBValues:
    #     area_idValues.append(area_id[0])
    GameNameDBValues = Game_Version_Config.objects.values_list('name').distinct()
    GameNameValues = []
    for GameName in GameNameDBValues:
        GameNameValues.append(GameName[0])
    return render(request, 'retain/online_remain_dime.html', locals())


@login_required()
@permission_required('OpsManage.can_read_up_online_remain_dime_record', login_url='/noperm/')
def up_online_remain_dime_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    now_time_date = datetime.datetime.now()
    if request.method == "POST":
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        language = request.POST.getlist('languages[]')
        country = request.POST.getlist('countrys[]')
        platform = request.POST.getlist('platforms[]')
        # items = request.POST.getlist('items[]')
        channeltypes = request.POST.getlist('channeltypes[]')
        channels = request.POST.getlist('channels[]')
        sub1_channels = request.POST.get('sub1_channels')
        sub2_channels = request.POST.get('sub2_channels')
        sub3_channels = request.POST.get('sub3_channels')
        dimensions = set(request.POST.getlist('dimensions[]'))
        project = request.POST.get('items')

        sg_conf = Game_Version_Config.objects.filter(name=project)
        app_id = None
        if sg_conf.count() == 1:
            app_id = []
            app_id.append(sg_conf[0].ios_appid)
            app_id.append(sg_conf[0].android_appid)
        else:
            print("Get Project Return More Than 1")

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '1913-01-01'

        ddate = datetime.datetime.strptime(start_time, '%Y-%m-%d')
        dayCnt = (now_time_date - ddate).days

        sql_temp = ''
        if language:
            sql_temp += SelectSqlStrFormat('language', language)
        if country:
            sql_temp += SelectSqlStrFormat('country', country)
        if platform:
            sql_temp += SelectSqlStrFormat('platform', platform)
        if channeltypes:
            sql_temp += SelectSqlStrFormat('channeltype', channeltypes)
        if channels:
            sql_temp += SelectSqlStrFormat('channel', channels)
        if sub1_channels:
            # sql_temp += SelectSqlStrFormat('sub1_channel', sub1_channels)
            sql_temp += ' and sub1_channel like "%{}%" '.format(sub1_channels)
        if sub2_channels:
            # sql_temp += SelectSqlStrFormat('sub2_channel', sub2_channels)
            sql_temp += ' and sub2_channel like "%{}%" '.format(sub2_channels)
        if sub3_channels:
            # sql_temp += SelectSqlStrFormat('sub3_channel', sub3_channels)
            sql_temp += ' and sub3_channel like "%{}%" '.format(sub3_channels)
        if app_id:
            sql_temp += SelectSqlStrFormat('app_id', app_id)

        sql = ''
        # if items:
        #     for item in items:
        #         sql_base = 'SELECT '
        #         for di in dimensions:
        #             sqlStr = di + ','
        #             sql_base += sqlStr
        #         sql_part = 'new_online,remain_2,remain_3,remain_4,remain_5,remain_6,remain_7,remain_8,remain_9,remain_10,remain_11,remain_12,remain_13,remain_14,remain_15,remain_16,remain_17,remain_18,remain_19,remain_20,remain_21,remain_22,remain_23,remain_24,remain_25,remain_26,remain_27,remain_28,remain_29,remain_30 FROM opsmanage_up_online_remain as r,opsmanage_game_version_config as g WHERE (r.app_id = g.android_appid or r.app_id = g.ios_appid) and g.app_name = "%s" and date >= "%s" and date <= "%s"' % (
        #             item, start_time, end_time)
        #         sql_base += sql_part
        #         if sql_temp:
        #             sql_items = sql_base + sql_temp + ' union '
        #         else:
        #             sql_items = sql_base + ' union '
        #         sql += sql_items
        #     sql = sql[:-6]
        # else:
        selectValue = ['remain_2', 'remain_3', 'remain_4', 'remain_5', 'remain_6', 'remain_7', 'remain_8', 'remain_9',
                       'remain_10', 'remain_11', 'remain_12', 'remain_13', 'remain_14', 'remain_15', 'remain_16',
                       'remain_17', 'remain_18', 'remain_19', 'remain_20', 'remain_21', 'remain_22', 'remain_23',
                       'remain_24', 'remain_25', 'remain_26', 'remain_27', 'remain_28', 'remain_29', 'remain_30',
                       'remain_35', 'remain_40', 'remain_45', 'remain_50', 'remain_55', 'remain_60', 'remain_65',
                       'remain_70', 'remain_75', 'remain_80', 'remain_85', 'remain_90']
        sqlSelect = ','.join(selectValue)

        sql_base = 'SELECT '
        for di in dimensions:
            sqlStr = di + ','
            sql_base += sqlStr
        sql_part = 'date,new_online, %s FROM opsmanage_up_online_remain WHERE date >= "%s" and date <= "%s"' % (sqlSelect, start_time, end_time)
        sql_base += sql_part
        if sql_temp:
            sql_base = sql_base + sql_temp
        sql = sql_base
        # show_dict = {}
        # if country_show and country_show != 'country':
        #     dime_list.append('国家')
        #     show_dict['国家'] = 'country,'
        # if channel_show and channel_show != 'channel':
        #     dime_list.insert(int(channel_show)-1,'渠道')
        #     show_dict['渠道'] = 'channel,'
        dime_head = []
        dime_common = ['新增上线数', '次日', '3日', '4日', '5日', '6日', '7日', '8日', '9日', '10日', '11日', '12日', '13日', '14日',
                       '15日', '16日', '17日', '18日', '19日', '20日', '21日', '22日', '23日', '24日', '25日', '26日', '27日', '28日',
                       '29日', '30日', '35日', '40日', '45日', '50日', '55日', '60日', '65日', '70日', '75日', '80日', '85日', '90日']
        dime_head.extend(dimensions)
        dime_head.extend(dime_common[:dayCnt])

        # dime_str = ''
        # dime_group = []
        # for di in dimensions:
        #     dime_str += show_dict.get(di, '')
        #     dime_group.append(show_dict.get(di, ' ')[:-1])
        df = pd.read_sql(sql, connection)

        if not df.index.tolist():
            return render(request, 'retain/online_remain_dime_detail.html', locals())

        # sum_list = df.sum().tolist()
        # sum_list[0] = '平均留存'
        # if dimensions:
        #     for i in range(len(dimensions) - 1):
        #         sum_list[i + 1] = ''
        # df = df.fillna(0)
        # if dimensions and len(dimensions) > 0:
        #     df_sum = df.groupby(list(dimensions), as_index=False).agg(['sum']).reset_index()
        # else:
        #     df_sum = df.agg(['sum']).reset_index()
        # df_sum.loc[df_sum.shape[0] + 1] = sum_list
        # #计算留存率
        # if not df_sum.empty:
        #     for i in range(2, 31):
        #         df_sum['remain_' + str(i)] = (df_sum['remain_' + str(i)] / df_sum.new_online).applymap(
        #             lambda x: "%.2f" % (x * 100)).astype('str') + '%(' + df_sum['remain_' + str(i)].astype('str') + ')'
        # online_remain_dime_list = np.array(df_sum).tolist()

        df = df.fillna(0)
        date_dimensions = copy.deepcopy(dimensions)
        date_dimensions.add('date')

        df_sum = df.groupby(list(dimensions), as_index=False).agg(['sum']).reset_index()

        if date_dimensions and len(date_dimensions) > 0:
            df_sum_tmp = df.groupby(list(date_dimensions), as_index=False).agg(['sum']).reset_index()
        else:
            df_sum_tmp = df.agg(['sum']).reset_index()

        if not df_sum_tmp.empty:
            i = 2
            # for i in range(2, 31):
            while i <= 90:
                dateSet = set(df_sum_tmp['date'].tolist())
                # activateList = df_sum_tmp[('new_activate','sum')].tolist()
                cur_date_list = []
                for strDate in dateSet:
                    ddate = datetime.datetime.strptime(strDate, '%Y-%m-%d')
                    dayCnt = (now_time_date - ddate).days
                    if i <= dayCnt:
                        cur_date_list.append(strDate)

                head_dimensions = copy.deepcopy(date_dimensions)
                head_dimensions.add('new_online')
                calc_df = df_sum_tmp[df_sum_tmp['date'].isin(cur_date_list)][list(head_dimensions)]
                df_sum_calc_df = calc_df.groupby(list(dimensions), as_index=False).agg(['sum']).reset_index()
                del df_sum_calc_df['date']
                df_sum_calc_df.rename(columns={'new_online': 'online_' + str(i)}, inplace=True)
                if not df_sum_calc_df.empty:
                    df_sum = pd.merge(df_sum, df_sum_calc_df, how="left", on=list(dimensions))
                else:
                    df_sum[('online_' + str(i), 'sum')] = 0
                # print(df_sum)
                i = i + 5 if i >= 30 else i + 1

        del df_sum['date']
        sum_list = df_sum.sum().tolist()
        sum_list[0] = '平均留存'
        if dimensions:
            for i in range(len(dimensions) - 1):
                sum_list[i + 1] = ''
        df_sum.loc[df_sum.shape[0] + 1] = sum_list
        df_sum.fillna(0, inplace=True)
        if not df_sum.empty:
            i = 2
            while i <= 90:
            # for i in range(2, 31):
                df_sum['remain_' + str(i)] = df_sum['remain_' + str(i)].astype(int)
                df_sum['online_' + str(i)] = df_sum['online_' + str(i)].astype(int)

                def formatData(remain, online):
                    remain = remain['sum']
                    online = online['sum']
                    pre = 0.0
                    if online != 0:
                        pre = float(remain) / online
                    sData = "%.2f" % (pre * 100) + '%(' + str(remain) + '/' + str(online) + ')'
                    return sData

                df_sum['remain_' + str(i)] = df_sum.apply(
                    lambda x: formatData(x['remain_' + str(i)], x['online_' + str(i)]), axis=1)

                i = i + 5 if i >= 30 else i + 1

        online_remain_dime_list_tmp = np.array(df_sum).tolist()
        online_remain_dime_list = []

        column = len(dime_head)
        for data in online_remain_dime_list_tmp:
            online_remain_dime_list.append(data[:column])
        return render(request, 'retain/online_remain_dime_detail.html', locals())


def retain_month(request):
    recharge = False
    languageDBValues = Remain_Month.objects.values_list('language').distinct()
    languageValues = []
    for language in languageDBValues:
        languageValues.append(language[0])

    countryDBValues = Remain_Month.objects.values_list('country').distinct()
    countryValues = []
    for country in countryDBValues:
        countryValues.append(country[0])

    channelDBValues = Remain_Month.objects.values_list('channel').distinct()
    channelValues = []
    for channel in channelDBValues:
        channelValues.append(channel[0])

    GameNameDBValues = Game_Version_Config.objects.values_list('name').distinct()
    GameNameValues = []
    for GameName in GameNameDBValues:
        GameNameValues.append(GameName[0])
    # 来源
    sourceDBValues = Remain_Month.objects.values_list('source').distinct()
    sourceValues = []
    for source in sourceDBValues:
        sourceValues.append(source[0])
    return render(request, 'retain/retain_month.html', locals())


@login_required()
@permission_required('OpsManage.can_read_remain_month_record', login_url='/noperm/')
def retain_month_check(request):
    now_time = str(datetime.date.today())[0:7] + '-01'
    if request.method == "POST":
        sources = request.POST.getlist('sources[]')
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        language = request.POST.getlist('languages[]')
        country = request.POST.getlist('countrys[]')
        platform = request.POST.getlist('platforms[]')
        channeltypes = request.POST.getlist('channeltypes[]')
        channels = request.POST.getlist('channels[]')
        sub1_channels = request.POST.get('sub1_channels')
        sub2_channels = request.POST.get('sub2_channels')
        sub3_channels = request.POST.get('sub3_channels')
        project = request.POST.get('items')
        recharge = request.POST.get('recharge') == 'True'
        sg_conf = Game_Version_Config.objects.filter(name=project)
        app_id = None
        if sg_conf.count() == 1:
            app_id = []
            app_id.append(sg_conf[0].ios_appid)
            app_id.append(sg_conf[0].android_appid)
        else:
            print("Get Project Return More Than 1")

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '1989-11-07'

        start_date = datetime.datetime.strptime(start_time, '%Y-%m-%d')
        end_date = datetime.datetime.strptime(now_time, '%Y-%m-%d')
        dateData = relativedelta(end_date, start_date)
        if dateData.years:
            lDayCnt = dateData.years * 12 + dateData.months + 1
        else:
            lDayCnt = dateData.months + 1

        sql_temp = ''
        if language:
            sql_temp += SelectSqlStrFormat('language', language)
        if country:
            sql_temp += SelectSqlStrFormat('country', country)
        if platform:
            sql_temp += SelectSqlStrFormat('platform', platform)
        if channeltypes:
            sql_temp += SelectSqlStrFormat('channeltype', channeltypes)
        if channels:
            sql_temp += SelectSqlStrFormat('channel', channels)
        if sub1_channels:
            # sql_temp += SelectSqlStrFormat('sub1_channel', sub1_channels)
            sql_temp += ' and sub1_channel like "%{}%" '.format(sub1_channels)
        if sub2_channels:
            # sql_temp += SelectSqlStrFormat('sub2_channel', sub2_channels)
            sql_temp += ' and sub2_channel like "%{}%" '.format(sub2_channels)
        if sub3_channels:
            # sql_temp += SelectSqlStrFormat('sub3_channel', sub3_channels)
            sql_temp += ' and sub3_channel like "%{}%" '.format(sub3_channels)
        if app_id:
            sql_temp += SelectSqlStrFormat('app_id', app_id)
        if sources:
            sql_temp += SelectSqlStrFormat('source', sources)

        selectValue = ['remain_2', 'remain_3', 'remain_4', 'remain_5', 'remain_6', 'remain_7', 'remain_8', 'remain_9',
                       'remain_10', 'remain_11', 'remain_12']
        sqlSelect = ','.join(selectValue)

        sql = 'SELECT date,new_activate, %s FROM opsmanage_remain_month WHERE date >= "%s" and date <= "%s"' % (
        sqlSelect, start_time, end_time)
        if sql_temp:
            sql += sql_temp
        df = pd.read_sql(sql, connection)

        df_sum = df.groupby('date').agg(['sum']).reset_index()
        df_sum = df_sum.fillna(0)
        if not df_sum.empty:
            for i in range(2, 13):
                df_sum['remain_' + str(i)] = df_sum['remain_' + str(i)].astype('int')
                df_sum['remain_' + str(i)] = (df_sum['remain_' + str(i)] / df_sum.new_activate).applymap(
                    lambda x: "%.2f" % (x * 100)).astype('str') + '%(' + df_sum['remain_' + str(i)].astype('str') + ')'

        remain_day_tmp = np.array(df_sum).tolist()

        tb_head = []
        common = ['日期', '新增激活数', '次月', '第3月', '第4月', '第5月', '第6月', '第7月', '第8月', '第9月', '第10月', '第11月', '第12月']
        emptyVal = []
        for i in range(len(common)):
            emptyVal.append("E")
        tb_head.extend(common[:lDayCnt])

        remain_month_list = []
        dataMaxGrid = len(common)
        maxGrid = dataMaxGrid if lDayCnt > dataMaxGrid else lDayCnt
        for data in remain_day_tmp:
            ddate = datetime.datetime.strptime(data[0], '%Y-%m-%d')
            dayCnt =relativedelta(ddate, start_date).years * 12 + relativedelta(ddate, start_date).months + 1 if relativedelta(ddate, start_date).years else relativedelta(ddate, start_date).months + 1
            if dayCnt < lDayCnt:
                data[dayCnt:] = emptyVal[dayCnt:]
            remain_month_list.append(data[:lDayCnt])
    return render(request, 'retain/retain_month_detail.html', locals())

