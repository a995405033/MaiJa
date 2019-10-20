#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import numpy as np
import pandas as pd
import json
from django.db import connection
from django.shortcuts import render
from django.http import JsonResponse,HttpResponse,HttpResponseRedirect,HttpResponseServerError
from django.contrib.auth.decorators import login_required
from django.contrib.auth.decorators import permission_required
from OpsManage.models import *
from OpsManage.tasks import *
from OpsManage.cron import *
from OpsManage.utils.common import SelectSqlStrFormat


from dateutil.parser import parse
import datetime
import pytz


@login_required()
def report_day(request):
    return render(request, 'aset/report_day.html', locals())


@login_required()
def report_quantity(request):
    return render(request, 'report/report_quantity.html', locals())


# @login_required()
# @permission_required('OpsManage.can_read_report_day_record', login_url='/noperm/')
# def report_day_check(request):
#     now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
#     if request.method == "POST":
#         start_time = request.POST.get('start_time')
#         end_time = request.POST.get('end_time')
#         f_start_time = request.POST.get('f_start_time')
#         f_end_time = request.POST.get('f_end_time')
#         checkbox = request.POST.get('checkbox', '')
#         if not end_time:
#             end_time = now_time
#         if not start_time:
#             start_time = '0'
#
#         if not f_end_time:
#             f_end_time = now_time
#         if not f_start_time:
#             f_start_time = '0'
#
#         sql = 'SELECT date,new_activate,ad_activate FROM opsmanage_report_day WHERE activate_type = "all" and date >= "%s" and date <= "%s"' % (start_time, end_time)
#         df = pd.read_sql(sql, connection)
#         date = df['date'].tolist()
#         date = json.dumps(date)
#         df['nature_activate'] = df.new_activate - df.ad_activate
#         df['nature_rate'] = df.nature_activate / df.new_activate
#
#         df['ad_cost'] = None
#         df['ad_per_cost'] = df.ad_cost / df.ad_activate
#         #新增设备注册
#         df['new_reg'] = None
#         #注册率
#         df['new_per_cost'] = df.new_reg / df.new_activate
#         # 新增设备上线
#         df['new_online'] = None
#         #上线率
#         df['new_online_rate'] = df.new_online / df.new_reg
#         df['new_online_rate2'] = None
#         df['new_online_rate3'] = None
#         df['new_online_rate4'] = None
#         df['new_online_rate5'] = None
#         df['new_online_rate6'] = None
#         ave_list = ['平均']
#         sum_list = ['总计']
#
#         df_colums = df.columns.tolist()
#         df_colums.remove('date')
#         for col in df_colums:
#             ave_list.append(round(df[col].mean(),2))
#             sum_list.append(round(df[col].sum(),2))
#
#         df['nature_rate'] = df['nature_rate'].map(lambda x: str(round(x * 100, 2)) + '%')
#
#         result = df.fillna(' ')
#         report_day_list = np.array(result).tolist()
#         report_day_list.append(ave_list)
#         report_day_list.append(sum_list)
#
#         new_act = json.dumps(df['new_activate'].tolist())
#         ad_act = json.dumps(df['ad_activate'].tolist())
#         nature_act = json.dumps(df['nature_activate'].tolist())
#
#         df['new_online_rate8'] = None
#         df['new_online_rate9'] = None
#         result = df.fillna(' ')
#         report_showpay_day_list = np.array(result).tolist()
#         report_showpay_day_list.append(ave_list)
#         report_showpay_day_list.append(sum_list)
#
#         if checkbox == 'true':
#             return render(request, 'report/report_showpay_day_detail.html', locals())
#         return render(request, 'report/report_day_detail.html', locals())


@login_required()
def retain_day(request):
    return render(request, 'retain/retain_day.html', locals())


@login_required()
def retain_day_dime(request):
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
@permission_required('OpsManage.can_read_report_day_record', login_url='/noperm/')
def retain_day_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        language = request.POST.get('language')
        country = request.POST.get('country')
        platform = request.POST.get('platform')
        channels = request.POST.getlist('channels[]')

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '0'

        sql_temp = ''
        if language:
            sql_temp += ' and language = "{}" '.format(language)
        if country:
            sql_temp += ' and country = "{}" '.format(country)
        if platform:
            sql_temp += ' and platform = "{}" '.format(platform)
        if channels:
            sql_channel = ''
            for ch in channels:
                sql_channel += ' channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')

        sql = 'SELECT date,new_activate,remain_2,remain_3,remain_4,remain_5,remain_6,remain_7,remain_8,remain_9,remain_10,remain_11,remain_12,remain_13,remain_14,remain_15,remain_16,remain_17,remain_18,remain_19,remain_20 FROM opsmanage_remain_day WHERE date >= "%s" and date <= "%s"' % (
        start_time, end_time)

        if sql_temp:
            sql = sql + sql_temp

        df = pd.read_sql(sql, connection)
        df_sum = df.groupby('date').agg(['sum']).reset_index()
        df_sum = df_sum.fillna(0)
        for i in range(2,21):
            df_sum['remain_' + str(i)] = (df_sum['remain_' + str(i)] / df_sum.new_activate).applymap(lambda x: "%.2f" % (x * 100)).astype('str') + '%(' + df_sum['remain_' + str(i)].astype('str') + ')'
        remain_day_list = np.array(df_sum).tolist()

        return render(request, 'retain/retain_day_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_read_report_day_record', login_url='/noperm/')
def retain_day_dime_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        channels = request.POST.getlist('channels[]')
        countrys = request.POST.getlist('countrys[]')
        country_show = request.POST.get('country_show')
        channel_show = request.POST.get('channel_show')
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        language = request.POST.get('language')
        platform = request.POST.get('platform')

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '0'

        sql_temp = ''
        if language:
            sql_temp += ' and language = "{}" '.format(language)
        if platform:
            sql_temp += ' and platform = "{}" '.format(platform)
        if channels:
            sql_channel = ''
            for ch in channels:
                sql_channel += ' channel = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_channel[:-3] + ')')

        if countrys:
            sql_country = ''
            for ch in countrys:
                sql_country += ' country = "{}" or '.format(ch)
            sql_temp += ' and {} '.format('(' + sql_country[:-3] + ')')

        dime_list = []
        show_dict = {}
        if country_show and country_show != 'country':
            dime_list.append('国家')
            show_dict['国家'] = 'country,'
        if channel_show and channel_show != 'channel':
            dime_list.insert(int(channel_show)-1,'渠道')
            show_dict['渠道'] = 'channel,'
        dime_head = []
        dime_common = ['新增激活数','次日','3日','4日','5日','6日','7日','8日','9日','10日','11日','12日','13日','14日','15日','16日','17日','18日','19日','20日']
        dime_head.extend(dime_list)
        dime_head.extend(dime_common)

        dime_str = ''
        dime_group = []
        for di in dime_list:
            dime_str += show_dict.get(di, '')
            dime_group.append(show_dict.get(di, ' ')[:-1])

        sql = 'SELECT %s new_activate,remain_2,remain_3,remain_4,remain_5,remain_6,remain_7,remain_8,remain_9,remain_10,remain_11,remain_12,remain_13,remain_14,remain_15,remain_16,remain_17,remain_18,remain_19,remain_20 FROM opsmanage_remain_day WHERE date >= "%s" and date <= "%s"' % (dime_str,start_time,end_time)
        if sql_temp:
            sql = sql + sql_temp

        df = pd.read_sql(sql, connection)

        if not df.index.tolist():
            return render(request, 'retain/retain_day_dime_detail.html', locals())

        df = df.fillna(0)
        if dime_group:
            df_sum = df.groupby(dime_group).agg(['sum']).reset_index()
        else:
            dime_head.insert(0,'平均留存')
            df_sum = df.agg(['sum']).reset_index()
            for i in range(2,21):
                df_sum['remain_' + str(i)] = (df_sum['remain_' + str(i)] / df_sum.new_activate).apply(lambda x: "%.2f" % (x * 100)).astype('str') + '%(' + df_sum['remain_' + str(i)].astype('str') + ')'
            remain_day_dime_list = np.array(df_sum).tolist()
            return render(request, 'retain/retain_day_dime_detail.html', locals())
        sum_list = df.sum().tolist()
        sum_list[0] = '平均留存'
        if dime_group:
            for i in range(len(dime_group)-1):
                sum_list[i+1] = ''
        df_sum.loc[df_sum.shape[0] + 1] = sum_list

        for i in range(2,21):
            df_sum['remain_' + str(i)] = (df_sum['remain_' + str(i)] / df_sum.new_activate).applymap(lambda x: "%.2f" % (x * 100)).astype('str') + '%(' + df_sum['remain_' + str(i)].astype('str') + ')'
        remain_day_dime_list = np.array(df_sum).tolist()

        return render(request, 'retain/retain_day_dime_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_change_ad_cost_conf_record', login_url='/noperm/')
def ad_config_upload(request):
    if request.method == "POST":
        try:
            ll = []
            ad_file = request.FILES.get('ad_file')
            if ad_file:
                df = pd.read_csv(ad_file)
                df_list = np.array(df).tolist()
                ad_config_list = []
                for list in df_list:
                    sg_conf = Game_Version_Config.objects.filter(name=list[1])
                    AppId = sg_conf[0].android_appid if list[2] == 'android' else sg_conf[0].ios_appid
                    ad_config_list.append(Ad_Cost_Conf(date=list[0], project=list[1], app_id=AppId, platform=list[2],language=list[3],channel=list[4],ad_cost=list[5], source=list[6]))
                Ad_Cost_Conf.objects.bulk_create(ad_config_list)
                AdCostDayInput(ReportData=Report_Day, Ad_Cost_Conf='opsmanage_ad_cost_conf')
                AdCostInput(ReportData=Report_Week, Ad_Cost_Conf='opsmanage_ad_cost_conf', DBName='opsmanage_report_week')
                AdCostInput(ReportData=Report_Month, Ad_Cost_Conf='opsmanage_ad_cost_conf', DBName='opsmanage_report_month')

                operation_log(str(request.user), '广告配置', '广告花费文件导入', '0')
        except Exception as e:
            print(e)
            return render(request, 'noperm.html', locals())
        return HttpResponseRedirect('/#/ad_config/')


def AdCostDayInput(ReportData, Ad_Cost_Conf):
    try:
        Sql = 'select date, app_id, platform, language, source, ad_cost from {}'.format(Ad_Cost_Conf)
        dfAdCostConf = pd.read_sql(Sql, connection)
        dfAdCostConf['platform'] = dfAdCostConf['platform'].replace('ios', 'IOS').replace('android', 'Android')
        dfAdCostConf['language'] = dfAdCostConf['language'].replace('en', 'en-us')
        AdCostConf = np.array(dfAdCostConf).tolist()
        for i in AdCostConf:
            try:
                keyargs = dict(list(zip(['date', 'app_id', 'platform', 'language', 'source', 'ad_cost'], i)))
                ReportData.objects.filter(**keyargs).update(ad_cost=keyargs['ad_cost'])
            except Exception as e:
                print(e)
                continue
    except Exception as e:
        print(e)


def AdCostInput(ReportData, Ad_Cost_Conf, DBName):
    try:
        Sql = 'select date, end_date, app_id, platform, language, source  from {}'.format(DBName)
        Df = pd.read_sql(Sql, connection)
        List = np.array(Df).tolist()
        for i in List:
            try:
                Arg = dict(list(zip(['date', 'end_date', 'app_id', 'platform', 'language', 'source'], i)))
                KeyArg = Arg.copy()
                Arg['platform'] = Arg['platform'].replace('IOS', 'ios').replace('Android', 'android')
                Arg['language'] = Arg['language'].replace('en-us', 'en')
                SqlCost = 'select (case when sum(ad_cost) is null then 0 else sum(ad_cost) end) as sum from {} where date>="{}" and date<="{}" and app_id="{}" and platform ="{}" and language="{}" and source="{}"'.format(Ad_Cost_Conf, Arg['date'], Arg['end_date'], Arg['app_id'], Arg['platform'], Arg['language'], Arg['source'])
                dfAdCostConf = pd.read_sql(SqlCost, connection)
                if float(dfAdCostConf['sum']) > 0:
                    ReportData.objects.filter(**KeyArg).update(ad_cost=float(dfAdCostConf['sum']))
            except Exception as e:
                print(e)
                continue
    except Exception as e:
        print(e)


@login_required()
@permission_required('OpsManage.can_read_ad_cost_conf_record', login_url='/noperm/')
def ad_config(request):
    sql_client_country = "SELECT country_code FROM opsmanage_push_appsflyer GROUP BY country_code"
    df_client_country = pd.read_sql(sql_client_country, connection)
    country_list = df_client_country.country_code.tolist()
    sql_client_language = "SELECT language FROM opsmanage_push_appsflyer GROUP BY language"
    df_client_language = pd.read_sql(sql_client_language, connection)
    language_list = df_client_language.language.tolist()
    sql_client_channel = "SELECT channel FROM opsmanage_push_appsflyer GROUP BY channel"
    df_client_channel = pd.read_sql(sql_client_channel, connection)
    ad_channel_list = df_client_channel.channel.tolist()
    projectDBValues = Ad_Cost_Conf.objects.values_list('project').distinct()
    projectValues = []
    for project in projectDBValues:
        projectValues.append(project[0])
    return render(request, 'aset/ad_config.html', locals())


@login_required()
@permission_required('OpsManage.can_read_ad_channel_record', login_url='/noperm/')
def ad_channel(request):
    ad_channel_list = Ad_Channel.objects.filter()
    return render(request, 'aset/ad_channel.html', locals())

@login_required()
@permission_required('OpsManage.can_read_appsflyer_data_extraction_record', login_url='/noperm/')
def appsflyer_data(request):
    return render(request, 'aset/appsflyer_data.html', locals())

@login_required()
@permission_required('OpsManage.can_read_appsflyer_data_extraction_record', login_url='/noperm/')
def appsflyer_data_check(request):
    if request.method == "POST":
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        start_time_stamp = time.mktime(time.strptime(start_time, "%Y-%m-%d"))
        end_time_stamp = time.mktime(time.strptime(end_time, "%Y-%m-%d"))
        if start_time_stamp < end_time_stamp:
            days = (end_time_stamp - start_time_stamp) / 86400
            for day in range(int(days)):
                checkData = datetime.date.fromtimestamp(start_time_stamp) + datetime.timedelta(days=day + 1)
                data_channel_analysis_day_up(checkData)

    return render(request, 'aset/appsflyer_data.html', locals())

@login_required()
@permission_required('OpsManage.can_read_ad_cost_conf_record', login_url='/noperm/')
def roi_check(request):
    if request.method == "POST":
        # GetChargeDataFromGame()
        roi_start_time = request.POST.get('roi_start_time')
        roi_start_time = datetime.datetime.strptime(roi_start_time, "%Y-%m-%d")
        roi_offset = request.POST.get('roi_offset')
        if roi_offset == '1':
            AddWeekVal = 0
            while roi_start_time + datetime.timedelta(days=7 * AddWeekVal) <= datetime.datetime.now():
                print(str(roi_start_time + datetime.timedelta(days=7 * AddWeekVal)))
                cal_roi_week_up(roi_start_time + datetime.timedelta(days=7 * AddWeekVal))
                AddWeekVal += 1
        elif roi_offset == '2':
            AddMonthVal = 0
            while roi_start_time +  relativedelta(months=AddMonthVal) <= datetime.datetime.now():
                print(str(roi_start_time + relativedelta(months=AddMonthVal)))
                cal_roi_month_up(roi_start_time + relativedelta(months=AddMonthVal))
                AddMonthVal += 1
        elif roi_offset == '3':
            AddDayVal = 0
            while roi_start_time + relativedelta(days=AddDayVal) <= datetime.datetime.now():
                print(str(roi_start_time + relativedelta(days=AddDayVal)))
                cal_roi_day_up(roi_start_time + relativedelta(days=AddDayVal))
                AddDayVal += 1
        elif roi_offset == '4':
            AddWeekVal = 0
            while roi_start_time + relativedelta(days=7 * AddWeekVal) <= datetime.datetime.now():
                print(str(roi_start_time + relativedelta(days=7 * AddWeekVal)))
                charge_week_lost_cal(roi_start_time + relativedelta(days=7 * AddWeekVal))
                AddWeekVal += 1
        elif roi_offset == '5':
            AddmonthVal = 0
            while roi_start_time + relativedelta(months=AddmonthVal) <= datetime.datetime.now():
                print(str(roi_start_time + relativedelta(months=AddmonthVal)))
                charge_month_lost_cal(roi_start_time + relativedelta(months=AddmonthVal))
                AddmonthVal += 1
            # cal_roi_month_up(roi_start_time)
    return render(request, 'aset/appsflyer_data.html', locals())

@login_required()
@permission_required('OpsManage.can_read_appsflyer_data_extraction_record', login_url='/noperm/')
def charge_data_check(request):
    if request.method == "POST":
        # GetChargeDataFromGame()
        start_time = request.POST.get('charge_start_time')
        offset = request.POST.get('offset')
        calChargeData(start_time, offset)
        # channel_assortment_cal(start_time[0:10])

    return render(request, 'aset/appsflyer_data.html', locals())

@login_required()
@permission_required('OpsManage.can_read_appsflyer_data_extraction_record', login_url='/noperm/')
def channel_data_check(request):
    if request.method == "POST":
        # GetChargeDataFromGame()
        start_time = request.POST.get('channel_start_time')
        channel_assortment_cal(start_time)

    return render(request, 'aset/appsflyer_data.html', locals())



@login_required()
@permission_required('OpsManage.can_read_isolate_channel_record', login_url='/noperm/')
def isolate_channel(request):
    isolate_channel_list = Isolate_Channel.objects.filter()
    ad_Channel_list = Ad_Channel.objects.filter()
    return render(request, 'aset/isolate_channel.html', locals())


@login_required()
@permission_required('OpsManage.can_read_area_config_record', login_url='/noperm/')
def area_config(request):
    area_config_list = Area_Config.objects.filter()
    return render(request, 'aset/area_config.html', locals())


@login_required()
@permission_required('OpsManage.can_read_language_version_config_record', login_url='/noperm/')
def language_config(request):
    # 修改
    language_config_list = Language_Version_Config.objects.filter()
    return render(request, 'aset/language_config.html', locals())


@login_required()
@permission_required('OpsManage.can_read_game_version_config_record', login_url='/noperm/')
def game_config(request):
    game_config_list = Game_Version_Config.objects.filter()
    return render(request, 'aset/game_config.html', locals())


@login_required()
@permission_required('OpsManage.can_read_ad_cost_conf_record', login_url='/noperm/')
def ad_config_check(request):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if request.method == "POST":
        channels = request.POST.getlist('channels[]')
        countrys = request.POST.getlist('countrys[]')
        languages = request.POST.getlist('languages[]')
        start_time = request.POST.get('start_time')
        end_time = request.POST.get('end_time')
        project = request.POST.getlist('project[]')
        platform = request.POST.getlist('platform[]')

        if not end_time:
            end_time = now_time
        if not start_time:
            start_time = '0'

        sql_temp = ''
        if project:
            sql_temp += SelectSqlStrFormat('project', project)
        if platform:
            sql_temp += SelectSqlStrFormat('platform', platform)
        if languages:
            sql_temp += SelectSqlStrFormat('language', languages)
        if countrys:
            sql_temp += SelectSqlStrFormat('country', countrys)
        if platform:
            sql_temp += SelectSqlStrFormat('platform', platform)
        if channels:
            sql_temp += SelectSqlStrFormat('channel', channels)

        sql = 'SELECT * FROM opsmanage_ad_cost_conf WHERE date >= "%s" and date <= "%s"' % (start_time,end_time)
        if sql_temp:
            sql = sql + sql_temp
        ad_config_list = Ad_Cost_Conf.objects.raw(sql + ' ORDER BY date ASC')

        return render(request, 'aset/ad_config_detail.html', locals())


@login_required()
@permission_required('OpsManage.can_change_ad_cost_conf_record', login_url='/noperm/')
def ad_config_edit(request):
    if request.method == "POST":
        id = request.POST.get('ad_id')
        operation_log(str(request.user), '广告配置', '更改广告花费 ID:' + id, '0')
        try:
            Ad_Cost_Conf.objects.filter(id=id).update(
                date=request.POST.get('date_b'),
                project=request.POST.get('project_b'),
                platform=request.POST.get('platform_b'),
                country=request.POST.get('country_b'),
                language=request.POST.get('language_b'),
                channel=request.POST.get('channel_b'),
                cpi=request.POST.get('cpi_b'),
                ad_cost=request.POST.get('ad_cost_b'),
            )
        except Exception as e:
            pass
        return HttpResponseRedirect('/ad_update_list/' + id + '/')


@login_required()
@permission_required('OpsManage.can_change_ad_cost_conf_record', login_url='/noperm/')
def ad_config_mod(request, cid):
    if request.method == "POST":
        id = request.POST.get('id')
        value = request.POST.get('value')
        column = int(request.POST.get('column'))
        operation_log(str(request.user), '广告配置', '更改广告花费 ID:' + cid, '0')
        if column == 8:
            try:
                Ad_Cost_Conf.objects.filter(id=id).update(ad_cost=value)
            except Exception as e:
                return HttpResponse('')
        elif column == 7:
            try:
                Ad_Cost_Conf.objects.filter(id=id).update(cpi=value)
            except Exception as e:
                return HttpResponse('')
        else:
            return HttpResponse('')
        return HttpResponse(value)
    if request.method == "DELETE":
        try:
            adconf = Ad_Cost_Conf.objects.select_related().get(id=cid)
        except Exception as e:
            return JsonResponse({'msg': '删除失败：配置不存在 ' + str(e), "code": 500, 'data': []})
        try:
            adconf.delete()
            operation_log(str(request.user),'广告配置','删除广告花费 ID:'+cid, '0')
        except Exception as e:
            return JsonResponse({'msg': '删除失败：' + str(e), "code": 500, 'data': []})
        return JsonResponse({'msg': '删除成功', "code": 200, 'data': []})


@login_required()
@permission_required('OpsManage.can_change_isolate_channel_record', login_url='/noperm/')
def isolate_channel_mod(request, cid):
    if request.method == "DELETE":
        try:
            adconf = Isolate_Channel.objects.select_related().get(id=cid)

        except Exception as e:
            return JsonResponse({'msg': '删除失败：配置不存在 ' + str(e), "code": 500, 'data': []})
        try:
            adconf.delete()
            operation_log(str(request.user),'广告配置','删除渠道提取规则 ID:'+cid, '0')
            adchannel = Ad_Channel.objects.select_related().get(channel=adconf.newname)
            adchannel.delete()
        except Exception as e:
            return JsonResponse({'msg': '删除失败：' + str(e), "code": 500, 'data': []})
        return JsonResponse({'msg': '删除成功', "code": 200, 'data': []})


@login_required()
@permission_required('OpsManage.can_change_area_config_record', login_url='/noperm/')
def area_config_mod(request, cid):
    if request.method == "DELETE":
        try:
            adconf = Area_Config.objects.select_related().get(id=cid)
        except Exception as e:
            return JsonResponse({'msg': '删除失败：配置不存在 ' + str(e), "code": 500, 'data': []})
        try:
            adconf.delete()
            operation_log(str(request.user),'游戏配置','删除区域配置 ID:'+cid, '0')
        except Exception as e:
            return JsonResponse({'msg': '删除失败：' + str(e), "code": 500, 'data': []})
        return JsonResponse({'msg': '删除成功', "code": 200, 'data': []})


@login_required()
@permission_required('OpsManage.can_change_language_version_config_record', login_url='/noperm/')
def language_config_mod(request, cid):
    if request.method == "DELETE":
        try:
            adconf = Language_Version_Config.objects.select_related().get(id=cid)
        except Exception as e:
            return JsonResponse({'msg': '删除失败：配置不存在 ' + str(e), "code": 500, 'data': []})
        try:
            adconf.delete()
            operation_log(str(request.user),'游戏配置','删除语言版本配置 ID:'+cid, '0')
        except Exception as e:
            return JsonResponse({'msg': '删除失败：' + str(e), "code": 500, 'data': []})
        return JsonResponse({'msg': '删除成功', "code": 200, 'data': []})


@login_required()
@permission_required('OpsManage.can_change_game_version_config_record', login_url='/noperm/')
def game_config_mod(request, cid):
    if request.method == "DELETE":
        try:
            adconf = Game_Version_Config.objects.select_related().get(id=cid)
        except Exception as e:
            return JsonResponse({'msg': '删除失败：配置不存在 ' + str(e), "code": 500, 'data': []})
        try:
            adconf.delete()
            operation_log(str(request.user),'游戏信息','删除游戏信息配置 ID:'+cid, '0')
        except Exception as e:
            return JsonResponse({'msg': '删除失败：' + str(e), "code": 500, 'data': []})
        return JsonResponse({'msg': '删除成功', "code": 200, 'data': []})


@login_required()
@permission_required('OpsManage.can_change_ad_cost_conf_record', login_url='/noperm/')
def ad_cost_add(request):
    if request.method == "POST":
        date = request.POST.get('date_a')
        end_date = request.POST.get('date_b')
        project = request.POST.get('project_a')
        platforms = request.POST.getlist('platform_a[]')
        countrys = request.POST.getlist('country_a[]')
        languages = request.POST.getlist('language_a[]')
        channels = request.POST.getlist('channel_a[]')
        cpi = request.POST.get('cpi_a', 0)
        ad_cost = request.POST.get('ad_cost_a', 0)
        if date == "" or end_date == "" or project == "" or len(platforms) == 0 or len(countrys) == 0 or len(languages) == 0 or len(channels) == 0:
            return JsonResponse({'msg': '添加失败：', "code": 500, 'data': []})
        if end_date < date:
            return JsonResponse({'msg': '添加失败：', "code": 500, 'data': []})
        if cpi == "" and ad_cost == "":
            return JsonResponse({'msg': '添加失败：', "code": 500, 'data': []})
        if cpi == "":
            cpi = 0
        if ad_cost == "":
            ad_cost = 0
        sg_conf = Game_Version_Config.objects.filter(name=project)
        if sg_conf.count() != 1:
            return
        try:
            start_date = datetime.datetime.strptime(date, "%Y-%m-%d")
            over_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
            day = start_date
            while day <= over_date:
                for platform in platforms:
                    app_id = sg_conf[0].ios_appid if platform == 'IOS' else sg_conf[0].android_appid
                    for country in countrys:
                        for language in languages:
                            for channel in channels:
                                obj = Ad_Cost_Conf.objects.create(
                                    date=str(day)[:10],
                                    project=project,
                                    app_id=app_id,
                                    platform=platform,
                                    country=country,
                                    language=language,
                                    channel=channel,
                                    cpi=cpi,
                                    ad_cost=ad_cost,
                                )
                                cid = str(obj.id)
                                operation_log(str(request.user), '广告配置', '添加广告花费 ID:' + cid, '0')
                day = day + datetime.timedelta(days=1)
        except Exception as e:
            return JsonResponse({'msg': '添加失败：' + str(e), "code": 500, 'data': []})
        return JsonResponse({'msg': '添加成功', "code": 200, 'data': []})

@login_required()
@permission_required('OpsManage.can_change_isolate_channel_record', login_url='/noperm/')
def isolate_channel_add(request):
    if request.method == "POST":
        try:
            Isolate_Channel.objects.create(
                prename=request.POST.get('channel_a'),
                rule=request.POST.get('rule_a'),
                newname=request.POST.get('n_channel_a'),
                )

            adchannel = Ad_Channel.objects.select_related().get(channel=request.POST.get('channel_a'))
            Ad_Channel.objects.create(
                channel=request.POST.get('n_channel_a'),
                type=adchannel.type

            )
            operation_log(str(request.user), '广告配置', '添加渠道提取规则', '0')
        except Exception as e:
            return JsonResponse({'msg': '添加失败：' + str(e), "code": 500, 'data': []})
        return JsonResponse({'msg': '添加成功', "code": 200, 'data': []})


@login_required()
@permission_required('OpsManage.can_change_area_config_record', login_url='/noperm/')
def area_config_add(request):
    if request.method == "POST":
        try:
            Area_Config.objects.create(
                name=request.POST.get('name'),
                countrys='/'.join(request.POST.getlist('countrys[]'))
                )
            operation_log(str(request.user), '游戏配置', '添加区域配置', '0')
        except Exception as e:
            return JsonResponse({'msg': '添加失败：' + str(e), "code": 500, 'data': []})
        return JsonResponse({'msg': '添加成功', "code": 200, 'data': []})


@login_required()
@permission_required('OpsManage.can_change_language_version_config_record', login_url='/noperm/')
def language_config_add(request):
    if request.method == "POST":
        try:
            language_a = request.POST.get('language_a')
            if Language_Version_Config.objects.filter(language=language_a).count() > 0:
                return JsonResponse({'msg': '添加失败: 已添加语言', "code": 500, 'data': []})
            Language_Version_Config.objects.create(
                language=language_a,
                name=request.POST.get('name_a')
                )
            operation_log(str(request.user), '游戏配置', '添加语言版本配置', '0')
        except Exception as e:
            return JsonResponse({'msg': '添加失败：' + str(e), "code": 500, 'data': []})
        return JsonResponse({'msg': '添加成功', "code": 200, 'data': []})


@login_required()
@permission_required('OpsManage.can_change_game_version_config_record', login_url='/noperm/')
def game_config_add(request):
    if request.method == "POST":
        try:
            Game_Version_Config.objects.create(
                name=request.POST.get('name_ga'),
                app_name=request.POST.get('app_name_ga'),
                statistics=int(request.POST.get('statistics_ga')),
                zone=request.POST.get('zone_ga'),
                profit=int(request.POST.get('profit_ga')),
                android_flag=request.POST.get('android_flag'),
                android_appid = request.POST.get('android_appid'),
                ios_flag=request.POST.get('ios_flag'),
                ios_appid=request.POST.get('ios_appid'),
                game_host=request.POST.get('game_host'),
                game_user=request.POST.get('game_user'),
                game_pwd=request.POST.get('game_pwd'),
                game_dbname=request.POST.get('game_dbname')
                )
            operation_log(str(request.user), '游戏信息', '添加游戏信息', '0')
        except Exception as e:
            return HttpResponseServerError({'msg': '添加失败', "status": 500, 'data': []})
        return JsonResponse({'msg': '添加成功', "code": 200, 'data': []})

@login_required()
@permission_required('OpsManage.can_read_game_version_config_record',login_url='/noperm/')
def game_detail(request, cid):
    try:
        game_config = Game_Version_Config.objects.filter(id = int(cid))[0]
    except Exception as e:
        print(e)
    return render(request, 'aset/game_detail.html', locals())

@login_required()
@permission_required('OpsManage.can_change_game_version_config_record',login_url='/noperm/')
def game_detail_change(request, cid):
    try:
        game_config = Game_Version_Config.objects.filter(id = int(cid))[0]
    except Exception as e:
        print(e)
    return render(request, 'aset/game_detail_change.html', locals())


@login_required()
@permission_required('OpsManage.can_change_game_version_config_record',login_url='/noperm/')
def detail_change(request):
    if request.method == "POST":
        name_id = request.POST.get('name_id')
        name_ga = request.POST.get('name_ga')
        app_name_ga = request.POST.get('app_name_ga')
        zone_ga = request.POST.get('zone_ga')
        profit_ga = request.POST.get('profit_ga')
        statistics_ga = request.POST.get('statistics_ga')
        ios_flag = request.POST.get('ios_flag')
        ios_appid = request.POST.get('ios_appid')
        android_flag = request.POST.get('android_flag')
        android_appid = request.POST.get('android_appid')
        game_host = request.POST.get('game_host')
        game_user = request.POST.get('game_user')
        game_pwd = request.POST.get('game_pwd')
        game_dbname = request.POST.get('game_dbname')
        try:
             Game_Version_Config.objects.filter(id = int(name_id)).update(name=name_ga, app_name=app_name_ga,zone=zone_ga,
              profit=profit_ga, statistics = statistics_ga, ios_flag= ios_flag,ios_appid = ios_appid, android_flag= android_flag,
             android_appid = android_appid, game_host= game_host, game_user = game_user, game_pwd= game_pwd,game_dbname= game_dbname )
        except Exception as e:
            print(e)
    game_config_list = Game_Version_Config.objects.filter()
    return render(request, 'aset/game_config.html', locals())


@login_required()
@permission_required('OpsManage.can_change_ad_cost_conf_record', login_url='/noperm/')
def ad_update_list(request,cid):
    try:
        adconf = Ad_Cost_Conf.objects.select_related().get(id=cid)
    except Exception as e:
        return JsonResponse({'msg': '编辑失败', "code": 500, 'data': []})
    return render(request, 'aset/ad_update_list.html', locals())
