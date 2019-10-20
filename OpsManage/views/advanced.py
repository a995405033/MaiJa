#!/usr/bin/env python
# -*- coding: utf-8 -*-


from OpsManage.models import *
from OpsManage.tasks import *
from OpsManage import cron
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from OpsManage.views.report import update_day_report
import datetime
from dateutil.relativedelta import relativedelta


@login_required()
def advanced_operations(request):
    return render(request, 'aset/advanced_operations.html', locals())


@login_required()
def report_day_delete(request):
    if request.method == "POST":
        try:
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')

            condtions = {'date__gte': start_time, 'date__lte': end_time}
            Report_Day.objects.filter(**condtions).delete()
        except Exception as e:
            print(e)


@login_required()
def report_day_create(request):
    if request.method == "POST":
        try:
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')

            syear,smon,sday = start_time.split('-')
            eyear,emon,eday = end_time.split('-')
            # day
            day_s_date = datetime.datetime(int(syear), int(smon), int(sday))
            day_s_date = day_s_date + datetime.timedelta(days=1)
            day_e_date = datetime.datetime(int(eyear), int(emon), int(eday))
            day_e_date = day_e_date + datetime.timedelta(days=1)
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


@login_required()
def recharge_remain_day_create(request):
    if request.method == "POST":
        try:
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')

            syear, smon, sday = start_time.split('-')
            eyear, emon, eday = end_time.split('-')
            # day
            day_s_date = datetime.datetime(int(syear), int(smon), int(sday))
            day_s_date = day_s_date + datetime.timedelta(days=1)
            day_e_date = datetime.datetime(int(eyear), int(emon), int(eday))
            day_e_date = day_e_date + datetime.timedelta(days=1)
            while day_s_date <= day_e_date:
                today = day_s_date
                yesterday = str(today + datetime.timedelta(days=-1))[:10]
                strToday = str(today)[:10]
                try:
                    cron.recharge_remain_day_data(day_s_date)
                    day_s_date = day_s_date + datetime.timedelta(days=1)
                except Exception as e:
                    print(strToday)
                    print(e)

        except Exception as e:
            print(e)


@login_required()
def recharge_remain_day_clear(request):
    if request.method == "POST":
        try:
            Recharge_Remain_Day.objects.all().delete()
        except Exception as e:
            print(e)


@login_required()
def report_week_delete(request):
    if request.method == "POST":
        try:
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')

            condtions = {'date__gte': start_time, 'date__lte': end_time}
            Report_Week.objects.filter(**condtions).delete()
        except Exception as e:
            print(e)


@login_required()
def report_week_create(request):
    if request.method == "POST":
        try:
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')

            syear, smon, sday = start_time.split('-')
            eyear, emon, eday = end_time.split('-')
            # day
            day_s_date = datetime.datetime(int(syear), int(smon), int(sday))
            day_e_date = datetime.datetime(int(eyear), int(emon), int(eday))
            while day_s_date <= day_e_date:
                if day_s_date.weekday() == 4:
                    #周五
                    today = day_s_date
                    yesterday = str(today + datetime.timedelta(days=-1))[:10]
                    week_start_day = str(today + datetime.timedelta(days=-8))[:10]
                    show_end_date = str(today + datetime.timedelta(days=-1, seconds=-1))[:10]
                    try:
                        cron.report_data(week_start_day, yesterday, today, show_end_date, Report_Week)
                        day_s_date = day_s_date + datetime.timedelta(days=1)
                    except Exception as e:
                        print(week_start_day)
                        print(e)
                day_s_date = day_s_date + datetime.timedelta(days=1)

        except Exception as e:
            print(e)


@login_required()
def report_month_delete(request):
    if request.method == "POST":
        try:
            start_time = request.POST.get('start_time')
            start_time = start_time + '-01'
            condtions = {'date': start_time}
            Report_Month.objects.filter(**condtions).delete()
        except Exception as e:
            print(e)


@login_required()
def report_month_create(request):
    if request.method == "POST":
        try:
            start_time = request.POST.get('start_time')
            syear, smon = start_time.split('-')
            iyear = int(syear)
            imon = int(smon)
            next_monht_year, next_month = (iyear, imon + 1) if imon != 12 else (iyear + 1, 1)

            month_start_date = datetime.datetime(iyear, imon, int(1))
            next_month_start_date = datetime.datetime(next_monht_year, next_month, 1)
            next_month_count_date = datetime.datetime(next_monht_year, next_month, 2)
            month_end_date = str(next_month_start_date + datetime.timedelta(days=(-1)))[:10]
            str_month_start_date = str(month_start_date)[:10]
            str_next_month_start_date = str(next_month_start_date)[:10]
            try:
                cron.report_data(str_month_start_date, str_next_month_start_date, next_month_count_date, month_end_date, Report_Month)
            except Exception as e:
                print((str(month_start_date)))
                print(e)

        except Exception as e:
            print(e)


@login_required()
def remain_day_delete(request):
    if request.method == "POST":
        try:
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')

            condtions = {'date__gte': start_time, 'date__lte': end_time}
            Remain_Day.objects.filter(**condtions).delete()
        except Exception as e:
            print(e)


@login_required()
def remain_day_create(request):
    if request.method == "POST":
        try:
            start_time = request.POST.get('start_time')
            end_time = request.POST.get('end_time')

            syear,smon,sday = start_time.split('-')
            eyear,emon,eday = end_time.split('-')
            # day
            day_s_date = datetime.datetime(int(syear), int(smon), int(sday))
            day_s_date = day_s_date + datetime.timedelta(days=1)
            day_e_date = datetime.datetime(int(eyear), int(emon), int(eday))
            day_e_date = day_e_date + datetime.timedelta(days=1)
            while day_s_date <= day_e_date:
                today = day_s_date
                strToday = str(today)[:10]
                try:
                    cron.remain_day_data(day_s_date)
                    day_s_date = day_s_date + datetime.timedelta(days=1)
                except Exception as e:
                    print(strToday)
                    print(e)

        except Exception as e:
            print(e)


@login_required()
def remain_month_delete(req):
    if req.method == 'POST':
        try:
            start_time = req.POST.get('start_time')[0:7]
            end_time = req.POST.get('end_time')[0:7]
            condtions = {}
            if start_time:
                condtions['date__gte'] = start_time
            if end_time:
                condtions['date__lte'] = end_time
            remainData = Remain_Month.objects.filter(**condtions)
            remainData.delete()
            return True
        except Exception as e:
            print(e)


@login_required()
def remain_month_create(req):
    if req.method == 'POST':
        start_time = req.POST.get('start_time')
        end_time = req.POST.get('end_time')
        try:

            start_date = datetime.datetime.strptime(start_time, '%Y-%m-%d')
            start_date = start_date + relativedelta(months=1)
            end_date = datetime.datetime.strptime(end_time, '%Y-%m-%d')
            end_date = end_date + relativedelta(months=1)

            dateData = relativedelta(end_date, start_date)
            if dateData.years:
                monthNum = dateData.years * 12 + dateData.months + 1
            else:
                monthNum = dateData.months + 1

            i = 1
            while i <= monthNum:
                toMonth = str(start_date - relativedelta(months=1))[0:7]
                try:
                    cron.remain_month_data(start_date)
                    start_date = start_date + relativedelta(months=1)
                    i += 1
                except Exception as e:
                    print(toMonth)
                    print(e)
            return True
        except Exception as e:
            print(e)
