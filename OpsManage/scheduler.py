#!/usr/bin/env python
# -*- coding: utf-8 -*-
from apscheduler.schedulers.background import BackgroundScheduler

from django.db import connections
# from OpsManage.utils.MonitorAlarmMng import Monitor_Alarm_Check
# from OpsManage.views.report import Report_Del_Check
from OpsManage.cron import data_week_info_up, data_month_info_up, remain_day_up, data_real_up, charge_from_game, Channel_assortment, roi_week_up

# https://blog.csdn.net/mydriverc2/article/details/50820915
import logging
logging.basicConfig()

# 每次操作前确认下连接,避免mysql gone away! 
def close_old_connections():
    for conn in connections.all():
        conn.close_if_unusable_or_obsolete()

def bi_mysql_check():
    try:
        close_old_connections()
    except Exception as e:
        print(e)

scheduler = BackgroundScheduler({
    'apscheduler.jobstores.default': {
        'type': 'sqlalchemy',
        'url': 'sqlite:///jobs.sqlite'
    },
    'apscheduler.executors.default': {
        'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
        'max_workers': '30'
    },
    'apscheduler.executors.processpool': {
        'type': 'processpool',
        'max_workers': '30'
    },
    'apscheduler.job_defaults.coalesce': 'false',
    'apscheduler.job_defaults.max_instances': '3',
    'apscheduler.job_defaults.misfire_grace_time': '86400',
})

# 启动程序调用add_job需要指定ID和添加replace_existing=True字段，否则会累计多个定时器
# scheduler.add_job(DatePrint, 'cron',  hour='0-23',minute='2', id='corn1', replace_existing=True)
scheduler.add_job(bi_mysql_check, 'interval', seconds=60, id='BI_Mysql_Check_ID', replace_existing=True)
scheduler.add_job(data_week_info_up, 'cron',  day_of_week='fri',hour='3',minute='6', id='corn2', replace_existing=True)
scheduler.add_job(data_month_info_up, 'cron',  day='2',month='1-12', hour='1',minute='3',id='corn3', replace_existing=True)
scheduler.add_job(remain_day_up, 'cron',  day='1-31', hour='3',minute='5',id='corn4', replace_existing=True)
scheduler.add_job(data_real_up, 'cron',  hour='0-23',minute='10', id='corn5', replace_existing=True)
scheduler.add_job(charge_from_game, 'cron',  hour='0-23',minute='1', id='corn6', replace_existing=True)
# scheduler.add_job(charge_from_game_copy, 'cron',  hour='0-23',minute='1', id='corn7', replace_existing=True)
scheduler.add_job(Channel_assortment, 'cron',  day='1-31', hour='3',minute='1',id='corn7', replace_existing=True)
scheduler.add_job(roi_week_up, 'cron',  day_of_week='mon',hour='3',minute='5', id='corn8', replace_existing=True)

scheduler.start()


