#!/usr/bin/env python  
# _#_ coding:utf-8 _*_
import time
import json
import urllib.request, urllib.error, urllib.parse
import langid
from celery import task
from OpsManage.models import *
from django.contrib.auth.models import User
'''
Django 版本大于等于1.7的时候，需要加上下面两句
import django
django.setup()
否则会抛出错误 django.core.exceptions.AppRegistryNotReady: Models aren't loaded yet.
'''
 
import django
if django.VERSION >= (1, 7):#自动判断版本
    django.setup()
    

@task
def operation_log(user,name,content,type,id=None):
    try:
        logs = Operation_Log.objects.create(
                                    oper_user=user,
                                    oper_name = name,
                                    oper_content = content,
                                    oper_type = type
                                    )
        return logs.id
    except Exception as e:
        print(e)
        return False



@task
def send_gift_activity(cid,start_time):
    start_time = time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M"))
    now_time = time.mktime(time.localtime())
    time_stamp = start_time - now_time
    if time_stamp > 0:
        time.sleep(time_stamp)
    try:
        gift_obj = Gift_Activity.objects.get(id=cid)
        n_start_time = time.mktime(time.strptime(gift_obj.start_time, "%Y-%m-%d %H:%M"))
        n_end_time = time.mktime(time.strptime(gift_obj.end_time, "%Y-%m-%d %H:%M"))
        send_url = 'http://%s:9998/broadcastgift?id=%s&operate=%s&giftID=11&startTime=%s&endTime=%s' % \
                   (gift_obj.master_ip, gift_obj.activity_id,"1", gift_obj.gift_id, n_start_time, n_end_time)
    except Exception as e:
        return False
    response = ''
    retry = 3
    while retry:
        retry -= 1
        try:
            response = urllib.request.urlopen(send_url)
        except Exception as e:
            print(e)
        if response:
            data = response.read()
            resp = json.loads(data)
            if resp['ret']:
                Gift_Activity.objects.filter(id=cid).update(
                    status=3,
                    resp_desc=resp['desc']
                )
                return True
            else:
                Gift_Activity.objects.filter(id=cid).update(
                    status=2,
                    err_desc=resp['desc']
                )
                return False
        else:
            time.sleep(60)
    return False

@task
def send_game_activity(cid,start_time):
    start_time = time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M"))
    now_time = time.mktime(time.localtime())
    time_stamp = start_time - now_time
    if time_stamp > 0:
        time.sleep(time_stamp)
    try:
        game_obj = Game_Activity.objects.get(id=cid)
        n_start_time = time.mktime(time.strptime(game_obj.start_time, "%Y-%m-%d %H:%M"))
        n_end_time = time.mktime(time.strptime(game_obj.end_time, "%Y-%m-%d %H:%M"))
        send_url = 'http://%s:9998/broadcastgift?id=%s&operate=%s&giftID=11&startTime=%s&endTime=%s' % \
                   (game_obj.master_ip, game_obj.activity_id,"1", game_obj.gift_id, n_start_time, n_end_time)
    except Exception as e:
        return False
    response = ''
    retry = 3
    while retry:
        retry -= 1
        try:
            response = urllib.request.urlopen(send_url)
        except Exception as e:
            print(e)
        if response:
            data = response.read()
            resp = json.loads(data)
            if resp['ret']:
                Game_Activity.objects.filter(id=cid).update(
                    status=3,
                    resp_desc=resp['desc'],
                    open_status = 1
                )
                game_obj.activity_time.time += 1
                return True
            else:
                Game_Activity.objects.filter(id=cid).update(
                    status=2,
                    err_desc=resp['desc']
                )
                return False
        else:
            time.sleep(60)
    return False


@task
def start_gift_activity(cid, start_time):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if now_time < start_time:
        try:
            Gift_Activity.objects.filter(id=cid).update(
                status=1
            )
            return True
        except Exception as e:
            return False
    else:
        try:
            gift_obj = Gift_Activity.objects.get(id=cid)
            n_start_time = time.mktime(time.strptime(gift_obj.start_time, "%Y-%m-%d %H:%M"))
            n_end_time = time.mktime(time.strptime(gift_obj.end_time, "%Y-%m-%d %H:%M"))
            send_url = 'http://%s:9998/broadcastgift?id=%s&operate=%s&giftID=11&startTime=%s&endTime=%s' % \
                       (gift_obj.master_ip, gift_obj.activity_id, "1", gift_obj.gift_id, n_start_time, n_end_time)
        except Exception as e:
            return False
        response = ''
        retry = 3
        while retry:
            retry -= 1
            try:
                response = urllib.request.urlopen(send_url)
            except Exception as e:
                print(e)
            if response:
                data = response.read()
                resp = json.loads(data)
                if resp['ret']:
                    Gift_Activity.objects.filter(id=cid).update(
                        status=3,
                        resp_desc=resp['desc']
                    )
                    return True
                else:
                    Gift_Activity.objects.filter(id=cid).update(
                        status=2,
                        err_desc=resp['desc']
                    )
                    return False
            else:
                time.sleep(60)
    return False


@task
def start_game_activity(cid, start_time):
    now_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if now_time < start_time:
        try:
            Game_Activity.objects.filter(id=cid).update(
                status=1
            )
            return True
        except Exception as e:
            return False
    else:
        try:
            game_obj = Game_Activity.objects.get(id=cid)
            n_start_time = time.mktime(time.strptime(game_obj.start_time, "%Y-%m-%d %H:%M"))
            n_end_time = time.mktime(time.strptime(game_obj.end_time, "%Y-%m-%d %H:%M"))
            send_url = 'http://%s:9998/broadcastgift?id=%s&operate=%s&giftID=11&startTime=%s&endTime=%s' % \
                       (game_obj.master_ip, game_obj.activity_id, "1", game_obj.game_act_id, n_start_time, n_end_time)
        except Exception as e:
            return False
        response = ''
        retry = 3
        while retry:
            retry -= 1
            try:
                response = urllib.request.urlopen(send_url)
            except Exception as e:
                print(e)
            if response:
                data = response.read()
                resp = json.loads(data)
                if resp['ret']:
                    if game_obj.open_status:
                        Game_Activity.objects.filter(id=cid).update(
                            status=3,
                            resp_desc=resp['desc']
                        )
                        return True
                    else:
                        Game_Activity.objects.filter(id=cid).update(
                            status=3,
                            resp_desc=resp['desc'],
                            open_status=1
                        )
                        game_obj.activity_time.time += 1
                        return True

                else:
                    Game_Activity.objects.filter(id=cid).update(
                        status=2,
                        err_desc=resp['desc']
                    )
                    return False
            else:
                time.sleep(60)
    return False


@task
def edit_web_activity(cid, start_time, end_time):
    try:
        Web_Activity.objects.filter(id=cid).update(
            end_time=end_time
        )
        return True
    except Exception as e:
        return False


@task
def record_player_report(r):
    try:
        report,created = Player_Report.objects.get_or_create(player_id=r.player_id)
    except Exception as e:
        print(e)
        return False
    r.player_report = report
    r.save()
    if created:
        report.report_type = r.report_type
        report.first_content = r.report_content
        report.first_date = r.report_date
        report.language = langid.classify(r.report_content)[0]
        report.save()
    return True