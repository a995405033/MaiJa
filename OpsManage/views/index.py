#!/usr/bin/env python  
# _#_ coding:utf-8 _*_  
from django.http import HttpResponseRedirect,JsonResponse
from django.shortcuts import render_to_response,render
from django.contrib import auth
from django.template import RequestContext
from django.contrib.auth.decorators import login_required
from OpsManage.models import *
from django.contrib.auth.decorators import permission_required

@login_required(login_url='/login')
def index(request):
    return render(request, 'index.html')

def login(request):
    if request.session.get('username') is not None:
        return HttpResponseRedirect('/',{"user":request.user})
    else:
        username = request.POST.get('username')
        password = request.POST.get('password') 
        user = auth.authenticate(username=username,password=password)
        if user and user.is_active:
            auth.login(request,user)
            request.session['username'] = username
            return HttpResponseRedirect('/user/center/',{"user":request.user})
        else:
            if request.method == "POST":
                return render_to_response('login.html',{"login_error_info":"用户名不错存在，或者密码错误！"},
                                                        context_instance=RequestContext(request))  
            else:
                return render_to_response('login.html',context_instance=RequestContext(request)) 
            
            
def logout(request):
    auth.logout(request)
    return HttpResponseRedirect('/login')


@login_required(login_url='/login')
def dashboard(request):
    return render(request, 'dashboard.html', locals())


def noperm(request):
    return render(request, 'noperm.html',{"user":request.user})

@login_required(login_url='/login')
@permission_required('OpsManage.can_change_global_config',login_url='/noperm/')
def config(request):
    if request.method == "GET":
        try:
            config = Global_Config.objects.get(id=1)
        except:
            config = None
        try:
            email = Email_Config.objects.get(id=1)
        except:
            email =None
        return render_to_response('config.html',{"user":request.user,"config":config,
                                                 "email":email},
                                  context_instance=RequestContext(request))
    elif request.method == "POST":
        if request.POST.get('op') == "log":
            try:
                count = Global_Config.objects.filter(id=1).count()
            except:
                count = 0
            if count > 0:
                Global_Config.objects.filter(id=1).update(
                                                      ansible_model =  request.POST.get('ansible_model'),
                                                      ansible_playbook =  request.POST.get('ansible_playbook'),
                                                      cron =  request.POST.get('cron'),
                                                      project =  request.POST.get('project'),
                                                      assets =  request.POST.get('assets',0),
                                                      server =  request.POST.get('server',0),
                                                      email =  request.POST.get('email',0),
                                                    )
            else:
                config = Global_Config.objects.create(
                                                      ansible_model =  request.POST.get('ansible_model'),
                                                      ansible_playbook =  request.POST.get('ansible_playbook'),
                                                      cron =  request.POST.get('cron'),
                                                      project =  request.POST.get('project'),
                                                      assets =  request.POST.get('assets'),
                                                      server =  request.POST.get('server'),
                                                      email =  request.POST.get('email')
                                                    )
            return JsonResponse({'msg':'配置修改成功',"code":200,'data':[]})
        elif request.POST.get('op') == "email":
            try:
                count = Email_Config.objects.filter(id=1).count()
            except:
                count = 0
            if count > 0:
                Email_Config.objects.filter(id=1).update(
                                                      site =  request.POST.get('site'),
                                                      host =  request.POST.get('host',None),
                                                      port =  request.POST.get('port',None),
                                                      user =  request.POST.get('user',None),
                                                      passwd =  request.POST.get('passwd',None),
                                                      subject =  request.POST.get('subject',None),
                                                      cc_user =  request.POST.get('cc_user',None),
                                                    )
            else:
                Email_Config.objects.create(
                                            site =  request.POST.get('site'),
                                            host =  request.POST.get('host',None),
                                            port =  request.POST.get('port',None),
                                            user =  request.POST.get('user',None),
                                            passwd =  request.POST.get('passwd',None),
                                            subject =  request.POST.get('subject',None),
                                            cc_user =  request.POST.get('cc_user',None),
                                            )
            return JsonResponse({'msg':'配置修改成功',"code":200,'data':[]})

