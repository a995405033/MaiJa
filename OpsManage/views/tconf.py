#!/usr/bin/env python
# -*- coding: utf-8 -*-

from django.shortcuts import render_to_response,render
from django.contrib.auth.decorators import login_required
from OpsManage.models import Operation_Log
from django.contrib.auth.decorators import permission_required


@login_required(login_url='/login')
@permission_required('OpsManage.can_read_operation_log_record', login_url='/noperm/')
def conf_log(request):
    if request.method == "GET":
        oper_list = Operation_Log.objects.all().order_by('-id')
        return render(request, 'tconf/conf_log.html', locals())
