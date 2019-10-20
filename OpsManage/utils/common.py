#!/usr/bin/env python
# _#_ coding:utf-8 _*_

import csv
import codecs
import datetime
from django.http import HttpResponse

ONE_PAGE_OF_DATA = 20


def get_pagenate(request, obj, kwargs={}):
    try:
        curPage = int(request.GET.get('curPage', '1'))
        allPage = int(request.GET.get('allPage', '1'))
        pageType = str(request.GET.get('pageType', ''))
    except ValueError:
        curPage = 1
        allPage = 1
        pageType = ''

    if pageType == 'pageDown':
        curPage += 1
    elif pageType == 'pageUp':
        curPage -= 1

    startPos = (curPage - 1) * ONE_PAGE_OF_DATA
    endPos = startPos + ONE_PAGE_OF_DATA
    obj_list = []
    try:
        if kwargs:
            obj_list = obj.objects.filter(**kwargs).order_by('-id')[startPos:endPos]
        else:
            obj_list = obj.objects.all().order_by('-id')[startPos:endPos]
    except Exception as e:
        print(e)

    if curPage == 1 and allPage == 1:
        if kwargs:
            allPostCounts = obj.objects.filter(**kwargs).count()
        else:
            allPostCounts = obj.objects.all().count()
        allPage = allPostCounts / ONE_PAGE_OF_DATA
        remainPost = allPostCounts % ONE_PAGE_OF_DATA
        if remainPost > 0:
            allPage += 1
    return [curPage, allPage, pageType, obj_list]


def get_csv(title, kwargs):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="%s_%s.csv"' % (
            title, datetime.datetime.now().strftime('%Y%m%d%H%M%S'),)
    response.write(codecs.BOM_UTF8)
    writer = csv.writer(response)
    for k,v in list(kwargs.items()):
        writer.writerow((k,))
        for res in v:
            writer.writerow(res)
        writer.writerow((' ',))
    response.close()
    return response


def SelectSqlStrFormat(fieldname, vals):
    sql = ''
    if vals and isinstance(vals, list):
        for idx in range(len(vals)):
            if idx == 0:
                sql += ' and ({} = "{}" '.format(fieldname, vals[idx])
            else:
                sql += ' or {} = "{}" '.format(fieldname, vals[idx])
    sql += ')'
    return sql

