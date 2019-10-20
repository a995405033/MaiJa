#!/usr/bin/env python
# _#_ coding:utf-8 _*_

from django.db import models
import sys
import imp
imp.reload(sys)
# sys.setdefaultencoding("utf-8")


class Operation_Log(models.Model):
    oper_user = models.CharField(max_length=50,verbose_name='操作用户',default=None)
    oper_name = models.CharField(max_length=100,verbose_name='名称',default=None)
    oper_content = models.CharField(max_length=100,verbose_name='操作内容',default=None)
    oper_type = models.CharField(max_length=100,verbose_name='操作类型',default=None)
    create_time = models.DateTimeField(auto_now_add=True,blank=True,null=True,verbose_name='执行时间')
    class Meta:
        db_table = 'opsmanage_operation_log'
        permissions = (
            ("can_read_operation_log_record", "读取操作日志权限"),
            ("can_change_operation_log_record", "更改操作日志权限"),
            ("can_add_operation_log_record", "添加操作日志权限"),
            ("can_delete_operation_log_record", "删除操作日志权限"),
        )
        verbose_name = '操作日志记录表'
        verbose_name_plural = '操作日志记录表'



class Language_Version_Config(models.Model):
    language = models.CharField(max_length=10, verbose_name='语言', null=True, default=None)
    name = models.CharField(max_length=50, verbose_name='名称', null=True, default=None)

    class Meta:
        db_table = 'opsmanage_language_version_config'
        permissions = (
            ("can_read_language_version_config_record", "读取语言版本配置权限"),
            ("can_change_language_version_config_record", "更改语言版本配置权限"),
        )
        verbose_name = '语言版本配置'
        verbose_name_plural = '语言版本配置'



class Language_Form(models.Model):

    class Meta:
        managed = False
        permissions = (
            ("can_read_language_form_record", "读取语言构成权限"),
        )
        verbose_name = '语言构成'
        verbose_name_plural = '语言构成'