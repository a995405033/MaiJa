#!/usr/bin/env python  
# _#_ coding:utf-8 _*_  
from rest_framework import serializers
# from OpsManage.models import *
from django.contrib.auth.models import Group,User


class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ('id','last_login','is_superuser','username',
                  'first_name','last_name','email','is_staff',
                  'is_active','date_joined')

class GroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = ('id','name')