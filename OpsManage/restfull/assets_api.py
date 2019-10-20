#!/usr/bin/env python  
# _#_ coding:utf-8 _*_
from OpsManage.serializers import *
from OpsManage.models import *
from rest_framework import status
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.contrib.auth.decorators import permission_required

    
@api_view(['GET', 'POST' ])
@permission_required('Opsmanage.add_group',raise_exception=True)
def group_list(request,format=None):
    """
    List all order, or create a server assets order.
    """
    if not  request.user.has_perm('Opsmanage.read_group'):
        return Response(status=status.HTTP_403_FORBIDDEN)     
    if request.method == 'GET':      
        snippets = Group.objects.all()
        serializer = GroupSerializer(snippets, many=True)
        return Response(serializer.data)     
    elif request.method == 'POST':
        if not  request.user.has_perm('Opsmanage.change_group'):
            return Response(status=status.HTTP_403_FORBIDDEN)         
        serializer = GroupSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

@api_view(['GET', 'PUT', 'DELETE'])
@permission_required('Opsmanage.change_group',raise_exception=True)
def group_detail(request, id,format=None):
    """
    Retrieve, update or delete a server assets instance.
    """    
    try:
        snippet = Group.objects.get(id=id)
    except Group.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
 
    if request.method == 'GET':
        serializer = GroupSerializer(snippet)
        return Response(serializer.data)
 
    elif request.method == 'PUT':
        serializer = GroupSerializer(snippet, data=request.data)
        old_name = snippet.name
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
     
    elif request.method == 'DELETE':
        if not request.user.has_perm('Opsmanage.delete_group'):  
            return Response(status=status.HTTP_403_FORBIDDEN)
        snippet.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
