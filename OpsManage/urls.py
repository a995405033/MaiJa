"""OpsManage URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.9/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""

from django.conf.urls import url,include
from django.contrib import admin,auth
from django.views import static
from django.conf import settings
from OpsManage.views import index,users,tconf,aset
from rest_framework.urlpatterns import format_suffix_patterns
from OpsManage.restfull import users_api,assets_api
from OpsManage.scheduler import scheduler

from OpsManage.views import new_register

urlpatterns = [
    url(r'^operate_data/',index.noperm), #用户登录
    url(r'^static/(?P<path>.*)$', static.serve, {'document_root': settings.STATIC_ROOT }, name='static'),

    url(r'^$',index.index),
    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    url(r'^admin/', admin.site.urls),
    url(r'^login$', auth.views.login, {'template_name': 'login.html'}),
    url(r'^logout/$',index.logout),
    url(r'^dashboard/',index.dashboard),

    url(r'^language_config/$',aset.language_config),

    url(r'^language_config_mod/(?P<cid>[0-9]+)/$',aset.language_config_mod),

    url(r'^language_config_add/$',aset.language_config_add),
    url(r'^conf_log/$',tconf.conf_log),
    url(r'^users/manage/$',users.user_manage),
    url(r'^register/',users.register),
    url(r'^user/(?P<uid>[0-9]+)/$',users.user),
    url(r'^user/center/$',users.user_center),
    url(r'^group/(?P<gid>[0-9]+)/$',users.group),
    url(r'^api/user/$', users_api.user_list),
    url(r'^api/user/(?P<id>[0-9]+)/$',users_api.user_detail),
    url(r'^api/group/$', assets_api.group_list),
    url(r'^api/group/(?P<id>[0-9]+)/$', assets_api.group_detail),


    url(r'^new_register/$',new_register.new_register),  #新用户注册


]

urlpatterns = format_suffix_patterns(urlpatterns)
