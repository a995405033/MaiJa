from django.shortcuts import render, redirect, HttpResponse
from django.contrib.auth.models import User



def new_register(request):
    if request.method == "GET":
        return render(request, 'register.html')

    if request.method == "POST":
        error = {'message': None}
        '''校验两次密码是否一致'''
        if request.POST.get('password') == request.POST.get('c_password'):
            try:
                user = User.objects.filter(username=request.POST.get('username'))
                if len(user) > 0:
                    error['message'] = '用户已存在'
                    return render(request, "register.html", locals())
                else:
                    user = User()
                    user.username = request.POST.get('username')
                    user.first_name = request.POST.get('name')
                    user.email = request.POST.get('email')
                    user.is_staff = 0
                    user.is_active = 1
                    user.is_superuser = 0
                    user.set_password(request.POST.get('password'))
                    user.save()
                    return redirect("/login?next=/")
            except Exception as e:
                error['message'] = '用户注册失败'
                return render(request, "register.html", locals())
        else:
            error['message'] = '两次密码输入不一致'
            return render(request, "register.html", locals())
