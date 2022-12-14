from django.shortcuts import render,redirect
from django.http import HttpResponse
# Create your views here.


def home(request):
    context = {
        'one' : 'Hello Django',
        'two' : 'Hello Java'
    }
    return render(request,'home.html',context)


def about(request):
    return render(request,'about.html')

def services(request):
    return render(request,'service.html')

def contact(request):
    return render(request,'contact.html')
