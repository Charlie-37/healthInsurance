from django.urls import path
from . import views as v

#Urlr Conf
urlpatterns = [
# this will be a home page
  path('', v.home,name='Home'),
  path('about', v.about,name='About'),
  path('services', v.services,name='Services'),
  path('contact', v.contact,name='Contact'),
]