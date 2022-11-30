
from django.urls import path,include
from . import views
urlpatterns = [
    path('update', views.update_function),
    # path('create', views.createanother),

]
