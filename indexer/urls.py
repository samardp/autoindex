from django.urls import path
from .views import index_view, start_indexing_task

urlpatterns = [
    path('', index_view, name='index'),
    path('indexing/', start_indexing_task, name='start-indexing'),
]
