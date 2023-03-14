import os
import json 
import django
from kafka import KafkaConsumer
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.backend_pre.config.settings.dev')
django.setup()


channel_layer = get_channel_layer()
def send_data_to_channels_layer(data):
    async_to_sync(channel_layer.group_send)(
        "stream_group",
        {
            "type": "stream_data",
            "data": data
        },
    )
    
    

