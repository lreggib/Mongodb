#!/usr/bin/env python
# -*- coding:utf-8 -*-
# coding=utf-8
'''
@author: biggerl
@time: 2021/2/24 11:06
'''

from dingtalkchatbot.chatbot import DingtalkChatbot

def dingError(msg):

    webhook ='https://oapi.dingtalk.com/robot/send?access_token=6e1244f892e7fc3e2285784ae7d450ef764a9814a4cc7fef1c7ec66f7d2b92a0'
    
    ding = DingtalkChatbot(webhook)
    ding.send_text(msg)
