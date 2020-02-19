# -*- coding: utf-8 -*-

from slackclient import SlackClient
import logging


class SlackHandler(logging.Handler):
    def __init__(self, level=0, module_name='', slack_bot_token='xoxb-312554961652-uSmliU84rFhnUSBq9YdKh6lS',  slack_crons_channel_id='CAM3ND487'):
        super(SlackHandler, self).__init__(level)
        self.module_name = module_name
        self.slack_bot_token = slack_bot_token
        self.slack_crons_channel_id = slack_crons_channel_id
        self.sc = SlackClient(slack_bot_token)

    def emit(self, record):
        if record:
            if record.levelname in ['ERROR', 'WARNING']:
                text = ''
                if record.exc_text:
                    text = record.exc_text
                elif record.message:
                    text = record.message
                if 'Product not saved in' in text:
                    return
                try:
                    msg = "%s [%s] %s: %s" % (record.asctime, record.levelname, record.funcName, text)
                except:
                    msg = text
                self.sc.api_call("chat.postMessage",
                                 channel=self.slack_crons_channel_id,
                                 as_user=False,
                                 username=self.module_name + ' ' + record.levelname,
                                 text=text)


def notify_slack(source, message, slack_bot_token='xoxb-312554961652-uSmliU84rFhnUSBq9YdKh6lS',  slack_cron_info_channel_id='CGNGTRB7X'):
    sc = SlackClient(slack_bot_token)
    sc.api_call(
        "chat.postMessage",
        channel=slack_cron_info_channel_id,
        as_user=False,
        username=source,
        text=message,
    )
