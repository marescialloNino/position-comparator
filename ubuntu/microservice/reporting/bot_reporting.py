import logging
from datetime import datetime
import requests


# In ubuntu/microservice/reporting/bot_reporting.py
class TGMessenger:
    api = 'https://api.telegram.org/bot'
    tg_bot_name = 'leperespreader'
    tg_username = 'alphabet_pairspreader_bot'
    TOKEN = ''
    chat_name = {'CM': '1867239837', 'CryptoResearch': '-520427256', 'AwsMonitor': '-920364090'}

    @staticmethod
    def send_file(filename, chat_channel, use_telegram=True):
        if not use_telegram:
            logging.info(f"Would send file {filename} to {chat_channel}")
            return
        url_tg = f'{TGMessenger.api}{TGMessenger.TOKEN}/sendDocument?'
        chat_id = TGMessenger.chat_name[chat_channel]
        data = {
            'parse_mode': 'HTML',
            'caption': 'Report',
            'chat_id': chat_id
        }
        file = {
            'document': open(filename, 'rb')
        }
        response = requests.post(url_tg, data=data, files=file, stream=True)
        if response.status_code != 200:
            print(response.status_code, response.text)
        return

    @staticmethod
    async def send_async(session, message, chat_channel, use_telegram=True):
        if not use_telegram:
            logging.info(f"Would send message to {chat_channel}: {message}")
            return {'ok': True}
        chat_id = TGMessenger.chat_name[chat_channel]
        url_tg = f'{TGMessenger.api}{TGMessenger.TOKEN}/sendMessage?chat_id={chat_id}&text={message}'
        async with session.get(url_tg) as resp:
            response = await resp.json()
            return response

    @staticmethod
    async def send_message_async(session, comment, s1, s2, direction, spread, p1, p2, chat_channel, use_telegram=True):
        if not use_telegram:
            logging.info(f"Would send to {chat_channel}: {comment} for {s1}/{s2}")
            return {'ok': True}
        message = f'{comment} for {s1}/{s2}'
        return await TGMessenger.send_async(session, message, chat_channel, use_telegram)
    
    @staticmethod
    def send_message(message, chat_channel, use_telegram=True):
        if not use_telegram:
            logging.info(f"Would send message to {chat_channel}: {message}")
            return {'ok': True}
        chat_id = TGMessenger.chat_name[chat_channel]
        url_tg = f'{TGMessenger.api}{TGMessenger.TOKEN}/sendMessage?chat_id={chat_id}&text={message}'
        try:
            response = requests.get(url_tg)
            if response.status_code != 200:
                logging.error(f"Failed to send message to {chat_channel}: {response.status_code} {response.text}")
                return {'ok': False, 'error': response.text}
            return response.json()
        except Exception as e:
            logging.error(f"Error sending message to {chat_channel}: {e}")
            return {'ok': False, 'error': str(e)}