import logging
from datetime import datetime
import requests


class TGMessenger:
    api = 'https://api.telegram.org/bot'
    tg_bot_name = 'leperespreader'
    tg_username = 'alphabet_pairspreader_bot'
    TOKEN = '6146310312:AAGJYJwOWgEdj3-OxISv2C_FLAT_KfnIabk'
    chat_name = {'CM': '1867239837', 'CryptoResearch': '-520427256', 'AwsMonitor':'-920364090'}

    @staticmethod
    def send_file(filename, chat_channel):
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
    async def send_async(session, message, chat_channel):
        """

        :param session: aiohttp.ClientSession
        :param message:
        :param chat_channel: str, ['CM', 'CryptoResearch']
        :return:
        """

        chat_id = TGMessenger.chat_name[chat_channel]
        url_tg = f'{TGMessenger.api}{TGMessenger.TOKEN}/sendMessage?chat_id={chat_id}&text={message}'

        async with session.get(url_tg) as resp:
            response = await resp.json()
            return response

    @staticmethod
    def send(message, chat_channel):
        """

        :param chat_id:
        :param message:
        :param chat_channel: str, ['CM', 'CryptoResearch']
        :return:
        """

        if chat_channel != '':
            chat_id = TGMessenger.chat_name[chat_channel]
            url_tg = f'{TGMessenger.api}{TGMessenger.TOKEN}/sendMessage?chat_id={chat_id}&text={message}'
            try:
                result_tg = requests.get(url_tg).json()
            except Exception as e:
                result_tg = f'TG connection exception: {e.args}'
        else:
            result_tg = None
        return result_tg

    @staticmethod
    async def send_message_async(session, comment, s1, s2, direction, spread, p1, p2, chat_channel):
        """
        :param session: aiohttp.ClientSession
        :param comment: session for async stuff
        :param s1: str
        :param s2: str
        :param direction: int
        :param spread: float
        :param p1: float
        :param p2: float
        :param chat_channel: str, ['CM', 'CryptoResearch']
        :return:
        """
        message = f'{comment} for {s1}/{s2}'#: {direction}@{spread}, p1={p1},p2={p2}'

        return await TGMessenger.send_async(session, message, chat_channel)



