import asyncio
import datetime
import psycopg2
from psycopg2 import sql
from telebot.async_telebot import AsyncTeleBot
from datetime import datetime

dbname = 'raiting'
user = 'postgres'
password = '1234'
host = 'localhost'
port = 5432


conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)


bot = AsyncTeleBot('7851478686:AAHUp3d0Y-B9ecXD5NeZ3HRjMF1NoAfARUQ')



def change_rating(mapping_list):
    cursor = conn.cursor()

    query = f"SELECT * FROM rating WHERE LOWER(name) = LOWER('{mapping_list[0]}')"
    cursor.execute(query)
    results = cursor.fetchall()
    if len(results) == 0:
        query_insert = f"INSERT INTO rating (name, rating) VALUES (LOWER('{mapping_list[0]}'), {mapping_list[1]})"
        cursor.execute(query_insert)
        conn.commit()
    else:
        query_update = f"update rating set rating = (select rating from rating where LOWER(name) = LOWER('{mapping_list[0]}')) {mapping_list[1]} WHERE LOWER(name) = LOWER('{mapping_list[0]}')"
        cursor.execute(query_update)
    conn.commit()



def add_log(mapping_list):

    create_date = datetime.now()
    formatted_date = create_date.strftime('%Y-%m-%d %H:%M:%S')
    cursor = conn.cursor()
    query_name_id = f"SELECT id FROM rating WHERE LOWER(name) = LOWER('{mapping_list[0]}')"
    cursor.execute(query_name_id)
    name_id_result = cursor.fetchone()
    name_id = name_id_result[0]
    query_log_insert = f"INSERT INTO rating_log (name, name_id, reason, create_date, mmr) VALUES (LOWER('{mapping_list[0]}'), {name_id}, '{mapping_list[2]}', '{formatted_date}', '{mapping_list[1]}')"
    cursor.execute(query_log_insert)
    conn.commit()




@bot.message_handler(commands=["рейтинг"])
async def mapping_data(message):

    if message.from_user.id != 441332902 and message.from_user.id !=397446387:
        print(message.from_user.id)
        await bot.reply_to(message, "только для ванюши")
        return
    mapping = message.text.replace('/рейтинг ', '').split()  # [миша, +33, лалала, блаблабла]
    if not mapping[1][1::].isdigit():
        return await bot.reply_to(message,'ошибка')
    mapping_list = [mapping[0],mapping[1]]+[(' '.join(mapping[2::]))]  #[миша, +33, лалала блаблабла]
    change_rating(mapping_list)
    add_log(mapping_list)
    cursor = conn.cursor()
    curr_mmr = f"SELECT rating FROM rating WHERE LOWER(name) = LOWER('{mapping[0]}')"
    cursor.execute(curr_mmr)
    history_data = cursor.fetchone()
    await bot.reply_to(message,f'рейтинг {mapping[0]}  изменён на {mapping[1]} и теперь равняется {history_data[0]}')

@bot.message_handler(commands=["история"])
async def history(message):
    history_name = message.text.replace('/история ','')
    cursor = conn.cursor()
    query = f"SELECT name, reason, create_date, mmr FROM rating_log WHERE LOWER(name) = LOWER('{history_name}')"
    cursor.execute(query)
    history_data = cursor.fetchall()
    history_msg =''
    for z in history_data:
        history_msg += f'{z[0]} {z[3]} Причина: {z[1]} Дата: {z[2]}\n'
    await bot.reply_to(message,history_msg)

@bot.message_handler(commands=["список"])
async def list_mmr(message):
    cursor = conn.cursor()
    query = f"SELECT LOWER(name), rating FROM rating"
    cursor.execute(query)
    history_data = cursor.fetchall()
    history_msg = ''
    for z in history_data:
        history_msg += f'{z[0]} {z[1]}\n'
    await bot.reply_to(message, history_msg)

asyncio.run(bot.polling())