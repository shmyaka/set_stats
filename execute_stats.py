from config import TOKEN_139
import json

url = 'https://api.vk.com/method/execute'
params = {
    'v': '5.139',
    'access_token': TOKEN_139
}


def get_code(arr, group_id, interval):
    return f'var x = {arr}; var k = [];' \
           f' var count = 0; ' \
           f'while (count < x.length) {{' \
           f'k.push(API.stats.get({{"group_id":  {group_id}, "timestamp_from": x[count][0], ' \
           f'"timestamp_to": x[count][1], "interval": "{interval}"}})[6]);' \
           f'count = count + 1;}}' \
           f'return k;'


async def get_execute_stats(session, weeks, group_id, interval):
    interval = 'week' if interval == 7 else 'day'
    params['code'] = get_code(weeks, group_id, interval)

    async with session.get(url, params=params) as r:
        # r_json = r.json()
        data = await r.read()
        r_json = json.loads(data)
        # print(f'await = {r_json}')
        return r_json.get('response')
