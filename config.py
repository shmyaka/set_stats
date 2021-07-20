import bs4
import requests


def get_host_ip():
    s = requests.get('https://2ip.ua/ru/')

    b = bs4.BeautifulSoup(s.text, "html.parser")

    a = b.select(" .ipblockgradient .ip")[0].getText()

    return a.strip()


# Тут собираем все константы
TOKEN = '6d9f51bee4e2873806a3d14cb8b3ebfda913dbfcc11a40726de82375d188d6487f582bacf0c21bd1c97af'
TOKEN_139 = '584a5af64f7847bf98b46fc9708a709a170f9d35b0cc93143a8f84e538f1c4d2557ac0390bf55895a5670'
tokens = [TOKEN, TOKEN_139]
host = get_host_ip()
user = 'root'
password = 'yfrfpetvf1313'
db_name = 'groups'
# Поля, которые дополнительно мы будем запрашивать через VK API
group_fields = 'screen_name,activity,age_limits,can_message,can_post,city,country,market,place,site,status,trending,' \
               'verified,wall,members_count'
# Поля, которые нам нужны. Совпадают с именами полеё объекта, который возвращает VK API по запросу groups.getById
values = [
    'id', 'name', 'screen_name', 'is_closed', 'deactivated', 'type', 'photo_50', 'activity', 'age_limits',
    'can_message', 'can_post', 'city', 'country', 'market', 'place', 'site',
    'status', 'trending', 'verified', 'wall', 'members_count'
]
MEMBERS_MIN_LIMIT = 10000
LENGTH = 500
TYPES = ['group', 'page']
