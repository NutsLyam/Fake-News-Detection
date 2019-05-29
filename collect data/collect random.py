import sqlite3
import pandas as pd
import tqdm
import requests
import pymorphy2
import lxml.etree
import lxml.html
import re
from nltk.corpus import stopwords
import random

conn = sqlite3.connect("random_database.db")
cursor = conn.cursor()

#cursor.execute("""CREATE TABLE News
#    ( Header, Content, Date, Label)""")
def str_format(num):
    if (len(str(num)) == 1):
        num = '0' + str(num)
    else:
        num = str(num)
    return (num)
path = "http://ria.ru/"
N = 100
for i in tqdm.tqdm(range(300)):

    month = random.randint(1, 11)
    day = random.randint(1, 28)
    year = '2018'
    date = year + str_format(month) + str_format(day) + "/"
    link = path + date
    response = requests.get(link, verify=True)
    tree = lxml.html.fromstring(response.text)
    refs = tree.xpath('//div[@class = "list-item"]//a[@class= "list-item__title color-font-hover-only"]/@href')
    n = random.randint(0, len(refs)-1)
    new_response = requests.get(refs[n], verify=True)
    new_tree = lxml.html.fromstring(new_response.text)
    Content = new_tree.xpath('//div[@class="article__text"]//p//text()')
    Content = ' '.join(Content)
    Header = new_tree.xpath('//h1[@class="article__title"]/text()')
    Date = new_tree.xpath('//div[@class="article__info-date"]/text()')
    cursor.execute("INSERT INTO News VALUES (?,?,?,?)", (Header[0], Content,
                                                             Date[0],"2"))
    conn.commit()


