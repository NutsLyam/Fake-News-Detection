import sqlite3
import pandas as pd
import tqdm
import requests
import pymorphy2
import lxml.etree
import lxml.html
import re
from nltk.corpus import stopwords

conn = sqlite3.connect("total_database.db")
cursor = conn.cursor()

#cursor.execute("""CREATE TABLE News
#     ( Header, Content, Date, Fake_id, Label, Topic)""")

##Creating a list of stop words and adding custom stopwords
stop_words = set(stopwords.words('russian'))
new_words = ['что', 'это', 'так', 'вот', 'ещё', 'свой', 'быть', 'й', 'как', 'в', 'наш', 'к', 'на', 'свой',
             'который', 'очень', 'которые', 'кстати', 'также', 'весь']
stop_words = stop_words.union(new_words)


def processing(text):
    # for i in range(0, len(texts)):
    # Remove punctuations
    text = re.sub(r'\W', ' ', text)
    # print(text)
    # Convert to lowercase
    text = text.lower()

    # remove tags
    text = re.sub("&lt;/?.*?&gt;", " &lt;&gt; ", text)

    # remove special characters and digits
    text = re.sub("(\\d|\\W)+", " ", text)

    ##Convert to list from string
    text = text.split()

    ## pymorphy
    analyzer = pymorphy2.MorphAnalyzer()
    text = (analyzer.normal_forms(word)[0] for word in text)
    corpus = " ".join(text)

    return corpus


def get_links():
    # даты
    month_31 = ['01','03']  # не забыть вернуть январь '01','03','05','07','08', '10','12'
    month_30 = ['04' ]#'04','06', '09', '11']
    month_29 = ['02']

    day_31 = []
    day_30 = []
    day_10 = ['01', '02', '03', '04', '05', '06', '07', '08', '09']

    day_29 = []
    for k in range(10, 31, 1):
        str_k = str(k)
        day_30.append(str_k)

    day_30 = day_10 + day_30

    for k in range(10, 32, 1):
        str_k = str(k)
        day_31.append(str_k)

    day_31 = day_10 + day_31

    for k in range(10, 30, 1):
        str_k = str(k)
        day_29.append(str_k)

    day_29 = day_10 + day_29

    date = []
    year = 2019
    for month in month_31:
        for day in day_31:
            d = str(year) + str(month) + str(day)
            date.append(d)

    for month in month_30:
        for day in day_30:
            d = str(year) + str(month) + str(day)
            date.append(d)
    for month in month_29:
        for day in day_29:
            d = str(year) + str(month) + str(day)
            date.append(d)

    links = []
    path = "http://ria.ru/"
    for d in date:
        links.append(path + d + "/")
    return links


def search_news(links):
    for link in tqdm.tqdm(links, desc = "links"):
        print("\nDate: ",link[14:23])
            response = requests.get(link, verify=True)
            tree = lxml.html.fromstring(response.text)
            refs = tree.xpath('//div[@class = "list-item"]//a[@class= "list-item__title color-font-hover-only"]/@href')
        for ref in tqdm.tqdm(refs, desc="refs",ascii=True,position=0):
            new_response = requests.get(ref, verify=True)
            new_tree = lxml.html.fromstring(new_response.text)
            #Content = new_tree.xpath('//div[@class="article__text"]//p//text()')
            #для 2019 года
            Content = new_tree.xpath('//div[@class="article__text"]//text()')
            Content = ' '.join(Content)
            content = processing(Content)
            for fake_id in indexes:
                counter = 0
                keywords = data['keywords'].iloc[fake_id].split(',')
                for word in keywords:
                    if (content.find(word) != -1):
                        counter += 1
                # print(counter)
                if counter > len(keywords) * 0.5 or counter > 6:
                    print("add", counter)
                    Header = new_tree.xpath('//h1[@class="article__title"]/text()')
                    Date = new_tree.xpath('//div[@class="article__info-date"]/text()')
                    print("fake_id", fake_id)

                    cursor.execute("INSERT INTO News VALUES (?,?,?,?,?,?)", (Header[0], Content,
                                                                             Date[0], fake_id,
                                                                             2, ""))
                    conn.commit()
                    break


links = get_links()


data = pd.read_csv('fake news data.csv', sep='\t', index_col=False )
data = data.drop(['Unnamed: 0'], axis = 1)
indexes = data.index

search_news(links)
