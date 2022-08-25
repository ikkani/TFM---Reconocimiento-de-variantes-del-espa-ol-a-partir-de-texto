"""
scraper_base.py
Iñaki Urrutia Sánchez
Class to scrap news from digital newspaper, getting those urls from twitter
"""

from email import header
import re
import datetime
from bs4 import BeautifulSoup
import requests
import os
import pandas as pd


class Scraper:
    
    def __init__(self, path, twitter_id, name = "", re_url = "https://t.co/\w+", headers = None):
        self.path = path	# Path to store the data
        self.twitter_id = twitter_id	# ID of the twitter account of the target
        self.re_url = re.compile(re_url)	# Regular expression to identify newspaper's urls
        self.name = name	# Name of the newspaper
        self.headers = headers	# Could be needed in some cases

    def get_news_urls(self, twitter_client, n_requests = 10):
    	"""Gets the news' urls using its twitter account"""
        urls = []
        next_token = None

        for i in range(0, n_requests):
            try:
                tweets = twitter_client.get_users_tweets(self.twitter_id, max_results = 100, pagination_token = next_token)    
                next_token = tweets.meta["next_token"]
                tweets_text = [t.text for t in tweets.data]
                urls = urls + [self.re_url.search(t).group(0) for t in tweets_text if self.re_url.search(t) != None]
            except:
                print("Exception on twitter request number " + str(i))

        return urls

    def unify_excels(self):
    	"""Store all the data in one unique excel"""
        total_df = pd.DataFrame()
        listdir = os.listdir(self.path)
        for file in listdir:
            total_df = pd.concat([total_df, pd.read_excel(self.path + file)])
        total_df.drop_duplicates().to_excel(self.path + "TOTAL_" + self.name + "_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".xlsx", encoding="utf-8", index = None, engine='xlsxwriter')
        self.total_df = total_df.drop_duplicates()
        return self.total_df.shape[0]

    def scrap_news(self, urls_twitter):
    	"""Scrap the news of the urls given"""
        texts = []
        titles = []
        urls_newspaper = []
        themes = []
        cont = 0
        
        for url in urls_twitter:
            try:
                r = requests.get(url, headers=self.headers)
                if r.status_code != 200:	# Connection error
                    print("url => " + url + " failed :(")
                else:
                    news_bs = BeautifulSoup(r.text, features="html.parser")
                    title, text, theme = self.scrap_article(news_bs)

                    if title is not None:
                        titles.append(title)
                        texts.append(text)
                        urls_newspaper.append(r.url)
                        themes.append(theme)
                        cont = cont + 1

                        if (cont % 250 == 0):	# Each 250 news, a security checkpoint is created
                            df = pd.DataFrame(zip(urls_newspaper, titles, texts, themes), columns=["url", "title", "text", "themes"])
                            df.to_excel(self.path + str(cont) + "_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".xlsx", encoding="utf-8", index = None, engine='xlsxwriter')
                            texts = []
                            titles = []
                            urls_newspaper = []
                            themes = []

            except:
                print("url => " + url + " failed :(")
        
        df = pd.DataFrame(zip(urls_newspaper, titles, texts, themes), columns=["url", "title", "text", "themes"])
        df.to_excel(self.path + str(cont) + "_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".xlsx", encoding="utf-8", index = None, engine='xlsxwriter')

    def scrap_article(self, news_bs):
    	"""Must be overrided for every newspaper's format"""
        pass
