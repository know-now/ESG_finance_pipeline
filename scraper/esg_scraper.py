import time
import json
import feedparser 

print("=== ESG Financial News Scraper Started ===")

#RSS of financial and ecologic news

FINANCE_FEEDS = [
    "https://feeds.a.dj.com/rss/RSSWSJ.xml",          # Wall Street Journal
    "https://search.cnbc.com/rs/search/combinedcms/view.xml?id=10000664", # CNBC Finance
]

# Keywords needed for choosing correct info
ESG_KEYWORDS = ['carbon', 'emission', 'climate', 'green', 'sustainable', 'pollution', 'esg', 'penalty', 'lawsuit']

def fetch_esg_news():
    print(f"\n [*] Fetching news at [time.strftime('%H:%M:)]")