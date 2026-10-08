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
    print(f"\n [*] Fetching news at {time.strftime('%H:%M:%S')}")
    found_articles=0
    
    for url in FINANCE_FEEDS:
        try:
            feed = feedparser.parse(url)
            #checking forst 10 news out of each feed
            for entry in feed.entries[:10]:
                title = entry.title 
                summary = entry.get('summary', '')
                full_text = (title + " " + summary).lower()
                
                # Checking if there is in a text at least one ESG-word
                if any(keyword in full_text for keyword in ESG_KEYWORDS):
                    article_data = {
                        "title": title,
                        "source": url,
                        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                    }
                    print(f" [ESG MATCH] -> {title}")
                    found_articles += 1
        except Exception as e:
            print(f"[-] Error parsing {url}: {e}")
            
    print(f"[*] Cycle complete. Found {found_articles} ESG-related articles.")
if __name__ ==   "__main__":
    while True: 
        fetch_esg_news()
        print("Waiting 60 seconds before next fetch...")
        time.sleep(60)
        