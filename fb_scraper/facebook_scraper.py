# Imports
import re
import pandas as pd
from datetime import date
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from fake_useragent import UserAgent
import time
import hashlib
import random
import json
import redis as rds
import openpyxl

# Function to read places from a file, removing numbers and dots
def read_places_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        places = [line.strip() for line in file.readlines()]
    return [re.sub(r'^\d+\.', '', place).strip() for place in places]

# Function to extract post content
def PostContent(post):
    links = post.findAll('a', {'role':'link'})
    poster = ""
    poster_link = ""
    post_time = ""
    post_link = ""
    content = ""
    all_link = []
    all_text = post.findAll(string=True)
    
    for link in links:
        text = link.find(string=True)
        href = link['href']
        all_link.append({'text': text, 'href': href})
        if text not in ['Active', None]:
            if poster == "" and text:
                poster = text
                poster_link = href
            if link.has_attr('aria-label') and post_time == "" and text:
                post_time = text
                post_link = href
    
    if poster == post_time:
        poster = post.find(string=True)
        poster_link = ""
        
    message = post.find('div', {"data-ad-preview":"message"})
    if message:
        content = "".join(message.findAll(string=True))
    
    hash_id = hashlib.md5("".join(all_text).encode('utf-8')).hexdigest() if all_text else ""
    
    return {
        'id': hash_id,
        'post_link': post_link,
        'time': post_time,
        'poster_name': poster,
        'poster_link': poster_link,
        'content': content,
        'all_link': all_link,
        'all_text': all_text,
    }

# Main function to orchestrate the scraping process
def main(txt_file):
    ua = UserAgent()
    user_agent = ua.random
    print(user_agent)

    options = Options()
    options.add_argument(f'user-agent={user_agent}')
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--headless=chrome")

    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    places = read_places_from_file(txt_file)
    base_url = "https://www.facebook.com/hashtag/{}"

    for place in places:
        url = base_url.format(place)
        driver.get(url)

        try:
            close_btn = driver.find_element(By.CSS_SELECTOR, "div[aria-label='關閉']")
            if close_btn:
                close_btn.click()
        except:
            print('no need to click')
        
        cnt = 0
        stop_times = 0
        PostsInformation = pd.DataFrame()

        while True:
            if stop_times > 2:
                break

            soup = BeautifulSoup(driver.page_source)
            posts = soup.findAll('div', {'role':'article'})

            for idx, post in enumerate(posts):
                try:
                    obj = PostContent(post)
                    json_str = json.dumps(obj, sort_keys=True)
                    key = obj['id']
                    if key != "":
                        PostsInformation = pd.concat([PostsInformation, pd.DataFrame([obj])],ignore_index=True)
                except Exception as e:
                    print('Failed: %d' % (cnt+idx))
                    print(e)

            total_posts = PostsInformation.shape[0]
            add_posts = total_posts - cnt
            print('Dealing: %d (%d)' % (total_posts, add_posts))
            
            if add_posts == 0:
                stop_times += 1
                print('Stop times: %d' % (stop_times))

            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(random.uniform(2,7))  # Random sleep to mimic user and avoid getting blocked
            
            today = date.today()
            filename = f'PostsInformation_{place}_{today}.xlsx'
            PostsInformation = PostsInformation.drop_duplicates("id", keep='last')
            PostsInformation.to_excel(filename, index=False)

            cnt = total_posts

    driver.quit()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python .py <txt_file_path>")
    else:
        main(sys.argv[1])
