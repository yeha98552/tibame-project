# Imports
import os
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


def read_places_from_file(file_path):
    "Function to read places from a file, removing numbers and dots"
    with open(file_path, 'r', encoding='utf-8') as file:
        places = [line.strip() for line in file.readlines()]
    return [re.sub(r'^\d+\.', '', place).strip() for place in places]

def read_attraction_ids(csv_file_path):
    df = pd.read_csv(csv_file_path)
    return df.set_index('name')['attraction_id'].to_dict()

def PostContent(post):
    "Function to extract post content"
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

def main(txt_file, csv_file):
    "Main function to orchestrate the scraping process"
    ua = UserAgent()
    user_agent = ua.random
    print(user_agent)

    options = Options()
    options.add_argument(f'user-agent={user_agent}')
    options.add_argument("--disable-notifications")# 禁用瀏覽器的彈跳通知，可防止在自動化過程出現視窗干擾
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--headless=chrome")
    options.add_argument("--disable-gpu")  # 禁用 GPU 硬件加速。在某些情况下，如果不禁用，headless 模式可能會出出現問題。
    options.add_argument("--disable-extensions")  # 禁用瀏覽器extension，可以減少瀏覽器啟動時間
    options.add_argument("--disable-infobars")  # 禁止顯示訊息欄，避免訊息欄位擋住頁面其他元素

    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    places = read_places_from_file(txt_file)
    attraction_ids = read_attraction_ids(csv_file)
    base_url = "https://www.facebook.com/hashtag/{}"

    # Create the directory if it does not exist
    os.makedirs('source', exist_ok=True)

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
        postsInformation = pd.DataFrame()

        while True:
            if stop_times > 2:
                break

            soup = BeautifulSoup(driver.page_source)
            posts = soup.findAll('div', {'role':'article'})

            for idx, post in enumerate(posts):
                try:
                    obj = PostContent(post)
                    obj['attraction_id'] = attraction_ids.get(place, 'Unknown ID')
                    postsInformation = pd.concat([postsInformation, pd.DataFrame([obj])],ignore_index=True)
                except Exception as e:
                    print('Failed: %d' % (cnt+idx))
                    print(e)

            total_posts = postsInformation.shape[0]
            add_posts = total_posts - cnt
            print('Dealing: %d (%d)' % (total_posts, add_posts))
            
            if add_posts == 0:
                stop_times += 1
                print('Stop times: %d' % (stop_times))

            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(random.uniform(2,6))  # Random sleep to mimic user and avoid getting blocked
            
            today = date.today()
            filename = f'source/postsInformation_{place}_{today}.csv'  # Modified path to include 'source' directory
            postsInformation.drop_duplicates(subset=["post_link"], keep='last', inplace=True)
            postsInformation.to_csv(filename, index=False)  # Changed to_csv

            cnt = total_posts

    driver.quit()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python script_name.py <txt_file_path> <csv_file_path>")
    else:
        main(sys.argv[1], sys.argv[2])
