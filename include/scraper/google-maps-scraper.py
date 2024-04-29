import csv
import time
from datetime import datetime

from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# URL of the google map (台北101)
URL = "https://www.google.com/maps/place/%E5%8F%B0%E5%8C%97101%E8%B3%BC%E7%89%A9%E4%B8%AD%E5%BF%83/@25.033976,121.561964,17z/data=!4m17!1m8!3m7!1s0x3442abb6da9c9e1f:0x1206bcf082fd10a6!2zVGFpcGVpIDEwMSwgTm8uIDfkv6Hnvqnot6_kupTmrrXkv6HnvqnljYDlj7DljJfluIIxMTA!3b1!8m2!3d25.033976!4d121.5645389!16zL20vMDFjeTZ5!3m7!1s0x3442abb6da80a7ad:0xacc4d11dc963103c!8m2!3d25.0341222!4d121.5640212!9m1!1b1!16s%2Fg%2F11fx91ft3n?entry=ttu"
# "https://www.google.com/maps/place/%E8%B1%A1%E5%B1%B1%E5%B4%97/@25.0266169,121.5701129,17z/data=!4m8!3m7!1s0x3442abcc09ac3461:0x241a96a73259458b!8m2!3d25.026617!4d121.5749785!9m1!1b1!16s%2Fg%2F11sf3g8ffv?entry=ttu" # for test

current_date = datetime.now()
year = current_date.year
month = current_date.month


def setup_driver() -> webdriver.Chrome:
    """
    Setup the driver
    """
    service = Service(executable_path=ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_window_size(1120, 1000)
    return driver


def click_sort_by_recent(driver: webdriver.Chrome):
    """
    Click sort by recent
    """
    driver.find_element(By.XPATH, ".//button[@data-value='排序']").click()
    driver.find_element(
        By.XPATH, './/div[@class="fontBodyLarge yu5kgd vij30 kA9KIf "]/div[2]'
    ).click()


def compute_clicks(driver: webdriver.Chrome) -> int:
    """
    Compute the number of clicks to load all the reviews
    """
    result = driver.find_element(By.CSS_SELECTOR, "div.jANrlb").text
    result = result.split("\n")[1]

    # processing text
    result = result.replace(",", "")
    result = result.split(" ")
    result = result[0].split("\n")

    return int(int(result[0]) / 10) + 1  # FIXME


def scroll_down(driver: webdriver.Chrome, counts: int):
    """
    Scroll down the page to load all the reviews
    """
    wait = WebDriverWait(driver, 10)
    for _ in range(counts):
        element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "lXJj5c")))
        driver.execute_script("arguments[0].scrollIntoView(true);", element)


def convert_date(date: str) -> str:
    """
    Convert the date format
    """
    if "小時前" in date:
        hours = int(date.split("小時前")[0])
        return current_date - relativedelta(hours=hours)
    elif "周前" in date:
        weeks = int(date.split("周前")[0])
        return current_date - relativedelta(weeks=weeks)
    elif "個月前" in date:
        months = int(date.split("個月前")[0])
        return current_date - relativedelta(months=months)
    elif "年前" in date:
        years = int(date.split("年前")[0])
        return current_date - relativedelta(years=years)
    else:
        return None  # 未知格式


def scrape_and_save_to_csv(
    driver: webdriver.Chrome,
    yr_limit: int = None,
    filepath: str = f"{year}_{month:02d}_data.csv",
):
    """
    Scrape the review and save to csv
    """

    def generate_review() -> list:
        """
        Generate (scrape) the review
        """
        elements = driver.find_elements(By.CLASS_NAME, "jftiEf")
        for data in elements:
            name = data.find_element(By.XPATH, './/div[@class="d4r55 "]').text

            # if more button, click it
            try:
                button = data.find_element(By.CLASS_NAME, "w8nwRe.kyuRq")
                button.click()
            except NoSuchElementException:
                pass

            try:
                text = data.find_element(
                    By.XPATH, './/div[@class="MyEned"]/span[1]'
                ).text
            except NoSuchElementException:  # 如果沒有評論
                text = "No comment"

            score = (
                data.find_element(By.XPATH, './/span[@class="kvMYJc"]')
                .get_attribute("aria-label")
                .split(" ")[0]
            )
            date = data.find_element(By.XPATH, './/span[@class="rsqaWe"]').text
            date = convert_date(date)
            if date is None:
                continue
            elif (yr_limit is not None) and (
                date < current_date - relativedelta(years=yr_limit)
            ):
                print("Date is too old, stop scraping...")
                break
            else:
                date = date.strftime("%Y-%m-%d")

            yield [name, text, score, date]

    cols = ["name", "comment", "rating", "date"]
    with open(filepath, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(cols)  # Write header

        for row in generate_review():
            writer.writerow(row)

    driver.close()


def main():
    driver = setup_driver()
    driver.get(URL)
    time.sleep(5)
    print("Finished setup driver. And start...")

    click_sort_by_recent(driver)

    counts = compute_clicks(driver)
    print(f"Computed {counts} clicks.")
    scroll_down(driver, counts)

    print("Start scraping...")
    scrape_and_save_to_csv(driver, yr_limit=1)

    print("Done!")


if __name__ == "__main__":
    main()
