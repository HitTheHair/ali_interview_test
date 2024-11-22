import aiohttp
import asyncio
import zipfile
import os
import pandas as pd
from datetime import datetime
from selenium import webdriver
from bs4 import BeautifulSoup

# 构建 aggTrades 文件夹的 URL
aggtrades_url = "https://data.binance.vision/?prefix=data/futures/um/daily/aggTrades/"

def get_url_by_selenium(url):
    # 启动浏览器
    driver = webdriver.Chrome()

    # 访问网页
    driver.get(url)

    # 获取页面源码
    page_source = driver.page_source

    # 解析 HTML 内容
    soup = BeautifulSoup(page_source, 'html.parser')

    # 查找 id 为 "listing" 的 tbody 标签
    listing_tag = soup.find('tbody', id='listing')

    # 查找所有的链接
    links = listing_tag.find_all('a')

    # 提取链接的 href 属性
    urls = [link['href'] for link in links]

    # 关闭浏览器
    driver.quit()
    return urls

async def download_and_process_file(file_link):
    if file_link.endswith("USDT"):
        # 构建文件名和 URL
        file_name = file_link.split("/")[-1]
        url = aggtrades_url + file_name

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    with open(file_name, 'wb') as f:
                        while True:
                            chunk = await response.content.read(1024)#1024为了防止文件太大导致卡住
                            if not chunk:
                                break
                            f.write(chunk)

        # 解压文件
        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall('./data')

        # 校验文件完整性
        csv_file = file_name.replace(".zip", ".csv")
        if not os.path.exists(f"./data/{csv_file}"):
            print(f"文件完整性校验失败：{csv_file}")
            return

        # 读取 CSV 文件
        data = pd.read_csv(f"./data/{csv_file}")

        # 转换为 Pickle 文件
        pickle_file = csv_file.replace(".csv", ".pickle")
        data.to_pickle(pickle_file)

        print(f"数据已成功转换并存储为 {pickle_file} 文件。")


async def process_folder(folder_link):
    # 构建文件夹的 URL
    async with aiohttp.ClientSession() as session:
        async with session.get(folder_link) as response:
            if response.status == 200:
                for file_link in get_url_by_selenium(response.text):
                    if file_link.endswith("USDT"):
                        await download_and_process_file(file_link)

async def main(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                for folder_link in get_url_by_selenium(response.text):
                    await process_folder(folder_link)

if __name__ == "__main__":
    asyncio.run(main(aggtrades_url))


