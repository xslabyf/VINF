import asyncio
import os
import re
import time
import requests
from urllib.parse import urlparse
from playwright.async_api import async_playwright

BASE_URL = "https://www.motorsportmagazine.com/database/drivers/"
LINKS_FILE = "driver_links.txt"
HEADERS = {
    "User-Agent": "VINF-Project-Crawler/1.0 (+https://stuba.sk; contact: xslabyf@stuba.sk)"
}
SAVE_DIR = "data/html"
SLEEP_TIME = 1.5

def parse_links_file():
    pairs = []
    if not os.path.exists(LINKS_FILE):
        return pairs
    with open(LINKS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            visited = line.endswith(" VISITED")
            url = line[:-8] if visited else line
            pairs.append((url, visited))
    return pairs


def save_links_file(pairs):
    seen = set()
    lines = []
    for url, visited in pairs:
        if url in seen:
            continue
        seen.add(url)
        lines.append(url + (" VISITED" if visited else ""))
    with open(LINKS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + ("\n" if lines else ""))


def merge_new_links(existing_pairs, new_urls):
    existing_set = {u for u, _ in existing_pairs}
    for u in sorted(set(new_urls)):
        if u not in existing_set:
            existing_pairs.append((u, False))
    return existing_pairs

def extract_driver_links(html):
    pattern = r'https://www\.motorsportmagazine\.com/database/drivers/[a-z0-9\-]+/'
    return list(set(re.findall(pattern, html)))


async def generate_driver_links(playwright):
    browser = await playwright.chromium.launch(headless=False)
    context = await browser.new_context(user_agent=HEADERS["User-Agent"])
    page = await context.new_page()

    await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)

    try:
        await page.wait_for_selector(".qc-cmp2-persistent-link", timeout=5000)
        print("Clicking 'Accept Cookies'...")
        await page.click(".qc-cmp2-persistent-link")
        await asyncio.sleep(2)
    except:
        print("No cookies prompt found.")

    all_links = set()

    while True:
        await page.wait_for_selector("table.database-table", timeout=30000)
        html = await page.content()
        links = extract_driver_links(html)
        all_links.update(links)

        try:
            next_button = await page.query_selector("button.datatable-paging__next")
            if next_button and await next_button.is_enabled():
                await next_button.click()
                await page.wait_for_timeout(3000)
            else:
                break
        except Exception as e:
            break

    await browser.close()
    print(f"Collected {len(all_links)} unique links.")
    return all_links

def save_html_from_url(url):
    os.makedirs(SAVE_DIR, exist_ok=True)
    parsed = urlparse(url)
    filename = parsed.path.strip("/").replace("/", "_") or "index"
    filepath = f"{SAVE_DIR}/{filename}.html"

    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        return True

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(resp.text)
        return True
    except Exception as e:
        return False


async def crawl():
    pairs = parse_links_file()
    if not pairs:
        async with async_playwright() as p:
            new_links = await generate_driver_links(p)
        pairs = merge_new_links(pairs, new_links)
        save_links_file(pairs)

    pages_saved = 0
    for idx, (url, visited) in enumerate(pairs):
        if visited:
            continue
        success = save_html_from_url(url)
        if success:
            pairs[idx] = (url, True)
            save_links_file(pairs)
            pages_saved += 1

        time.sleep(SLEEP_TIME)

    print(f"\n Saved {pages_saved} new driver pages.")


if __name__ == "__main__":
    asyncio.run(crawl())
