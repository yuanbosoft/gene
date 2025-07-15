import requests
import time
import csv
import os

# === 配置参数 ===
API_KEY = "k0ploNu2EY7dAq5DRPrf4e8bM3BJKQ9n"   # ←←←← 在这里填写你的 CORE API KEY
QUERY = "agriculture"
MAX_RECORDS = 100
PAGE_SIZE = 100
OUTPUT_CSV = "core_agriculture_metadata.csv"
PDF_DIR = "core_pdfs"
SLEEP_TIME = 1.2  # 请求间隔时间，防止被封IP

# === 创建 PDF 存储目录 ===
if not os.path.exists(PDF_DIR):
    os.makedirs(PDF_DIR)

# === 请求头 ===
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# === 初始化 CSV 文件 ===
with open(OUTPUT_CSV, "w", newline='', encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "title", "authors", "year", "url", "pdfLink", "publisher", "pdfDownloaded"])

print("开始抓取 CORE 文献并下载 PDF...")

total_downloaded = 0
next_cursor = None

while total_downloaded < MAX_RECORDS:
    payload = {
        "q": QUERY,
        "limit": PAGE_SIZE
    }
    if next_cursor:
        payload["cursor"] = next_cursor

    try:
        # === 调用 CORE API ===
        response = requests.post(
            "https://api.core.ac.uk/v3/search/works",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        data = response.json()

        works = data.get("results", [])
        next_cursor = data.get("next")

        if not works:
            print("没有更多数据了。")
            break

        with open(OUTPUT_CSV, "a", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            for item in works:
                core_id = item.get("id", "")
                title = item.get("title", "").replace("\n", " ").strip()
                year = item.get("yearPublished", "")
                url_link = item.get("url", "")
                pdf_link = item.get("downloadUrl", "")
                authors = ", ".join([a.get("name", "") for a in item.get("authors", [])])
                publisher = item.get("publisher", "")
                pdf_file = os.path.join(PDF_DIR, f"{core_id}.pdf")
                pdf_downloaded = "no"

                # === 下载 PDF ===
                if pdf_link and not os.path.exists(pdf_file):
                    try:
                        pdf_resp = requests.get(pdf_link, timeout=20)
                        if pdf_resp.status_code == 200 and b"%PDF" in pdf_resp.content[:1024]:
                            with open(pdf_file, "wb") as pdf_out:
                                pdf_out.write(pdf_resp.content)
                            pdf_downloaded = "yes"
                        else:
                            print(f"跳过无效 PDF: {core_id}")
                    except Exception as e:
                        print(f"下载失败 {core_id}: {e}")

                elif os.path.exists(pdf_file):
                    pdf_downloaded = "yes"

                # === 写入行 ===
                writer.writerow([core_id, title, authors, year, url_link, pdf_link, publisher, pdf_downloaded])

        total_downloaded += len(works)
        print(f"[{total_downloaded}] 篇已抓取，下一页...")
        time.sleep(SLEEP_TIME)

        if not next_cursor:
            break

    except Exception as e:
        print(f"抓取失败：{e}")
        time.sleep(5)

print("✅ 所有任务完成！")
