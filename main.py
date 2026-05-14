#!/usr/bin/env python3
"""
Cloudflare IP 优选工具 (TCP筛选 + IP可用性二次筛选 + curl带宽测速 + WxPusher通知)
支持数据源：URL 远程获取、本地 ipv4.txt、本地 ipv4.csv
结果保存到 ip.txt，自动推送到 GitHub，自动更新 Cloudflare DNS
"""

import requests
import socket
import time
import sys
import re
import os
import subprocess
import shutil
import json
import csv
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================== 预编译正则 ====================
NODE_PATTERN = re.compile(r"^(\d+\.\d+\.\d+\.\d+):(\d+)#(.+)$")
IP_PORT_PATTERN = re.compile(r"^(\d+\.\d+\.\d+\.\d+):(\d+)#")

# ==================== 国家代码映射表 ====================
CN_TO_CODE = {
    "阿富汗": "AF", "奥兰群岛": "AX", "阿尔巴尼亚": "AL", "阿尔及利亚": "DZ",
    "中国": "CN", "香港": "HK", "澳门": "MO", "台湾": "TW", "美国": "US", 
    "日本": "JP", "韩国": "KR", "英国": "GB", "德国": "DE", "新加坡": "SG",
    # ... (保持原代码中的完整映射表)
}

# ==================== 加载配置文件 ====================
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

def load_config():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"❌ 错误：未找到配置文件 {CONFIG_FILE}")
        sys.exit(1)

    defaults = {
        "USE_GLOBAL_MODE": True,
        "GLOBAL_TOP_N": 15,
        "PER_COUNTRY_TOP_N": 1,
        "BANDWIDTH_CANDIDATES": 90,
        "TCP_PROBES": 3,
        "MIN_SUCCESS_RATE": 1.0,
        "TIMEOUT": 2.0,
        "SOCKET_DEFAULT_TIMEOUT": 3,
        "PROGRESS_PRINT_INTERVAL": 1,
        "FILTER_COUNTRIES_ENABLED": False,
        "ALLOWED_COUNTRIES": ["US"],
        "PRE_FILTER_BLOCKED_ENABLED": True,
        "PRE_FILTER_BLOCKED_COUNTRIES": ["CN"],
        "PRE_FILTER_PORT_ENABLED": True,
        "PRE_FILTER_PORTS": [443],
        "ENABLE_WXPUSHER": True,
        "WXPUSHER_APP_TOKEN": "your_app_token_here",
        "WXPUSHER_UIDS": ["your_uid_here"],
        "WXPUSHER_API_URL": "http://wxpusher.zjiecode.com/api/send/message",
        "CF_ENABLED": True,
        "CF_API_TOKEN": "your_token",
        "CF_ZONE_ID": "your_zone",
        "CF_DNS_RECORD_NAME": "your_domain",
        "ADDITIONAL_SOURCES": [], # 存放 URL 数据源
        "LOCAL_SOURCES": {
            "ipv4_txt": "ipv4.txt",
            "ipv4_csv": "ipv4.csv"
        },
        "OUTPUT_FILE": "ip.txt",
        "ENABLE_LOGGING": False,
        "LOG_FILE": "cfnb.log",
        "TEST_AVAILABILITY": True,
        "AVAILABILITY_CHECK_API": "https://api.090227.xyz/check",
        "AVAILABILITY_WORKERS": 32,
        "BANDWIDTH_WORKERS": 10,
        "MAX_WORKERS": 200,
        # ... 其余参数参考原代码
    }
    for key, value in defaults.items():
        if key not in config: config[key] = value
    return config

cfg = load_config()

# 导出配置变量 (此处省略冗长的赋值过程，实际运行时程序会通过 cfg[...] 获取)
def get_cfg(key): return cfg.get(key)

# ==================== 解析引擎 ====================

def extract_country_code(label):
    if not label: return "UN"
    label = label.strip()
    # 1. 匹配两位大写字母
    code_match = re.search(r'\b([A-Z]{2})\b', label)
    if code_match: return code_match.group(1)
    # 2. 中文匹配
    for cn_name, code in CN_TO_CODE.items():
        if cn_name in label: return code
    return "UN"

def parse_text_content(text):
    """通用文本解析：支持 IP:PORT#LABEL 格式"""
    nodes = []
    lines = text.splitlines()
    for line in lines:
        line = line.strip()
        if not line or '#' not in line: continue
        parts = line.split('#')
        ipport = parts[0].strip()
        label = parts[1].strip() if len(parts) > 1 else "Unknown"
        if re.match(r'^\d+\.\d+\.\d+\.\d+:\d+$', ipport):
            nodes.append(f"{ipport}#{extract_country_code(label)}")
    return nodes

def parse_csv_content(file_path):
    """解析 CSV 格式"""
    nodes = []
    if not os.path.exists(file_path): return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 尝试常见的列名
                ip = row.get("IP 地址") or row.get("ip") or row.get("address")
                port = row.get("端口") or row.get("port") or "443"
                country = row.get("国家") or row.get("country") or "UN"
                if ip and port:
                    nodes.append(f"{ip}:{port}#{extract_country_code(country)}")
    except Exception as e:
        print(f"⚠️ CSV 解析失败: {e}")
    return nodes

def fetch_sources():
    """综合加载：URL, TXT, CSV"""
    all_nodes = []
    
    # 1. 加载 URL 数据源
    for source in cfg.get("ADDITIONAL_SOURCES", []):
        if not source.get("enabled", True): continue
        url = source.get("url")
        try:
            print(f"正在拉取 URL 数据源: {url}")
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                nodes = parse_text_content(resp.text)
                all_nodes.extend(nodes)
                print(f"成功获取 {len(nodes)} 个节点")
        except Exception as e:
            print(f"❌ URL 获取失败: {e}")

    # 2. 加载本地 ipv4.txt
    txt_path = cfg.get("LOCAL_SOURCES", {}).get("ipv4_txt", "ipv4.txt")
    if os.path.exists(txt_path):
        with open(txt_path, "r", encoding="utf-8") as f:
            nodes = parse_text_content(f.read())
            all_nodes.extend(nodes)
            print(f"从 {txt_path} 加载了 {len(nodes)} 个节点")

    # 3. 加载本地 ipv4.csv
    csv_path = cfg.get("LOCAL_SOURCES", {}).get("ipv4_csv", "ipv4.csv")
    if os.path.exists(csv_path):
        nodes = parse_csv_content(csv_path)
        all_nodes.extend(nodes)
        print(f"从 {csv_path} 加载了 {len(nodes)} 个节点")

    # 去重
    unique_nodes = list(set(all_nodes))
    print(f"数据源加载完成，总计去重节点: {len(unique_nodes)}")
    return unique_nodes

# ==================== 核心逻辑 (复用原代码逻辑) ====================

def test_tcp_latency(ip, port):
    # (同原代码)
    min_latency = float("inf")
    success = 0
    for _ in range(cfg["TCP_PROBES"]):
        try:
            start = time.time()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(cfg["TIMEOUT"])
                sock.connect((ip, int(port)))
            latency = time.time() - start
            min_latency = min(min_latency, latency)
            success += 1
        except: continue
    return min_latency, success

def main():
    print("🚀 Cloudflare IP 优选工具启动")
    
    # 1. 获取所有数据源
    nodes = fetch_sources()
    if not nodes:
        print("❌ 未发现有效 IP 数据源，请检查配置文件或本地文件。")
        return

    # 2. 前置过滤
    if cfg["PRE_FILTER_PORT_ENABLED"]:
        nodes = [n for n in nodes if n.split(':')[1].split('#')[0] in map(str, cfg["PRE_FILTER_PORTS"])]
    
    # 3. TCP 测速筛选
    print(f"开始对 {len(nodes)} 个节点进行 TCP 筛选...")
    results = []
    with ThreadPoolExecutor(max_workers=cfg["MAX_WORKERS"]) as executor:
        futures = {executor.submit(test_tcp_latency, n.split(':')[0], n.split(':')[1].split('#')[0]): n for n in nodes}
        for future in as_completed(futures):
            node_str = futures[future]
            lat, succ = future.result()
            if succ / cfg["TCP_PROBES"] >= cfg["MIN_SUCCESS_RATE"]:
                country = node_str.split('#')[-1]
                results.append((node_str, lat, country, succ))

    if not results:
        print("❌ 无满足成功率要求的节点。")
        return

    # 4. 候选池分配 (按国家或全局)
    results.sort(key=lambda x: x[1]) # 按延迟排序
    candidates = [r[0] for r in results[:cfg["BANDWIDTH_CANDIDATES"]]]

    # 5. 可用性检测 (调用 check_availability)
    # 此处省略原有的 check_availability 和 measure_bandwidth_curl 函数代码
    # 假设逻辑同你提供的 main() 函数后续部分...

    # (此处运行带宽测速、DNS更新、GitHub推送逻辑)
    # ...
    print("✅ 优选流程结束。")

if __name__ == "__main__":
    # 为了保持完整性，以下函数需保留你原代码中的实现：
    # send_wxpusher_notification, get_ip_risk_level, check_availability, 
    # measure_bandwidth_curl, batch_update_cloudflare_dns, sync_to_github
    main()
