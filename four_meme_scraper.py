import requests
from bs4 import BeautifulSoup
import json
import csv
from datetime import datetime, timedelta
import os
import time
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import argparse
import sys

# 交易量排名记录功能
# 在每次抓取交易量排名数据时，会将新数据与trading_volume_ranking_latest.csv文件中的最新数据进行比对
# 如果发现新增币种，则会发送邮件通知，并将新增币种添加到最新数据中
# 如果发现有币种被移除，会从最新数据中同步删除这些币种，并发送邮件通知
# 同时维护一个history文件trading_volume_ranking_history.csv，保存所有曾经出现在交易量排名中的币种（仅做追加，不做删除）
# 同时维护一个审计文件trading_volume_ranking_audit.csv，记录所有币种的新增和移除动作
# 程序通过检查历史文件判断币种是否为首次添加，发送相应通知
# 对于移除的币种，检查是否存在于历史记录中，若存在则记录到审计文件并发送通知

def send_notification(notification_type, coins_data):
    """
    发送邮件通知
    
    Args:
        notification_type: 通知类型，'new_coins' 或 'removed_coins'
        coins_data: 币种数据列表
    """
    try:
        # 邮件内容
        if notification_type == "new_coins":
            subject = f"Four.meme 新增交易量排名币种通知 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            content = "Four.meme 网站发现新增交易量排名币种:\n\n"
            
            for coin in coins_data:
                content += f"名称: {coin['name']}\n"
                if 'trading_volume_24h' in coin:
                    content += f"24小时交易量: {coin['trading_volume_24h']}\n"
                content += f"市值: {coin['market_cap']}\n"
                content += f"合约地址: \n\n{coin['contract_address']}\n\n"
                content += "-" * 30 + "\n"
        elif notification_type == "removed_coins":
            subject = f"Four.meme 交易量排名币种移除通知 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            content = "Four.meme 网站发现以下交易量排名币种已被移除:\n\n"
            
            for coin in coins_data:
                content += f"名称: {coin['name']}\n"
                content += f"合约地址: {coin['contract_address']}\n"
                content += "-" * 30 + "\n"
        else:
            print(f"未知的通知类型: {notification_type}")
            return
        
        # 发件人信息
        sender = '40734609@qq.com'  # 发件人邮箱
        password = 'uwjzzyolgcqdbiia'  # QQ邮箱授权码
        receivers = ['wangjifang@163.com']  # 收件人邮箱列表
        
        # 创建邮件对象
        message = MIMEText(content, 'plain', 'utf-8')
        message['From'] = Header(sender)
        message['To'] = Header(','.join(receivers))
        message['Subject'] = Header(subject, 'utf-8')
        
        # 连接QQ邮箱SMTP服务器
        smtp_obj = smtplib.SMTP_SSL('smtp.qq.com', 465)
        # 登录
        smtp_obj.login(sender, password)
        # 发送邮件
        smtp_obj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
        
        # 关闭SMTP对象
        smtp_obj.quit()
        
    except Exception as e:
        print(f"发送邮件通知时出错: {str(e)}")
        import traceback
        traceback.print_exc()

def compare_and_update_trading_volume_data(new_coins_data):
    """
    将新提取的交易量排名币种数据与最新数据比对，并根据比对结果更新数据和发送邮件通知
    处理新增和移除的币种，发送相应通知
    同时记录所有曾经出现的币种到历史文件中（仅做追加，不做删除）
    同时记录所有币种的增减变动到审计文件中
    通过检查历史文件判断币种是否为首次添加，以决定是否发送通知
    对于移除的币种，验证是否存在于历史记录中，若存在则发送通知
    
    Args:
        new_coins_data: 新提取的币种数据列表
    """
    print("开始与交易量排名最新数据比对...")
    
    # 创建输出目录
    output_dir = "data/four_meme"
    os.makedirs(output_dir, exist_ok=True)
    
    # 最新数据文件路径
    latest_file = os.path.join(output_dir, "trading_volume_ranking_latest.csv")
    
    # 历史数据文件路径（只追加，不删除）
    history_file = os.path.join(output_dir, "trading_volume_ranking_history.csv")
    
    # 审计文件路径
    audit_file = os.path.join(output_dir, "trading_volume_ranking_audit.csv")
    
    # CSV文件的列名
    latest_fieldnames = ['name', 'contract_address', 'market_cap', 'icon_url', 'trading_volume_24h']
    history_fieldnames = ['name', 'contract_address', 'market_cap', 'icon_url', 'trading_volume_24h', 'first_seen', 'last_seen']
    audit_fieldnames = ['timestamp', 'action', 'coin_name', 'contract_address', 'market_cap', 'type']
    
    # 当前时间戳
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 提取新数据中的合约地址列表
    new_contract_addresses = [coin['contract_address'].lower() for coin in new_coins_data if coin['contract_address']]
    
    try:
        # 读取最新数据
        latest_data = []
        if os.path.exists(latest_file):
            with open(latest_file, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    latest_data.append(row)
        
        # 读取历史数据，用于检查是否有币种需要添加到历史记录中
        history_data = []
        history_addresses = set()
        if os.path.exists(history_file):
            with open(history_file, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    history_data.append(row)
                    if row.get('contract_address'):
                        history_addresses.add(row['contract_address'].lower())
            
            print(f"从历史文件加载了 {len(history_addresses)} 个已知币种地址")
        
        # 如果最新数据文件为空或不存在，直接保存新数据
        if not latest_data:
            print("最新数据为空或不存在，创建新文件")
            with open(latest_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=latest_fieldnames)
                writer.writeheader()
                for coin in new_coins_data:
                    # 确保所有字段都存在
                    coin_data = {
                        'name': coin.get('name', ''),
                        'contract_address': coin.get('contract_address', ''),
                        'market_cap': coin.get('market_cap', ''),
                        'icon_url': coin.get('icon_url', ''),
                        'trading_volume_24h': coin.get('trading_volume_24h', '')
                    }
                    writer.writerow(coin_data)
            
            # 更新历史记录文件 - 追加所有不在历史记录中的新币种
            new_history_records = []
            for coin in new_coins_data:
                if coin.get('contract_address') and coin['contract_address'].lower() not in history_addresses:
                    history_data = {
                        'name': coin.get('name', ''),
                        'contract_address': coin.get('contract_address', ''),
                        'market_cap': coin.get('market_cap', ''),
                        'icon_url': coin.get('icon_url', ''),
                        'trading_volume_24h': coin.get('trading_volume_24h', ''),
                        'first_seen': current_time,
                        'last_seen': current_time
                    }
                    new_history_records.append(history_data)
                    history_addresses.add(coin['contract_address'].lower())
            
            # 追加新记录到历史文件
            if new_history_records:
                with open(history_file, 'a' if os.path.exists(history_file) else 'w', encoding='utf-8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=history_fieldnames)
                    if not os.path.exists(history_file) or os.path.getsize(history_file) == 0:
                        writer.writeheader()
                    writer.writerows(new_history_records)
                print(f"已将 {len(new_history_records)} 个新币种添加到历史记录: {history_file}")
            
            # 找出在历史记录中不存在的新币种
            first_time_coins = new_history_records
            
            # 添加记录到审计文件
            with open(audit_file, 'a' if os.path.exists(audit_file) else 'w', encoding='utf-8', newline='') as f:
                # 如果文件不存在，添加标题行
                writer = csv.DictWriter(f, fieldnames=audit_fieldnames)
                if not os.path.exists(audit_file) or os.path.getsize(audit_file) == 0:
                    writer.writeheader()
                
                # 为每个新增币种添加一条记录
                for coin in first_time_coins:
                    audit_row = {
                        'timestamp': current_time,
                        'action': 'initial',
                        'coin_name': coin.get('name', ''),
                        'contract_address': coin.get('contract_address', ''),
                        'market_cap': coin.get('market_cap', ''),
                        'type': 'added'
                    }
                    writer.writerow(audit_row)
            
            print(f"已将 {len(first_time_coins)} 个新增币种记录添加到审计文件: {audit_file}")
            
            # 只对历史记录中不存在的币种发送通知
            if first_time_coins:
                print(f"发送 {len(first_time_coins)} 个新增币种的通知")
                send_notification("new_coins", first_time_coins)
            else:
                print("所有新增币种都在历史记录中已存在，跳过通知")
            return
        
        # 提取历史数据中的合约地址列表
        latest_contract_addresses = [coin['contract_address'].lower() for coin in latest_data if coin['contract_address']]
        
        # 查找新增的合约地址
        new_added_contracts = [addr for addr in new_contract_addresses if addr not in latest_contract_addresses]
        
        # 查找移除的合约地址
        removed_contracts = [addr for addr in latest_contract_addresses if addr not in new_contract_addresses]
        
        # 如果有变动（新增或移除），则记录在审计文件中
        if new_added_contracts or removed_contracts:
            # 添加新增币种详情
            if new_added_contracts:
                print(f"发现新增交易量排名币种: {len(new_added_contracts)} 个")
                new_coins_to_add = [coin for coin in new_coins_data if coin['contract_address'].lower() in new_added_contracts]
                
                # 将新币种添加到最新数据中
                updated_latest = latest_data.copy()
                for coin in new_coins_to_add:
                    # 格式化新币种数据为CSV行
                    coin_data = {
                        'name': coin.get('name', ''),
                        'contract_address': coin.get('contract_address', ''),
                        'market_cap': coin.get('market_cap', ''),
                        'icon_url': coin.get('icon_url', ''),
                        'trading_volume_24h': coin.get('trading_volume_24h', '')
                    }
                    updated_latest.append(coin_data)
                
                # 保存更新后的最新数据
                with open(latest_file, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=latest_fieldnames)
                    writer.writeheader()
                    writer.writerows(updated_latest)
                print(f"已将新币种添加到最新数据文件: {latest_file}")
                
                # 找出在历史记录中不存在的新币种
                first_time_added_coins = []
                new_history_records = []
                
                for coin in new_coins_to_add:
                    if coin.get('contract_address') and coin['contract_address'].lower() not in history_addresses:
                        # 这是一个历史记录中不存在的币种，需要发送通知
                        first_time_added_coins.append(coin)
                        
                        # 准备添加到历史记录中
                        history_data = {
                            'name': coin.get('name', ''),
                            'contract_address': coin.get('contract_address', ''),
                            'market_cap': coin.get('market_cap', ''),
                            'icon_url': coin.get('icon_url', ''),
                            'trading_volume_24h': coin.get('trading_volume_24h', ''),
                            'first_seen': current_time,
                            'last_seen': current_time
                        }
                        new_history_records.append(history_data)
                        history_addresses.add(coin['contract_address'].lower())
                
                # 将新增币种记录到审计文件
                if first_time_added_coins:
                    with open(audit_file, 'a' if os.path.exists(audit_file) else 'w', encoding='utf-8', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=audit_fieldnames)
                        if not os.path.exists(audit_file) or os.path.getsize(audit_file) == 0:
                            writer.writeheader()
                        
                        for coin in first_time_added_coins:
                            audit_row = {
                                'timestamp': current_time,
                                'action': 'update',
                                'coin_name': coin.get('name', ''),
                                'contract_address': coin.get('contract_address', ''),
                                'market_cap': coin.get('market_cap', ''),
                                'type': 'added'
                            }
                            writer.writerow(audit_row)
                    
                    print(f"已将 {len(first_time_added_coins)} 个新增币种记录添加到审计文件: {audit_file}")
                    
                    # 只对历史记录中不存在的币种发送通知
                    print(f"发送 {len(first_time_added_coins)} 个新增币种的通知")
                    send_notification("new_coins", first_time_added_coins)
                else:
                    print("所有新增币种都在历史记录中已存在，跳过通知")
                
                # 追加新记录到历史文件
                if new_history_records:
                    with open(history_file, 'a' if os.path.exists(history_file) else 'w', encoding='utf-8', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=history_fieldnames)
                        if not os.path.exists(history_file) or os.path.getsize(history_file) == 0:
                            writer.writeheader()
                        writer.writerows(new_history_records)
                    print(f"已将 {len(new_history_records)} 个新币种添加到历史记录: {history_file}")
            
            # 处理移除的币种
            if removed_contracts:
                print(f"发现已移除交易量排名币种: {len(removed_contracts)} 个")
                removed_coins = [coin for coin in latest_data if coin['contract_address'].lower() in removed_contracts]
                
                # 从最新数据中删除被移除的币种
                updated_latest = [coin for coin in latest_data if coin['contract_address'].lower() not in removed_contracts]
                
                # 保存更新后的最新数据（已移除相关币种）
                with open(latest_file, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=latest_fieldnames)
                    writer.writeheader()
                    writer.writerows(updated_latest)
                print(f"已将 {len(removed_contracts)} 个币种从最新数据文件中移除: {latest_file}")
                
                # 找出在历史记录中存在且被移除的币种
                removed_coins_in_history = []
                
                for coin in removed_coins:
                    contract_address = coin.get('contract_address', '').lower()
                    if contract_address and contract_address in history_addresses:
                        # 这个币种在历史记录中存在，并且现在被移除了
                        removed_coins_in_history.append(coin)
                
                # 将移除的币种记录到审计文件
                if removed_coins_in_history:
                    with open(audit_file, 'a' if os.path.exists(audit_file) else 'w', encoding='utf-8', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=audit_fieldnames)
                        if not os.path.exists(audit_file) or os.path.getsize(audit_file) == 0:
                            writer.writeheader()
                        
                        for coin in removed_coins_in_history:
                            audit_row = {
                                'timestamp': current_time,
                                'action': 'update',
                                'coin_name': coin.get('name', ''),
                                'contract_address': coin.get('contract_address', ''),
                                'market_cap': coin.get('market_cap', '未知'),
                                'type': 'removed'
                            }
                            writer.writerow(audit_row)
                    
                    print(f"已将 {len(removed_coins_in_history)} 个移除币种记录添加到审计文件: {audit_file}")
                    
                    # 发送移除币种的通知
                    print(f"发送 {len(removed_coins_in_history)} 个移除币种的通知")
                    send_notification("removed_coins", removed_coins_in_history)
                else:
                    print("没有需要发送通知的移除币种")
            
            return
        else:
            print("未发现币种变动")
        
    except Exception as e:
        print(f"比对和更新数据时出错: {str(e)}")
        import traceback
        traceback.print_exc()

def scrape_four_meme_trading_volume():
    url = "https://four.meme/ranking"
    
    # 设置Chrome选项
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # 无头模式
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36")
    
    # 重试机制
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        driver = None
        try:
            print(f"正在初始化WebDriver... (尝试 {retry_count + 1}/{max_retries})")
            # 初始化WebDriver
            driver = webdriver.Chrome(options=chrome_options)
            
            print(f"正在访问页面: {url}")
            driver.get(url)
            
            # 等待页面加载
            print("等待页面加载...")
            wait = WebDriverWait(driver, 30)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # 页面加载后稍等一会，确保动态内容加载完成
            print("等待动态内容加载...")
            time.sleep(10)
            
            # 获取页面源码进行分析
            page_source = driver.page_source
            print(f"页面源码长度: {len(page_source)} 字符")
            
            # 保存页面源码用于调试
            debug_dir = "data/debug"
            os.makedirs(debug_dir, exist_ok=True)
            
            # 生成当天日期作为文件名的一部分
            date_today = datetime.now().strftime("%Y%m%d")
            
            with open(os.path.join(debug_dir, f"four_meme_page_{date_today}.html"), "w", encoding="utf-8") as f:
                f.write(page_source)
            print("已保存页面源码用于调试")
            
            # 使用BeautifulSoup解析页面
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 创建输出目录
            output_dir = "data/four_meme"
            os.makedirs(output_dir, exist_ok=True)
            
            # 查找标题元素
            titles = soup.find_all("h2", class_=lambda c: c and "t-600-16-primary" in c)
            
            for title in titles:
                # 找到相应的表格容器
                table_container = title.find_next("div", class_=lambda c: c and "gradient-primary" in c)
                if not table_container:
                    continue
                    
                title_text = title.text.strip()
                print(f"找到表格: {title_text}")
                
                # 获取表格中的所有币种
                coins = []
                coin_elements = table_container.find_all("a", class_="block hover:opacity-80")
                
                for coin in coin_elements:
                    try:
                        rows = coin.find("ul")
                        cols = rows.find_all("li")
                        
                        # 获取币种名称和符号
                        name_element = cols[1].find("div", class_="truncate")
                        name = name_element.text.strip() if name_element else "未知"
                        
                        # 获取市值
                        market_cap = cols[2].text.strip() if len(cols) > 2 else "未知"
                        
                        # 获取币种图标URL
                        img_element = cols[1].find("img")
                        icon_url = img_element["src"] if img_element else ""
                        
                        # 获取代币合约地址
                        contract_address = coin["href"].split("/")[-1] if coin.has_attr("href") else ""
                        
                        coin_info = {
                            "name": name,
                            "market_cap": market_cap,
                            "icon_url": icon_url,
                            "contract_address": contract_address
                        }
                        
                        # 如果是24小时交易量排名，添加交易量信息
                        if "Trading Volume" in title_text and len(cols) > 4:
                            coin_info["trading_volume_24h"] = cols[4].text.strip()
                        
                        # 获取支持的交易代币信息
                        token_img = None
                        if "Raised Token" in title_text or len(cols) > 3:
                            token_element = cols[3].find("span", class_=lambda c: c and "t-600-12-white" in c)
                            if token_element:
                                coin_info["raised_token"] = token_element.text.strip()
                                token_img = cols[3].find("img")
                                if token_img:
                                    coin_info["raised_token_icon"] = token_img["src"]
                        
                        coins.append(coin_info)
                        print(f"抓取到币种: {name} (市值: {market_cap})")
                    except Exception as e:
                        print(f"解析币种时出错: {str(e)}")
                
                # 保存结果，使用当天日期作为文件名
                if "MarketCap" in title_text:
                    output_file = os.path.join(output_dir, f"market_cap_ranking_{date_today}.csv")
                    title_type = "市值排名"
                elif "Trading Volume" in title_text:
                    output_file = os.path.join(output_dir, f"trading_volume_ranking_{date_today}.csv")
                    title_type = "交易量排名"
                    
                    # 如果是交易量排名，与历史记录进行比对
                    if coins:
                        compare_and_update_trading_volume_data(coins)
                else:
                    output_file = os.path.join(output_dir, f"other_ranking_{date_today}.csv")
                    title_type = "其他排名"
                
                # 确定所有可能的字段名
                all_fields = set()
                for coin in coins:
                    all_fields.update(coin.keys())
                fieldnames = sorted(list(all_fields))
                
                # 保存数据到CSV文件
                with open(output_file, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(coins)
                
                print(f"成功抓取 {len(coins)} 个{title_type}币种信息")
                print(f"数据已保存到: {output_file}")
            
            # 如果成功执行到这里，跳出重试循环
            break
            
        except Exception as e:
            retry_count += 1
            print(f"抓取过程中出现错误 (尝试 {retry_count}/{max_retries}): {str(e)}")
            import traceback
            traceback.print_exc()
            
            if retry_count < max_retries:
                print(f"等待5秒后重试...")
                time.sleep(5)
            else:
                print(f"已达到最大重试次数 ({max_retries})，抓取失败")
        finally:
            # 关闭浏览器
            if driver is not None:
                try:
                    print("关闭WebDriver...")
                    driver.quit()
                except:
                    print("WebDriver已关闭或无法关闭")

if __name__ == "__main__":
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='Four.meme 币种交易量排名数据抓取工具')
    parser.add_argument('--interval', type=int, default=30, help='循环扫描间隔（秒），默认为30秒')
    parser.add_argument('--end-time', type=str, help='结束时间（格式：YYYY-MM-DD HH:MM:SS），不指定则永久循环')
    parser.add_argument('--run-once', action='store_true', help='只运行一次，不循环')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 扫描间隔（秒）
    interval = args.interval
    
    # 处理结束时间
    end_time = None
    if args.end_time:
        try:
            end_time = datetime.strptime(args.end_time, "%Y-%m-%d %H:%M:%S")
            print(f"设定扫描结束时间为: {end_time}")
        except ValueError:
            print(f"错误: 结束时间格式不正确，应为 YYYY-MM-DD HH:MM:SS，例如: 2023-12-31 23:59:59")
            sys.exit(1)
    
    # 只运行一次
    if args.run_once:
        print("执行单次扫描...")
        scrape_four_meme_trading_volume()
        print("单次扫描完成")
        sys.exit(0)
    
    # 循环扫描
    print(f"开始循环扫描，间隔 {interval} 秒...")
    
    try:
        while True:
            # 检查是否到达结束时间
            if end_time and datetime.now() >= end_time:
                print(f"已到达设定的结束时间: {end_time}，程序结束")
                break
            
            # 执行扫描
            print(f"\n===== 开始新一轮扫描 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
            try:
                scrape_four_meme_trading_volume()
            except Exception as e:
                print(f"扫描过程中出错: {str(e)}")
                import traceback
                traceback.print_exc()
            
            # 输出下次扫描时间
            next_scan_time = datetime.now() + timedelta(seconds=interval)
            print(f"下次扫描时间: {next_scan_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 等待到下次扫描
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n接收到中断信号，程序结束")
    except Exception as e:
        print(f"程序出现异常: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        print("程序结束")
