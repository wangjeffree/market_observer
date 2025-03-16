import requests
import re
import json
import csv
import os
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import argparse
import logging
from logging.handlers import RotatingFileHandler

class PumpFunScraper:
    """
    用于从pump.fun网站抓取trending币种数据的爬虫
    """
    
    def __init__(self, interval_minutes=5, end_time=None, log_level=logging.INFO):
        """
        初始化爬虫
        
        Args:
            interval_minutes: 执行间隔时间（分钟）
            end_time: 结束时间，格式为 "HH:MM" 或 datetime 对象，如果为None则一直运行
            log_level: 日志级别，默认为 INFO
        """
        # 设置数据存储目录
        self.data_dir = self._get_data_dir()
        
        # 设置日志
        self._setup_logger(log_level)
        
        self.board_url = "https://pump.fun/board"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive"
        }
        
        # 设置历史数据文件名
        self.history_file = os.path.join(self.data_dir, "pump_fun_trending_coins_history.csv")
        
        # 设置执行间隔（分钟）
        self.interval_minutes = interval_minutes
        
        # 设置结束时间
        self.end_time = None
        if end_time:
            if isinstance(end_time, str):
                # 如果是字符串格式（如 "23:30"），转换为datetime对象
                now = datetime.now()
                hour, minute = map(int, end_time.split(':'))
                
                # 创建今天的结束时间
                today_end_time = datetime.combine(now.date(), datetime.min.time().replace(hour=hour, minute=minute))
                
                # 如果今天的结束时间已经过了，设置为明天的同一时间
                if today_end_time <= now:
                    tomorrow = now.date() + pd.Timedelta(days=1)
                    self.end_time = datetime.combine(tomorrow, datetime.min.time().replace(hour=hour, minute=minute))
                else:
                    self.end_time = today_end_time
                
                self.logger.info(f"设置结束时间为: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                # 如果是datetime对象，直接使用
                self.end_time = end_time
                self.logger.info(f"使用提供的datetime对象作为结束时间: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 设置运行标志
        self.is_running = False
        
    def _setup_logger(self, log_level):
        """
        设置日志记录器
        
        Args:
            log_level: 日志级别
        """
        # 创建日志目录
        log_dir = os.path.join(self.data_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        # 设置日志文件路径
        log_file = os.path.join(log_dir, "pump_fun_scraper.log")
        
        # 创建日志记录器
        self.logger = logging.getLogger("PumpFunScraper")
        self.logger.setLevel(log_level)
        
        # 清除现有的处理器
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # 创建文件处理器（使用 RotatingFileHandler 限制文件大小）
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,          # 保留5个备份文件
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # 设置日志格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器到日志记录器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # 防止日志传播到根日志记录器
        self.logger.propagate = False
        
        self.logger.info("日志系统初始化完成")
        
    def _get_data_dir(self):
        """获取数据存储目录"""
        # 获取项目根目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 数据存储目录改为当前项目路径下的data文件夹
        data_dir = os.path.join(current_dir, "data", "pump_fun")
        
        # 确保目录存在
        os.makedirs(data_dir, exist_ok=True)
        
        return data_dir
        
    def _get_date_based_filename(self, prefix, extension):
        """
        生成基于日期的文件名
        
        Args:
            prefix: 文件名前缀
            extension: 文件扩展名（不包含点）
            
        Returns:
            str: 生成的文件名
        """
        date_str = datetime.now().strftime("%Y%m%d")
        return f"{prefix}_{date_str}.{extension}"

    def _get_today_file(self, prefix, extension):
        """
        获取当天的文件路径
        
        Args:
            prefix: 文件名前缀
            extension: 文件扩展名（不包含点）
            
        Returns:
            str: 文件路径，如果文件不存在则返回None
        """
        filename = self._get_date_based_filename(prefix, extension)
        file_path = os.path.join(self.data_dir, filename)
        return file_path if os.path.exists(file_path) else None

    def save_html_content(self, html_content, filename=None):
        """保存HTML内容到文件"""
        if not filename:
            filename = self._get_date_based_filename("pump_fun_board_selenium", "html")
            
        # 构建完整文件路径
        file_path = os.path.join(self.data_dir, filename)
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        self.logger.info(f"HTML内容已保存到 {file_path}")
        
        return file_path
    
    def extract_coins_from_carousel(self, soup):
        """
        专门从CoinCarousel部分提取币种信息
        
        Args:
            soup: BeautifulSoup对象
            
        Returns:
            list: 包含币种信息的字典列表
        """
        coins_data = []
        
        # 查找CoinCarousel部分
        carousel = soup.find('section', attrs={'data-sentry-component': 'CoinCarousel'})
        if not carousel:
            self.logger.info("未找到CoinCarousel部分")
            return coins_data
            
        # 在CoinCarousel中查找所有币种链接
        coin_links = carousel.find_all('a', class_='carousel-card')
        if not coin_links:
            # 尝试其他可能的选择器
            coin_links = carousel.find_all('a', href=lambda href: href and '/coin/' in href)
            
        if not coin_links:
            self.logger.info("在CoinCarousel中未找到币种链接")
            return coins_data
            
        self.logger.info(f"在CoinCarousel中找到币种链接: {len(coin_links)}个")
        
        for link in coin_links:
            try:
                # 提取币种标题
                title_elem = link.find('div', class_=lambda c: c and 'line-clamp-2' in c)
                title = title_elem.text.strip() if title_elem else ""
                
                # 提取币种符号
                symbol_elem = link.find('span', class_=lambda c: c and 'text-[14px]' in c)
                symbol = symbol_elem.text.strip() if symbol_elem else ""
                
                # 不再拆分name和symbol，直接使用完整文本作为name
                name = symbol if symbol else title if title else link.text.strip()
                
                # 构建完整链接并提取ca_address
                coin_link = 'https://pump.fun' + link['href'] if link['href'].startswith('/') else link['href']
                ca_address = coin_link.split('/')[-1] if coin_link else ""
                
                # 提取市值 - 多种方法尝试
                market_value = "N/A"
                
                # 方法1: 查找包含"market cap: $"的元素
                market_cap_elem = link.find(string=lambda text: text and 'market cap:' in text.lower())
                if market_cap_elem:
                    # 获取父元素的完整文本
                    parent_text = market_cap_elem.parent.get_text()
                    # 尝试提取市值
                    if "$" in parent_text:
                        # 获取$后面的数字部分
                        market_value_text = parent_text.split("$", 1)[1].strip()
                        # 提取数字和单位(如M, K等)
                        import re
                        match = re.search(r'([0-9.]+)([A-Za-z]*)', market_value_text)
                        if match:
                            market_value = match.group(0)
                
                # 方法2: 如果方法1失败，尝试查找包含"market cap"的div元素
                if market_value == "N/A":
                    market_div = link.find('div', class_=lambda c: c and 'text-green-300' in c)
                    if market_div:
                        market_text = market_div.get_text()
                        if "market cap: $" in market_text:
                            # 提取市值数字部分
                            market_value_text = market_text.split("market cap: $")[1].strip()
                            # 提取数字和单位
                            import re
                            match = re.search(r'([0-9.]+)([A-Za-z]*)', market_value_text)
                            if match:
                                market_value = match.group(0)
                
                # 提取回复数
                reply_count = "N/A"
                # 方法1: 直接查找包含"replies:"的文本
                reply_elem = link.find(string=lambda text: text and 'replies:' in text.lower())
                if reply_elem:
                    reply_text = reply_elem.strip()
                    # 提取数字部分
                    import re
                    match = re.search(r'replies:\s*(\d+)', reply_text, re.IGNORECASE)
                    if match:
                        reply_count = match.group(1)
                    else:
                        reply_count = reply_text.replace("replies:", "").strip()
                
                # 方法2: 如果方法1失败，查找可能包含回复数的div
                if reply_count == "N/A":
                    reply_div = link.find('div', class_=lambda c: c and 'items-center' in c)
                    if reply_div and 'replies' in reply_div.get_text().lower():
                        reply_text = reply_div.get_text().strip()
                        # 提取数字部分
                        import re
                        match = re.search(r'replies:\s*(\d+)', reply_text, re.IGNORECASE)
                        if match:
                            reply_count = match.group(1)
                
                coins_data.append({
                    "name": name,
                    "title": title,
                    "market_value": market_value,
                    "reply_count": reply_count,
                    "link": coin_link,
                    "ca_address": ca_address
                })
                
                self.logger.info(f"提取币种: {name}, 市值: {market_value}, 回复数: {reply_count}, CA地址: {ca_address}")
            except Exception as e:
                self.logger.error(f"处理币种链接时出错: {str(e)}")
                
        return coins_data
    
    def save_to_csv(self, data, filename=None):
        """将数据保存为CSV文件"""
        if not data:
            self.logger.info("没有数据可保存")
            return
        
        if not filename:
            filename = self._get_date_based_filename("pump_fun_trending_coins", "csv")
        
        # 构建完整文件路径
        file_path = os.path.join(self.data_dir, filename)
        
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for row in data:
                    writer.writerow(row)
                
                self.logger.info(f"数据已保存到 {file_path}")
                return file_path
        except Exception as e:
            self.logger.error(f"保存CSV文件时出错: {str(e)}")
            return None
    
    def fetch_and_save(self, output_filename=None, save_html=True):
        """
        一次性爬取网页并生成提取币种的文件
        
        Args:
            output_filename: 输出CSV文件名，如果为None则使用基于日期的文件名
            save_html: 是否保存HTML内容
            
        Returns:
            tuple: (是否成功, 币种数据列表, CSV文件路径)
        """
        self.logger.info("开始一次性爬取网页并生成提取币种的文件...")
        
        try:
            html_content = None
            html_filename = None
            
            self.logger.info("使用Selenium进行爬取...")
            # 设置Chrome选项
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # 无头模式
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            # 初始化WebDriver，使用webdriver-manager自动管理ChromeDriver
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            
            try:
                # 访问网页
                self.logger.info(f"正在访问网页: {self.board_url}")
                driver.get(self.board_url)
                
                # 等待页面加载
                self.logger.info("等待页面加载...")
                time.sleep(5)  # 等待5秒
                
                # 获取页面内容
                html_content = driver.page_source
                
                # 保存HTML内容（如果需要）
                if save_html:
                    html_filename = self._get_date_based_filename("pump_fun_board_selenium", "html")
                    html_file_path = self.save_html_content(html_content, html_filename)
                    
            finally:
                # 关闭WebDriver
                driver.quit()
            
            # 解析HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 尝试提取币种信息
            coins_data = []
            
            # 从CoinCarousel部分提取
            self.logger.info("尝试从CoinCarousel部分提取币种信息...")
            carousel_coins = self.extract_coins_from_carousel(soup)
            if carousel_coins:
                self.logger.info(f"从CoinCarousel成功提取了{len(carousel_coins)}个币种信息")
                coins_data = carousel_coins
            else:
                # 查找所有币种链接
                self.logger.info("从CoinCarousel提取失败，尝试查找所有币种链接...")
                coin_links = soup.find_all('a', href=lambda href: href and '/coin/' in href)
                if coin_links:
                    self.logger.info(f"找到币种链接: {len(coin_links)}个")
                    temp_data = []
                    for link in coin_links[:20]:  # 只处理前20个
                        try:
                            # 提取币种信息
                            coin_text = link.text.strip()
                            coin_link = 'https://pump.fun' + link['href'] if link['href'].startswith('/') else link['href']
                            ca_address = coin_link.split('/')[-1] if coin_link else ""
                            
                            # 处理币种名称和符号
                            name = coin_text
                            symbol = ""
                            
                            # 尝试从文本中提取名称和符号
                            import re
                            match = re.match(r'([^(]+)\s*\(([^)]+)\)', coin_text)
                            if match:
                                name = match.group(1).strip()
                                symbol = match.group(2).strip()
                            
                            temp_data.append({
                                "name": name,
                                "symbol": symbol,
                                "title": "",
                                "market_value": "N/A",
                                "reply_count": "N/A",
                                "link": coin_link,
                                "ca_address": ca_address
                            })
                        except Exception as e:
                            self.logger.error(f"处理币种链接时出错: {str(e)}")
                    
                    if temp_data:
                        coins_data = temp_data
            
            # 保存数据到CSV
            csv_file_path = None
            if coins_data:
                if not output_filename:
                    output_filename = self._get_date_based_filename("pump_fun_trending_coins", "csv")
                
                csv_file_path = self.save_to_csv(coins_data, output_filename)
                self.logger.info(f"成功获取并保存了{len(coins_data)}个trending币种数据到 {csv_file_path}")
                
                # 与历史数据比对并更新
                self.compare_and_update_history(coins_data)
                
                return True, coins_data, csv_file_path
            else:
                self.logger.info("未能从网页中提取到trending币种数据")
                return False, [], None
                
        except Exception as e:
            self.logger.error(f"爬取和保存数据时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False, [], None
    
    def compare_and_update_history(self, new_coins_data):
        """
        将新提取的币种数据与历史数据比对，并根据比对结果更新历史数据和发送邮件通知
        
        Args:
            new_coins_data: 新提取的币种数据列表
        """
        self.logger.info("开始与历史数据比对...")
        
        # 提取新数据中的name列表
        new_names = [coin['name'].lower() for coin in new_coins_data if coin['name']]
        
        # 检查历史数据文件是否存在
        if not os.path.exists(self.history_file):
            self.logger.info(f"历史数据文件不存在，创建新文件: {self.history_file}")
            # 创建历史数据文件
            self.save_to_csv(new_coins_data, os.path.basename(self.history_file))
            # 发送新币种通知
            self.send_notification("new_coins", new_coins_data)
            return
        
        try:
            # 读取历史数据
            history_df = pd.read_csv(self.history_file)
            
            # 如果历史数据为空，直接保存新数据
            if history_df.empty:
                self.logger.info("历史数据为空，保存新数据")
                self.save_to_csv(new_coins_data, os.path.basename(self.history_file))
                # 发送新币种通知
                self.send_notification("new_coins", new_coins_data)
                return
            
            # 提取历史数据中的name列表，确保只处理字符串类型的值
            history_names = []
            for name in history_df['name'].tolist():
                # 检查是否为字符串类型且非空
                if isinstance(name, str) and name:
                    history_names.append(name.lower())
                elif isinstance(name, float) and not pd.isna(name):
                    # 如果是数值类型且非NaN，转换为字符串
                    history_names.append(str(name).lower())
            
            # 比对结果
            new_added_names = [name for name in new_names if name not in history_names]
            removed_names = [name for name in history_names if name not in new_names]
            
            # 根据比对结果处理
            if new_added_names:
                self.logger.info(f"发现新增币种: {', '.join(new_added_names)}")
                # 更新历史数据（追加新币种）
                new_coins_to_add = [coin for coin in new_coins_data if coin['name'].lower() in new_added_names]
                
                # 将新币种添加到历史数据中
                for coin in new_coins_to_add:
                    new_row = pd.DataFrame([coin])
                    history_df = pd.concat([history_df, new_row], ignore_index=True)
                
                # 保存更新后的历史数据
                history_df.to_csv(self.history_file, index=False)
                self.logger.info(f"已将新币种添加到历史数据文件: {self.history_file}")
                
                # 发送新币种通知
                self.send_notification("new_coins", new_coins_to_add)
                
            elif removed_names:
                self.logger.info(f"发现已移除币种: {', '.join(removed_names)}")
                # 更新历史数据（替换为新数据）
                self.save_to_csv(new_coins_data, os.path.basename(self.history_file))
                self.logger.info(f"已用新数据替换历史数据文件: {self.history_file}")
                
                # 发送币种移除通知
                removed_coins = [{'name': name} for name in removed_names]
                self.send_notification("removed_coins", removed_coins)
                
            else:
                self.logger.info("币种列表未发生变化，无需更新历史数据")
                
        except Exception as e:
            self.logger.error(f"比对和更新历史数据时出错: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def send_notification(self, notification_type, coins_data):
        """
        发送邮件通知
        
        Args:
            notification_type: 通知类型，'new_coins' 或 'removed_coins'
            coins_data: 币种数据列表
        """
        try:
            # 邮件内容
            if notification_type == "new_coins":
                subject = f"Pump.fun 新增币种通知 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                content = "Pump.fun 网站发现新增币种:\n\n"
                
                for coin in coins_data:
                    content += f"名称: {coin['name']}\n"
                    content += f"市值: {coin['market_value']}\n"
                    content += f"回复数: {coin['reply_count']}\n"
                    content += f"链接: {coin['link']}\n"
                    content += f"CA地址: \n\n{coin['ca_address']}\n\n"
                    content += "-" * 30 + "\n"
                    
            elif notification_type == "removed_coins":
                subject = f"Pump.fun 币种移除通知 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                content = "Pump.fun 网站发现以下币种已被移除:\n\n"
                
                for coin in coins_data:
                    content += f"名称: {coin['name']}\n"
                    content += "-" * 30 + "\n"
            else:
                self.logger.error(f"未知的通知类型: {notification_type}")
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
            self.logger.info("邮件发送成功")
            
            # 关闭SMTP对象
            smtp_obj.quit()
            
        except Exception as e:
            self.logger.error(f"发送邮件通知时出错: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def run(self, output_filename=None, save_html=True):
        """
        运行爬虫并保存数据
        
        Args:
            output_filename: 输出CSV文件名，如果为None则自动生成
            save_html: 是否保存HTML内容
            
        Returns:
            bool: 是否成功
        """
        self.logger.info("开始运行PumpFun爬虫...")
        
        try:
            # 一次性爬取并保存
            success, coins_data, csv_file_path = self.fetch_and_save(
                output_filename=output_filename,
                save_html=save_html
            )
            
            if success:
                self.logger.info(f"爬虫运行成功，共获取了{len(coins_data)}个trending币种数据")
                self.logger.info(f"数据已保存到: {csv_file_path}")
                return True
            else:
                self.logger.info("爬虫运行失败，未能获取trending币种数据")
                return False
                
        except Exception as e:
            self.logger.error(f"运行爬虫时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def test_extract_from_html_file(self, html_file_path=None):
        """
        从本地HTML文件测试提取币种信息
        
        Args:
            html_file_path: HTML文件路径，如果为None则使用当天的HTML文件
            
        Returns:
            list: 提取的币种信息列表
        """
        try:
            # 如果未提供文件路径，则使用当天的HTML文件
            if not html_file_path:
                html_file_path = self._get_today_file("pump_fun_board_selenium", "html")
                if not html_file_path:
                    self.logger.error("错误: 未找到今天的HTML文件")
                    return []
            
            # 如果提供的是相对路径，则转换为绝对路径
            if not os.path.isabs(html_file_path):
                html_file_path = os.path.join(self.data_dir, html_file_path)
                
            self.logger.info(f"使用HTML文件: {html_file_path}")
            
            if not os.path.exists(html_file_path):
                self.logger.error(f"错误: HTML文件不存在: {html_file_path}")
                return []
                
            with open(html_file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            self.logger.info(f"成功读取HTML文件: {html_file_path}")
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 使用专门的方法提取CoinCarousel中的币种信息
            coins_data = self.extract_coins_from_carousel(soup)
            
            if coins_data:
                self.logger.info(f"成功从HTML文件中提取了{len(coins_data)}个币种信息")
                # 保存到CSV
                csv_filename = self._get_date_based_filename("test_extracted_coins", "csv")
                csv_file_path = self.save_to_csv(coins_data, filename=csv_filename)
                self.logger.info(f"数据已保存到 {csv_file_path}")
                
                # 与历史数据比对并更新
                self.compare_and_update_history(coins_data)
                
                return coins_data
            else:
                self.logger.info("未能从HTML文件中提取币种信息")
                return []
                
        except Exception as e:
            self.logger.error(f"测试提取币种信息时出错: {str(e)}")
            return []

    def run_scheduled(self):
        """
        按照设定的时间间隔运行爬虫，直到超过结束时间
        """
        self.is_running = True
        self.logger.info(f"开始定时运行PumpFun爬虫，间隔时间：{self.interval_minutes}分钟")
        
        if self.end_time:
            self.logger.info(f"结束时间设置为：{self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            self.logger.info("未设置结束时间，将一直运行直到手动停止")
        
        try:
            run_count = 0
            while self.is_running:
                run_count += 1
                current_time = datetime.now()
                
                self.logger.info(f"\n===== 第{run_count}次运行 - {current_time.strftime('%Y-%m-%d %H:%M:%S')} =====")
                
                # 检查是否超过结束时间（精确到日期+时间）
                if self.end_time and current_time >= self.end_time:
                    self.logger.info(f"当前时间 {current_time.strftime('%Y-%m-%d %H:%M:%S')} 已超过结束时间 {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}，停止运行")
                    break
                
                # 运行爬虫
                success = self.run()
                
                if success:
                    self.logger.info(f"第{run_count}次运行成功")
                else:
                    self.logger.info(f"第{run_count}次运行失败")
                
                # 计算下次运行时间
                next_run_time = current_time + pd.Timedelta(minutes=self.interval_minutes)
                
                # 如果设置了结束时间且下次运行时间超过结束时间，则结束循环
                if self.end_time and next_run_time >= self.end_time:
                    self.logger.info(f"下次运行时间 {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} 将超过结束时间 {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}，停止运行")
                    break
                
                # 计算需要等待的秒数
                wait_seconds = (next_run_time - datetime.now()).total_seconds()
                
                if wait_seconds > 0:
                    self.logger.info(f"等待 {wait_seconds:.1f} 秒后进行下一次运行...")
                    # 每10秒检查一次是否应该停止
                    for _ in range(int(wait_seconds / 10) + 1):
                        if not self.is_running:
                            break
                        remaining = min(10, wait_seconds)
                        time.sleep(remaining)
                        wait_seconds -= remaining
                
            self.logger.info("定时运行已结束")
            
        except KeyboardInterrupt:
            self.logger.info("\n检测到键盘中断，停止运行")
            self.is_running = False
        except Exception as e:
            self.logger.error(f"定时运行时出错: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            self.is_running = False
    
    def stop(self):
        """停止定时运行"""
        self.is_running = False
        self.logger.info("已发出停止运行信号")

if __name__ == "__main__":
    # 暂不使用命令行
    args = argparse.Namespace()
    args.mode = "scheduled" # 
    interval_minutes = 0.5  # 修改为0.5分钟，即30秒
    end_time = datetime.strptime("2025-03-31 12:00:00", "%Y-%m-%d %H:%M:%S")
    
    # 不再设置根日志记录器，让类内部的日志处理即可
    # logging.basicConfig(
    #     level=logging.INFO,
    #     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    #     handlers=[
    #         logging.StreamHandler()
    #     ]
    # )
    # logger = logging.getLogger("PumpFunMain")
    
    scraper = PumpFunScraper(interval_minutes=interval_minutes, end_time=end_time)
    
    # 根据运行模式执行相应操作
    if args.mode == 'once':
        # 运行一次
        print("运行模式：单次运行")
        scraper.run()
    elif args.mode == 'scheduled':
        # 定时运行
        print("运行模式：定时运行")
        scraper.run_scheduled()
    elif args.mode == 'test_file':
        # 从HTML文件测试
        print("运行模式：从HTML文件测试")
        if hasattr(args, 'file') and args.file:
            scraper.test_extract_from_html_file(args.file)
        else:
            # 如果未提供文件路径，则自动查找最新的HTML文件
            scraper.test_extract_from_html_file() 
