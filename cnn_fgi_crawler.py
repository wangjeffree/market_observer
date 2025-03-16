#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
from datetime import datetime
from fear_and_greed import get

# 添加项目根目录到Python路径
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
# sys.path.append(project_root)

def get_latest_fgi_and_update_csv():
    latest_fgi = get();
    print(latest_fgi.value)
    print(latest_fgi.last_update.strftime('%Y-%m-%d'))

    # 更新CSV文件
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    csv_path = os.path.join(data_dir, 'cnn_fear_greed_index.csv')
    
    # 如果文件不存在，创建一个空的DataFrame
    if not os.path.exists(csv_path):
        df = pd.DataFrame(columns=['date', 'value'])
        df.to_csv(csv_path, index=False)
    
    df = pd.read_csv(csv_path)
    new_row = pd.DataFrame([{'date': latest_fgi.last_update.strftime('%Y-%m-%d'), 'value': latest_fgi.value}])

    # 检查是否已存在该日期的数据
    date = new_row['date'].iloc[0]
    if date in df['date'].values:
        # 如果存在,则更新对应日期的value值
        df.loc[df['date'] == date, 'value'] = new_row['value'].iloc[0]
    else:
        # 如果不存在,则追加新行
        df = pd.concat([df, new_row], ignore_index=True)
    
    # 保存更新后的数据
    df.to_csv(csv_path, index=False)
    
    send_fgi_alert_email(latest_fgi.value)

def send_fgi_alert_email(fgi_value):
    """
    当FGI值超出阈值时发送邮件提醒
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.header import Header
    
    # 阈值设置
    LOW_1_THRESHOLD = 28
    LOW_2_THRESHOLD = 38  
    LOW_3_THRESHOLD = 45
    HIGH_1_THRESHOLD = 56
    HIGH_2_THRESHOLD = 65
    HIGH_3_THRESHOLD = 78

    # 检查是否需要发送提醒
    need_alert = False
    alert_type = ""
    
    if fgi_value <= LOW_1_THRESHOLD:
        need_alert = True
        alert_type = f"低于LOW_1阈值({LOW_1_THRESHOLD})"
    elif fgi_value <= LOW_2_THRESHOLD:
        need_alert = True  
        alert_type = f"低于LOW_2阈值({LOW_2_THRESHOLD})"
    elif fgi_value <= LOW_3_THRESHOLD:
        need_alert = True
        alert_type = f"低于LOW_3阈值({LOW_3_THRESHOLD})"
    elif fgi_value >= HIGH_3_THRESHOLD:
        need_alert = True
        alert_type = f"高于HIGH_1阈值({HIGH_3_THRESHOLD})"
    elif fgi_value >= HIGH_2_THRESHOLD:
        need_alert = True
        alert_type = f"高于HIGH_2阈值({HIGH_2_THRESHOLD})"
    elif fgi_value >= HIGH_1_THRESHOLD:
        need_alert = True
        alert_type = f"高于HIGH_3阈值({HIGH_1_THRESHOLD})"

    if need_alert:
        # 邮件内容
        content = f"""
        CNN恐惧贪婪指数提醒
        
        当前FGI值: {fgi_value}
        提醒原因: {alert_type}
        时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        # 发件人信息
        sender = '40734609@qq.com'  # 发件人邮箱
        password = 'uwjzzyolgcqdbiia'  # QQ邮箱授权码
        receivers = ['wangjifang@163.com']  # 收件人邮箱列表

        # 创建邮件对象
        message = MIMEText(content, 'plain', 'utf-8')
        message['From'] = Header(sender)
        message['To'] = Header(','.join(receivers))
        message['Subject'] = Header('CNN Fear & Greed Index Alert[' + str(fgi_value) + ']', 'utf-8')

        try:
            # 连接QQ邮箱SMTP服务器
            smtp_obj = smtplib.SMTP_SSL('smtp.qq.com', 465)
            # 登录
            smtp_obj.login(sender, password)
            # 发送邮件
            smtp_obj.sendmail(sender, receivers, message.as_string())
            print("邮件发送成功")
        except smtplib.SMTPException as e:
            print(f"邮件发送失败: {str(e)}")
        finally:
            # 关闭SMTP对象
            smtp_obj.quit()


def main():
    get_latest_fgi_and_update_csv() 


if __name__ == "__main__":
    main() 
