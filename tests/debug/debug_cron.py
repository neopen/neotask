"""
@FileName: debug_cron.py
@Description: 
@Author: HiPeng
@Time: 2026/4/22 15:01
"""
import time

from neotask.scheduler import CronParser

if __name__ == '__main__':

    print(time.ctime())
    # 每分钟
    cron = CronParser.parse("* * * * *")
    print(cron.next())  # 下一分钟

    # 每5分钟
    cron = CronParser.parse("*/5 * * * *")
    print(cron.next())  # 下一个符合5的倍数的分钟

    # 每天9点
    cron = CronParser.parse("0 9 * * *")
    print(cron.next())  # 今天9点或明天9点

    # 工作日9点
    cron = CronParser.parse("0 9 * * 1-5")
    print(cron.next())  # 下一个工作日9点