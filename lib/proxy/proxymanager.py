import requests
import time
import math
import asyncio
import aiohttp
import aiosocks
import random
import struct
import ipaddress
import os

from multiprocessing import Process
from aiosocks.connector import ProxyConnector, ProxyClientRequest
from psycopg2._psycopg import ProgrammingError

from utils.db import DBWorker
from lib.logger import Logger
from lib.generator.caching import ONE_MINUTE, Cacheable, cache

BAD_COUNTRIES = ('AZ', 'AM', 'BY', 'KZ', 'KG', 'RU', 'MD', 'TJ', 'TM', 'UZ', 'UA')

sql_insert_proxy = "INSERT INTO proxy  VALUES (DEFAULT, %s, %s, %s)"


class EmptyProxyList(BaseException):
    """
    If there are no available proxies.
    """


class BadProxy(BaseException):
    pass


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def convert_ip_address(value):
    """
    IP addresses are stored in DB as integers.
    Example:
        >>> convert_ip_address('127.0.0.1')
        2130706433
    """
    return struct.unpack('>I', ipaddress.IPv4Address(value).packed)[0]

def to_db(chunk, config):
        db = DBWorker(config)
        for proxy in chunk:
            try:
                ip, port = proxy.split(":")
                db.run_sql(sql_insert_proxy, [ip, port, 0])
            except ValueError:
                continue

class ProxyManager(Cacheable):
    def __init__(self, config, timeout=None, cache_url=None):
        self.__config = config
        self.__timeout = timeout if timeout else 5
        self.__logger = Logger(name="ProxyManager", color=True)
        self.__check_counter = 0
        self.__check_progress_bar = None
        self.__check_proxy_count = 0
        self.__proxy_load_time = 0
        self.__pool = []
        super().__init__(cache_url)

    def crawl_proxy(self, no_check=False):
        db = DBWorker(self.__config)
        db.run_sql("TRUNCATE TABLE proxy;")
        self.__logger.log_console("Fetch proxy from %s" % self.__config["producer"]["proxy_url"], status='+')
        proxy_data = None
        try:
            proxy_data = [x.rstrip("\n\r") for x in
                          requests.get(self.__config["producer"]["proxy_url"]).text.split("\n")]
        except requests.exceptions.ReadTimeout:
            self.__logger.log("Proxy url request. Timeout", status="~",
                              mformat=["%s" % self.__config["producer"]["proxy_url"]])
            return False

        except requests.exceptions.ConnectionError:
            self.__logger.log("Connection Error to proxy url. Down", status="~",
                              mformat=["%s" % self.__config["producer"]["proxy_url"]])
            return False

        except requests.exceptions.InvalidHeader:
            self.__logger.log("Invalid header from proxy server. Invalid header return", status="~",
                              mformat=["%s" % self.__config["producer"]["proxy_url"]])
            return False
        except requests.exceptions.ContentDecodingError:
            self.__logger.log("Proxy url request. Decode error", status="~",
                              mformat=["%s" % self.__config["producer"]["proxy_url"]])
        except IOError:
            self.__logger.log("Can't open proxy fetch file", status="~",
                              mformat=["%s" % self.__config["producer"]["proxy_url"]])
            return False
        self.__logger.log_console("Fetch %s proxies" % len(proxy_data), status='+')

        if no_check:
            self.__check_proxy_count = len(proxy_data)
            workers = []
            for chunk in chunks(proxy_data, 10000):
                pr = Process(target=to_db, args=(chunk, self.__config,))
                pr.start()
                workers.append(pr)
            self.__logger.log_console("Processes created. Count %s" % (len(workers)), status='+')
            self.__check_progress_bar = self.__logger.progress_bar(status='?')
            self.__check_proxy_count = len(proxy_data)
            next(self.__check_progress_bar)
            for worker in workers:
                worker.join()
                self.__check_counter += 1000
                if self.__check_counter % math.floor(self.__check_proxy_count / 100) == 0:
                        next(self.__check_progress_bar)
            try:
                next(self.__check_progress_bar)
            except StopIteration:
                pass
            self.__logger.log_console("No check data saved")
            return True

        self.__logger.log_console("Check proxies", status='+')
        checked_proxy = 0
        self.__check_progress_bar = self.__logger.progress_bar(status='?')
        self.__check_proxy_count = len(proxy_data)
        next(self.__check_progress_bar)

        loop = asyncio.get_event_loop()
        for chunk in chunks(proxy_data, 100):
            results = loop.run_until_complete(self._check_many(chunk, loop))
            for item in results[0]:
                result_check_record = item.result()
                if result_check_record["result"]:
                    db.run_sql(sql_insert_proxy, [result_check_record["ip"], result_check_record["port"],
                                                  result_check_record["elapsed"]])
                    checked_proxy += 1

        try:
            next(self.__check_progress_bar)
        except StopIteration:
            pass
        self.__logger.log_console("Checked %s/%s" % (checked_proxy, len(proxy_data)))
        return True

    async def _check_many(self, proxies, loop):
        tasks = [loop.create_task(self.async_check(proxy)) for proxy in proxies]
        return await asyncio.wait(tasks)

    async def async_check(self, proxy):
        start_time = time.time()
        try:
            ip, port = proxy.split(":")
            pr_connector = ProxyConnector(remote_resolve=True, verify_ssl=False)
            with aiohttp.ClientSession(connector=pr_connector, request_class=ProxyClientRequest) as session, \
                    aiohttp.Timeout(self.__timeout):
                async with session.get('http://www.httpbin.org/get?show_env=1',
                                       proxy="socks5://%s:%s" % (ip, port)) as resp:
                    await resp.json()

            result_dict = {
                'ip': ip,
                'port': port,
                'result': True,
                'elapsed': time.time() - start_time,
                'exc': None
            }
        except BaseException as exc:
            result_dict = {
                'ip': None,
                'port': None,
                'result': False,
                'elapsed': -1,
                'exc': exc
            }
        self.__check_counter += 1
        if self.__check_counter % math.floor(self.__check_proxy_count / 100) == 0:
            next(self.__check_progress_bar)
        return result_dict

    def __load_proxies(self):
        db = DBWorker(self.__config)
        with db.cursor() as cursor:
            cursor.execute("select*from proxy order by delay;")
            self.__proxy_load_time = time.time()
            return cursor.fetchall()

    def get(self):
        """
        Returns random proxy from the pool.
        """
        if not self.__pool or time.time() - self.__proxy_load_time > 30 * ONE_MINUTE:
            while True:
                pool = self.__load_proxies()
                self.__logger.log('Load proxy complete . Length %s' % len(pool))
                if len(pool) > 1000:
                    self.__pool = pool
                    break
                time.sleep(ONE_MINUTE)

        result = random.choice(self.__pool)
        return result

    def remove_from_pool(self, proxy):
        self.__pool.remove(proxy)

    def get_all(self):
        """
        Returns proxy, real IP address, timezone & language from the pool.
        """
        while True:
            try:
                _, ip, port, delay = self.get()
                answer = requests.get('http://httpbin.org/get?show_env=1',
                                      proxies=dict(http="socks5://%s:%s" % (ip, port)), timeout=self.__timeout).json()
                check_proxy = requests.get('http://www.iconspedia.com/',
                                      proxies=dict(http="socks5://%s:%s" % (ip, port)), timeout=5)
                proxy = "%s:%s" % (ip, port)
                if "http" not in proxy:
                    proxy = "http://" + proxy
                ip_address = answer["origin"]
                timezone, country, language = self.get_timezone_and_language(ip_address)
                if country in BAD_COUNTRIES:
                    raise BadProxy
                self.__logger.log('Chosen proxy: %s with external ip %s' % (proxy, ip_address))
                #open("/root/clickbot2/check_connect/good_"+str(os.getpid())+".txt","w")
                return proxy, ip_address, timezone, language
            except BadProxy:
                #open("/root/clickbot2/check_connect/bad_proxy_error_"+str(os.getpid())+".txt","w")
                print("Bad proxy")
                pass
            except requests.exceptions.ReadTimeout:
                #open("/root/clickbot2/check_connect/timeout_error_"+str(os.getpid())+".txt","w")
                print("Timeout")
                pass
            except requests.exceptions.ConnectionError:
                #open("/root/clickbot2/check_connect/connection_error_"+str(os.getpid())+".txt","w")
                print("Connection error")
                pass
            except requests.exceptions.InvalidHeader:
                #open("/root/clickbot2/check_connect/header_error_"+str(os.getpid())+".txt","w")
                print("InvalidHeader")
                pass
            except requests.exceptions.ContentDecodingError:
                #open("/root/clickbot2/check_connect/decoding_error_"+str(os.getpid())+".txt","w")
                print("Decoding error")
                pass

    @cache()
    def get_timezone_and_language(self, ip_address, default_time_zone_offset=0, default_language='en'):
        """
        Returns timezone for IP address or default if nothing was found.
        """
        db = DBWorker(self.__config)
        value = convert_ip_address(ip_address)
        with db.cursor() as cursor:
            cursor.execute('SELECT time_zone_offset, country_code, language FROM ip_data WHERE ip_range @> %s::BIGINT',
                           [value])
            try:
                time_zone_offset, country, language = cursor.fetchone()
                if time_zone_offset is None:
                    time_zone_offset = default_time_zone_offset
                if language is None:
                    language = default_language
                self.__logger.log('Timezone offset for %s: %s'% (ip_address, time_zone_offset))
                self.__logger.log('Language for %s: %s' % (ip_address, language))
                return time_zone_offset, country, language
            except (ProgrammingError, TypeError) as ex:
                # No results to fetch.
                return default_time_zone_offset, None, default_language
