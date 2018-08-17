import argparse
import os.path
from time import sleep

from lib.logger import Logger
from lib.config import Config
from lib.proxy.proxymanager import ProxyManager
from lib.generator.generator import TaskGenerator
from lib.pyqtbot.consumer import Worker
from lib.starter import RunManager
from utils.db import DBWorker

main_logger = Logger(name="Main", color=True, logfile="data/logs/main.log")


def load_config(path):
    if not os.path.isfile(path):
        main_logger.log_console("Config file does not exists '%s'" % path, status="~")
        return None
    config = Config(path)
    if not config.load():
        return None
    return config


def db_handler(db_namespace):
    if not db_namespace.config:
        main_logger.log_console("Config file not set. See -h", status="~")
        return
    config = load_config(db_namespace.config)
    if not config:
        return

    db = DBWorker(config.config, autocommit_mode=True)
    if db_namespace.import_db:
        if not db_namespace.ip_data_file:
            main_logger.log_console("Ip data file not set. See -h", status="~")
            return
        db.init_db(db_namespace.ip_data_file)
    elif db_namespace.reset_db:
        db.reset_db(drop_proxy=db_namespace.drop_proxy)
    elif db_namespace.reset_proxy:
        db.reset_proxy()
    else:
        main_logger.log_console("DB action undefined. See -h", status="~")


def proxies_handler(p_namespace):
    if not p_namespace.config:
        main_logger.log_console("Config file not set. See -h", status="~")
        return
    config = load_config(p_namespace.config)
    if not config:
        return
    p = ProxyManager(config.config, timeout=p_namespace.timeout)
    if p_namespace.crawl:
        if p_namespace.automate:
            while True:
                p.crawl_proxy(no_check=p_namespace.no_check)
                main_logger.log("Crawl proxies complete. Sleep on %s min" % p_namespace.automate)
                sleep(60 * p_namespace.automate)

        p.crawl_proxy(no_check=p_namespace.no_check)
    else:
        main_logger.log_console("Proxies action undefined. See -h", status="~")


def generator_handler(g_namespace):
    if not g_namespace.config:
        main_logger.log_console("Config file not set. See -h", status="~")
        return
    config = load_config(g_namespace.config).config
    g = TaskGenerator(config)
    g.run()


def consume_handler(c_namespace):
    if not c_namespace.config:
        main_logger.log_console("Config file not set. See -h", status="~")
        return
    config = load_config(c_namespace.config).config
    c = Worker(config)
    c.run()


def start_handler(s_namespace):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    r = RunManager(consumer_sh=dir_path + "/consume.sh", producer_sh=dir_path + "/produce.sh")
    r.start(s_namespace.generators, s_namespace.consumers, from_systemd=s_namespace.from_systemd)


def stop_handler(s_namespace):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    r = RunManager(consumer_sh=dir_path + "/consume.sh", producer_sh=dir_path + "/produce.sh")
    r.stop(s_namespace.generators, s_namespace.consumers, from_systemd=s_namespace.from_systemd)

def delete_handler(s_namespace):
    r = RunManager()
    r.delete(s_namespace.generators, s_namespace.consumers, from_systemd=s_namespace.from_systemd)

if __name__ == '__main__':
    main_parser = argparse.ArgumentParser(prog="Eskalina ClickBot mod (c) 2017")
    sub_parsers = main_parser.add_subparsers()

    db_parser = sub_parsers.add_parser("db", help="ClickBot::db. Import or reset proxy database")
    db_parser.add_argument("-c", "--config", action="store", help="Path to config file")
    db_parser.add_argument("--import-db", action="store_true", help="Import db proxy")
    db_parser.add_argument("--ip-data-file", action="store", help="Ip data file for import")
    db_parser.add_argument("--reset-db", action="store_true", help="Reset db ")
    db_parser.add_argument("--drop-proxy", action="store_true", help="Reset db proxy table")
    db_parser.add_argument("--reset-proxy", action="store_true", help="Reset data proxy",)
    db_parser.set_defaults(func=db_handler)

    proxy_parser = sub_parsers.add_parser("proxies", help="ClickBot::proxies. Crawl proxies or stat proxies")
    proxy_parser.add_argument("-c", "--config", action="store", help="Path to config file")
    proxy_parser.add_argument("--crawl", action="store_true", help="Crawl proxies")
    proxy_parser.add_argument("--no-check", action="store_true", help="Disable check proxies")
    proxy_parser.add_argument("--automate", action="store", type=int, help="Set automate delay recheck proxy")
    proxy_parser.add_argument("--timeout", action="store", help="Crawl proxy timeout")
    proxy_parser.set_defaults(func=proxies_handler)

    generate_parser = sub_parsers.add_parser("generate", help="ClickBot::generate. Start process generating tasks")
    generate_parser.add_argument("-c", "--config", action="store", help="Path to config file")
    generate_parser.set_defaults(func=generator_handler)

    consumer_parser = sub_parsers.add_parser("consume", help="ClickBot::consume. Start process clicking")
    consumer_parser.add_argument("-c", "--config", action="store", help="Path to config file")
    consumer_parser.set_defaults(func=consume_handler)

    start_parser = sub_parsers.add_parser("start", help="ClickBot::start. Start bot")
    start_parser.add_argument("--consumers", action="store", type=int, help="Count of consumers")
    start_parser.add_argument("--generators", action="store", type=int, help="Count of generators")
    start_parser.add_argument("--from-systemd", action="store_true", help="Run from systemd")
    start_parser.set_defaults(func=start_handler)

    stop_parser = sub_parsers.add_parser("stop", help="ClickBot::stop. Stop bot")
    stop_parser.add_argument("--consumers", action="store", type=int, help="Count of consumers")
    stop_parser.add_argument("--generators", action="store", type=int, help="Count of generators")
    stop_parser.add_argument("--from-systemd", action="store_true", help="Run from systemd")
    stop_parser.set_defaults(func=stop_handler)
    delete_parser = sub_parsers.add_parser("delete", help="ClickBot::delete. Delete")
    delete_parser.add_argument("--consumers", action="store", type=int, help="Count of consumers")
    delete_parser.add_argument("--generators", action="store", type=int, help="Count of generators")
    delete_parser.add_argument("--from-systemd", action="store_true", help="Run from systemd")
    delete_parser.set_defaults(func=delete_handler)
    args = main_parser.parse_args()
    args.func(args)
