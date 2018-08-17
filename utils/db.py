import psycopg2
import contextlib
import math

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from lib.logger import Logger

ip_data_query = '''
CREATE TABLE IF NOT EXISTS ip_data(
    ip_from bigint NOT NULL,
    ip_to bigint NOT NULL,
    country_code character(2) NOT NULL,
    country_name character varying(64) NOT NULL,
    region_name character varying(128) NOT NULL,
    city_name character varying(128) NOT NULL,
    latitude real NOT NULL,
    longitude real NOT NULL,
    zip_code character varying(30) NOT NULL,
    time_zone character varying(8) NOT NULL,
    CONSTRAINT ip2location_db11_pkey PRIMARY KEY (ip_from, ip_to)
);'''

create_proxy_table = ''' CREATE TABLE IF NOT EXISTS proxy(proxy_id SERIAL UNIQUE,
                         ip_address char(16), port int, delay int) '''


copy_ip_data_csv = '''COPY ip_data FROM STDIN WITH CSV QUOTE AS '"';'''

ADD_COLUMNS = '''
ALTER TABLE ip_data ADD COLUMN time_zone_offset INTEGER;
ALTER TABLE ip_data ADD COLUMN "language" VARCHAR(5);
ALTER TABLE ip_data ADD COLUMN ip_range int8range;'''
CREATE_IP_RANGE = '''
UPDATE ip_data SET ip_range = int8range(ip_from, ip_to, '[]');
ALTER TABLE ip_data ALTER COLUMN ip_range SET NOT NULL;'''
DROP_COLUMNS = '''
ALTER TABLE ip_data DROP COLUMN region_name;
ALTER TABLE ip_data DROP COLUMN city_name;
ALTER TABLE ip_data DROP COLUMN latitude;
ALTER TABLE ip_data DROP COLUMN longitude;
ALTER TABLE ip_data DROP COLUMN zip_code;
ALTER TABLE ip_data DROP COLUMN ip_from;
ALTER TABLE ip_data DROP COLUMN ip_to;
ALTER TABLE ip_data DROP COLUMN time_zone;
ALTER TABLE ip_data DROP COLUMN country_name;'''
CLEANUP = '''
CREATE INDEX ON ip_data USING gist (ip_range);
ALTER TABLE ip_data CLUSTER ON ip_data_ip_range_idx;'''

LANGUAGES = [
    ('AD', 'ca'),
    ('AE', 'ar-AE'),
    ('AF', 'fa-AF'),
    ('AG', 'en-AG'),
    ('AI', 'en-AI'),
    ('AL', 'sq'),
    ('AM', 'hy'),
    ('AO', 'pt-AO'),
    ('AQ', ''),
    ('AR', 'es-AR'),
    ('AS', 'en-AS'),
    ('AT', 'de-AT'),
    ('AU', 'en-AU'),
    ('AW', 'nl-AW'),
    ('AX', 'sv-AX'),
    ('AZ', 'az'),
    ('BA', 'bs'),
    ('BB', 'en-BB'),
    ('BD', 'bn-BD'),
    ('BE', 'nl-BE'),
    ('BF', 'fr-BF'),
    ('BG', 'bg'),
    ('BH', 'ar-BH'),
    ('BI', 'fr-BI'),
    ('BJ', 'fr-BJ'),
    ('BL', 'fr'),
    ('BM', 'en-BM'),
    ('BN', 'ms-BN'),
    ('BO', 'es-BO'),
    ('BQ', 'nl'),
    ('BR', 'pt-BR'),
    ('BS', 'en-BS'),
    ('BT', 'dz'),
    ('BV', ''),
    ('BW', 'en-BW'),
    ('BY', 'be'),
    ('BZ', 'en-BZ'),
    ('CA', 'en-CA'),
    ('CC', 'ms-CC'),
    ('CD', 'fr-CD'),
    ('CF', 'fr-CF'),
    ('CG', 'fr-CG'),
    ('CH', 'de-CH'),
    ('CI', 'fr-CI'),
    ('CK', 'en-CK'),
    ('CL', 'es-CL'),
    ('CM', 'en-CM'),
    ('CN', 'zh-CN'),
    ('CO', 'es-CO'),
    ('CR', 'es-CR'),
    ('CU', 'es-CU'),
    ('CV', 'pt-CV'),
    ('CW', 'nl'),
    ('CX', 'en'),
    ('CY', 'el-CY'),
    ('CZ', 'cs'),
    ('DE', 'de'),
    ('DJ', 'fr-DJ'),
    ('DK', 'da-DK'),
    ('DM', 'en-DM'),
    ('DO', 'es-DO'),
    ('DZ', 'ar-DZ'),
    ('EC', 'es-EC'),
    ('EE', 'et'),
    ('EG', 'ar-EG'),
    ('EH', 'ar'),
    ('ER', 'aa-ER'),
    ('ES', 'es-ES'),
    ('ET', 'am'),
    ('FI', 'fi-FI'),
    ('FJ', 'en-FJ'),
    ('FK', 'en-FK'),
    ('FM', 'en-FM'),
    ('FO', 'fo'),
    ('FR', 'fr-FR'),
    ('GA', 'fr-GA'),
    ('GB', 'en-GB'),
    ('GD', 'en-GD'),
    ('GE', 'ka'),
    ('GF', 'fr-GF'),
    ('GG', 'en'),
    ('GH', 'en-GH'),
    ('GI', 'en-GI'),
    ('GL', 'kl'),
    ('GM', 'en-GM'),
    ('GN', 'fr-GN'),
    ('GP', 'fr-GP'),
    ('GQ', 'es-GQ'),
    ('GR', 'el-GR'),
    ('GS', 'en'),
    ('GT', 'es-GT'),
    ('GU', 'en-GU'),
    ('GW', 'pt-GW'),
    ('GY', 'en-GY'),
    ('HK', 'zh-HK'),
    ('HM', ''),
    ('HN', 'es-HN'),
    ('HR', 'hr-HR'),
    ('HT', 'ht'),
    ('HU', 'hu-HU'),
    ('ID', 'id'),
    ('IE', 'en-IE'),
    ('IL', 'he'),
    ('IM', 'en'),
    ('IN', 'en-IN'),
    ('IO', 'en-IO'),
    ('IQ', 'ar-IQ'),
    ('IR', 'fa-IR'),
    ('IS', 'is'),
    ('IT', 'it-IT'),
    ('JE', 'en'),
    ('JM', 'en-JM'),
    ('JO', 'ar-JO'),
    ('JP', 'ja'),
    ('KE', 'en-KE'),
    ('KG', 'ky'),
    ('KH', 'km'),
    ('KI', 'en-KI'),
    ('KM', 'ar'),
    ('KN', 'en-KN'),
    ('KP', 'ko-KP'),
    ('KR', 'ko-KR'),
    ('XK', 'sq'),
    ('KW', 'ar-KW'),
    ('KY', 'en-KY'),
    ('KZ', 'kk'),
    ('LA', 'lo'),
    ('LB', 'ar-LB'),
    ('LC', 'en-LC'),
    ('LI', 'de-LI'),
    ('LK', 'si'),
    ('LR', 'en-LR'),
    ('LS', 'en-LS'),
    ('LT', 'lt'),
    ('LU', 'lb'),
    ('LV', 'lv'),
    ('LY', 'ar-LY'),
    ('MA', 'ar-MA'),
    ('MC', 'fr-MC'),
    ('MD', 'ro'),
    ('ME', 'sr'),
    ('MF', 'fr'),
    ('MG', 'fr-MG'),
    ('MH', 'mh'),
    ('MK', 'mk'),
    ('ML', 'fr-ML'),
    ('MM', 'my'),
    ('MN', 'mn'),
    ('MO', 'zh'),
    ('MP', 'fil'),
    ('MQ', 'fr-MQ'),
    ('MR', 'ar-MR'),
    ('MS', 'en-MS'),
    ('MT', 'mt'),
    ('MU', 'en-MU'),
    ('MV', 'dv'),
    ('MW', 'ny'),
    ('MX', 'es-MX'),
    ('MY', 'ms-MY'),
    ('MZ', 'pt-MZ'),
    ('NA', 'en-NA'),
    ('NC', 'fr-NC'),
    ('NE', 'fr-NE'),
    ('NF', 'en-NF'),
    ('NG', 'en-NG'),
    ('NI', 'es-NI'),
    ('NL', 'nl-NL'),
    ('NO', 'no'),
    ('NP', 'ne'),
    ('NR', 'na'),
    ('NU', 'niu'),
    ('NZ', 'en-NZ'),
    ('OM', 'ar-OM'),
    ('PA', 'es-PA'),
    ('PE', 'es-PE'),
    ('PF', 'fr-PF'),
    ('PG', 'en-PG'),
    ('PH', 'tl'),
    ('PK', 'ur-PK'),
    ('PL', 'pl'),
    ('PM', 'fr-PM'),
    ('PN', 'en-PN'),
    ('PR', 'en-PR'),
    ('PS', 'ar-PS'),
    ('PT', 'pt-PT'),
    ('PW', 'pau'),
    ('PY', 'es-PY'),
    ('QA', 'ar-QA'),
    ('RE', 'fr-RE'),
    ('RO', 'ro'),
    ('RS', 'sr'),
    ('RU', 'ru'),
    ('RW', 'rw'),
    ('SA', 'ar-SA'),
    ('SB', 'en-SB'),
    ('SC', 'en-SC'),
    ('SD', 'ar-SD'),
    ('SS', 'en'),
    ('SE', 'sv-SE'),
    ('SG', 'cmn'),
    ('SH', 'en-SH'),
    ('SI', 'sl'),
    ('SJ', 'no'),
    ('SK', 'sk'),
    ('SL', 'en-SL'),
    ('SM', 'it-SM'),
    ('SN', 'fr-SN'),
    ('SO', 'so-SO'),
    ('SR', 'nl-SR'),
    ('ST', 'pt-ST'),
    ('SV', 'es-SV'),
    ('SX', 'nl'),
    ('SY', 'ar-SY'),
    ('SZ', 'en-SZ'),
    ('TC', 'en-TC'),
    ('TD', 'fr-TD'),
    ('TF', 'fr'),
    ('TG', 'fr-TG'),
    ('TH', 'th'),
    ('TJ', 'tg'),
    ('TK', 'tkl'),
    ('TL', 'tet'),
    ('TM', 'tk'),
    ('TN', 'ar-TN'),
    ('TO', 'to'),
    ('TR', 'tr-TR'),
    ('TT', 'en-TT'),
    ('TV', 'tvl'),
    ('TW', 'zh-TW'),
    ('TZ', 'sw-TZ'),
    ('UA', 'uk'),
    ('UG', 'en-UG'),
    ('UM', 'en-UM'),
    ('US', 'en-US'),
    ('UY', 'es-UY'),
    ('UZ', 'uz'),
    ('VA', 'la'),
    ('VC', 'en-VC'),
    ('VE', 'es-VE'),
    ('VG', 'en-VG'),
    ('VI', 'en-VI'),
    ('VN', 'vi'),
    ('VU', 'bi'),
    ('WF', 'wls'),
    ('WS', 'sm'),
    ('YE', 'ar-YE'),
    ('YT', 'fr-YT'),
    ('ZA', 'zu'),
    ('ZM', 'en-ZM'),
    ('ZW', 'en-ZW'),
    ('CS', 'cu'),
    ('AN', 'nl-AN'),
]


def timezone_to_offset(value):
    multiplier = 1 if value.startswith('+') else -1
    value = value[1:]
    if not value:
        return None
    hours, minutes = value.split(':')
    return multiplier * (int(hours) * 60 + int(minutes))


class DBWorker:
    def __init__(self, config, autocommit_mode=False):
        self.__config = config
        self.__autocommit_mode = autocommit_mode
        self.__logger = Logger(name="DB", color=True)

    def __copy_from_csv(self, file):
        with psycopg2.connect(self.__config["producer"]["db_dsn"]) as connection:
            cur = connection.cursor()
            with open(file, 'r') as fd:
                cur.copy_expert(copy_ip_data_csv, fd)
            connection.commit()

    @contextlib.contextmanager
    def cursor(self):
        with psycopg2.connect(self.__config["producer"]["db_dsn"]) as connection:
            if self.__autocommit_mode:
                connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            yield connection.cursor()
            connection.commit()

    def run_sql(self, sql, params=None):
        with self.cursor() as cursor:
            cursor.execute(sql, params)

    def __update_timezone_offset(self):
        with self.cursor() as cursor:
            cursor.execute('SELECT DISTINCT time_zone from ip_data')
            time_zones = [item[0] for item in cursor.fetchall()]
            for time_zone in time_zones:
                offset = timezone_to_offset(time_zone)
                cursor.execute('UPDATE ip_data SET time_zone_offset = %s WHERE time_zone = %s', [offset, time_zone])

    def __update_language(self):
        with self.cursor() as cursor:
            pb = self.__logger.progress_bar(status='?')
            next(pb)
            check_counter = 0
            for country_code, language in LANGUAGES:
                cursor.execute('UPDATE ip_data SET language = %s WHERE country_code = %s', [language, country_code])
                check_counter += 1
                if check_counter % math.floor(len(LANGUAGES) / 100) == 0:
                    try:
                        next(pb)
                    except StopIteration:
                        pass
            try:
                next(pb)
            except StopIteration:
                pass

    def init_db(self, file):
        self.__logger.log_console("1/10 Create 'proxy' table", status='+')
        self.run_sql(create_proxy_table)
        self.__logger.log_console("2/10 Create 'ip_data' table", status='+')
        self.run_sql(ip_data_query)
        self.__logger.log_console("3/10 Copy csv to 'ip_data' table", status='+')
        self.__copy_from_csv(file)
        self.__logger.log_console("4/10 Add columns: time_zone_offset, language, ip_range to 'ip_data' table", status='+')
        self.run_sql(ADD_COLUMNS)
        self.__logger.log_console("5/10 Create ip ranges 'ip_data' table", status='+')
        self.run_sql(CREATE_IP_RANGE)
        self.__logger.log_console("6/10 Update language 'ip_data' table", status='+')
        self.__update_language()
        self.__logger.log_console("7/10 Update timezone offset 'ip_data' table", status='+')
        self.__update_timezone_offset()
        self.__logger.log_console("8/10 Drop columns 'ip_data' table", status='+')
        self.run_sql(DROP_COLUMNS)
        self.__logger.log_console("9/10 Cleanup 'ip_data' table", status='+')
        self.run_sql(CLEANUP)
        self.__logger.log_console("10/10 Vacuum data 'ip_data' table", status='+')
        self.run_sql('VACUUM FULL VERBOSE ANALYZE ip_data;')

    def reset_db(self, drop_proxy=False):
        self.__logger.log_console("Drop ip_data")
        self.run_sql("DROP TABLE ip_data;")
        if drop_proxy:
            self.__logger.log_console("Drop proxy")
            self.run_sql("DROP TABLE proxy;")

    def reset_proxy(self):
        self.run_sql("TRUNCATE proxy;")
