from urllib.parse import urlparse, urljoin
from scrapy.settings import Settings
import time
import pymysql.cursors

def validate_url(url):
    """
    Checks if a valid HTTP or HTTPS URL has been provided: does it have a protocol and netloc?  
    Returns boolean.  

    Arguments:  
    - url: URL to check  
    """
    o = urlparse(url)
    if((o.scheme == "http" or o.scheme == "https") and o.netloc != ""):
        return True
    return False

def is_internal(url,start_url):
    """
    Checks if a URL is internal: does it has the same scheme and netloc as the start url?  
    Returns boolean.  

    Arguments:  
    - url: URL to check  
    - start_url: reference URL to compare to  
    """
    u = urlparse(url)
    s = urlparse(start_url)
    if (u.scheme == s.scheme and u.netloc == s.netloc):
        return True
    return False

def get_dbname(basename):
    """
    Generates a database name by adding timestamp at the end.  
    """
    timestr = time.strftime("%Y%m%d-%H%M%S")
    return basename + '_' + timestr

def create_database(basename,host,port,user,password):
    """
    Creates crawl database.  
    """
    connection = pymysql.connect(host=host,
        port=int(port),
        user=user,
        password=password,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create database
            sql = "CREATE DATABASE `{}`;".format(basename)
            cursor.execute(sql)
        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

    finally:
        connection.close()

def create_urls_table(basename,host,port,user,password):
    """
    Creates urls table
    """
    # Establish connection
    connection = pymysql.connect(host=host,
        port=int(port),
        db=basename,
        user=user,
        password=password,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create links table
            sql = """
            CREATE TABLE `urls` (
                `id` int(11) NOT NULL AUTO_INCREMENT,
                `url` varchar(4096) NOT NULL,
                `response_code` int(11) NOT NULL DEFAULT '0',
                `content_type` varchar(128) DEFAULT NULL,
                `level` int(11) NOT NULL DEFAULT '-1',
                `referer` varchar(4096) DEFAULT NULL,
                `latency` float(11) DEFAULT '0',
                `crawled_at` varchar(128) DEFAULT '0',
                `nb_title` int(11) NOT NULL DEFAULT '0',
                `title` varchar(512) DEFAULT NULL,
                `nb_meta_robots` int(11) NOT NULL DEFAULT '0',
                `meta_robots` varchar(256) DEFAULT NULL,
                `meta_description` varchar(1024) DEFAULT NULL,
                `meta_viewport` varchar(256) DEFAULT NULL,
                `meta_keywords` varchar(256) DEFAULT NULL,
                `canonical` varchar(4096) DEFAULT NULL,
                `prev` varchar(4096) DEFAULT NULL,
                `next` varchar(4096) DEFAULT NULL,
                `h1` varchar(512) DEFAULT NULL,
                `nb_h1` int(11) NOT NULL DEFAULT '0',
                `nb_h2` int(11) NOT NULL DEFAULT '0',
                `wordcount` int(11) DEFAULT '0',
                `content` text DEFAULT NULL,
                `XRobotsTag` varchar(256) DEFAULT NULL,
                `outlinks` int(11) DEFAULT '0',
                `x_cache` varchar(256) DEFAULT NULL,
                `http_date` varchar(256) DEFAULT NULL,
                `size` int(11) DEFAULT NULL,
                `html_lang` varchar(128) DEFAULT NULL,
                `content_lang` varchar(128) DEFAULT NULL,
                `content_lang_note` float(11) DEFAULT '0',
                `hreflangs` text DEFAULT NULL,
                `microdata` text DEFAULT NULL,
                `extractors` text DEFAULT NULL,
                `request_headers` text DEFAULT NULL,
                `response_headers` text DEFAULT NULL,
                `redirect` varchar(4096) DEFAULT NULL,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
            """
            cursor.execute(sql)
        connection.commit()

    finally:
        connection.close()

def create_links_table(basename,host,port,user,password):
    """
    Creates links table
    """
    # Establish connection
    connection = pymysql.connect(host=host,
        port=int(port),
        db=basename,
        user=user,
        password=password,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create links table
            sql = """
            CREATE TABLE `links` (
                `id` int(11) NOT NULL AUTO_INCREMENT,
                `source` varchar(4096) NOT NULL,
                `target` varchar(4096) NOT NULL,
                `text` varchar(1024) NOT NULL,
                `weight` float DEFAULT '1',
                `nofollow` tinyint(1) DEFAULT NULL,
                `disallow` tinyint(1) DEFAULT NULL,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
            """
            cursor.execute(sql)
        connection.commit()

    finally:
        connection.close()

def get_settings():
    """
    Creates Scrapy Settings object and sets basic values.
    Other values will be set in the project config file.
    """
    settings = Settings({
        # Crawling URLs from the same level before going deeper
        'DEPTH_PRIORITY': 1, # Don't touch
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue', # Don't touch
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue', # Don't touch

        # Internal Scrapy stuff
        'HTTPERROR_ALLOW_ALL': True, # Allows to store results for non-200 URLs
        'RETRY_ENABLED': False,
        'MEDIA_ALLOW_REDIRECTS' : True,
        'LOG_LEVEL': 'INFO',
    })

    return settings