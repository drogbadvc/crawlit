from pymysql.cursors import DictCursor
from pymysql import OperationalError
from pymysql.constants.CR import CR_SERVER_GONE_ERROR,  CR_SERVER_LOST, CR_CONNECTION_ERROR
from twisted.internet import defer
from twisted.enterprise import adbapi
from scrapy.exporters import CsvItemExporter
import copy
import logging

class CrowlMySQLPipeline:
    """
    Stores crawled data into MySQL.  
    Inspired by https://github.com/IaroslavR/scrapy-mysql-pipeline  
    """
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        self.logger = logging.getLogger(__name__)
        self.stats = crawler.stats
        self.settings = crawler.settings
        db_args = {
            'host': self.settings.get('MYSQL_HOST', 'localhost'),
            'port': int(self.settings.get('MYSQL_PORT', 3306)),
            'user': self.settings.get('MYSQL_USER', None),
            'password': self.settings.get('MYSQL_PASSWORD', ''),
            'db': self.settings.get('OUTPUT_NAME', None),
            'charset': 'utf8',
            'cursorclass': DictCursor,
            'cp_reconnect': True,
        }
        self.retries = self.settings.get('MYSQL_RETRIES', 3)
        self.close_on_error = self.settings.get('MYSQL_CLOSE_ON_ERROR', True)
        self.upsert = self.settings.get('MYSQL_UPSERT', False)
        self.urls_table = self.settings.get('MYSQL_URLS_TABLE', 'urls')
        self.links_table = self.settings.get('MYSQL_LINKS_TABLE', 'links')
        self.db = adbapi.ConnectionPool('pymysql', **db_args)

    def close_spider(self, spider):
        self.db.close()

    @staticmethod
    def preprocess_item(item):
        """Can be useful with extremly straight-line spiders design without item loaders or items at all
        CAVEAT: On my opinion if you want to write something here - you must read
        http://scrapy.readthedocs.io/en/latest/topics/loaders.html before
        """
        return item

    def postprocess_item(self, *args):
        """Can be useful if you need to update query tables depends of mysql query result"""
        pass

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        retries = self.retries
        status = False
        while retries:
            try:
                item = self.preprocess_item(item)
                yield self.db.runInteraction(self._process_item, item)
            except OperationalError as e:
                if e.args[0] in (
                        CR_SERVER_GONE_ERROR,
                        CR_SERVER_LOST,
                        CR_CONNECTION_ERROR,
                ):
                    retries -= 1
                    self.logger.info('%s %s attempts to reconnect left', e, retries)
                    self.stats.inc_value('{}/reconnects'.format(self.stats_name))
                    continue
                self.logger.exception('%s', pprint.pformat(item))
                self.stats.inc_value('{}/errors'.format(self.stats_name))
            except Exception:
                self.logger.exception('%s', pprint.pformat(item))
                self.stats.inc_value('{}/errors'.format(self.stats_name))
            else:
                status = True  # executed without errors
            break
        else:
            if self.close_on_error:  # Close spider if connection error happened and MYSQL_CLOSE_ON_ERROR = True
                spider.crawler.engine.close_spider(spider, '{}_fatal_error'.format(self.stats_name))
        self.postprocess_item(item, status)
        yield item

    def _generate_sql(self, data, table):
        """
        Added a `table` argument to switch between urls and links tables.  
        """
        columns = lambda d: ', '.join(['`{}`'.format(k) for k in d])
        values = lambda d: [v for v in d.values()]
        placeholders = lambda d: ', '.join(['%s'] * len(d))
        if self.upsert:
            sql_template = 'INSERT INTO `{}` ( {} ) VALUES ( {} ) ON DUPLICATE KEY UPDATE {}'
            on_duplicate_placeholders = lambda d: ', '.join(['`{}` = %s'.format(k) for k in d])
            return (
                sql_template.format(
                    table, columns(data),
                    placeholders(data), on_duplicate_placeholders(data)
                ),
                values(data) + values(data)
            )
        else:
            sql_template = 'INSERT INTO `{}` ( {} ) VALUES ( {} )'
            return (
                sql_template.format(table, columns(data), placeholders(data)),
                values(data)
            )

    def _process_item(self, tx, row):
        # Prevents crushing data before yielding item
        tmprow = copy.deepcopy(row) 
        # First we insert the links  
        if tmprow.get('outlinks'):
            links = tmprow['outlinks']
            for link in links:
                sql, data = self._generate_sql(link,self.links_table)
                try:
                    tx.execute(sql, data)
                except Exception:
                    self.logger.error("SQL: %s", sql)
                    raise

            # Replace outlinks dict with count of outlinks before inserting url data
            tmprow['outlinks'] = len(links)
        sql, data = self._generate_sql(tmprow,self.urls_table)
        try:
            tx.execute(sql, data)
        except Exception:
            self.logger.error("SQL: %s", sql)
            raise


class CrowlCsvPipeline:
    """
    Writes data to CSV files.
    """
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)
    
    def __init__(self, crawler):
        self.logger = logging.getLogger(__name__)
        self.stats = crawler.stats
        self.settings = crawler.settings

        self.urls_file = open('{}_urls.csv'.format(self.settings.get('OUTPUT_NAME', 'output')), 'ab')
        self.urls_exporter   = CsvItemExporter(self.urls_file, include_headers_line=True)
        # Listing the fields ensures their order stays the same. Be sure to update the list if you add more fields!
        self.urls_exporter.fields_to_export = [
            'url',
            'response_code',
            'content_type',
            'level',
            'referer',
            'latency',
            'crawled_at',
            'nb_title',
            'title',
            'nb_meta_robots',
            'meta_robots',
            'meta_description',
            'meta_viewport',
            'meta_keywords',
            'canonical',
            'prev',
            'next',
            'h1',
            'nb_h1',
            'nb_h2',
            'wordcount',
            'content',
            'content_lang',
            'XRobotsTag',
            'outlinks',
            'http_date',
            'size',
            'html_lang',
            'hreflangs',
            'microdata',
            'extractors',
            'request_headers',
            'response_headers',
            'redirect',
            'pagerank'
        ]
        self.urls_exporter.start_exporting()

        self.links_file = open('{}_links.csv'.format(self.settings.get('OUTPUT_NAME', 'output')), 'ab')
        self.links_exporter   = CsvItemExporter(self.links_file, include_headers_line=True)
        self.links_exporter.fields_to_export = [
            'source',
            'target',
            'text',
            'nofollow',
            'disallow',
        ]
        self.links_exporter.start_exporting()

    def close_spider(self, spider):
        self.urls_exporter.finish_exporting()
        self.links_exporter.finish_exporting()
        self.urls_file.close()
        self.links_file.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        # Prevents crushing data before yielding item
        tmprow = copy.deepcopy(item) 
        # First we insert the links  
        if tmprow.get('outlinks'):
            links = tmprow['outlinks']
            for link in links:
                self.links_exporter.export_item(link)

            # Replace outlinks dict with count of outlinks before inserting url data
            tmprow['outlinks'] = len(links)
        self.urls_exporter.export_item(tmprow)

        yield item
