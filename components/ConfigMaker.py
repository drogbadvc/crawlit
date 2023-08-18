import configparser


class cfgMaker:
    def cfg(self, datas, root):
        config = configparser.RawConfigParser()
        config.optionxform = str
        config['PROJECT'] = {'PROJECT_NAME': 'crowl',
                             'START_URL': datas[0]}
        config['CRAWLER'] = {'USER_AGENT': 'Crowl (+https://www.crowl.tech/)',
                             'ROTATE_USER_AGENTS': False,
                             'DOWNLOAD_DELAY': 0.5,
                             'CONCURRENT_REQUESTS': datas[1]}
        config['EXTRACTION'] = {'LINKS': True,
                                'LINKS_UNIQUE': False,
                                'CONTENT': True,
                                'DEPTH': datas[2],
                                'MAX_REQUESTS': 20000,
                                'CHECK_LANG': datas[3],
                                'SURFER': datas[4]
                                }
        config['OUTPUT'] = {'crowl.CrowlCsvPipeline': 100}

        with open(root + '/config.ini', 'w') as configfile:
            config.write(configfile)
