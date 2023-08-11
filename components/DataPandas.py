class DataPandas:
    def response_code(self, code, panda):
        return int(((panda['response_code'] == f'{code}').sum()).tolist())
