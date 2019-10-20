# -*- coding:utf-8 -*-
from sqlalchemy import create_engine
from OpsManage.settings import DATABASES


def create_connection(DataFrame, database, chart):
    """
    将DataFrame 写入mysql数据库
    DataFrame: pandas DataFrame数据
    database: 数据库
    chart: 表
    """
    try:
        database_config = DATABASES['default']
        if 'PORT' in list(database_config.keys()):
            if database_config['PORT'] != '':
                port = database_config['PORT']
            else:
                port = '3306'
        else:
            port = '3306'

        api = "mysql+mysqldb://{}:{}@{}:{}/{}?charset=utf8".format(database_config['USER'], database_config['PASSWORD'],
                                                                   database_config['HOST'], port, database)
        print(api)

        connections = create_engine(api, echo=True)
        DataFrame.to_sql(chart, connections, if_exists='append', index=False)
    except Exception as e:
        print(e)
