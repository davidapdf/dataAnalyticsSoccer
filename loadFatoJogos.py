import os
from dao import set
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
path_sql = os.path.join(BASE_DIR,'jogos/querys_create_tables/load_fato_jogo.sql')

sql_file= open(path_sql)
sql_as_string = sql_file.read()

set(sql_as_string)

