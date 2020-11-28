from dao import get,set,set_batch
import pandas as pd

sql_drop = """drop table if exists stg.campeonatoBrasileiroFull"""
sql_create = """create table stg.campeonatobrasileirofull (
	horaio varchar(5), 
	dia varchar(15), 
	data date,
	clube1 varchar(30),
	clube2 varchar(30),
	vencedor varchar(30),
	rodada varchar(30),
	arena varchar(30),
	clube1gols int,
	clube2gols int,
	clube1estado char(2),
	clube2estado char(2),
	estadoclubevencedor varchar(10)
)"""

set(sql_drop)
set(sql_create)


df = pd.read_csv("campeonato-brasileiro-full.csv",names=['horaio','dia','data','clube1','clube2','vencedor','rodada','arena','clube1gols','clube2gols','clube1estado','clube2estado','estadoclubevencedor'], header=0)

df_columns = list(df)
columns = ",".join(df_columns)
table = "stg.campeonatobrasileirofull"

values = "values({})".format(",".join(["%s" for x in df_columns]))
insert = "insert into {} ({}) {}".format(table,columns,values)

set_batch(insert,df.values)

