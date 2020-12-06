from dao import get,set, set_batch
import pandas as pd
import getpass as getpass
from datetime import datetime

def dataFrameIsNotEmpty(dataFrame):
    if  dataFrame is None:
        return False
    elif dataFrame.empty:
        return False
    else:
         return True


def insert(dataFrame,tb):
    if dataFrameIsNotEmpty(dataFrame):
        df_columns = list(dataFrame)
        columns = ",".join(df_columns)
        table = tb
        values = "values({})".format(",".join(["%s" for x in df_columns]))
        insert = "insert into {} ({}) {}".format(table,columns,values)
        set_batch(insert,dataFrame.values)

def dataInst(dataFrame):
    if dataFrameIsNotEmpty(dataFrame):
        dataFrame.insert(3,'dt_insert',datetime.now().replace(microsecond=0))
        dataFrame.insert(4,'user_insert',getpass.getuser())
        dataFrame.insert(5,'fleg','Y')
        return dataFrame

def dataUpDate(dataFrame):
    if dataFrameIsNotEmpty(dataFrame):
        dataFrame.insert(4,'dt_update',datetime.now().replace(microsecond=0))
        dataFrame.insert(5,'user_update',getpass.getuser())
        return dataFrame

def getSql(sql):
    result = get(sql)
    return result

def aDataUp(dataFrameStg,DataFrameDim):
    if dataFrameIsNotEmpty(DataFrameDim) and dataFrameIsNotEmpty(dataFrameStg):
        #stg_data['clube'].isin(dim_data['clube'])
        #teste =  stg_data.isin({'clube':dim_data['clube']})
        stg_merge = dataFrameStg.merge(DataFrameDim.drop_duplicates(),on=['clube','arena'],how='left',indicator=True)
        stg_merge = stg_merge.drop_duplicates('sk_id')
        stg_merge.loc[(stg_merge._merge == 'both') & (stg_merge.estado_x != stg_merge.estado_y),'up-ins'] = 'up'
        stg_merge = stg_merge[['up-ins','sk_id','clube','estado_x','arena']]
        stg_merge = stg_merge.rename(columns={'estado_x':'estado'})
        filter_up = stg_merge['up-ins'] == 'up'
        return stg_merge[filter_up]

def aDataInst(dataFrameStg,DataFrameDim):
    if dataFrameIsNotEmpty(DataFrameDim) and dataFrameIsNotEmpty(dataFrameStg):
        stg_merge = dataFrameStg.merge(DataFrameDim.drop_duplicates({'clube','arena'}),on=['clube','arena'],how='left',indicator=True)
        stg_merge = stg_merge.drop_duplicates({'clube','arena'})
        stg_merge.loc[(stg_merge._merge != 'both'),'up-ins'] = 'ins'
        stg_merge = stg_merge[['up-ins','clube','estado_x','arena']]
        stg_merge = stg_merge.rename(columns={'estado_x':'estado'})
        filter_in = stg_merge['up-ins'] == 'ins'
        return stg_merge[filter_in]
    else:
        dataFrameStg['up-ins'] = dataFrameStg['up-ins'] = 'ins'
        return dataFrameStg
        
def createTmp(dataFrame,tmp):
    df_columns = list(dataFrame)
    columns = " varchar(100), ".join(df_columns) 
    create = "DROP TABLE IF EXISTS {}; CREATE TABLE IF NOT EXISTS {} ({} varchar(100));".format(tmp,tmp,columns)
    set(create)


sql_stg = """select  distinct clube1,clube1estado, arena  from stg.campeonatoBrasileiroFull"""
sql_stg2 = """select distinct clube2,clube2estado, arena  from stg.campeonatoBrasileiroFull"""
sql_dim = """select sk_id, clube, estado,arena from dw.dim_arena where fleg = 'Y'"""


stg_data = getSql(sql_stg)
stg_data2 = getSql(sql_stg2)
dim_data = getSql(sql_dim)

df1 = pd.DataFrame(stg_data)
df2 = pd.DataFrame(stg_data2)
frames = [df1,df2]


stg_data =  pd.concat(frames)


stg_data = stg_data.rename(columns={0:'clube',1:'estado',2:'arena'})
dim_data = pd.DataFrame(dim_data)
dim_data = dim_data.rename(columns={0:'sk_id',1:'clube',2:'estado',3:'arena'})


if  dataFrameIsNotEmpty(dim_data):
    dim_data['clube'] = dim_data['clube'].str.upper()
    dim_data['arena'] = dim_data['arena'].str.upper()
    dim_data['estado'] = dim_data['estado'].str.upper()

if dataFrameIsNotEmpty(stg_data):
    stg_data['clube'] = stg_data['clube'].str.upper()
    stg_data['arena'] = stg_data['arena'].str.upper()
    stg_data['estado'] = stg_data['estado'].str.upper()
stg_data = stg_data.sort_values(['clube','arena']).drop_duplicates(subset=['clube','arena'], keep='last')
stg_data.insert(0,'up-ins',None)


base_insert = aDataInst(stg_data,dim_data)
if dataFrameIsNotEmpty(base_insert):
    base_insert = dataInst(base_insert)
    base_insert = base_insert.drop(columns=['up-ins'])
    insert(base_insert,"dw.dim_arena") 


base_upDate = aDataUp(stg_data,dim_data)
if dataFrameIsNotEmpty(base_upDate):
    base_upDate = dataUpDate(base_upDate)
    base_upDate = base_upDate.drop(columns=['up-ins'])
    createTmp(base_upDate,'stg.dimArenaStg')
    insert(base_upDate,'stg.dimArenaStg')
    set("UPDATE dw.dim_Arena SET estado = tmp.estado FROM stg.dimArenaStg as tmp WHERE CAST(dim_arena.sk_id AS INT) = CAST(tmp.sk_id AS INT); drop table stg.dimArenaStg;")
