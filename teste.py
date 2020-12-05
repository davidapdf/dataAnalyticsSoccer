import pandas as pd

a = {'Teste1':[1,2,3,4,5,6,7,8],'Teste2':['f','v','v','f','f','v','v','f']}
b = {'Teste1':[1,2,4,4,6,6,7,8],'Teste2':['f','v','v','f','v','v','v','f']}

db1 = pd.DataFrame(data = b)
db1 = db1.sort_values(['Teste1','Teste2']).drop_duplicates(subset=['Teste1'],keep='last')



print (db1)



