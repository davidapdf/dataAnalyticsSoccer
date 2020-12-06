
#%%
from dao import get
import pandas as pd
import seaborn as sns

def getSql(sql):
    result = get(sql)
    return result


sql_string = """select 
case when lugar = 'Fora de casa' then 2 else 1 end as lugar,
case 
	when statuspartida = 'Derrota' then 3
	when statuspartida = 'Empate' then 2
	when statuspartida = 'Vitoria' then 1
	end as statuspartida,
    sum(qtd_gols) as qtd_gols,
    sum(qtd_gols_sofridos) as qtd_gols_sofridos,
    sk_time,
    count(*) as quantidade_partidas
from   dw.f_jogos 
group by 
case when lugar = 'Fora de casa' then 2 else 1 end,
case 
	when statuspartida = 'Derrota' then 3
	when statuspartida = 'Empate' then 2
	when statuspartida = 'Vitoria' then 1
	end,
	sk_time"""


matriz_data = getSql(sql_string)

data_avaliacao = pd.DataFrame(matriz_data)
data_avaliacao = data_avaliacao.rename(columns={0:'lugar',1:'statuspartida',2:'qtd_gols',3:'qtd_gols_sofridos',4:'sk_time',5:'quantidade_partidas'})

sns.pairplot(data_avaliacao,hue='statuspartida')
from sklearn import svm
from sklearn.model_selection import train_test_split

X = data_avaliacao.to_numpy().tolist()
y = data_avaliacao.statuspartida

X_train,X_test,y_train,y_test = train_test_split(X,y,test_size = 0.3, random_state =13)

clf = svm.SVC(C=1.0)
clf.fit(X_train,y_train)

clf.predict(X_test)
y_pred = clf.predict(X_test)
print("a acurácia é: {}".format(clf.score(X_test,y_test)))

from sklearn.metrics import classification_report
print(classification_report(y_test,y_pred))




# %%
