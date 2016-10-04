import numpy as np
import pandas as pd
from scipy import stats, integrate
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(color_codes=True)

# pass in column names each file
data_cols = ['user_id', 'movie_id', 'rating', 'timestamp']	#modify item_id in movie_id to be able to merge dataframes
data = pd.read_csv('data/u.data', sep='\t', names=data_cols)

user_cols = ['user_id', 'age', 'sex', 'occupation', 'zip_code']
user = pd.read_csv('data/u.user', sep='|', names=user_cols)

#take only the 5 first colums for the u.item file
item_cols = ['movie_id', 'movie_title', 'release_date', 'video_release_date', 'imdb_url']
item = pd.read_csv('data/u.item', sep='|', names=item_cols, usecols=range(5))

#Merge the different dataframes
item_data = pd.merge(item, data)
df= pd.merge(item_data, user)

############# QUESTION a


answer1=user.user_id.nunique()
answer2=item.movie_id.nunique()
answer=pd.DataFrame(columns=[answer1,answer2], dtype=int)
answer.to_csv('movies.txt', sep='\n',mode='w')

############# QUESTION b

top_5 = item_data.movie_title.value_counts().sort_values(ascending=False)[:5]
top_5.to_csv('movies.txt', header=None, sep='\t', mode='a')


############# QUESTION c

movie_age= df.groupby('movie_title').agg({'rating': [np.size],'age': [np.mean]})
more_than_100 = movie_age['rating']['size'] >=100
result = movie_age[more_than_100].sort_values([('age','mean')],ascending=True)[:5]
result.to_csv('movies.txt', header=None, sep='\t', mode='a')

############# QUESTION d

x=df.age.drop_duplicates()
y=df.groupby('age').rating.mean()
dataset=zip(x,y)
df1= pd.DataFrame(data=dataset, columns=['Age', 'Rating'])
h=sns.jointplot(x="Age", y="Rating", data=df1)


############# QUESTION e

g= sns.jointplot(x="Age", y="Rating", data=df1, kind="kde")
g.plot_joint(plt.scatter)

plt.show()

