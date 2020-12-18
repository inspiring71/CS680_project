import pandas as pd
from pprint import pprint



data = pd.read_csv('computed_10.csv')
print(data.columns)
data = data.drop(['commit_sbj','commit_msg','created','updated','subimitted','change_id','id','number_of_patches_so_far','status'],axis=1)
data['topic'] = data['topic'].fillna('NO_TOPIC')
data['mergeable'] = data['mergeable'].fillna(-1)
print(data.columns)

def convert_target_to_hour(row):
    row['eval_time'] = row['eval_time']/60
    return row
data = data.apply(convert_target_to_hour,axis=1)

def convert_decision(row):
    row['decision_made'] = int(row['decision_made'])
    return row

data = data.apply(convert_decision,axis=1)

#-----------------
# status_list = list(data.status.unique())
# status_to_index = {}
# index_to_status = {}
# def make_status_categories():
#   for status,i in enumerate(status_list):
#     status_to_index[i] = status
#     index_to_status[status] = i
#   return status_to_index, index_to_status
# status_to_index, index_to_status = make_status_categories()

# def convert_status_to_code(row):
#   row['status_code'] = status_to_index[row['status']]
#   return row
# only_determined = only_determined.apply(convert_status_to_code,axis=1).drop(['status'], axis=1)
# #-----------------------
data['topic'] = data['topic'].fillna(-1)

topics_list = list(data.topic.unique())
topic_to_index = {}
index_to_topic = {}

def make_topic_categories():
  for topic,i in enumerate(topics_list):
    topic_to_index[i] = topic
    index_to_topic[topic] = i
  return topic_to_index, index_to_topic
topic_to_index, index_to_topic = make_topic_categories()


def convert_topic_to_code(row):
  row['topic_code'] = topic_to_index[row['topic']]
  return row
data = data.apply(convert_topic_to_code,axis=1).drop(['topic'], axis=1)
#------------------------------
projects_list = list(data.project.unique())
project_to_index = {}
index_to_project = {}

def make_category():
  for project,i in enumerate(projects_list):
    project_to_index[i] = project
    index_to_project[project] = i
  return project_to_index, index_to_project
project_to_index, index_to_project = make_category()


def convert_project_to_code(row):
  # print(row)
  row['project_code'] = project_to_index[row['project']]
  return row
data = data.apply(convert_project_to_code,axis=1).drop(['project'], axis=1)
#----------------------------
data = data.astype('float')
pprint(data)
data.to_csv('10_out.csv',index=False)