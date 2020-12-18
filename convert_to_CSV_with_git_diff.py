import time
import datetime
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
from pprint import pprint
import math
import gc
import queue
import requests
import json
import urllib.parse
# ch_id='chromiumos%2Fthird_party%2Fu-boot-next~chromeos-v2011.03~Ibdffce58e903cd81e3ee76d13b7cde2fdb36b38f'
def get_git_diff(ch_id):
    #first we get the list of files changed in this commit
    response = requests.get(url='https://chromium-review.googlesource.com/changes/'+ch_id+'/revisions/1/files')
    res = json.loads(response.content[4:].decode())
    file_list = res.keys()
    change_list ={}
    for file in file_list:
        # file = file.replace('/','%2F').replace(' ','%20')
        c_file = (urllib.parse.quote_plus(file))
        response = requests.get(url='https://chromium-review.googlesource.com/changes/'+ch_id+'/revisions/1/files/'+c_file+'/diff?intraline&whitespace=IGNORE_NONE')
        # try:
        res = json.loads(response.content[4:].decode())
        for change in res['content']:
            if 'ab' in change:
                continue
            if ('a' in change):
                if not(c_file in change_list):
                    change_list[c_file]=[]
                    for k in range(len(change['a'])):
                        change_list[c_file].append('<del> '+change['a'][k]) 
            if ('b' in change):
                if not(c_file in change_list):
                    change_list[c_file]=[]
                    for k in range(len(change['b'])):
                        change_list[c_file].append('<add> '+change['b'][k]) 
            # print(res)
        # except:
        #     print(file)
        #     print(c_file)
        #     pass
        # break

        # change_list[file] = res
    for file in change_list:
        change_list[file] = "<NL>".join(change_list[file])
    return change_list



batch_size=100000
client = MongoClient('mongodb://localhost:27017/sample')
# client.adminCommand({'setParameter': 1, 'internalQueryExecMaxBlockingSortBytes': 5e+9})
# client.admin.command(({'setParameter': 1, 'internalQueryExecMaxBlockingSortBytes': 5e+9}))
db=client.chromium
record_num=db.reviews.count_documents({})
recent_topic_queues=[]
# queue_size=500 #used for area hotness
# def add_to_queue(item):
#     if

def calculate_entropy(files):
    # calculate_portions
    total=0
    changes={}
    entropy = 0
    base=len(files)
    if base==1 or base==0 :
        return 0

    for commit_file in files:
        changes[commit_file] = 0
        if('lines_inserted' in files[commit_file]):
            changes[commit_file] += files[commit_file]['lines_inserted']
        if('lines_deleted' in files[commit_file]):
            changes[commit_file] += files[commit_file]['lines_deleted']

        total += changes[commit_file]

    if total==0:
        return 0

    for change in changes:
        pk = changes[change]/total
        if (pk):
            entropy -= pk*math.log(pk,base)  

    return entropy


def get_commit_info(revisions):
    max_files_addition = 0
    max_files_deletion = 0
    max_files_delta = 0
    max_files_size = 0
    commit_sbj = ''
    commit_msg = ''
    commit_id = ''
    entropy = 0
    files_touched = 0
    for i in revisions:
        if entropy==0:
            commit_id = i
            commit_sbj = revisions[i]['commit']['subject']
            commit_msg = revisions[i]['commit']['message']
            entropy = calculate_entropy(revisions[i]['files'])
            # files_touched = revisions[i]['_number']

            # Calculate NUR for this commit
            AVG_NUR=0
            for commit_file in revisions[i]['files']:
                AVG_NUR += NUR[commit_file] if commit_file in NUR else 0
            if len(revisions[i]['files']):
                AVG_NUR /= len(revisions[i]['files'])
            else:
                AVG_NUR = 0 


            list_file = []
            hot_files = 0
            all_files = 0
            for commit_file in revisions[i]['files']:
                list_file.append(commit_file)
                all_files += 1
                if is_file_recently_edited(commit_file):
                    hot_files += 1
            if all_files == 0:
                hotness_percentage=0
            else:
                hotness_percentage = hot_files / all_files
            files_touched = all_files

            put_file_list_in_queue(list_file)

            # we calcualte evrything for the first revision
            for commit_file in revisions[i]['files']:
                if 'lines_inserted' in revisions[i]['files'][commit_file] and revisions[i]['files'][commit_file]['lines_inserted'] > max_files_addition:
                    max_files_addition = revisions[i]['files'][commit_file]['lines_inserted']
                
                if 'lines_deleted' in revisions[i]['files'][commit_file] and revisions[i]['files'][commit_file]['lines_deleted'] > max_files_deletion:
                    max_files_deletion = revisions[i]['files'][commit_file]['lines_deleted']
                
                if 'size_delta' in revisions[i]['files'][commit_file] and revisions[i]['files'][commit_file]['size_delta'] > max_files_delta:
                    max_files_delta = revisions[i]['files'][commit_file]['size_delta']
                
                if 'size' in revisions[i]['files'][commit_file] and revisions[i]['files'][commit_file]['size'] > max_files_size:
                    max_files_size = revisions[i]['files'][commit_file]['size']
                if not(commit_file in NUR):
                    NUR[commit_file] = 1
                else:
                    NUR[commit_file] += 1

            
        #we don't want need other revisions here
        break


    return max_files_addition, max_files_deletion, max_files_delta, max_files_size, commit_sbj, commit_msg, commit_id, entropy, files_touched, AVG_NUR, hotness_percentage
            
def classify_time(time):
    time_class=0
    if(time < 3600):
        time_class = 0
    elif (time < 86400): #1 day
        time_class = 1
    elif (time < 86400*3): #3 day
        time_class = 2
    elif (time < 604800): #7 days
        time_class = 3
    elif (time < 604800*2): #14 days == one sprint
        time_class = 4
    elif (time < 2592000): #30 days
        time_class = 5
    elif (time < 7776000): #3 months
        time_class = 6
    else: #more than 3 months!
        time_class = 7
    return time_class



def put_file_list_in_queue(file_list):
    if len(touched_files_last_queue)>= history_length:
        touched_files_last_queue.pop(0)
    touched_files_last_queue.append(file_list)


def is_file_recently_edited(file_name):
    for i in range(len(touched_files_last_queue)):
        for j in range(len(touched_files_last_queue[i])):
            if file_name == touched_files_last_queue[i][j]:
                return True
    return False



topicObj={'total':0}
project_start={}
project_reviews={}
developer_review_req={}
developer_review_req_success={}
NUR = {} #number of unique last reviewed to the files

history_length = 50
touched_files_last_queue = []

for i in tqdm(range(0,record_num,batch_size), position=0, leave=True):
    collection_batch = db.reviews.aggregate([{ '$sort' : { "created" : 1 } },{ '$skip': i },{ '$limit': batch_size }] , allowDiskUse=True)#.skip(i).limit(batch_size)
    extracted_reviews = pd.DataFrame([],columns=['additions', 'deletions', 'number_of_patches_so_far', 'initial_files_changed', 'max_files_addition',
                                             'max_files_deletion', 'max_files_delta', 'max_files_size', 'commit_sbj', 'commit_msg', 'entropy', 'files_touched',
                                             'has_topic', 'topic', 'topic_prevalency', 'mergeable', 'has_assignee', 'owner_is_submitter', 'project', 'project_age',
                                             'project_prev_commits', 'created', 'updated', 'subimitted', 'eval_time', 'status', 'average_NUR', 'owener', 'dev_prev_exp',
                                             'dev_merged_CR', 'change_id', 'id', 'decision_made'])   

    for review in tqdm(collection_batch, position=0, leave=True):
        max_files_addition, max_files_deletion, max_files_delta, max_files_size, commit_sbj, commit_msg, commit_id, entropy, files_touched, AVG_NUR, hotness_percentage = get_commit_info(review['revisions'])

        creation = time.mktime(datetime.datetime.strptime(review['created'][:-4], '%Y-%m-%d %H:%M:%S.%f').timetuple())
        updated = time.mktime(datetime.datetime.strptime(review['updated'][:-4], '%Y-%m-%d %H:%M:%S.%f').timetuple())
        submitted = time.mktime(datetime.datetime.strptime(review['submitted'][:-4], '%Y-%m-%d %H:%M:%S.%f').timetuple()) if 'submitted' in review else -1
        if ('topic' in review):
            if review['topic'] in topicObj:
                topicObj[review['topic']] += 1
                topicObj['total'] += 1
            else:
                topicObj[review['topic']] = 1
                topicObj['total'] += 1
        if not (review['project'] in project_start):
           project_start[review['project']] = creation
           project_reviews[review['project']] = 1
        else:
           project_reviews[review['project']] += 1
        if ('submitted' in review or review['status']=="ABANDONED"):
            decision = True
        else:
            decision = False

        if (review['owner']['_account_id'] in developer_review_req):
            developer_review_req[review['owner']['_account_id']].append(review['change_id'])
        else:
            developer_review_req[review['owner']['_account_id']]=[review['change_id']]

        tmp = {
            'additions':review['insertions'],
            'deletions':review['deletions'],
            'number_of_patches_so_far':len(review['revisions']),
            'initial_files_changed':len(review['revisions']),
            'max_files_addition': max_files_addition,
            'max_files_deletion': max_files_deletion,
            'max_files_delta': max_files_delta,
            'max_files_size': max_files_size,
            'commit_sbj': commit_sbj,
            'commit_msg': commit_msg,
            'entropy': entropy,
            'files_touched' : files_touched,
            'has_topic':int('topic' in review),
            'topic': review['topic'] if 'topic' in review else -1,
            'topic_prevalency': (topicObj[review['topic']]/topicObj['total']) if 'topic' in review else 0,
            'mergeable': int(review['mergeable']) if 'mergeable' in review else None,# (?)
            'has_assignee':int(True if 'assignee' in review else False),
            'owner_is_submitter': int(True if ('submitter' in review and review['submitter']['_account_id'] == review['owner']['_account_id']) else False),
            'project':review['project'],
            'project_prev_commits' : project_reviews[review['project']]-1,
            'sent_on_friday': datetime.datetime.strptime(review['created'][:-4], '%Y-%m-%d %H:%M:%S.%f').weekday() == 4,
            'subimitted' : submitted,
            'eval_time': updated - creation ,
            'duration_class': classify_time(updated - creation) ,
            'status' : review['status'],
            
            'average_NUR' : AVG_NUR,
            'dev_prev_exp' : len(developer_review_req[review['owner']['_account_id']])-1 ,
            'dev_merged_CR' : len(developer_review_req_success[review['owner']['_account_id']]) if review['owner']['_account_id'] in developer_review_req_success else 0,
            'hotness_percentage' : hotness_percentage,
            'decision_made': decision,
        }

        if review['status'] == "MERGED":
            if (review['owner']['_account_id'] in developer_review_req_success):
                developer_review_req_success[review['owner']['_account_id']].append(review['change_id'])
            else:
                developer_review_req_success[review['owner']['_account_id']]=[review['change_id']]
        else:
            developer_review_req_success[review['owner']['_account_id']]=[]
        
        extracted_reviews = extracted_reviews.append(tmp, ignore_index=True)
        # break
        i+=1
    extracted_reviews.to_csv('computed_'+str(int((i+1)/batch_size))+'.csv',index=False)
    gc.collect()
pprint(extracted_reviews)