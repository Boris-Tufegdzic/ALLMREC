import os
import os.path
import gzip
import json
import pickle
from tqdm import tqdm
from collections import defaultdict

def parse(path):
    g = gzip.open(path, 'rb')
    for l in tqdm(g):
        yield json.loads(l)
        
def preprocess(fname):
    countU = defaultdict(lambda: 0)
    countP = defaultdict(lambda: 0)
    line = 0
    usermap = dict()
    usernum = 0
    itemmap = dict()
    itemnum = 0
    User = dict()
    review_dict = {}

    base_dir = '/kaggle/input/yelpdata/philadelphia'
    user_path = base_dir + f'/philadelphia_users.json'
    business_path = base_dir + f'/philadelphia_businesses.json'
    review_path = base_dir + f'/philadelphia_reviews.json'
            
    with open(review_path, 'r') as f:
        for line in f:
            l = json.loads(line.strip())
            user = l['user_id']
            business = l['business_id']
            time = l['date']

            if user in usermap:
                userid = usermap[user]
            else:
                usernum += 1
                userid = usernum
                usermap[user] = userid
                User[userid] = []

            if business in itemmap:
                businessid = itemmap[business]
            else:
                itemnum += 1
                businessid = itemnum
                itemmap[business] = businessid

            User[userid].append([time, businessid])

            if itemmap[business] in review_dict:
                try:
                    review_dict[itemmap[business]]['review'][usermap[user]] = l['text']
                except:
                    a = 0
            else:
                review_dict[itemmap[business]] = {'review': {}, 'summary':{}}
                try:
                    review_dict[itemmap[business]]['review'][usermap[user]] = l['text']
                except:
                    a = 0
    
    #Filling meta info
    name_dict = {'title':{}, 'description':{}}

    with open(business_path, 'r') as f:
        for line in f:
            l = json.loads(line)
            business = l['business_id']
            description = l['categories']
            try:
                if len(description) == 0:
                    name_dict['description'][itemmap[business]] = 'Empty description'
                else:
                    name_dict['description'][itemmap[business]] = description
                name_dict['title'][itemmap[business]] = l['name']
            except:
                a = 0
            
    
    with open(f'/kaggle/working/ALLMREC/data/yelp/yelp_text_name_dict.json.gz', 'wb') as tf:
        pickle.dump(name_dict, tf)
    
    for userid in User.keys():
        User[userid].sort(key=lambda x: x[0])
        
    print(usernum, itemnum)

    # Define the directory path
    dir_path = '/kaggle/working/ALLMREC/data/yelp/philadelphia/'

    # Ensure the directory exists
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    #f = open('/kaggle/input/yelp-philadelphia/philadelphia/reviews.txt', 'w')
    f = open(f'{dir_path}/reviews.txt', 'w')
    for user in User.keys():
        for i in User[user]:
            f.write('%d %d\n' % (user, i[1]))
    f.close()