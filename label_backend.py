import sqlite3
from sqlite3 import Error
import pandas as pd
import datetime

import json

##############
## DB PART
def create_tables(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        #print(sqlite3.version)
        conn.execute("DROP TABLE IF EXISTS to_label")
        conn.execute("""CREATE TABLE IF NOT EXISTS to_label (id integer PRIMARY KEY,
                                        case_id text NOT NULL,
                                        content text NOT NULL,
                                        creation_date text NOT NULL,
                                        user text,
                                        label text,
                                        labeled_by text,
                                        labeled_date text,
                                        project text,
										Keyword_Based_Label text
                                        );""")
        
    except Error as e:
        print(e)
    finally:
        conn.close()


def select_all(db_file):
    try:
        conn = sqlite3.connect(db_file) 
        result = pd.read_sql_query('SELECT * FROM to_label', con=conn)
        return result
    except Error as e:
        print('ERROR:')
        print(e)
    finally:
        conn.close()


def sql_get(db_file, query):
    try:
        conn = sqlite3.connect(db_file)
        result = pd.read_sql(query, con=conn)
        return result
    except Error as e:
        print(e)
    finally:
        conn.close()


def sql(db_file, query):
    try:
        conn = sqlite3.connect(db_file)
        conn.execute(query)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        conn.close()


def normalize_text(txt):
    return txt.replace('"', ' ').replace(';', '.').replace('/n', ' ').replace("'", ' ')


def insert_to_label(db_file,case_id_list,kw_label_list,content, user='user1',label='None',project='default'):
    current_date = str(datetime.datetime.now())
    try:
        conn = sqlite3.connect(db_file)
        sql = """INSERT INTO to_label (case_id,content, creation_date,user, label, labeled_by, labeled_date, project,Keyword_Based_Label) 
        VALUES """
        lst = []
        for item,case_id,kw_label in zip(content,case_id_list,kw_label_list):
            row = "('"+(case_id)+"','"+normalize_text(item)+"','"+current_date+"','"+user+"','"+label+"','','','"+project+"','"+kw_label+"')"
            lst.append(row)
        sql+=','.join(lst)
        #print(sql)
        conn.execute(sql)
        conn.commit()
    except Error as e:
        print("THERE WAS AN ERROR ADDING NEW ROWS IN DB:")
        print(e)
    finally:
        conn.close()


###############
##  Config JSON part
def read_config(file_name):
    with open(file_name,'r') as f:
        content = f.readlines()
    #print(''.join(content))
    return json.loads(''.join(content))


def update_db(db_file,config):
    try:
        conn = sqlite3.connect(db_file)
        sql = """SELECT DISTINCT project FROM to_label"""
        cursor = conn.execute(sql)
        res= cursor.fetchall()
        existing_projects = set()
        for tup in res:
            existing_projects.add(tup[0])
        print('EXISTING PROJECTS will be ignored:',existing_projects)
        for project in config['projects']:
            if project['project_name'] not in existing_projects:
                print('WILL ADD Project: ',project['project_name'])
                print('\t User setup:',project['users'])
                print('\t Labels:',project['labels'])
                total_rows = 0
                for user in project['users']:
                    tmp = int(project['users'][user].split(':')[1])
                    if tmp>=total_rows:
                        total_rows = tmp
                print('\t Total rows:',total_rows)
                input_data = pd.read_csv(project['data_source_file'],nrows = total_rows)
                tmp = []
                case_id_list = []
                keyword_label_list = []
                for idx,row in input_data.iterrows():
                    tmp.append('<h4>'+str(row['Subject_Description'])+'</h4><p>'+str(row['Issue_Text'])+'</p>')
                    case_id_list.append(str(row['Case_Internal_Id']))
                    keyword_label_list.append(str(row['Keyword_Based_Label']))
                #print(case_id_list)
                for user in project['users']:
                    start = int(project['users'][user].split(':')[0])
                    end = int(project['users'][user].split(':')[1])
                    content = tmp[start-1:end]
                    case_ids = case_id_list[start-1:end]
                    kw_labels = keyword_label_list[start-1:end]
                    #print(case_ids)
                    print('\tAdding rows ',start,':',end,' for user ',user,' to label')
                    insert_to_label(db_file,case_ids,kw_labels,content,user = user, project = project['project_name'],label='None')
    except Error as e:
        print("THERE WAS AN ERROR:")
        print(e)
    finally:
        conn.close()

#####################
## WIDGETS Part
import time
import ipywidgets as widgets
data = None
idx=-1
def initialize_widgets(db_file, config):
    def get_all_users(config):
        cfg = read_config(config)
        res_usr = set()
        for project in cfg['projects']:
            for user in project['users']:
                    res_usr.add(user)
        return list(res_usr)
    def get_projects_for_user(config,usr):
        cfg = read_config(config)
        res_prj = set()
        for project in cfg['projects']:
            for user in project['users']:
                    if user == usr:
                        res_prj.add(project['project_name'])
        return list(res_prj)
    def get_labels_for_project(config,prj):
        cfg = read_config(config)
        for project in cfg['projects']:
#             print(project['project_name'],project['labels'])
            if project['project_name']== prj:
                return project['labels']
        return {}
        
        
    users_list= get_all_users(config)
   
    lbls = {}#'hdd failure':'Issue where the hard drive is presented as failed','power supply issue':'Any sort of power supply issues','firmware issue':'Anything that has to do with firmware','other':'Anything else'}

    projects = widgets.Dropdown(options=get_projects_for_user(config,users_list[0]),
        description='PROJECT:',
        disabled=False,
        value = get_projects_for_user(config,users_list[0])[0]
    )
    
    
    def on_select_users(usr):
        rs = sql_get(db_file, 'SELECT DISTINCT project FROM to_label WHERE user="'+usr['new']+'"')
        projects.options=rs['project'].values
    
    users = widgets.Dropdown(
        options=users_list,
        description='USER:',
        disabled=False,
        value=users_list[0]
    )
    users.observe(on_select_users,'value')
    
    header = widgets.HBox([users,projects])
    
    get_data_btn = widgets.Button(description = 'GET DATA',button_style ='warning')

    def set_kw_btn_style(btn):
        btn.disabled = True
        btn.button_style = 'info'
	
    def get_current_label_for_kwb():
        global idx
        if idx<0:
            idx=0
        if idx>len(data)-1:
            idx = len(data)-1
        return data['Keyword_Based_Label'].values[idx]
	
    def get_current_content():
        global idx
        if idx<0:
            idx=0
        if idx>len(data)-1:
            idx = len(data)-1
        return data['content'].values[idx]
    
    def get_data(b):
        global data
        user = users.value
        project = projects.value
#         print(user,project)
#         print(type(user),type(project))
        lbls = get_labels_for_project(config,project)
        lst = []
        #print(lbls)
        lst.append(kw_label_btn)

        for lbl in lbls:
            tmp_b = widgets.Button(description = lbl,button_style = 'warning',tooltip=lbls[lbl])
            tmp_b.on_click(on_selection)
            lst.append(tmp_b)
		
        labels.children = lst
        sel_query = 'SELECT * FROM to_label WHERE user="'+user+'" AND project="'+project+'" and label="None"'
        #print(sel_query)
        rs = sql_get(db_file,sel_query)
        if len(rs)==0:
            content.value='<h3>END OF DATASET, NO MORE LABELING ON THIS PROJECT<h3>'
            bkW.disabled = True
            data = None
        else:
            data = rs
            idx = -1
            kw_label_btn.description = get_current_label_for_kwb()
            set_kw_btn_style(kw_label_btn)
            content.value = get_current_content()
            progress.value = idx+1
            progress.max = len(data)
            progress_lbl.value = str(idx+1)+'/'+str(len(data))
            

    get_data_btn.on_click(get_data)    

    progress = widgets.IntProgress(
        value=0,
        min=0,
        max=10,
        step=1,
        description='PROGRESS:',
        bar_style='success', # 'success', 'info', 'warning', 'danger' or ''
        orientation='horizontal'
    )
    progress_lbl = widgets.Label('0/0')
    progress_box = widgets.HBox([progress,progress_lbl])

    def on_selection(btn):
        def update_label(idx):
            index = data.id.values[idx]
            data.loc[data.id == index, ['label', 'labeled_by','labeled_date']] = btn.description, users.value, str(datetime.datetime.now())
            sql(db_file,query = 'UPDATE to_label SET label ="'+btn.description+'", labeled_by="'+users.value+'", labeled_date="'+str(datetime.datetime.now())+'" WHERE id='+str(index)+' ;')
        global idx
        if idx<len(data):
            #print('IDX:',idx)
            #print('SELECTED: '+btn.description)
            update_label(idx)
            btn.button_style='success'
            time.sleep(.2)
            btn.button_style='warning'
            idx+=1
            content.value = get_current_content()
            kw_label_btn.description = get_current_label_for_kwb()
            progress.value = idx+1
            progress_lbl.value = str(idx+1)+'/'+str(len(data))
            existing_label = data['label'].values[idx]
            for btn in labels.children:
                if btn.description == existing_label:
                    btn.button_style = 'success'
                else:
                    btn.button_style = 'warning'
            bkW.disabled = False
            fwW.disabled = False
            set_kw_btn_style(kw_label_btn)
        else:
            content.value = '<h3>END OF DATASET, NO MORE LABELING ON THIS PROJECT<h3>'
            set_kw_btn_style(kw_label_btn)
            kw_label_btn.description = "Not Available"
            fwW.disabled = True

    lst = []

    labels = widgets.HBox(lst)
    # Deifine the button first and utilize it to set the keyword based label
    kw_label_btn = widgets.Button(description='Keyword Label', button_style='info')
    kw_label_btn.disabled = True
    content = widgets.HTML(value = '<h3>SELECT A USER AND THEN A PROJECT AND PRESS "GET DATA" TO BEGIN<h3>',
                layout=widgets.Layout(width='800px', height='400px', overflow_x='auto',justify_content="center"))

    def back(b):
        global idx
        idx-=1
#         print('IDX:',idx)
        content.value = get_current_content()
        progress.value = idx+1
        progress_lbl.value = str(idx+1)+'/'+str(len(data))
        existing_label = data['label'].values[idx]
#         print(existing_label)
        for btn in labels.children:
            if btn.description == existing_label:
                btn.button_style = 'success'
            else:
                btn.button_style = 'warning'
        set_kw_btn_style(kw_label_btn)


    bkW = widgets.Button(description = '<<',button_style ='success',disabled=True)
    bkW.on_click(back)

    def forward(b):
        global idx
        idx+=1
#         print('IDX:',idx)
        content.value = get_current_content()
        progress.value = idx+1
        progress_lbl.value = str(idx+1)+'/'+str(len(data))
        existing_label = data['label'].values[idx]
#         print(existing_label)
        for btn in labels.children:
            if btn.description == existing_label:
                btn.button_style = 'success'
            else:
                btn.button_style = 'warning'
        set_kw_btn_style(kw_label_btn)

    fwW = widgets.Button(description = '>>', button_style = 'success',disabled=True)
    fwW.on_click(forward)
    bottom = widgets.HBox([bkW,fwW])


#     box = widgets.VBox([header,get_data_btn,progress_box,content,selection,labels,bottom])
    box = widgets.VBox([header,get_data_btn,progress_box,content,labels,bottom])
    
    return box
#display(initialize_widgets('data_to_label.sqdb','notebooks/labeling_projects.cfg'))
# print('Action Log:')