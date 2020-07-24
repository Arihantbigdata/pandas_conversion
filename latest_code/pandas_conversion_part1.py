#!/usr/bin/env python
# coding: utf-8

# # After From

# In[1]:


from moz_sql_parser import parse
# from snowflake.sqlalchemy import URL
# from sqlalchemy import create_engine
"""@uthor-> arshashank"""
"""
url = URL(user='akashdgupta',
password='in$piR1111',
account='NDA55275.us-east-1',
warehouse='PYTHON_MVP',
database='mvpdb',
schema='mvp_schema')

engine = create_engine(url)
connection = engine.connect()
"""
def flatten_json(nested_json: dict, exclude: list=[''], sep: str='_') -> dict:
    """
    Flatten a list of nested dicts.
    """
    out = dict()
    def flatten(x: (list, dict, str), name: str='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude:
                    flatten(x[a], f'{name}{a}{sep}')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, f'{name}{i}{sep}')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out

def get_column_details(select_list):
    column_list = []
    # Get column details    
    if type(select_list) == str:
        column_value = select_list
        column_table = ""
        alias=""
        collsttmp={"Column":column_value, "Table":column_table,"Alias":alias}
        column_list.append(collsttmp)
        
    elif type(select_list) == dict:
        if type(select_list['value']) != dict:
            if '.' in select_list['value']:
                column_value = select_list['value'].split('.')[1]
                column_table = select_list['value'].split('.')[0]
                try:
                    alias=select_list['name']
                except:
                    alias=""
                collsttmp={"Column":column_value, "Table":column_table,"Alias":alias}
                column_list.append(collsttmp)
                
    elif type(select_list) == list:
        for column_dict in select_list:
            if type(column_dict['value']) !=dict:
                if '.' in column_dict['value']:
                    column_value = column_dict['value'].split('.')[1]
                    column_table = column_dict['value'].split('.')[0]
                    try:
                        alias=column_dict['name']
                    except:
                        alias=""
                    collsttmp={"Column":column_value, "Table":column_table,"Alias":alias}
                    column_list.append(collsttmp)
                else:
                    column_value = column_dict['value']
                    column_table=""
                    try:
                        alias=column_dict['name']
                    except:
                        alias=""
                    collsttmp={"Column":column_value, "Table":column_table,"Alias":alias}
                    column_list.append(collsttmp)
                    
            else:
                cols= list(flatten_json(column_dict['value']).values())
                cols = [i for i in cols if type(i) !=int]
                
                for i in cols:
                    if '.' in i:
                        column_value = i.split('.')[1]
                        column_table = i.split('.')[0]
                        alias=""                        
                            
                        collsttmp={"Column":column_value, "Table":column_table,"Alias":alias}
                        column_list.append(collsttmp)
                        
                    else:
                        column_value = i
                        column_table=""
                        alias=""

                        collsttmp={"Column":column_value, "Table":column_table,"Alias":alias}
                        column_list.append(collsttmp)            
    return column_list





def get_table_details(from_dict_list):

    table_name=[]
    join_list=[]
    if type(from_dict_list) == str:
        tbl=from_dict_list
        alias=""
        tbltmp={"Table":tbl, "Alias":alias}
        table_name.append(tbltmp)
        
    elif type(from_dict_list) == dict:
        tbl=from_dict_list['value']
        alias=from_dict_list['name']
        tbltmp={"Table":tbl, "Alias":alias}
        table_name.append(tbltmp)
        
    elif type(from_dict_list) == list:
        for i in from_dict_list:
            if 'value' in list(i.keys()):
                #Means there is no join or first table
                tbl=i['value']
                try:
                    alias=i['name']
                except:
                    alias=""
                tbltmp={"Table":tbl, "Alias":alias}
                table_name.append(tbltmp)
                
            elif 'join' in list(i.keys()):
                #Means there is a join
                keyjoinlist= [i for i in list(i.keys()) if 'join' in i]
                JoinType=keyjoinlist[0]
                tbl=i[JoinType]['value']
                joincond=i['on']
                try:
                    alias=i[JoinType]['name']
                except:
                    alias=''
                for k, v in joincond.items():
                    if k =='and':
                        for i in v:
                            for key,val in i.items():
                                eqtyp=key
                                colleft=val[0]
                                if "." in colleft:
                                    colleftcol=val[0].split('.')[1]
                                    collefttbl=val[0].split('.')[0]
                                else:
                                    colleftcol=val[0]
                                    collefttbl=""
                                    
                                colright=val[1]
                                if "." in colright:
                                    colrightcol=val[1].split('.')[1]
                                    colrighttbl=val[1].split('.')[0]
                                else:
                                    colrightcol=val[1]
                                    colrighttbl=""
                                joindict={"EquationType":eqtyp, "LeftColumn":colleftcol,
                                          "LeftTable":collefttbl,"RightColumn":colrightcol,
                                          "RightTable":colrighttbl, "JoinType":JoinType}
                                join_list.append(joindict)
                                
                    elif k == "eq":
                        eqtyp=k
                        colleft=v[0]
                        colright=v[1]
                        if "." in colleft:
                            colleftcol=v[0].split('.')[1]
                            collefttbl=v[0].split('.')[0]
                        else:
                            colleftcol=v[0]
                            collefttbl=""
                                    
                        if "." in colright:
                            colrightcol=v[1].split('.')[1]
                            colrighttbl=v[1].split('.')[0]
                        else:
                            colrightcol=v[1]
                            colrighttbl=""
                        joindict={"EquationType":eqtyp, "LeftColumn":colleftcol,
                                          "LeftTable":collefttbl,"RightColumn":colrightcol,
                                          "RightTable":colrighttbl, "JoinType":JoinType}
                        join_list.append(joindict)
                        
                        
                tbltmp={"Table":tbl, "Alias":alias}
                table_name.append(tbltmp)
                
    return table_name, join_list


def get_where_details(where_list):
    wherelst=[]
    for k,v in where_list.items():
        if k =='and':
            for i in v:
                for key,val in i.items():
                    eqtyp=key
                    col=val[0]
                    if "." in col:
                        table=col.split(".")[0]
                        col=col.split(".")[1]
                    else:
                        table=""
                        col=col
                    condition=val[1]
                    wheredict={"EquationType":eqtyp, "Column":col,"Table":table, "Condition":condition}
                    wherelst.append(wheredict)
        else:
            eqtyp=k
            col=v[0]
            if "." in col:
                table=col.split(".")[0]
                col=col.split(".")[1]
            else:
                table=""
                col=col
            condition=v[1]
            wheredict={"EquationType":eqtyp, "Column":col,"Table":table, "Condition":condition}
            wherelst.append(wheredict)
    return wherelst

def get_orderby_details(order_list):
    order_dict=[]
    for col in order_list:
        if '.' in col['value']:
            colname=col['value'].split('.')[1]
            tblname=col['value'].split('.')[0]
            orderdict={"Column":colname, "Table":tblname}
            order_dict.append(orderdict)
        else:
            colname=col['value'].split('.')[1]
            tblname=""
            orderdict={"Column":colname, "Table":tblname}
            order_dict.append(orderdict)
            
    return order_dict



# In[2]:


def pandas_builder_sql(table_name_list,join_list,col_list):
    all=[]
    for cols in join_list:
        column_value=cols['LeftColumn']
        column_table=cols['LeftTable']
        alias=""
        collsttmp={"Column":column_value, "Table":column_table,"Alias":alias}
        col_list.append(collsttmp)
        
        column_value=cols['RightColumn']
        column_table=cols['RightTable']
        alias=""
        collsttmp={"Column":column_value, "Table":column_table,"Alias":alias}
        col_list.append(collsttmp)    
        
    for table_name in table_name_list:
        #Reset Column Rename List for different table
        col_rename_dict={}
        querycols=''

        for columns in col_list:
            
            #Querycolumnsonly
            if columns['Table'].upper() == table_name['Table'].upper() or columns['Table'].upper() == table_name['Alias'].upper():
                querycols=querycols+','+columns['Column'] 
            elif len(columns['Table']) ==0:
                querycols=querycols+','+columns['Column'] 
            #Column Renaming 
            if len(columns['Alias']) > 0:
                
                if columns['Table'].upper() == table_name['Table'].upper() or columns['Table'].upper() == table_name['Alias'].upper():
                    if columns['Column'] not in col_rename_dict.keys():
                        col_rename_dict[columns['Column']] = columns['Alias']
                                            
                elif len(columns['Table']) ==0:
                    if columns['Column'] not in col_rename_dict.keys():
                        col_rename_dict[columns['Column']] = columns['Alias']
                        
                        
        # 1. create connection to required database e.g. snowflake to read table
        querycols=querycols.strip(',')
        if len(querycols) > 0:
            sql = f"select {querycols} from {table_name['Table']}"
        else:
            sql = f"select * from {table_name['Table']}"
           
        
        # create where list
        if "where" in query_dict.keys():
            where_list = query_dict["where"]
            where_clauses = get_where_details(where_list)
            whereclausecntr=0
            # add where clause to table query  
            for where in where_clauses:
                whereclausecntr+=1
            
                if type(where['Condition']) == str:
                    wherecondtn = "\"{0}\"".format(where['Condition'])
                elif type(where['Condition']) == list:
                    wherecondtn = tuple(i for i in where['Condition'])
                else:
                    wherecondtn = where['Condition']                             
                                    
                
                if where['Table'].upper() == table_name['Table'].upper() or where['Table'].upper() == table_name['Alias'].upper():
                    if where['EquationType'] =='eq':
                    
                        if whereclausecntr ==1:
                            sql = sql+ " where {0}={1}".format(where['Column'],wherecondtn)
                        else:
                            sql = sql+ " and {0}={1}".format(where['Column'],wherecondtn)
                    if where['EquationType'] =='in':
                    
                        if whereclausecntr ==1:
                            sql = sql+ " where {0} in {1}".format(where['Column'],wherecondtn)
                        else:
                            sql = sql+ " and {0} in {1}".format(where['Column'],wherecondtn)
            
                elif len(where['Table']) ==0:
                    if where['EquationType'] =='eq':
                    
                        if whereclausecntr ==1:
                            sql = sql+ " where {0}={1}".format(where['Column'],wherecondtn )
                        else:
                            sql = sql+ " and {0}={1}".format(where['Column'],wherecondtn )
                    if where['EquationType'] =='in':
                    
                        if whereclausecntr ==1:
                            sql = sql+ " where {0} in {1}".format(where['Column'],wherecondtn)
                        else:
                            sql = sql+ " and {0} in {1}".format(where['Column'],wherecondtn)
                        
            
        else:
            pass
        
        defineDF = table_name['Table']+"_df" + " = " + "pd.read_sql('" + sql + "')"
        all.append(defineDF)
        if col_rename_dict:
            alpha1 = f"{table_name['Table']}_df={table_name['Table']}_df.rename(columns={col_rename_dict})"
            all.append(alpha1)
        else:
            alpha1 = f"{table_name['Table']}_df={table_name['Table']}_df"
            all.append(alpha1)
            
        
    #print("________(first function)________")                  
    #Add Join Conditions
    Lefttablelist=[]
    left_on=[]
    right_on=[]
    loopcntr=0
    mergecntr=0
    for joins in join_list:
        loopcntr+=1
        Lefttable=[i['Table'] for i in table_name_list if i['Table'].upper() == joins['LeftTable'].upper() 
                    or i['Alias'].upper() == joins['LeftTable'].upper()][0]
        
        Righttable=[i['Table'] for i in table_name_list if i['Table'].upper() == joins['RightTable'].upper() 
                    or i['Alias'].upper() == joins['RightTable'].upper()][0]
        
        Leftjoincol=joins['LeftColumn']
        Rightjoincol=joins['RightColumn']
                  
        
        if 'join' == joins['JoinType'] or 'inner' in joins['JoinType']:
            how='inner'
        elif 'left' in joins['JoinType']:
            how='left'
        elif 'right' in joins['JoinType']:
            how='right'
            
        Bothtable=Lefttable+"#"+Righttable
        if len(Lefttablelist) ==0:
            left_on.append(Leftjoincol)
            right_on.append(Rightjoincol)
            Lefttablelist.append(Bothtable)
            Lefttablelist=list(set(Lefttablelist))
            alpha2 = """{0} =pd.merge({1},{2},how={3},left_on={4},right_on ={4} )""".format(Lefttablelist[-1].split('#')[0]+'_'+Lefttablelist[-1].split('#')[1]+'_df',Lefttablelist[-1].split('#')[0]+'_df',Lefttablelist[-1].split('#')[1]+'_df',how, left_on, right_on )
            all.append(alpha2)
                  
            mergedfname=Lefttablelist[-1].split('#')[0]+'_'+Lefttablelist[-1].split('#')[1]

        elif Bothtable == Lefttablelist[-1] and loopcntr < len(join_list):
            left_on.append(Leftjoincol)
            right_on.append(Rightjoincol)
            Lefttablelist.append(Bothtable)
            Lefttablelist=list(set(Lefttablelist))
            
            
        elif Bothtable != Lefttablelist[-1] and loopcntr < len(join_list):
            mergecntr+=1
            if mergecntr ==1:
                alpha2 = """{0} =pd.merge({1},{2},how={3},left_on={4},right_on ={4} )""".format(Lefttablelist[-1].split('#')[0]+'_'+Lefttablelist[-1].split('#')[1]+'_df',Lefttablelist[-1].split('#')[0]+'_df',Lefttablelist[-1].split('#')[1]+'_df',how, left_on, right_on )
                all.append(alpha2)
                mergedfname=Lefttablelist[-1].split('#')[0]+'_'+Lefttablelist[-1].split('#')[1]
            else:
                mergedfname=mergedfname+'_'+Lefttablelist[-1].split('#')[1]
                alpha2 = """{0} =pd.merge({1},{2},how={3},left_on={4},right_on ={4} )""".format(mergedfname,mergedfname,Lefttablelist[-1].split('#')[1]+'_df', how, left_on, right_on)
                all.append(alpha2)
                  
            all.append(mergedfname)
            Lefttablelist=[]
            Lefttablelist.append(Bothtable)
            Lefttablelist=list(set(Lefttablelist))
            left_on=[]
            left_on.append(Leftjoincol)
            right_on=[]
            right_on.append(Rightjoincol)
            
        else:
            left_on.append(Leftjoincol)
            right_on.append(Rightjoincol)
            alpha2= """{0} =pd.merge({1},{2},how={3},left_on={4},right_on ={4} )""".format(mergedfname+'_'+Righttable+'_df',mergedfname+'_df',Righttable,how, left_on, right_on )
            all.append(alpha2)
            mergedfname=mergedfname+'_'+Righttable
            all.append(mergedfname)       
            
    Finaldf=mergedfname+'_df'
    print("Final Dataframe is:",Finaldf)
    
    #logger
    deletePart = "{0} = {0}.loc[:,~{0}.columns.duplicated()]".format(Finaldf)
    all.append(deletePart)
                  
    #Order By Cluases
    if "orderby" in query_dict:
        order_list=query_dict["orderby"]
        order_dict=get_orderby_details(order_list)
        order_list_fnl=[]
        for i in order_dict:
            order_list_fnl.append(i['Column'])
        order_by = "{0}.sort_values(by={1})".format(Finaldf,order_list_fnl)
        all.append(order_by)
    else:
        pass
    return all 
    


# In[ ]:





# # Second Part of code
# 

# # Minimum Mains

# In[3]:



    query = """SELECT coalesce( A.crossover_rms,0) as CO_RN_Goal, coalesce( ( A.crossover_rms*B.crossover_gadr),0) as CO_Rev_Goal,
     A.marsha as MARS, A.stay_year as stay_year_REN, A.CO_RN_Goal, A.CO_Rev_Goal, A.CO_RN_Goal_ADR, A.Def_OTB, A.Def_REV, A.Def_ADR, A.Target, A.Avg_Bkd  FROM merge_CrossOver1 A join merge_CrossOver B on A.marsha = B.marsha and A.marsha1 = B.marsha1
     join merge_CrossOver2 C on A.marsha = C.marsha and A.marsha1 = C.marsha1 Where A.Target=1 and A.Target in (1,2,3,4) and A.Avg_Bkd="ABCD" order by A.marsha,A.Avg_Bkd"""

# query= """SELECT a.col as newCol1, b.col2 as newCol2
#      FROM merge_CrossOver1 A join merge_CrossOver b on a.c = b.c"""   
    
query_dict = parse(query.lower())
    
select_list = query_dict["select"]
col_list = get_column_details(select_list)   
from_tag = query_dict["from"]
table_name_list,join_list = get_table_details(from_tag)
abc = pandas_builder_sql(table_name_list,join_list,col_list)
abc    


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




