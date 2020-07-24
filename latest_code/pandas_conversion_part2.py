#!/usr/bin/env python
# coding: utf-8

# # Pandas conversion from SQL

# ### import

# In[1]:


from moz_sql_parser import parse


# ### input query

# In[2]:


query = """SELECT sum(A.alpha) as alpha1, 
    coalesce( A.crossover_rms,0) as CO_RN_Goal, 
    coalesce( ( A.crossover_rms*B.crossover_gadr),0) as CO_Rev_Goal,
    A.marsha as MARS, 
    A.stay_year as stay_year_REN, 
    A.CO_RN_Goal, 
    A.CO_Rev_Goal, 
    A.CO_RN_Goal_ADR, 
    A.Def_OTB, 
    A.Def_REV, 
    A.Def_ADR, 
    A.Target, 
    A.Avg_Bkd  
    FROM merge_CrossOver1 A 
    join merge_CrossOver B 
    on A.marsha = B.marsha and A.marsha1 = B.marsha1
    join 
    merge_CrossOver2 C on A.marsha = C.marsha and A.marsha1 = C.marsha1 
    Where A.Target=1 
    and A.Target in (1,2,3,4) 
    and A.Avg_Bkd="ABCD" 
	group by 
    crossover_rms, 
	crossover_gadr,marsha,
	stay_year,
	CO_RN_Goal,
	CO_Rev_Goal,
	CO_RN_Goal_ADR,
	Def_OTB,
	Def_REV,
	Def_ADR,
	Target,
	Avg_Bkd
	order by 
    A.marsha,A.Avg_Bkd"""
query_dict = parse(query.lower())
query_dict
select_list = query_dict["select"]
select_list
group_section = query_dict['groupby']
group_section


# In[3]:


query_dict


# ### Intermediate step which seperates the UDF'S , original cols and its associated aliases if any

# In[4]:


def select_complex(select_list):
    column_list = []
    for column_dict in select_list:
        if type(column_dict['value'])== str:
            if '.' in column_dict['value']:
                column_value = column_dict['value'].split('.')[1]
                column_table = column_dict['value'].split('.')[0]
                try:
                    alias=column_dict['name']
                except:
                    alias=""
                collsttmp={"base_col":column_value,"udf":"","Alias":alias}
                column_list.append(collsttmp)
            else:
                column_value = column_dict['value']
                column_table=""
                try:
                    alias=column_dict['name']
                except:
                    alias=""
                collsttmp={"base_col":column_value,"udf":"","Alias":alias}
                column_list.append(collsttmp)
            
            
        elif type(column_dict['value'])== dict:
            if '.' in column_dict['value']:
                column_value = column_dict['value'].split('.')[1]
                column_table = column_dict['value'].split('.')[0]
                try:
                    alias=column_dict['name']
                except:
                    alias=""
                colsttmp={"base_col":column_value, "Table":column_table,"Alias":alias}
                column_list.append(colsttmp)
            else:
                column_value=column_dict['value']
                final_col=[]
                for k,v in column_dict['value'].items():
                    udf=k
                    cols=v
                    if type(cols)==str:
                        if '.' in cols:
                            col_name =cols.split('.')
                            final_col.append(col_name[1])
                        else:
                            final_col.append(cols)
                    else:
                        for i in cols:
                            if type(i)==str:
                                if '.' in i:
                                    column_name= i.split('.')
                                    col_name= column_name[1]
                                    final_col.append(col_name)
                                else:
                                    final_col.append(i)
                        
                            elif type(i)==int:
                                final_col.append(i)
                    
                            elif type(i)==dict:
                                new_dict=i
                                for k,v in new_dict.items():
                                    extra_udf=k
                                    udf=udf+","+extra_udf
                                    cols=v
                                    for i in cols:
                                        if '.' in i:
                                            splitter= i.split('.')
                                            part1=splitter[0] 
                                            part2=splitter[1]
                                            final_col.append(part2)
                                        else:
                                            final_col.append(i)    
                            else:
                                pass
                try:
                    alias=column_dict['name']
                except:
                    alias=""
                colltmp={"base_col":final_col, "udf":udf,"Alias":alias}
                column_list.append(colltmp)
    return column_list


# In[5]:


select_complex(select_list)


# In[ ]:





# ## SELECT PART

# ### This step will return the panda code of select part of the query in step1

# In[6]:



def panda_builder(final_df,sql_dict):
    query_list=[]
    for list_elements in sql_dict:
        columns =list_elements['base_col']
        final_columns =[s for s in columns if type(s)==str]
        alias=list_elements['Alias']
        udf=list_elements['udf']
        if list_elements['udf']!='':
            udf_splitter = list_elements['udf'].split(',')
            len_udf =len(udf_splitter)
            if  len_udf==1 and udf=="coalesce":
                coalesce_filler=str(columns[-1])
                query =final_df+"[`"+alias+"`]"+"="+final_df+"."+final_columns[0]+".fillna(value="+coalesce_filler+',inplace=True)'
                query_list.append(query)
            elif len_udf==1 and udf=="mul":
                list_of_col = ["row."+a for a in final_columns]
                cols='*'.join(list_of_col)
                query = final_df+"[`"+alias+"`]"+"="+final_df+'.apply(lambda row: '+cols+', axis = 1)'
                query_list.append(query)
                
            # THIS PART NEEDS TO BE REVISITED
            elif len_udf==1 and udf=="sum":
                columns=columns[0]
                query= final_df+"[`"+columns+"`]"+"="+final_df+"[`"+columns+"`]"
                query_list.append(query)
            
            elif len_udf>1:
                # here we need to consider a scenario where the udf is not more than2 udfs
                for udf in reversed(udf_splitter):
                    if udf =='mul':
                        list_of_col = ["row."+a for a in final_columns]
                        cols='*'.join(list_of_col)
                        query = final_df+"[`"+alias+"`]"+"="+final_df+'.apply(lambda row: '+cols+', axis = 1)'
                        query_list.append(query)
                    elif udf=='coalesce':
                        coalesce_filler=str(columns[-1])
                        query =final_df+"[`"+alias+"`]"+"="+final_df+"."+final_columns[0]+".fillna(value="+coalesce_filler+',inplace=True)'
                        query_list.append(query) 
                    
                    
        else:
            columns =list_elements['base_col']
            alias=list_elements['Alias']
            if alias=='':
                query=final_df+"[`"+columns+"`]"+"="+final_df+"[`"+columns+"`]"
                query_list.append(query)
            else:
                query=final_df+"[`"+alias+"`]"+"="+final_df+"[`"+columns+"`]"
                query_list.append(query)
    return query_list


# In[7]:


final_df = "merge_crossover1_merge_crossover_merge_crossover2_df"
sql_dict = select_complex(select_list)
panda_builder(final_df,sql_dict)


# ## GROUP_BY PART ---07/06/20

# ### This part will select the required col which needs to be grouped by

# In[8]:


# def grp_cols(select_list):
#     cols_udf= select_complex(select_list)
#     col_list = []
#     for item in cols_udf:
#         if item['udf']!="sum":
#             name = item['base_col']
#             if type(name)==str:
#                 col_list.append(name)
#             else:
#                 col_list.append(name)
#         else:
#             pass
#     list2 = []
#     for x in col_list:
#         list2 += x if type(x) == list else [x]
#     grp_cols = [x for x in list2 if not isinstance(x, int)]
#     final_cols = list(OrderedDict.fromkeys(grp_cols))
#     return final_cols
# grp_cols(select_list)


# In[9]:


def grp_cols(group_section):
    list1=[]
    for i in group_section:
        values =i['value']
        list1.append(values)
    return list1
grp_cols(group_section)


# ### This part will select the cols which will go before agg

# In[10]:


def group_agg(select_list):
    for i in select_list:
        if i['udf']=="sum":
            return i['base_col']
        else:
            pass
group_agg(select_complex(select_list))


# In[11]:


def group_by_func(final_df,query_dict,grp_cols,group_agg):
    if 'groupby' in query_dict.keys():
        abc = final_df+"="+final_df+".groupby("+str(grp_cols)+")"+str(group_agg)+".agg(sum)"
    return abc

    
    


# In[12]:


final_df="merge_crossover1_merge_crossover_merge_crossover2_df"
query_dict=query_dict
grp_cols=grp_cols(group_section)
group_agg=group_agg(select_complex(select_list))
group_by_func(final_df,query_dict,grp_cols,group_agg)


# In[ ]:





# In[ ]:





# # ROUGH

# In[ ]:





# In[ ]:


# abc = final_df+"="+"final_df"+".groupby("+str(grp_cols)+")"+str(group_agg)+".agg(sum)"
# abc


# In[ ]:


# def group_by_function(final_df,query_dict,group_section,group_agg):
#     group_agg=group_agg(select_complex(select_list))
#     group_agg=
#     if 'groupby' in query_dict.keys():
#         query = final_df+"="+final_df+"("+
#     else:
#         return panda_builder


# In[ ]:


# final_df = "merge_crossover1_merge_crossover_merge_crossover2_df"
# group_by_function(final_df,query_dict)


# In[ ]:





# In[ ]:





# In[ ]:


# def group_by_function(query_dict,panda_builder):
#     if 'groupby' in query_dict.keys():
#         print("yes")
#     else:
#         return panda_builder


# In[ ]:


# final_df = "merge_crossover1_merge_crossover_merge_crossover2_df"
# sql_dict = select_complex(select_list)
# panda_builder(final_df,sql_dict)
# group_by_function(query_dict,panda_builder)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# select_complex(select_list)


# 

# In[ ]:




    


# In[ ]:


# grp_cols(select_list)


# 

# In[ ]:


# cols_udf= select_complex(select_list)
# col_list = []
# for item in cols_udf:
#     name = item['base_col']
#     if type(name)==str:
#         col_list.append(name)
#     else:
#         col_list.append(name)
# list2 = []
# for x in col_list:
#     list2 += x if type(x) == list else [x]
# grp_cols = [x for x in list2 if not isinstance(x, int)]
# print(grp_cols)


# In[ ]:


# import itertools
# l1=['alpha','f','d',['alpha','beta',0]]
# flatlist = list(itertools.chain(*l1))
# flatlist


# In[ ]:


# from compiler.ast import flatten


# In[ ]:


# mylist =['a',0]
# no_integers = [x for x in mylist if not isinstance(x, int)]
# no_integers


# In[ ]:


# from collections import OrderedDict
# t=['alpha', 'crossover_rms', 'crossover_rms', 'crossover_gadr', 'marsha', 'stay_year', 'co_rn_goal', 'co_rev_goal', 'co_rn_goal_adr', 'def_otb', 'def_rev', 'def_adr', 'target', 'avg_bkd']

# a=list(OrderedDict.fromkeys(t))
# a


# In[ ]:


# abc = ['crossover_rms',
#  'crossover_gadr',
#  'marsha',
#  'stay_year',
#  'co_rn_goal',
#  'co_rev_goal',
#  'co_rn_goal_adr',
#  'def_otb',
#  'def_rev',
#  'def_adr',
#  'target',
#  'avg_bkd']
# str(abc)


# In[ ]:


# a =["alpha"]
# b =str(a)
# c="ab"
# b+c


# In[ ]:




