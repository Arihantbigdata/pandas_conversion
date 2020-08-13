"""Query: CREATE TABLE Currents AS  ( select marsha, stay_year, coalesce( crossover_rms,0) AS CO_RN_Goal, coalesce( ( crossover_rms*crossover_gadr),0) AS CO_Rev_Goal, def_rms AS Def_OTB, ( def_rms*def_gadr) AS Def_Rev, CID_Rms AS Target, Avg_rms AS Avg_Bkd FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER WHERE stay_year < YEARNXT3 AND ASOF_YRMO=CURYRPD ORDER BY marsha, stay_year) ;"""

import pandas as pd
import datetime

oy_annualcrossover = pd.read_sql('select marsha, stay_year, def_rms, cid_rms, avg_rms, crossover_rms, crossover_gadr, def_gadr from oy_annualcrossover')

oy_annualcrossover = oy_annualcrossover.rename(columns = {'def_rms': 'def_otb', 'cid_rms': 'target', 'avg_rms': 'avg_bkd'})



oy_annualcrossover['co_rn_goal']=oy_annualcrossover.crossover_rms.fillna(value=0,inplace=True)
oy_annualcrossover['co_rev_goal']=oy_annualcrossover.apply(lambda row: row.crossover_rms*row.crossover_gadr, axis = 1)
oy_annualcrossover['co_rev_goal']=oy_annualcrossover.co_rev_goal.fillna(value=0,inplace=True)
oy_annualcrossover['def_rev']=oy_annualcrossover.apply(lambda row: row.def_rms*row.def_gadr, axis = 1)


oy_annualcrossover = oy_annualcrossover.query('stay_year < yearnxt3 & asof_yrmo=curyrpd')
oy_annualcrossover.sort_values(by = ['marsha', 'stay_year'], inplace = True)

currents = oy_annualcrossover
pd.to_sql(currents, con = SQL_ENGINE, if_exists = 'replace', index = False)
"""Query: CREATE TABLE Futures AS  ( select marsha, 'Futures' AS Stay_Year, coalesce( SUM( crossover_rms),0) AS CO_RN_Goal, coalesce( SUM( crossover_rms*def_gadr),0) AS CO_Rev_Goal, SUM( def_rms) AS Def_OTB, SUM( def_rms*def_gadr) AS Def_Rev, SUM( cid_rms) AS Target, SUM( avg_rms) AS Avg_Bkd FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER WHERE stay_year > curyr AND ASOF_YRMO=CURYRPD GROUP BY marsha ORDER BY marsha) ;"""

import pandas as pd
import datetime

oy_annualcrossover = pd.read_sql('select marsha, stay_year, def_rms, cid_rms, avg_rms, crossover_rms, crossover_gadr, def_gadr, futures from oy_annualcrossover')



oy_annualcrossover = oy_annualcrossover.groupby(by = ['marsha'])
oy_annualcrossover['stay_year']='futures'
oy_annualcrossover['co_rn_goal'] = oy_annualcrossover['crossover_rms'].sum()
oy_annualcrossover['co_rn_goal']=oy_annualcrossover.co_rn_goal.fillna(value=0,inplace=True)
oy_annualcrossover['co_rev_goal']=oy_annualcrossover.apply(lambda row: row.crossover_rms*row.def_gadr, axis = 1)
oy_annualcrossover['co_rev_goal'] = oy_annualcrossover['crossover_rms'].sum()
oy_annualcrossover['co_rev_goal']=oy_annualcrossover.co_rev_goal.fillna(value=0,inplace=True)
oy_annualcrossover['def_otb'] = oy_annualcrossover['def_rms'].sum()
oy_annualcrossover['def_rev']=oy_annualcrossover.apply(lambda row: row.def_rms*row.def_gadr, axis = 1)
oy_annualcrossover['def_rev'] = oy_annualcrossover['def_rms'].sum()
oy_annualcrossover['target'] = oy_annualcrossover['cid_rms'].sum()
oy_annualcrossover['avg_bkd'] = oy_annualcrossover['avg_rms'].sum()


oy_annualcrossover = oy_annualcrossover.query('stay_year > curyr & asof_yrmo=curyrpd')
oy_annualcrossover.sort_values(by = ['marsha'], inplace = True)

futures = oy_annualcrossover
pd.to_sql(futures, con = SQL_ENGINE, if_exists = 'replace', index = False)
"""Query: CREATE TABLE merge_CrossOver1 AS ( SELECT  *, marsha AS marsha2, FORMAT(stay_year, VARCHAR(7)) AS stay_year , marsha2 AS marsha , CASE WHEN  CO_Rev_Goal=' ' OR CO_Rev_Goal=' ' OR CO_RN_Goal=0 OR CO_Rev_Goal=0  THEN 0 ELSE CO_Rev_Goal/CO_RN_Goal END AS CO_RN_Goal_ADR, CASE WHEN  Def_OTB=' ' OR Def_REV=' ' OR Def_OTB=' ' OR Def_REV=0  THEN 0 ELSE Def_REV/Def_OTB END AS Def_ADR FROM Currents A JOIN Futures B ON  A.marsha2 =  B.marsha2 AND  A.stay_year =  B.stay_year);"""

import pandas as pd
import datetime

oy_annualcrossover = pd.read_sql('select * from oy_annualcrossover')
currents = pd.read_sql('select marsha2, stay_year from currents')
futures = pd.read_sql('select marsha2, stay_year from futures')

oy_annualcrossover = oy_annualcrossover.rename(columns = {'marsha': 'marsha2', 'marsha2': 'marsha'})

currents = pd.merge(currents, futures, how = 'inner', left_on = ['marsha2', 'stay_year'], right_on = ['marsha2', 'stay_year'])
currents = currents.loc[:, ~currents.columns.duplicated()]


oy_annualcrossover['stay_year']=oy_annualcrossover['stay_year'].str[:7]

oy_annualcrossover['co_rn_goal_adr'] = oy_annualcrossover['co_rev_goal'] / oy_annualcrossover['co_rn_goal']
oy_annualcrossover.loc[(oy_annualcrossover['co_rev_goal'] == ' ') | (oy_annualcrossover['co_rev_goal'] == ' ') | (oy_annualcrossover['co_rn_goal'] == 0) | (oy_annualcrossover['co_rev_goal'] == 0), 'co_rn_goal_adr'] = 0
oy_annualcrossover['def_adr'] = oy_annualcrossover['def_rev'] / oy_annualcrossover['def_otb']
oy_annualcrossover.loc[(oy_annualcrossover['def_otb'] == ' ') | (oy_annualcrossover['def_rev'] == ' ') | (oy_annualcrossover['def_otb'] == ' ') | (oy_annualcrossover['def_rev'] == 0), 'def_adr'] = 0




merge_crossover1 = oy_annualcrossover
pd.to_sql(merge_crossover1, con = SQL_ENGINE, if_exists = 'replace', index = False)
"""Query: CREATE TABLE Currents AS  ( select marsha, stay_year, coalesce( crossover_rms,0) AS CO_RN_Goal, coalesce( ( crossover_rms*crossover_gadr),0) AS CO_Rev_Goal, def_rms AS Def_OTB, ( def_rms*def_gadr) AS Def_Rev, CID_Rms AS Target, Avg_rms AS Avg_Bkd FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER WHERE stay_year < YEARNXT3 AND ASOF_YRMO=CURYRPD ORDER BY marsha, stay_year) ;"""

import pandas as pd
import datetime

oy_annualcrossover = pd.read_sql('select marsha, stay_year, def_rms, cid_rms, avg_rms, crossover_rms, crossover_gadr, def_gadr from oy_annualcrossover')

oy_annualcrossover = oy_annualcrossover.rename(columns = {'def_rms': 'def_otb', 'cid_rms': 'target', 'avg_rms': 'avg_bkd'})



oy_annualcrossover['co_rn_goal']=oy_annualcrossover.crossover_rms.fillna(value=0,inplace=True)
oy_annualcrossover['co_rev_goal']=oy_annualcrossover.apply(lambda row: row.crossover_rms*row.crossover_gadr, axis = 1)
oy_annualcrossover['co_rev_goal']=oy_annualcrossover.co_rev_goal.fillna(value=0,inplace=True)
oy_annualcrossover['def_rev']=oy_annualcrossover.apply(lambda row: row.def_rms*row.def_gadr, axis = 1)


oy_annualcrossover = oy_annualcrossover.query('stay_year < yearnxt3 & asof_yrmo=curyrpd')
oy_annualcrossover.sort_values(by = ['marsha', 'stay_year'], inplace = True)

currents = oy_annualcrossover
pd.to_sql(currents, con = SQL_ENGINE, if_exists = 'replace', index = False)
"""Query: CREATE TABLE Futures AS  ( select marsha, 'Futures' AS Stay_Year, coalesce( SUM( crossover_rms),0) AS CO_RN_Goal, coalesce( SUM( crossover_rms*def_gadr),0) AS CO_Rev_Goal, SUM( def_rms) AS Def_OTB, SUM( def_rms*def_gadr) AS Def_Rev, SUM( cid_rms) AS Target, SUM( avg_rms) AS Avg_Bkd FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER WHERE stay_year > curyr AND ASOF_YRMO=CURYRPD GROUP BY marsha ORDER BY marsha) ;"""

import pandas as pd
import datetime

oy_annualcrossover = pd.read_sql('select marsha, stay_year, def_rms, cid_rms, avg_rms, crossover_rms, crossover_gadr, def_gadr, futures from oy_annualcrossover')



oy_annualcrossover = oy_annualcrossover.groupby(by = ['marsha'])
oy_annualcrossover['stay_year']='futures'
oy_annualcrossover['co_rn_goal'] = oy_annualcrossover['crossover_rms'].sum()
oy_annualcrossover['co_rn_goal']=oy_annualcrossover.co_rn_goal.fillna(value=0,inplace=True)
oy_annualcrossover['co_rev_goal']=oy_annualcrossover.apply(lambda row: row.crossover_rms*row.def_gadr, axis = 1)
oy_annualcrossover['co_rev_goal'] = oy_annualcrossover['crossover_rms'].sum()
oy_annualcrossover['co_rev_goal']=oy_annualcrossover.co_rev_goal.fillna(value=0,inplace=True)
oy_annualcrossover['def_otb'] = oy_annualcrossover['def_rms'].sum()
oy_annualcrossover['def_rev']=oy_annualcrossover.apply(lambda row: row.def_rms*row.def_gadr, axis = 1)
oy_annualcrossover['def_rev'] = oy_annualcrossover['def_rms'].sum()
oy_annualcrossover['target'] = oy_annualcrossover['cid_rms'].sum()
oy_annualcrossover['avg_bkd'] = oy_annualcrossover['avg_rms'].sum()


oy_annualcrossover = oy_annualcrossover.query('stay_year > curyr & asof_yrmo=curyrpd')
oy_annualcrossover.sort_values(by = ['marsha'], inplace = True)

futures = oy_annualcrossover
pd.to_sql(futures, con = SQL_ENGINE, if_exists = 'replace', index = False)
"""Query: CREATE TABLE merge_CrossOver1 AS ( SELECT  *, marsha AS marsha2, FORMAT(stay_year, VARCHAR(7)) AS stay_year , marsha2 AS marsha , CASE WHEN  CO_Rev_Goal=' ' OR CO_Rev_Goal=' ' OR CO_RN_Goal=0 OR CO_Rev_Goal=0  THEN 0 ELSE CO_Rev_Goal/CO_RN_Goal END AS CO_RN_Goal_ADR, CASE WHEN  Def_OTB=' ' OR Def_REV=' ' OR Def_OTB=' ' OR Def_REV=0  THEN 0 ELSE Def_REV/Def_OTB END AS Def_ADR FROM Currents A JOIN Futures B ON  A.marsha2 =  B.marsha2 AND  A.stay_year =  B.stay_year);"""

import pandas as pd
import datetime

oy_annualcrossover = pd.read_sql('select * from oy_annualcrossover')
currents = pd.read_sql('select marsha2, stay_year from currents')
futures = pd.read_sql('select marsha2, stay_year from futures')

oy_annualcrossover = oy_annualcrossover.rename(columns = {'marsha': 'marsha2', 'marsha2': 'marsha'})

currents = pd.merge(currents, futures, how = 'inner', left_on = ['marsha2', 'stay_year'], right_on = ['marsha2', 'stay_year'])
currents = currents.loc[:, ~currents.columns.duplicated()]


oy_annualcrossover['stay_year']=oy_annualcrossover['stay_year'].str[:7]

oy_annualcrossover['co_rn_goal_adr'] = oy_annualcrossover['co_rev_goal'] / oy_annualcrossover['co_rn_goal']
oy_annualcrossover.loc[(oy_annualcrossover['co_rev_goal'] == ' ') | (oy_annualcrossover['co_rev_goal'] == ' ') | (oy_annualcrossover['co_rn_goal'] == 0) | (oy_annualcrossover['co_rev_goal'] == 0), 'co_rn_goal_adr'] = 0
oy_annualcrossover['def_adr'] = oy_annualcrossover['def_rev'] / oy_annualcrossover['def_otb']
oy_annualcrossover.loc[(oy_annualcrossover['def_otb'] == ' ') | (oy_annualcrossover['def_rev'] == ' ') | (oy_annualcrossover['def_otb'] == ' ') | (oy_annualcrossover['def_rev'] == 0), 'def_adr'] = 0




merge_crossover1 = oy_annualcrossover
pd.to_sql(merge_crossover1, con = SQL_ENGINE, if_exists = 'replace', index = False)
