"""Query: CREATE TABLE Futures AS  ( SELECT marsha, 'Futures' AS Stay_Year, coalesce( SUM( crossover_rms),0) AS CO_RN_Goal, coalesce( SUM( crossover_rms*def_gadr),0) AS CO_Rev_Goal, SUM( def_rms) AS Def_OTB, SUM( def_rms*def_gadr) AS Def_Rev, SUM( cid_rms) AS Target, SUM( avg_rms) AS Avg_Bkd FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER WHERE stay_year > &curyr AND ASOF_YRMO=&CURYRPD GROUP BY marsha ORDER BY marsha);"""

import pandas as pd
import datetime

oy_annualcrossover = pd.read_sql('select marsha, futures, crossover_rms, def_gadr, def_rms, cid_rms, avg_rms from oy_annualcrossover')



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
