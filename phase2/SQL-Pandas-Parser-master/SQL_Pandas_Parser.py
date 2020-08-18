from Templates.PythonScript import PythonScript


class SQL_Pandas_Parser(PythonScript):

    def __init__(self, sqlQuery):
        super().__init__(sqlQuery)


if __name__ == '__main__':

    import pandas as pd
    import numpy as np
    query = """create table alpha as select marsha, stay_year, coalesce( crossover_rms,0) AS CO_RN_Goal, 
coalesce( ( crossover_rms*crossover_gadr),0) AS CO_Rev_Goal, def_rms AS Def_OTB,
( def_rms*def_gadr*-1) AS Def_Rev,
CID_Rms AS Target,
Avg_rms AS Avg_Bkd 
FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER
WHERE stay_year < YEARNXT3 AND ASOF_YRMO=CURYRPD ORDER BY marsha, stay_year ;"""

    query ="""SELECT 
 ACTUALS*-1 AS ACTUALS ,
 BUDGET*-1 AS BUDGET , 
 SUBSTRING( _NAME_,2,2 ) AS a1
 FROM SLCT_SRVC_CATERING_TRANS """

    query ="""SELECT distinct prop_code, year( stay_dt) AS year, month( stay_dt) AS month, COUNT( user_capped) AS act_ct FROM uat1_d WHERE act_status='Y' GROUP BY prop_code, year, month"""

    query ="""CREATE TABLE Futures AS  ( select marsha, 'Futures' AS Stay_Year, coalesce( SUM( crossover_rms),0) AS CO_RN_Goal, coalesce( SUM( crossover_rms*def_gadr),0) AS CO_Rev_Goal, SUM( def_rms) AS Def_OTB, SUM( def_rms*def_gadr) AS Def_Rev, SUM( cid_rms) AS Target, SUM( avg_rms) AS Avg_Bkd FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER WHERE stay_year > curyr AND ASOF_YRMO=CURYRPD GROUP BY marsha) """
    spp = SQL_Pandas_Parser(query)
    scripts = spp.buildPandasScript()
    for a in scripts:
        print(a)

    # sqlQueries = pd.read_csv("sqlqueries.csv")
    # queries = np.array(sqlQueries['SQL'])
    # fileNames = np.array(sqlQueries['filename'])
    #
    # for i in range(len(queries)):
    #     spp = SQL_Pandas_Parser(queries[i])
    #     scripts = spp.buildPandasScript()
    #     with open(fileNames[i] + ".py", 'a') as f:
    #         for script in scripts:
    #             f.write(script)
