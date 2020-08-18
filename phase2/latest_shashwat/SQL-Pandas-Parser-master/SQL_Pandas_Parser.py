from Templates.PythonScript import PythonScript


class SQL_Pandas_Parser(PythonScript):

    def __init__(self, sqlQuery):
        super().__init__(sqlQuery)


if __name__ == '__main__':

    import pandas as pd
    import numpy as np

    query ="""CREATE TABLE Futures AS  ( select marsha, 
    coalesce( SUM( crossover_rms*def_gadr),0) AS CO_Rev_Goal,
    coalesce( SUM( crossover_rms),0) AS CO_RN_Goal,  
    SUM( def_rms) AS Def_OTB, 
    SUM( def_rms*def_gadr) AS Def_Rev, 
    SUM( cid_rms) AS Target, SUM( avg_rms) AS Avg_Bkd 
    FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER WHERE stay_year > curyr AND ASOF_YRMO=CURYRPD GROUP BY marsha) ;"""
    spp = SQL_Pandas_Parser(query)
    scripts = spp.buildPandasScript()
    for a in scripts:
        print(a)