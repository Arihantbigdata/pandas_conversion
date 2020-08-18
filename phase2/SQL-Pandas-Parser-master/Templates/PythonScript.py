from SQL_Query.SQLQuery import *
from Utils.Functions import *
from Utils import *
import re
from moz_sql_parser import parse


class PythonScript():

    def __init__(self, sqlQuery):
        self.sqlQuery = sqlQuery
        self.queryObject = SQLQuery(sqlQuery)
        self.tableNames = self.queryObject.tableNames
        self.tableColumnsDict = self.queryObject.tableColumnsDict
        self.tableAliases = self.queryObject.tableAliases

    #
    # This methods imports the relevant packages as will be
    # needed by the python script to process SQL Query
    #
    def importPackage(self, impPkg, alias="", frmPkg=""):
        if frmPkg == "":
            if alias == "":
                return "import " + impPkg
            else:
                return "import " + impPkg + " as " + alias
        else:
            if alias == "":
                return "from " + frmPkg + " import " + impPkg
            else:
                return "from " + frmPkg + " import " + impPkg + " as " + alias

    #
    # This methods reads and imports all the columns needed for the Pandas processing
    # of the given SQL Query
    #
    def readPandasDFs(self):
        finalScript = []
        tables = self.tableColumnsDict.keys()
        for table in tables:
            columns = list(self.tableColumnsDict[table].keys())
            if "*" in columns:
                sqlScript = "select * from " + table
            else:
                sqlScript = "select " + ", ".join(columns) + " from " + table
            script = table + " = pd.read_sql('" + sqlScript + "')"
            finalScript.append(script)
        return finalScript

    #
    # This methods renames the columns of the respective tables
    # based on the aliases mentioned in the SQL Query
    #
    def renameColumns(self):
        finalScript = []
        tables = self.tableColumnsDict.keys()

        tables = [a for a in tables]
        for table in tables:
            columns = list(self.tableColumnsDict[table].keys())
            renameDict = {}
            for column in columns:
                columnAlias = self.tableColumnsDict[table][column]
                if columnAlias != "":
                    renameDict[column] = columnAlias
            if len(renameDict.keys()) > 0:
                script = table + " = " + table + ".rename(columns = " + str(renameDict) + ")"
                finalScript.append(script)
            else:
                continue
        return finalScript

    #
    # This methods builds the merge script for pandas to join multiple
    # Dataframes together imported from SQL
    #
    def joinPandasDFs(self):
        finalScript = []
        fromCols = self.queryObject.queryDict['from']
        if type(fromCols) != list:
            fromCols = [fromCols]
        if len(fromCols) > 1:
            baseTable = self.tableNames[0]
            for tableDetails in fromCols[1:]:
                conditionsDict = tableDetails['on']
                leftCols = []
                rightCols = []
                listOfCols = joinStatement(conditionsDict, [])
                for cols in listOfCols:
                    columnNameA, tableNumA = cleanColumnName(cols[0], self.tableAliases, self.tableColumnsDict,
                                                             self.tableNames)
                    columnNameB, tableNumB = cleanColumnName(cols[1], self.tableAliases, self.tableColumnsDict,
                                                             self.tableNames)
                    if tableNumA < tableNumB:
                        leftCols.append(columnNameA)
                        rightCols.append(columnNameB)
                    else:
                        rightCols.append(columnNameA)
                        leftCols.append(columnNameB)
                for joinClause in self.queryObject.joinClauses:
                    if joinClause in tableDetails.keys():
                        tableName = tableDetails[joinClause]['value']
                        script = baseTable + " = pd.merge(" + baseTable + ", " + tableName + ", how = '" + joinClause + \
                                 "', left_on = " + str(leftCols) + ", right_on = " + str(rightCols) + ")"
                        finalScript.append(script)
        return finalScript

    #
    # This methods filters the pandas dataframe created based on the
    # where clause as mentioned in the SQL Query
    #
    def whereClausePandasDF(self):
        whereScript = ""
        baseTable = self.tableNames[0]
        if "where" in self.queryObject.queryDict.keys():
            whereCondition = self.queryObject.queryDict['where']
            script = handleWhereClause(whereCondition, self.queryObject.columnList, self.tableColumnsDict, self.tableNames,
                                       self.tableAliases)
            whereScript = baseTable + " = " + baseTable + ".query('" + script + "')"
        return whereScript

    #
    # This methods groups by the columns of the pandas dataframe
    # as mentioned in the SQL Query
    #
    def groupPandasDFs(self):
        script = ""
        if "grouby" in self.queryObject.queryDict.keys():
            groupCols = self.queryObject.queryDict['groupby']
            columns = []
            baseTable = self.tableNames[0]
            for col in groupCols:
                columnName = col['value']
                updatedColumn, tabelNum = cleanColumnName(columnName, self.tableAliases, self.tableColumnsDict,
                                                          self.tableNames)
                columns.append(updatedColumn)
            script = baseTable + " = " + baseTable + ".groupby(by = " + str(columns) + ")"
        return script

    #
    # This methods orders by the columns of the pandas dataframe
    # as mentioned in the SQL Query
    #
    def orderPandasDFs(self):
        script = ""
        if "orderby" in self.queryObject.queryDict.keys():
            groupCols = self.queryObject.queryDict['orderby']
            columns = []
            baseTable = self.tableNames[0]
            for col in groupCols:
                columnName = col['value']
                updatedColumn, tableNum = cleanColumnName(columnName, self.tableAliases, self.tableColumnsDict,
                                                          self.tableNames)
                columns.append(updatedColumn)
            script = baseTable + ".sort_values(by = " + str(columns) + ", inplace = True)"
        return script

    #
    # This methods identifies nested UDFs if any within the select clause
    # and returns an equivalent pandas script for the same
    #
    def intermediate_select_dict(self):
        column_list = []
        select_list = self.queryObject.queryDict['select']
        for column_dict in select_list:
            if column_dict == "*":
                pass
            elif type(column_dict['value']) == str:
                if '.' in column_dict['value']:
                    column_value = column_dict['value'].split('.')[1]
                    column_table = column_dict['value'].split('.')[0]
                    try:
                        alias = column_dict['name']
                    except:
                        alias = ""
                    collsttmp = {"base_col": column_value, "udf": "", "Alias": alias}
                    column_list.append(collsttmp)
                else:
                    column_value = column_dict['value']
                    column_table = ""
                    try:
                        alias = column_dict['name']
                    except:
                        alias = ""
                    collsttmp = {"base_col": column_value, "udf": "", "Alias": alias}
                    column_list.append(collsttmp)


            elif type(column_dict['value']) == dict:
                if '.' in column_dict['value']:
                    column_value = column_dict['value'].split('.')[1]
                    column_table = column_dict['value'].split('.')[0]
                    try:
                        alias = column_dict['name']
                    except:
                        alias = ""
                    colsttmp = {"base_col": column_value, "Table": column_table, "Alias": alias}
                    column_list.append(colsttmp)
                else:
                    column_value = column_dict['value']
                    final_col = []
                    for k, v in column_dict['value'].items():
                        if k == "case":
                            pass
                        else:
                            udf = k
                            cols = v
                            if type(cols) == str:
                                if '.' in cols:
                                    col_name = cols.split('.')
                                    final_col.append(col_name[1])
                                else:
                                    final_col.append(cols)
                            elif type(cols) == dict:
                                for k, v in cols.items():  ###########needs to be coded
                                    udf = udf + "," + k
                                    cols = v
                                    for i in cols:
                                        final_col.append(i)

                            else:
                                for i in cols:
                                    if type(i) == str:
                                        if '.' in i:
                                            column_name = i.split('.')
                                            col_name = column_name[1]
                                            final_col.append(col_name)
                                        else:
                                            final_col.append(i)

                                    elif type(i) == int:
                                        final_col.append(i)

                                    elif type(i) == dict:  ## here adjustments needs to be done
                                        new_dict = i
                                        for k, v in new_dict.items():
                                            extra_udf = k
                                            udf = udf + "," + extra_udf
                                            cols = v
                                            if type(cols) == list:  ## for list
                                                for i in cols:
                                                    if type(i) == str:
                                                        if '.' in i:
                                                            splitter = i.split('.')
                                                            part1 = splitter[0]
                                                            part2 = splitter[1]
                                                            final_col.append(part2)
                                                        else:
                                                            final_col.append(i)
                                                    else:
                                                        final_col.append(i)
                                            elif type(cols) == str:  ## for str
                                                if '.' in cols:
                                                    splitter = cols.split('.')
                                                    part1 = splitter[0]
                                                    part2 = splitter[1]
                                                    final_col.append(part2)
                                                else:
                                                    final_col.append(cols)
                                            elif type(cols) == dict:  ## for dict
                                                for k, v in cols.items():
                                                    third_udf = k
                                                    udf = udf + "," + third_udf
                                                    cols = v
                                                    for i in cols:
                                                        if '.' in i:
                                                            splitter = i.split('.')
                                                            part1 = splitter[0]
                                                            part2 = splitter[1]
                                                            final_col.append(part2)
                                                        else:
                                                            final_col.append(i)
                                            else:
                                                pass

                                    else:
                                        pass
                        try:
                            alias = column_dict['name']
                        except:
                            alias = ""
                        colltmp = {"base_col": final_col, "udf": udf, "Alias": alias}
                        column_list.append(colltmp)
        return column_list


    def handleUDFs(self, sql_dict):
        query_list = []
        grp_cols = []
        query_dict = self.queryObject.queryDict
        if 'groupby' in query_dict.keys():
            group_section = query_dict['groupby']
            if type(group_section) == list:
                for i in group_section:
                    values = i['value']
                    grp_cols.append(values)
            elif type(group_section) == dict:
                for k, v in group_section.items():
                    grp_cols.append(v)
        else:
            pass
        for list_elements in sql_dict:
            columns = list_elements['base_col']
            print("cols are ", columns)
            if columns != '*':
                if columns != []:
                    final_columns = [s for s in columns]
                    alias = list_elements['Alias']
                    udf = list_elements['udf']
                    tableName = self.tableNames[0]
                    if list_elements['udf'] != '':
                        udf_splitter = list_elements['udf'].split(',')
                        columnNames = list_elements['base_col']
                        len_udf = len(udf_splitter)
                        if len_udf == 1 and udf == "coalesce":
                            query =coalesce_udf(columns, tableName, alias, final_columns, len_udf)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "mul":
                            query = multiplication_udf(final_columns, alias, tableName)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "sum":
                            # query= sum_initial_udf(columns,final_df,alias)
                            # query_list.append(query)
                            query = group_by_func(tableName, query_dict, grp_cols, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "year":
                            query = year_month_udf(tableName, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "month":
                            query = year_month_udf(tableName, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "literal":
                            query = literals_adjust(tableName, columns, alias)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "distinct":
                            query = distinct_unique(tableName, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "count":
                            # query = self.sum_initial_udf(columns, tableName, alias)
                            # query_list.append(query)
                            query = group_by_func(tableName, query_dict, grp_cols, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf > 1:
                            for udf in reversed(udf_splitter):
                                if udf == 'mul':
                                    query = multiplication_udf(final_columns, alias, tableName)
                                    query_list.append(query)
                                elif udf == 'sum':
                                    # query = self.sum_initial_udf(columns, tableName, alias)
                                    # query_list.append(query)
                                    query = group_by_func(tableName, query_dict, grp_cols, alias, udf, final_columns)
                                    # final_df, query_dict, grp_cols, alias, columns, udf, final_columns
                                    query_list.append(query)
                                elif udf == "coalesce":
                                    query = coalesce_udf(columns, tableName, alias, final_columns, len_udf)
                                    query_list.append(query)
                        # else:
                        #     columns =list_elements['base_col']
                        #     alias=list_elements['Alias']
                        #     if alias!='':
                        #         query=final_df+"['"+alias+"']"+"="+final_df+"['"+columns+"']"
                        #         query_list.append(query)

                        else:
                            pass
            else:
                pass
        return query_list
    def handleUDFStatements(self):
        finalScript = []
        selectCols = self.queryObject.queryDict['select']
        for col in selectCols:
            if type(col) == str:
                col = {'value' : col}
            if type(col['value']) == dict and "case" not in col['value'].keys():
                dataStructure = col
                details = dataStructure['value']
                columnAlias = dataStructure['name']
                udfList = handleUDFs(details, self.queryObject.columnList, columnAlias, self.tableNames, self.tableAliases,
                                     self.tableColumnsDict, [])
                for udf in udfList:
                    key = list(udf.keys())[0]
                    colList = udf[key]
                    script = udfScript(key, colList, self.queryObject.columnList, columnAlias, self.tableNames,
                                       self.tableAliases, self.tableColumnsDict)
                    finalScript.append(script)
        return finalScript

    #
    # This methods identifies case statements and tranverses through the query dict
    # to handle simple and nested case statements
    #
    def handleCaseStatements(self):
        finalScript = []
        baseTable = self.tableNames[0]
        selectCols = self.queryObject.queryDict['select']
        for col in selectCols:
            if type(col) == str:
                col = {'value' : col}
            if type(col['value']) == dict and "case" in col['value'].keys():
                dataStructure = col
                columnAlias = col['name']
                case = dataStructure['value']
                condList, resList = handleCases(case, self.queryObject.columnList, columnAlias, self.tableNames,
                                                self.tableAliases, self.tableColumnsDict, [], [])
                for i in range(len(resList) - 1, -1, -1):
                    if i == len(resList) - 1:
                        script = baseTable + "['" + columnAlias + "'] = " + resList[-1]
                    else:
                        script = baseTable + "['" + columnAlias + "'].iloc[" + condList[i] + "] = " + resList[i]
                    finalScript.append(script)
        return finalScript

    def buildPandasScript(self):
        finalScript = []
        extraLine = ""

        pandasImport = self.importPackage("pandas", 'pd')
        finalScript.append(pandasImport)
        finalScript.append(extraLine)

        readScripts = self.readPandasDFs()
        for readScript in readScripts:
            finalScript.append(readScript)
        finalScript.append(extraLine)

        renameScripts = self.renameColumns()
        if renameScripts != []:
            for renameScript in renameScripts:
                finalScript.append(renameScript)
            finalScript.append(extraLine)

        mergeScripts = self.joinPandasDFs()
        if mergeScripts != []:
            for mergeScript in mergeScripts:
                finalScript.append(mergeScript)
            finalScript.append(extraLine)

        groupScripts = self.groupPandasDFs()
        if groupScripts != "":
            finalScript.append(groupScripts)
            finalScript.append(extraLine)

        self.intermediate_select_dict()
        column_list = self.intermediate_select_dict()
        for script in self.handleUDFs(column_list):
            finalScript.append(script)




        # udfs = self.handleUDFStatements()
        # if udfs != []:
        #     for udf in udfs:
        #         finalScript.append(udf)
        #     finalScript.append(extraLine)

        cases = self.handleCaseStatements()
        if cases != []:
            for case in cases:
                finalScript.append(case)
            finalScript.append(extraLine)

        whereClauses = self.whereClausePandasDF()
        if whereClauses != "":
            finalScript.append(whereClauses)
            finalScript.append(extraLine)

        orderScripts = self.orderPandasDFs()
        if orderScripts != "":
            finalScript.append(orderScripts)
            finalScript.append(extraLine)

        createScripts = self.createTableScript()
        if createScripts != []:
            for createScript in createScripts:
                finalScript.append(createScript)
            finalScript.append(extraLine)

        insertScripts = self.insertTableScript()
        if insertScripts != []:
            for insertScript in insertScripts:
                finalScript.append(insertScript)
            finalScript.append(extraLine)

        return finalScript

    def createTableScript(self):
        finalScript = []
        if self.queryObject.createTable:
            script = self.queryObject.createTableAlias + " = " + self.tableNames[0]
            finalScript.append(script)
            script = "pd.to_sql(" + self.queryObject.createTableAlias + ", con = " + "SQL_ENGINE" + ", if_exists = " + "'replace', index = False)"
            finalScript.append(script)
        return finalScript

    def insertTableScript(self):
        finalScript = []
        if self.queryObject.insertTable:
            script = self.queryObject.insertTableAlias + " = " + self.tableNames[0]
            finalScript.append(script)
            script = "pd.to_sql(" + self.queryObject.insertTableAlias + ", con = " + "SQL_ENGINE" + ", if_exists = " + "'append', index = False)"
            finalScript.append(script)
        return finalScript


if __name__ == '__main__':

    """
    when A.Avg_Bkd=e then case when A.Avg_Bkd=g or A.Avg_Bkd=n then h else i end """

    query = """create table newTable SELECT count(B.alpha) as alpha1,
                coalesce( A.crossover_rms,0) as CO_RN_Goal,
                coalesce( ( sum(A.crossover_rms)*B.crossover_gadr*A.crossover_rms),0) as CO_Rev_Goal,
                case when A.crossover_rms>1 then True when c=d then True else false end as newCol,
                A.marsha as MARS,
                A.crossover_rms as newcolumn,
                c.avg_bkd as renamedColumn,
                A.stay_year as stay_year_REN,
                A.CO_RN_Goal,
                A.CO_Rev_Goal,
                A.CO_RN_Goal_ADR,
                A.Def_OTB,
                A.Def_REV,
                A.Def_ADR,
                A.Target,
                A.Avg_Bkd
                FROM tableA A
                left join tableB B
                on B.marsha = a.marsha and A.marsha1 = B.marsha1 or a.marsha2<=b.marsha2
                outer join tableC C
                on A.marsha = C.marsha and A.marsha1 = C.marsha1
                Where A.Target=1
                and A.Target in (1,2,3,4)
                or c.Avg_Bkd="ABCD" and b.marsha = ""
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


    # query = """CREATE TABLE ABC1 AS (SELECT * , SUM(ABC) as new_col FROM A GROUP BY ALPHA)"""

    # query = """SELECT * , SUM(ABC) as new_col FROM table GROUP BY ALPHA"""

    query = """select col1 as c, col2 d, sum(col) as a from table"""

    p = PythonScript(query)
    # print(p.sqlQuery)
    # print(p.tableNames)
    # print(p.tableAliases)
    # print(p.tableColumnsDict)

    for a in p.buildPandasScript():
        print(a)

    # print(p.readPandasDFs())
    # for a in p.handleCaseStatements():
    #     print(a)
