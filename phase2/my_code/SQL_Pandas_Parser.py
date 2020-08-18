#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 21 10:10:45 2020

@author: skoranne
"""

class SQL_Pandas_Parser():
    tableNames = []
    tableAlias = {}
    caseAlias = {}
    tableColumns = {}
    selectStatements = []
    allCaseStatements = []
    statementsWithoutCases = []
    operators = {" not ":" != "," equals ":" == ","eq" : " = ","neq":" != ","lte":" <= ",
                 "gte":" >= ","=": " == ","<=":" <= ",">=": " >= ","<":" < ",">":" > ",
                 " or ":" | ", " and ":" & ","/":" / ","+":" + ","-":" - ","*":" * ",
                 "true":"True", "false":"False"}
    script = []
    tableColumnsDict = {}
    createTableAlias = ""
    insertTableAlias = ""
    createTable = False
    insertTable = False
    keyWords = ['select', 'from', 'join', 'left', 'right',' inner', 'on', 'where', 'order', 'group']

    def __init__(self, sqlQuery):
        self.sqlQuery = sqlQuery.lower()
        self.cleanQuery()
        # print("updated query: ", self.sqlQuery)

    def cleanQuery(self):
        # self.sqlQuery = self.sqlQuery.replace("\n", "").strip()
        self.sqlQuery = re.sub("\s+", " ", self.sqlQuery)
        if "outer join" in self.sqlQuery:
            self.sqlQuery = self.sqlQuery.replace(" outer join ", " full outer join ")

        if "create table" in self.sqlQuery:
            regex = "CREATE TABLE (.*?)SELECT"
            matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
            for matchNum, match in enumerate(matches, start=1):
                createClause = match.group()
                createClauseWOSelect = createClause.replace(" as ", " ").replace("(", "").split("select")[0]
                splits = createClauseWOSelect.split()
                self.createTableAlias = splits[-1]
                self.createTable = True
                self.sqlQuery = self.sqlQuery.replace(createClause, "")
                if "(" == createClause.split()[-2]:
                    self.sqlQuery = "(select " + self.sqlQuery
                    closingBracketIndex = self.bracketStringIndex(self.sqlQuery, 0)
                    self.sqlQuery = self.sqlQuery[1:closingBracketIndex].strip()
                else:
                    self.sqlQuery = "select " + self.sqlQuery.strip()

        if "insert into" in self.sqlQuery:
            regex = "insert into(.*?)select"
            matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
            for matchNum, match in enumerate(matches, start=1):
                insertClause = match.group()
                tempInsertClause = insertClause.replace("(", "").strip().split("select")[0]
                splits = tempInsertClause.split()
                self.insertTableAlias = self.cleanTableName(splits[-1])
                self.insertTable = True
                self.sqlQuery = self.sqlQuery.replace(insertClause, "")
                if "(" == insertClause.split()[-2]:
                    self.sqlQuery = "(select " + self.sqlQuery
                    closingBracketIndex = self.bracketStringIndex(self.sqlQuery, 0)
                    self.sqlQuery = self.sqlQuery[1:closingBracketIndex].strip()
                else:
                    self.sqlQuery = "select " + self.sqlQuery.strip()

        regex = "\&(.*?)[\s;]|[)]"
        matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
        if matches is not None:
            for matchNum, match in enumerate(matches, start=1):
                word = match.group()
                newWord = word.replace("&", "")
                self.sqlQuery = self.sqlQuery.replace(word,newWord)
        return

    def getQueryDict(self):
        return parse(self.sqlQuery.lower())

    def identifyTables(self):
        queryWithTables = self.sqlQuery.split(" from ")[1]
        listOfWords = queryWithTables.split()
        tableName = self.cleanTableName(listOfWords[0])
        self.tableNames.append(tableName)
        self.sqlQuery = self.sqlQuery.replace(listOfWords[0], tableName)
        if len(listOfWords) > 1:
            if listOfWords[1] == "as":
                self.tableAlias[listOfWords[2]] = tableName
            elif listOfWords[1] in self.keyWords:
                pass
            else:
                self.tableAlias[listOfWords[1]] = tableName

        regex = "JOIN (.*?) ON"
        matches = re.finditer(regex, queryWithTables, re.IGNORECASE)
        for matchNum, match in enumerate(matches, start=1):
            match = match.group()
            # print("one table:", match)
            alias = ""
            tableName = ""
            if "from" in match:
                tableName = self.cleanTableName(match.split()[1])
                self.tableNames.append(tableName)
                self.sqlQuery = self.sqlQuery.replace(match.split()[1], tableName)
                if "join" in self.sqlQuery:
                    tablePhrase = self.sqlQuery.split("from")[1].split("join")[0]
                    alias = tablePhrase.split()[-1]
                else:
                    tablePhrase = self.sqlQuery.split(" from ")
                    splits = tablePhrase.split()
                    if splits[1] == "as":
                        alias = splits[2]
                    else:
                        alias = splits[1]
            elif "join" in match:
                splits = match.split()
                tableName = self.cleanTableName(splits[1])
                self.sqlQuery = self.sqlQuery.replace(splits[1], tableName)
                self.tableNames.append(tableName)
                if splits[2] == "as":
                    alias = splits[3]
                else:
                    alias = splits[2]
            else:
                pass
            self.tableAlias[alias] = tableName
        return

    def cleanTableName(self, tableName):
        print("table before clean:", tableName)
        if '.' in tableName:
            if '[' in tableName:
                return tableName.split("[")[-1].replace("]", "").replace(";", "")
            else:
                return tableName.split(".")[-1].replace(";", "")
        return tableName.replace(";", "")

    # =============================================================================
    #     Flatten a list of nested dicts
    # =============================================================================

    def flatten_json(self, nested_json: dict, exclude: list = [''], sep: str = '_') -> dict:
        out = dict()
        def flatten(x: (list, dict, str), name: str = '', exclude=exclude):
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

    # =============================================================================
    #     Select statement processing
    # =============================================================================

    def selectStatementScript(self):
        self.identifyTables()
        self.identifyCaseStatements()
        self.generateCaseAlias()
        sqlWithoutCase = self.getSQLWithoutCase()
        select_dict = parse(sqlWithoutCase)['select']
        sql_dict = a.select_complex(select_dict)
        selectQueries = self.panda_builder("data", sql_dict)
        for caseStatement in self.allCaseStatements:
            caseScript = self.buildCaseQuery(caseStatement)
            for case in caseScript:
                selectQueries.append(case)
        return selectQueries

    def generateCaseAlias(self):
        for caseStatement in self.allCaseStatements:
            splits = self.sqlQuery.split(caseStatement)
            if "from" not in splits[0].lower():
                listOfWords = splits[1].split()
                if listOfWords[0].lower() == "as":
                    self.caseAlias[caseStatement] = listOfWords[1].replace(",", "")
                elif listOfWords[0] == ")":
                    if listOfWords[1].lower() == "as":
                        self.caseAlias[caseStatement] = listOfWords[2].replace(",", "")
                    else:
                        self.caseAlias[caseStatement] = listOfWords[1].replace(",", "")
                else:
                    self.caseAlias[caseStatement] = listOfWords[0].replace(",", "")
            else:
                self.caseAlias[caseStatement] = ""
        return

    def getSQLWithoutCase(self):
        sqlWithoutCase = self.sqlQuery
        for caseStatement in self.allCaseStatements:
            if len(self.caseAlias[caseStatement]) > 0:
                sqlWithoutCase = sqlWithoutCase.replace(caseStatement, "")
                sqlWithoutCase = sqlWithoutCase.replace("()", "")
                alias = self.caseAlias[caseStatement]
                listOfWords = sqlWithoutCase.split()
                index = -1
                for word in listOfWords:
                    if alias == word.replace(",", ""):
                        index = listOfWords.index(word)
                        if listOfWords[index - 1] == "as" or listOfWords[index - 1] == "AS" or listOfWords[
                            index - 1] == "As":
                            listOfWords.pop(index - 1)
                            index = index - 1
                        if listOfWords[index + 1] == ",":
                            listOfWords.pop(index + 1)
                        listOfWords.pop(index)
                    if caseStatement == self.allCaseStatements[-1]:
                        listOfWords[index - 1] = listOfWords[index - 1].replace(",", "")
                sqlWithoutCase = " ".join(listOfWords)
        return sqlWithoutCase

    def buildCaseQuery(self, caseStatement):
        self.identifyTables()
        updatedCaseStatement = self.cleanCaseStatement(caseStatement)
        conditions, results = self.caseStatementDetails(updatedCaseStatement)
        alias = ""
        caseScript = []
        aliasPhrase = self.sqlQuery.split(caseStatement)[1].strip()
        if aliasPhrase.split(" ")[0] == "as":
            alias = aliasPhrase.split(" ")[1].strip()
            self.caseAlias[caseStatement] = alias
        table = self.getTableNames()[0]
        for i in range(len(conditions)):
            condition = " ".join(conditions[i].strip().split())
            condition = "(" + condition + ")"
            # if i == 0:
            #     condition = "(" + condition
            # if i == len(conditions) - 1:
            #     condition = condition + ")"
            result = " ".join(results[i].strip().split())
            if i == 0:
                caseScript.append(table + "['" + alias.replace(",", "") + "'] = " + results[-1])
            caseScript.append(table + ".loc[" + condition + ", '" + alias.replace(",", "") + "'] = " + result)
        return caseScript

    def getTableNames(self):
        self.identifyTables()
        return self.tableNames


    def case_or_clause(self, final_df, data):
        signs = ["="]
        for sign in signs:
            if sign in data:
                if sign == "=":
                    data_val = data.split("=")[1]
                    data_col = data.split("=")[0]
                    azeta = "(" + final_df + "['" + data_col + "']==" + data_val + ")"
                return azeta
            else:
                pass

    def case_alias_getter(self, query1):
        for column_dict in query1:
            if type(column_dict) == dict:
                if type(column_dict['value']) == dict:
                    for k, v in column_dict['value'].items():
                        if k == "case":
                            alias = column_dict['name']
                            return alias
                        else:
                            pass
                else:
                    pass
            else:
                pass

    def case_panda_builder(self):
        panda_case_syntax = []
        list_of_cols = self.identifyCaseStatements()
        final_df = self.tableNames[0]
        query_selection = parse(self.sqlQuery)['select']
        for i in list_of_cols:
            start = 'else'
            end = 'end'
            # final_df="df"
            else_cond = i[i.find(start) + len(start):i.rfind(end)]
            final_else_part = ''
            if "/" in else_cond:
                else_splitter = else_cond.split("/")
                div_part = final_df + "['" + else_splitter[0].strip() + "']" + "/" + final_df + "['" + else_splitter[
                    -1].strip() + "']"
                alias_assignment = self.case_alias_getter(query_selection)
                # print(alias_assignment)
                alias_assignment1 = final_df + "['" + alias_assignment + "']"
                final_else_part = alias_assignment1 + "=" + div_part
            else:
                pass
            panda_case_syntax.append(final_else_part)
            # print(final_else_part)

            conditions, results = self.caseStatementDetails(i)
            then_clause = results[0]
            # print(then_clause)
            conditions_main = conditions[-1]
            case1 = ""
            if "or" in conditions_main:
                orSplitter = conditions_main.split(" or ")
                when_part = ''
                for cond in orSplitter:
                    abc = self.case_or_clause(final_df, cond)
                    when_part += abc + "|"
                # print("-----this is when part------")
                # print(when_part[:-1])
                alias_assignment = self.case_alias_getter(query_selection)
                case1 = final_df + ".loc[" + when_part[:-1] + "," + "'" + alias_assignment + "']=" + then_clause
                # print((case1))
                panda_case_syntax.append(case1)
                # cases=cases.append(case1)


            elif "and" in conditions_main:
                andSplitter = conditions_main.split("and")
            else:
                pass
            # cases=cases.append(case1)
        return panda_case_syntax




    def identifyColumns(self):
        column_list = []
        query_dict = parse(self.sqlQuery)['select']
        if type(query_dict) != list:
            query_dict = [query_dict]

        for columnDetail in query_dict:
            if columnDetail == "*":
                if self.tableNames[0] in self.tableColumnsDict.keys():
                    self.tableColumnsDict[self.tableNames[0]]['*'] = ""
                else:
                    self.tableColumnsDict[self.tableNames[0]] = {"*": ""}
                continue
            value = columnDetail['value']
            try:
                alias = columnDetail['name']
            except:
                alias = ""
            if type(value) == str:
                if "." in value:
                    tableAlias = value.split(".")[0]
                    tableName = self.tableAlias[tableAlias]
                    columnName = value.split(".")[1]
                    columnDict = {columnName: alias}
                    if tableName in self.tableColumnsDict.keys():
                        self.tableColumnsDict[tableName][columnName] = alias
                    else:
                        self.tableColumnsDict[tableName] = columnDict
                else:
                    columnName = value
                    tableName = self.tableNames[0]
                    columnDict = {columnName: alias}
                    print("column dict:", columnDict)
                    if tableName in self.tableColumnsDict.keys():
                        self.tableColumnsDict[tableName][columnName] = alias
                    else:
                        self.tableColumnsDict[tableName] = columnDict

            elif type(value) == dict:
                if '.' in value:
                    column_value = value.split('.')[1]
                    column_table = value.split('.')[0]
                    # try:
                    #     alias = column_dict['name']
                    # except:
                    #     alias = ""
                    tableName = self.tableAlias[column_table]
                    colsttmp = {"base_col": column_value, "Table": column_table, "Alias": alias, "table": tableName}
                    column_list.append(colsttmp)
                else:
                    column_value = value
                    final_col = []
                    for k, v in value.items():
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
                                                    if '.' in i:
                                                        splitter = i.split('.')
                                                        part1 = splitter[0]
                                                        part2 = splitter[1]
                                                        final_col.append(part2)
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
                        # try:
                        #     alias = column_dict['name']
                        # except:
                        #     alias = ""
                        colltmp = {"base_col": final_col, "udf": udf, "Alias": alias, "table": self.tableNames[0]}
                        column_list.append(colltmp)

        regex = "\w+(?:\.\w+)"
        matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
        for matchNum, match in enumerate(matches, start=1):
            column = match.group()
            splits = column.split(".")
            tableName = self.tableAlias[splits[0]]
            columnName = splits[1]
            columnDict = {columnName: ""}
            if tableName in self.tableColumnsDict.keys():
                if columnName not in self.tableColumnsDict[tableName].keys():
                    self.tableColumnsDict[tableName][columnName] = ""
            else:
                self.tableColumnsDict[tableName] = columnDict
        self.intermediate_select_dict()
        return column_list

    def selectQuery(self):
        queryScript = []
        # self.getColumns()
        for table in self.tableColumnsDict.keys():
            columnDetails = self.tableColumnsDict[table]
            columnNames = list(columnDetails.keys())
            # columnNames = [column if not column.isnumeric() for column in columnNames]
            updatedColumns = []
            for column in columnNames:
                if not str(column).isnumeric():
                    updatedColumns.append(column)
            columnNames = updatedColumns
            script = table + " = pd.read_sql('select "
            if "*" in columnNames:
                script += "*"
            else:
                for column in columnNames:
                    script += str(column)
                    if column != columnNames[-1]:
                        script += ", "
            script += " from " + str(table) + "')"
            queryScript.append(script)
        return queryScript

    def renameColumns(self):
        queryScript = []
        for table in self.tableColumnsDict:
            columnDetails = self.tableColumnsDict[table]
            columns = list(columnDetails.keys())
            renameDict = {}
            for column in columns:
                newName = columnDetails[column]
                if newName != "":
                    renameDict[column] = newName
            if len(renameDict.keys()) > 0:
                script = table + " = " + table + ".rename(columns = " + str(renameDict) + ")"
                queryScript.append(script)
        return queryScript

    def joinQuery(self):
        queryScript = []
        baseTable = ""
        from_dict = parse(self.sqlQuery)['from']
        if type(from_dict) != list:
            from_dict = [from_dict]
        if len(from_dict) > 1:
            for tableDetails in from_dict:
                if "value" in tableDetails.keys():
                    baseTable = tableDetails['value']
                if "join" in tableDetails.keys():
                    joinDetails = tableDetails['join']
                    joinType = "'inner'"
                    tableToMerge = joinDetails['value']
                    joinConditions = tableDetails['on']
                    script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
                    queryScript.append(script)
                elif "left join" in tableDetails.keys():
                    joinDetails = tableDetails['left join']
                    joinType = "'left'"
                    tableToMerge = joinDetails['value']
                    joinConditions = tableDetails['on']
                    script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
                    queryScript.append(script)
                elif "right join" in tableDetails.keys():
                    joinDetails = tableDetails['right join']
                    joinType = "'right'"
                    tableToMerge = joinDetails['value']
                    joinConditions = tableDetails['on']
                    script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
                    queryScript.append(script)
                elif "full outer join" in tableDetails.keys():
                    joinDetails = tableDetails['full outer join']
                    joinType = "'outer'"
                    tableToMerge = joinDetails['value']
                    joinConditions = tableDetails['on']
                    script = self.handleJoinConditions(baseTable, tableToMerge, joinConditions, joinType)
                    queryScript.append(script)
            removeDuplicates = baseTable + " = " + baseTable + ".loc[:, ~" + baseTable + ".columns.duplicated()]"
            queryScript.append(removeDuplicates)
        return queryScript

    def handleJoinConditions(self, baseTable, tableToMerge, joinConditions, joinType):
        if "and" not in joinConditions.keys():
            script = self.handleSingleCondition(baseTable, tableToMerge, joinConditions, joinType)
        else:
            script = self.handleMultipleConditins(baseTable, tableToMerge, joinConditions, joinType)
        return script

    def handleSingleCondition(self, baseTable, tableToMerge, joinConditions, joinType):
        script = baseTable + " = pd.merge(" + baseTable + ", " + tableToMerge + ", how = " + joinType + ", "
        key = list(joinConditions.keys())[0]
        columns = joinConditions[key]
        leftColumn = []
        rightColumn = []
        if len(columns) == 2:
            for i in range(2):
                splits = columns[i].split(".")
                table = self.tableAlias[splits[0]]
                column = splits[1]
                if self.tableColumnsDict[table][column] != "":
                    column = self.tableColumnsDict[table][column]
                if baseTable == table:
                    leftColumn.append(column)
                else:
                    rightColumn.append(column)
            tabScript = "left_on = " + str(leftColumn) + ", right_on = " + str(rightColumn) + ")"
            script += tabScript
        return script

    def handleMultipleConditins(self, baseTable, tableToMerge, joinConditions, joinType):
        script = baseTable + " = pd.merge(" + baseTable + ", " + tableToMerge + ", how = " + joinType + ", "
        conditions = joinConditions['and']
        leftColumns = []
        rightColumns = []
        columnNames = []
        tables = []
        for condition in conditions:
            key = list(condition.keys())[0]
            columns = condition[key]
            for column in columns:
                splits = column.split(".")
                table = self.tableAlias[splits[0]]
                columnName = splits[1]
                if self.tableColumnsDict[table][columnName] != "":
                    columnName = self.tableColumnsDict[table][columnName]
                tables.append(table)
                columnNames.append(columnName)
        i = 0
        while i < len(columnNames):
            if self.tableNames.index(tables[i]) < self.tableNames.index(tables[i+1]):
                leftColumns.append(columnNames[i])
                rightColumns.append(columnNames[i+1])
            else:
                rightColumns.append(columnNames[i])
                leftColumns.append(columnNames[i + 1])
            i = i+2
        script += "left_on = " + str(leftColumns) + ", right_on = " + str(rightColumns) + ")"
        return script

    # def caseQuery(self, caseDict):
    #     script = ""
    #     query_dict = parse(self.sqlQuery)['select']
    #     conditions = []
    #     results = []
    #     if "case" in caseDict['value'].keys():
    #         elseResult = caseDict['value']['case'][-1]
    #         if "name" in caseDict['value'].keys():
    #             columnName = caseDict['value']['name']
    #             for caseCondition in caseDict['value']['case'][:-1]:
    #                 whenCon = caseCondition['when']
    #                 if "and" in whenCon.keys() and len(whenCon.keys()) == 1:
    #                     pass
    #                 elif len(whenCon.keys()) == 1 and list(whenCon.keys())[0] in self.operators:
    #                     key = list(whenCon.keys())[0]
    #                     columns = whenCon[key]
    #                     conditionCols = []
    #                     for column in columns:
    #                         colName = self.getColumnName(column)
    #                         conditionCols.append(colName)
    #                     condition = conditionCols[0] + self.operators[key] + conditionCols[1]
    #                     conditions.append(condition)
    #                 thenCon = caseCondition['then']
    #                 if "case" in thenCon.keys() and len(thenCon.keys()) == 1:
    #
    #     return script
    #
    # def singleCaseQuery(self, caseDict):
    #     caseBody = caseDict['case']
    #     for conditions in caseBody:


    def getColumnName(self, column):
        column = str(column)
        if "." in column:
            splits = column.split(".")
            tableName = self.tableAlias[splits[0]]
            tempColumnName = splits[1]
        else:
            tableName = self.tableNames[0]
            tempColumnName = column
        if tempColumnName in self.tableColumnsDict[tableName].keys():
            tempName = self.tableColumnsDict[tableName][tempColumnName]
            if tempName == "":
                columnName = tempColumnName
            else:
                columnName = tempName
        else:
            columnName = tempColumnName
        return columnName


    def orderByQuery(self, baseTable):
        script = ""
        query_dict = parse(self.sqlQuery)
        if 'orderby' in query_dict.keys():
            columns = query_dict['orderby']
            if type(columns) != list:
                columns = [columns]
            columnsToSortOn = []
            for columnDetails in columns:
                columnName = columnDetails['value']
                if "." in columnName:
                    splits = columnName.split(".")
                    table = self.tableAlias[splits[0]]
                    columnName = splits[1]
                    if self.tableColumnsDict[table][columnName] != "":
                        columnName = self.tableColumnsDict[table][columnName]
                else:
                    table = self.tableNames[0]
                    if self.tableColumnsDict[table][columnName] != "":
                        columnName = self.tableColumnsDict[table][columnName]
                columnsToSortOn.append(columnName)
            script = baseTable + ".sort_values(by = " + str(columnsToSortOn) + ", inplace = True)"
        return script

    def groupByQuery(self, baseTable):
        script = ""
        query_dict = parse(self.sqlQuery)
        if 'groupby' in query_dict.keys():
            columns = query_dict['groupby']
            if type(columns) != list:
                columns = [columns]
            columnsToSortOn = []
            print(self.tableColumnsDict)
            for columnDetails in columns:
                columnName = columnDetails['value']
                if "." in columnName:
                    splits = columnName.split(".")
                    table = self.tableAlias[splits[0]]
                    columnName = splits[1]
                    if self.tableColumnsDict[table][columnName] != "":
                        columnName = self.tableColumnsDict[table][columnName]
                else:
                    table = self.tableNames[0]
                    # print("table to look into:", table)
                    if self.tableColumnsDict[table][columnName] != "":
                        columnName = self.tableColumnsDict[table][columnName]
                columnsToSortOn.append(columnName)
            script = baseTable + " = " + baseTable + ".groupby(by = " + str(columnsToSortOn) + ")"
        return script

    def handleWhereClauses(self):
        baseTable = self.tableNames[0]
        script = ""
        if " where " in self.sqlQuery:
            whereClause = self.sqlQuery.split(' where ')[1]
            keyWords = [' order ', ' group ', ' left ', ' right ', ' inner ', ' full outer ']
            tempWhereClause = ""
            matchCount = 0
            for word in keyWords:
                if word in whereClause:
                    newTemp = whereClause.split(word)[0]
                    c = tempWhereClause  == "" and len(newTemp) > len(tempWhereClause)
                    matchCount += 1
                    if tempWhereClause != "" and len(newTemp) < len(tempWhereClause):
                        tempWhereClause = newTemp
                    elif tempWhereClause  == "" and len(newTemp) > len(tempWhereClause):
                        tempWhereClause = newTemp
            tempWhereClause = tempWhereClause.replace(" and", " &").replace(" or", " | ").replace(" not", " ~")
            listOfWords = tempWhereClause.split()
            for word in listOfWords:
                if "." in word:
                    columnName = self.getColumnName(word)
                    index = listOfWords.index(word)
                    listOfWords[index] = columnName
            finalWhereClause = " ".join(listOfWords)
            script = baseTable + " = " + baseTable + ".query('" + finalWhereClause + "')"
        return script

    def handleSingleWhereClause(self, clause):
        script = ""
        key = list(clause.keys())[0]
        lhsRhs = clause[key]
        for sides in lhsRhs:
            side = self.getColumnName(sides)
            script += side
            if sides != lhsRhs[-1]:
                script += self.operators[key]
        return script

    def cleanCaseStatement(self, caseStatement):
        regex = "\w+(?:\.\w+)"
        matches = re.finditer(regex, caseStatement, re.IGNORECASE)
        self.getTableNames()
        tableName = ""
        for operator in self.operators.keys():
            if operator in caseStatement:
                caseStatement = caseStatement.replace(operator, self.operators[operator])
        print("cleaned case:", caseStatement)
        listOfWords = caseStatement.split()
        # print("List of words:", listOfWords)
        colsDict = self.allCols()
        for word in listOfWords:
            # print("word:", word)
            column = ""
            if "." in word:
                # print("word:", word)
                column = word.split(".")[1]
                column = self.tableNames[0] + "['" + column + "']"
                index = listOfWords.index(word)
                listOfWords[index] = column
                # caseStatement = caseStatement.replace(word, column)
            else:
                for key in self.colsDict.keys():
                    # print("Keys:", self.tableColumnsDict[table])
                    if word in self.colsDict[key]:
                        # print("word:", word)
                        column = self.tableNames[0] + "['" + word + "']"
                        index = listOfWords.index(word)
                        listOfWords[index] = column
                        # caseStatement = caseStatement.replace(word, column)
        caseStatement = " ".join(listOfWords)
        caseStatement = "(" + caseStatement.replace(" & ", ") & (").replace(" | ", ") | (") + ")"
        # print("case statement:", caseStatement)
        # print("case statement:", caseStatement)
        # for matchNum, match in enumerate(matches, start=1):
        #     column = match.group()
        #     if type(column) == str:
        #         if "." in column:
        #             column = " " + column
        #             table_alias = column.split(".")[0]
        #             if table_alias.strip() in self.tableAlias.keys():
        #                 tableName = self.tableAlias[table_alias.strip()]
        #         caseStatement = caseStatement.replace(table_alias, " " + tableName)
        return caseStatement

    def getColumnTableNames(self, condition):
        # print("Condition:", condition)
        column = condition.split(" ")[0].strip().replace("[", "").replace("]", "")
        tableName = ""
        if "." in column:
            splits = column.split(".")
            tableName = splits[-2]
            columnName = splits[-1]
        else:
            columnName = column
        if tableName in self.tableAlias.keys():
            tableName = self.tableAlias[tableName]
        return columnName, tableName

    def caseStatementDetails(self, caseStatement):
        whenSplits = caseStatement.split("when")[1:]
        conditions = [whenSplit.split("then")[0].strip() for whenSplit in whenSplits]
        results = [whenSplit.split("then")[1].split("else")[0].strip() for whenSplit in whenSplits]
        # print("result:", results)
        if "else" in caseStatement:
            lastResult = caseStatement.split("else")[1].split("end")[0].strip()
            results.append(lastResult)
        else:
            results[-1] = results[-1].split("end")[0].strip()
        # print("conditions:", conditions)
        # print("results:", results)
        return conditions, results

    def getCaseStatements(self, sqlQuery):
        caseStatements = []
        regex = r"\bcase\b"
        matches = re.finditer(regex, sqlQuery, re.IGNORECASE)
        for matchNum, match in enumerate(matches, start=1):
            caseWord = match.group()
            cases = sqlQuery.split(caseWord)[1:]
            for case in cases:
                case = "(" + caseWord + case
                endIndex = self.bracketStringIndex(case, 0)
                statement = case[:endIndex + 1]
                caseStatements.append(statement)
        return caseStatements

    def identifyCaseStatements(self):
        regex = "CASE(.*?)END"
        matches = re.finditer(regex, self.sqlQuery, re.IGNORECASE)
        for matchNum, match in enumerate(matches, start=1):
            self.allCaseStatements.append(match.group())
        return self.allCaseStatements

    def bracketStringIndex(self, sql, start):
        dict = {'(': 1, ')': -1}
        indexCount = 0
        sum = 0
        for character in sql.lower():
            if character == '(':
                sum += dict['(']
            elif character == ')':
                sum += dict[')']
                if sum == 0:
                    return indexCount
            indexCount += 1
        return indexCount

    def createTableQuery(self):
        queryScript = []
        if self.createTable == True:
            baseTable = self.tableNames[0]
            script = self.createTableAlias + " = " + baseTable
            toSQLScript = "pd.to_sql(" + self.createTableAlias + ", con = " + "SQL_ENGINE" + ", if_exists = 'replace', index = False)"
            queryScript.append(script)
            queryScript.append(toSQLScript)
        return queryScript

    def insertTableQuery(self):
        queryScript = []
        if self.insertTable == True:
            baseTable = self.tableNames[0]
            script = self.insertTableAlias + " = " + baseTable
            toSQLScript = "pd.to_sql(" + self.insertTableAlias + ", con = " + "SQL_ENGINE" + ", if_exists = 'append', index = False)"
            queryScript.append(script)
            queryScript.append(toSQLScript)
        return queryScript

    def handleUDFs(self, sql_dict):
        query_list = []
        grp_cols = []
        query_dict = parse(self.sqlQuery)
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
                            query = self.coalesce_udf(columns, tableName, alias, final_columns, len_udf)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "mul":
                            query = self.multiplication_udf(final_columns, alias, tableName)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "sum":
                            # query= sum_initial_udf(columns,final_df,alias)
                            # query_list.append(query)
                            query = self.group_by_func(tableName, query_dict, grp_cols, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "year":
                            query = self.year_month_udf(tableName, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "month":
                            query = self.year_month_udf(tableName, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "literal":
                            query = self.literals_adjust(tableName, columns, alias)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "distinct":
                            query = self.distinct_unique(tableName, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf == 1 and udf == "count":
                            # query = self.sum_initial_udf(columns, tableName, alias)
                            # query_list.append(query)
                            query = self.group_by_func(tableName, query_dict, grp_cols, alias, udf, final_columns)
                            query_list.append(query)
                        elif len_udf > 1:
                            for udf in reversed(udf_splitter):
                                if udf == 'mul':
                                    query = self.multiplication_udf(final_columns, alias, tableName)
                                    query_list.append(query)
                                elif udf == 'sum':
                                    # query = self.sum_initial_udf(columns, tableName, alias)
                                    # query_list.append(query)
                                    query = self.group_by_func(tableName, query_dict, grp_cols, alias, udf, final_columns)
                                    # final_df, query_dict, grp_cols, alias, columns, udf, final_columns
                                    query_list.append(query)
                                elif udf == "coalesce":
                                    query = self.coalesce_udf(columns, tableName, alias, final_columns, len_udf)
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

    def coalesce_udf(self, columns, final_df, alias, final_columns, len_udf):
        coalesce_filler = str(columns[-1])
        if len_udf == 1:
            query = final_df + "['" + alias + "']" + "=" + final_df + "." + final_columns[
                0] + ".fillna(value=" + coalesce_filler + ',inplace=True)'
        else:
            query = final_df + "['" + alias + "']" + "=" + final_df + "." + alias + ".fillna(value=" + coalesce_filler + ',inplace=True)'
        return query

    # #### Multiplication

    # In[4]:

    # def multiplication_udf(self, final_columns, alias, final_df):
    #     list_of_col = ["row." + a for a in final_columns]
    #     cols = '*'.join(list_of_col)
    #     query = final_df + "['" + alias + "']" + "=" + final_df + '.apply(lambda row: ' + cols + ', axis = 1)'
    #     return query

    def multiplication_udf(self,final_columns, alias, final_df):
        final_columns = [a for a in final_columns if a != 0]
        type_instance = all(isinstance(item, str) for item in final_columns)
        if type_instance == True:
            str_list = [a for a in final_columns if type(a) == str]
            list_of_col = [final_df + "['" + a + "']" for a in str_list]
            final = '*'.join(list_of_col)
            result = final_df + "['" + alias + "']=" + final
            return result

        elif type_instance == False:
            str_list = [a for a in final_columns if type(a) == str]
            list_of_col = [final_df + "['" + a + "']" for a in str_list]
            cols = '*'.join(list_of_col)
            int_list = [str(a) for a in final_columns if type(a) == int or type(a) == float]
            cols1 = '*'.join(int_list)
            final = cols + "*" + cols1
            result = final_df + "['" + alias + "']=" + final
            return result



    # #### basic sum functionality of query

    # In[5]:

    def sum_initial_udf(self, columns, final_df, alias):
        columns = columns[0]
        query = final_df + "['" + alias + "']" + "=" + final_df + "['" + columns + "']"
        return query

    # In[6]:

    # def sum_initial_udf(columns,final_df,alias):
    #     columns=columns[0]
    #     query= final_df+"['"+alias+"']"+"="+final_df+"['"+columns+"']"
    #     return query

    # In[ ]:

    # #### Year and month udf's are being handled here

    # In[7]:

    def group_by_func(self, final_df, query_dict, grp_cols, alias, udf, final_columns):
        # column = columns[0]
        grp_by = ""
        if 'groupby' in query_dict.keys():
            final_fcol = final_columns[0]
            grp_by = final_df + "['" + alias + "']" + " = " + final_df + "['" + final_fcol + "']." + udf +"()"
            # grp_by = final_df + "['" + alias + "']" + "=" + final_df + ".groupby(" + str(
            #     grp_cols) + ")" + "['" + final_fcol + "']" + ".agg(" + udf + ")"
        return grp_by


    def year_month_udf(self, final_df, alias, udf, final_columns):
        query = final_df + "['" + alias + "']" + "=" + final_df + "['" + final_columns[0] + "']" + ".dt." + udf
        return query

    # #### Literal

    # In[8]:

    def literals_adjust(self, final_df, columns, alias):
        """add new column to pandas dataframe with default value"""
        columns = columns[0]
        query = final_df + "['" + alias + "']" + "=" + "'" + columns + "'"
        return query

    # #### Distinct or unique

    # In[9]:

    def distinct_unique(self, final_df, alias, udf, final_columns):
        if alias == "":
            final_fcol = final_columns[0]
            query = final_df + "['" + final_fcol + "']" + "=" + final_df + "['" + final_fcol + "'].unique()"
        else:
            final_fcol = final_columns[0]
            query = final_df + "['" + alias + "']" + "=" + final_df + "['" + final_fcol + "'].unique()"
        return query

    def grouped_columns(self):
        list1 = []
        query_dict = parse(self.sqlQuery)
        # if "group by" in query_dict.keys():
        if 'groupby' in query_dict.keys():
            group_section = query_dict['groupby']
            if type(group_section) == list:
                for i in group_section:
                    values = i['value']
                    list1.append(values)
            elif type(group_section) == dict:
                for k, v in group_section.items():
                    list1.append(v)
        else:
            pass
        return list1


    def handleCaseStatements(self):
        script = []
        cases = self.identifyCaseStatements()
        if len(cases) > 0:
            for case in cases:
                queries = a.buildCaseQuery(case)
                for query in queries:
                    script.append(query)
        return script

    def alter_handler(self):
        query = self.sqlQuery
        if query.startswith("alter"):
            if "drop" in query:
                # finding the table name:
                start_table = 'table'
                end_table = 'drop'
                table_name = query[query.find(start_table) + len(start_table):query.rfind(end_table)].strip()

                # finding the columns
                start_col = 'column'
                end_col = ';'
                drop_columns_list = query[query.find(start_col) + len(start_col):query.rfind(end_col)].strip().split(
                    ",")
                clean_drop_col_list = [i.strip() for i in drop_columns_list]
                if "." in table_name:
                    table_name = table_name.split(".")[-1]
                else:
                    table_name = table_name
                final_alter_df = table_name + ".drop(columns=" + str(clean_drop_col_list) + ")"
                return final_alter_df

            else:
                pass
        else:
            pass

    def truncate_table(self):
        query = self.sqlQuery
        if query.startswith("truncate"):
            start_table = 'table'
            end_table = ')'
            table_name = query[query.find(start_table) + len(start_table):query.rfind(end_table)].strip()
            if "." in table_name:
                table_name = table_name.split(".")[-1]
            else:
                table_name = table_name
            final_df = table_name + "=" + table_name + ".truncate(before=-1, after=-1)"
            return final_df
        else:
            pass
    # =============================================================================
    #     Write Pandas Script
    # =============================================================================

    def buildPandasScript(self):

        finalScript = []
        emptyLine = ""

        if query.startswith("ALTER"):
            file_1= self.alter_handler()
            finalScript.append(file_1)
            return finalScript
        if query.startswith("TRUNCATE"):
            file_1= self.truncate_table()
            finalScript.append(file_1)
            return finalScript
        else:
            self.identifyTables()
            column_list = self.identifyColumns()
            # self.allCols()

            # try:
            finalScript.append("import pandas as pd")
            finalScript.append("import datetime")

            finalScript.append(emptyLine)

            for script in self.selectQuery():
                finalScript.append(script)
            finalScript.append(emptyLine)

            for script in self.renameColumns():
                finalScript.append(script)
            finalScript.append(emptyLine)

            for script in self.joinQuery():
                finalScript.append(script)
            # case functions and UDFs
            finalScript.append(emptyLine)

            self.allCols()

            groupByScript = self.groupByQuery(self.tableNames[0])
            finalScript.append(groupByScript)


            print("(1)cols are :", column_list)
            for script in self.handleUDFs(column_list):
                finalScript.append(script)

            for script in self.format_udf_substr():
                finalScript.append(script)

            finalScript.append(emptyLine)
            for script in self.handleCaseStatements():
                finalScript.append(script)

            finalScript.append(emptyLine)
            whereClaseScript = self.handleWhereClauses()
            finalScript.append(whereClaseScript)

            orderByScript = self.orderByQuery(self.tableNames[0])
            finalScript.append(orderByScript)
            finalScript.append(emptyLine)

            for script in self.createTableQuery():
                finalScript.append(script)

            for script in self.insertTableQuery():
                finalScript.append(script)
            # except:
                # return finalScript

            return finalScript

    def intermediate_select_dict(self):
        column_list = []
        select_list = parse(self.sqlQuery)['select']
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


    allColumns = []
    def flatten_json(self, nested_json: dict, exclude: list = [''], sep: str = '_') -> dict:
        """
        Flatten a list of nested dicts.
        """
        out = dict()

        def flatten(x: (list, dict, str), name: str = '', exclude=exclude):
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

    colsDict = {}

    def allCols(self):
        cols = self.flatten_json(parse(self.sqlQuery))
        for key in cols.keys():
            if "value" in key:
                potentialCol = cols[key]
                if potentialCol not in self.tableNames and not str(potentialCol).isnumeric():
                    self.allColumns.append(potentialCol)
        # print("all cols:", self.allColumns)
        #print("cols are :",self.allColumns)
        self.allColumns=[a for a in self.allColumns if type(a)==str]
        for column in self.allColumns:
            if "." in column:
                splits = column.split(".")
                tableName = self.tableAlias[splits[0]]
                columnName = splits[1]
                if tableName in self.colsDict.keys():
                    self.colsDict[tableName].append(columnName)
                else:
                    self.colsDict[tableName] = [columnName]
                if tableName in self.tableColumnsDict.keys():
                    self.tableColumnsDict[tableName][columnName] = ""
                else:
                    self.tableColumnsDict[tableName] = {columnName:""}
            else:
                if column != "true" and column != "false":
                    # print("columns:", column)
                    if self.tableNames[0] in self.colsDict.keys():
                        self.colsDict[self.tableNames[0]].append(column)
                    else:
                        self.colsDict[self.tableNames[0]] = [column]
                    if self.tableNames[0] in self.tableColumnsDict.keys():
                        self.tableColumnsDict[self.tableNames[0]][column] = ""
                    else:
                        self.tableColumnsDict[self.tableNames[0]] = {column:""}
        return self.colsDict


    def format_udf_substr(self):
        select_list=parse(self.sqlQuery)['select']
        final_df= self.tableNames[0]
        data_frame = []
        for column_dict in select_list:
            if type(column_dict) == dict:
                if type(column_dict['value']) == dict:
                    for k, v in column_dict['value'].items():
                        if k == "format":
                            alias = column_dict['name']
                            values = v
                            sliced_col = values[0]
                            substr_val = values[-1]
                            for k, v in substr_val.items():  # needs to be extended
                                data_frame_str = final_df + "['" + alias + "']=" + final_df + "['" + sliced_col + "'].str[:" + str(
                                    v) + "]"
                                data_frame.append(data_frame_str)
                        else:
                            pass
                else:
                    pass
            else:
                pass
        return data_frame


    # =============================================================================
    #     FROM statement processing
    # =============================================================================

if __name__ == "__main__":
    from moz_sql_parser import parse
    import re
    import pandas as pd

    query = """ CREATE TABLE Futures AS  ( select marsha, 'Futures' AS Stay_Year, coalesce( SUM( crossover_rms),0) AS CO_RN_Goal, coalesce( SUM( crossover_rms*def_gadr),0) AS CO_Rev_Goal, SUM( def_rms) AS Def_OTB, SUM( def_rms*def_gadr) AS Def_Rev, SUM( cid_rms) AS Target, SUM( avg_rms) AS Avg_Bkd FROM AW_TGT_BUS_DBO.OY_ANNUALCROSSOVER WHERE stay_year > curyr AND ASOF_YRMO=CURYRPD GROUP BY marsha ORDER BY marsha) ;"""
    #query = "ALTER    TABLE Cal_pd DROP COLUMN MARAVAIL ,  MARSOLD ,  MARREV ,  MARAVAILLY ,  MARSOLDLY ,  MARREVLY ,  MKTAVAIL ,  MKTSOLD ,  MKTREV ,  MKTAVAILLY ,  MKTSOLDLY ,  MKTREVLY ;"

    # query =""" CREATE TABLE daysytd_ty AS  (
    # select b.year_cal , COUNT( b.date_dt) AS days_ytd_ty FROM mrdw_dim_date b
    # WHERE 100*b.year_cal+b.month_cal_id <= curr_mo
    # AND 100*b.year_cal+b.month_cal_id >= curr_mo-12 GROUP BY b.year_cal ORDER BY b.year_cal  )  ;"""
    # query ="""CREATE TABLE data_3a AS select distinct prop_code,EXTRACT(YEAR FROM stay_dt) AS year,EXTRACT(MONTH FROM stay_dt) AS month, COUNT( user_capped) AS act_ct
    # FROM uat1_d WHERE act_status='Y' GROUP BY prop_code, year, month;"""

    a = SQL_Pandas_Parser(query)

    for s in a.buildPandasScript():
        print(s)

    # code starts here:
    # reader = pd.read_csv("C:/Users/arshashank/Desktop/27/demo.csv")
    # aftergrouping_data = reader.groupby("filename")
    # for name, group in aftergrouping_data:
    #     file_name = name.split(".")[0] + ".py"
    #     query = group['SQL']
    #     f = open(file_name, 'a')
    #     for index, query in query.iteritems():
    #         a = SQL_Pandas_Parser(query)
    #         f.write('"""Query: ' + query + '"""\n\n')
    #         for s in a.buildPandasScript():
    #             f.write(s)
    #             f.write("\n")
    #     f.close()