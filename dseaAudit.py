#!/usr/bin/env/ python

"""
dseaAudit.py is used to run a SQL query on the Aeries database 
and print out any discrepancies in 
the data
"""
import pandas as pd
import pyodbc
from dotenv import dotenv_values

secrets = dotenv_values(".env")

username = secrets["SQLUSERNAME"]
password = secrets["SQLPASSWORD"]
database = secrets["SQLDATABASE"]
server = secrets["SQLSERVER"]

cnxn = pyodbc.connect(
    """Driver={SQL Server Native Client 11.0};
                      SERVER="""+server+""";
                      DATABASE="""+database+""";
                      UID="""
    + username
    + ";PWD="
    + password
)

final_list = []

for school_number in [68, 73, 72, 70, 60, 69, 61]:
    query = f"""
    SELECT TOP 100000 STU.LN + ', ' + STU.FN AS [Student Name], 
    [STU].[GR] AS [Grade], [ATT].[GR] AS [Grade1], [CSE].[DR] AS [DistRes], [STU].[ITD] AS [DST of Residence], 
    [ENR].[ITD] AS [DST of Residence1], [ATT].[ITD] AS [DST of Residence2], [STU].[AP1] AS [AttPrgm1], 
    [ENR].[AP1] AS [AttPrgm11], [ATT].[AP1] AS [AttPrgm12], [STU].[SP] AS [Prog], [ENR].[PR] AS [Program], 
    [ATT].[PR] AS [Program1], [STU].[TR] AS [Track], [ENR].[TR] AS [Track1], [ATT].[TR] AS [Track2], 
    CONVERT(VARCHAR(10),[ATT].[DT],101) AS [Date], [ENR].[YR] AS [Year] 
    FROM (SELECT [ENR].* FROM [ENR] WHERE DEL = 0) [ENR] RIGHT JOIN ((SELECT [CSE].* FROM [CSE] 
    WHERE DEL = 0) [CSE] RIGHT JOIN ((SELECT [STU].* FROM STU WHERE DEL = 0) [STU] 
    LEFT JOIN (SELECT [ATT].* FROM [ATT] WHERE DEL = 0) [ATT] ON [STU].[SC] = [ATT].[SC] 
    AND [STU].[SN] = [ATT].[SN]) ON [STU].[ID] = [CSE].[ID]) ON [STU].[ID] = [ENR].[ID] 
    WHERE (NOT STU.TG > ' ') AND ( [ENR].[ED] > '6/30/2024' AND [ATT].[GR] <> 0) 
    AND [STU].SC = {school_number} AND [ATT].SC = {school_number} AND [ENR].SC = {school_number}
    ORDER BY [STU].[LN], [STU].[FN];
    """
    # Aeries query version of the same thing
    # LIST STU STU.NM STU.GR ATT.GR CSE.DR STU.ITD ENR.ITD ATT.ITD STU.AP1 ENR.AP1 ATT.AP1 STU.SP ENR.PR ATT.PR STU.TR ENR.TR ATT.TR ATT.DT ENR.YR IF ENR.ED > 6/30/2024 AND IF ATT.GR # 0
    # ATT.CD = "E"
    df = pd.read_sql_query(query, cnxn)


    def tweak_dsea(df):
        """
        Summary or Description of the Function

        Parameters:
        argument1 (int): Description of arg1

        Returns:
        int:Returning value
        """
        dfa = df.replace('', pd.NA) # look for empty strings
        return (
            dfa.assign(Grade_check=df.Grade == df.Grade1)
            .assign(DistRes=df.DistRes + "0000000")
            .assign(
                dist_check=lambda df_: (df_["DistRes"] == df_["DST of Residence"])
                & (df_["DST of Residence"] == df_["DST of Residence1"])
                & (df_["DST of Residence1"] == df_["DST of Residence2"])
            )
            .assign(
                attpgm_check=(df.AttPrgm1 == df.AttPrgm11)
                & (df.AttPrgm11 == df.AttPrgm12)
            )
            .assign(pgm_check=(df.Prog == df.Program) & (df.Program == df.Program1))
            .assign(track_check=(df.Track == df.Track1) & (df.Track1 == df.Track2))
            .assign(school=school_number)
        )

    clean = tweak_dsea(df)

    limited = clean.query(
        "Grade_check == False | attpgm_check == False | dist_check == False | pgm_check == False | track_check == False"
    )


    final_list.append(limited)

    limited = clean[clean.isnull().any(axis=1)]

    final_list.append(limited)

final_df = pd.concat(final_list).drop_duplicates()
print(final_df)
final_df.to_csv('discrepancies_oct_10.csv',index=False)
