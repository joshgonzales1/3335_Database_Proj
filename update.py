import pymysql
import csi3335sp2022 as cfg
import pandas as pd
import numpy as np

## losses spelled wrong in series post
## data base was missing 2020 home games

## NAN from data frame replaced with 0
## Need to figure out how to get NAN from data frame to null in SQL


## dont know what to do with this data
fieldingof =pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/FieldingOF.csv')
fieldingof21= fieldingof[fieldingof['yearID']==2021]


batting=pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/Batting.csv')
batting21= batting[batting['yearID']==2021]
batting21=batting21.drop(columns=['lgID'])
batting21=batting21.rename(columns = {'G':'b_G', 'AB':'b_AB','R':'b_R',
                            'H':'b_H', '2B':'b_2B', '3B':'b_3B',
                            'HR':'b_HR','RBI':'b_RBI','SB':'b_SB',
                            'CS':'b_CS','BB':'b_BB','SO':'b_SO',
                            'IBB':'b_IBB','HBP':'b_HBP','SH':'b_SH',
                            'SF':'b_SF','GIDP':'b_GIDP'})


allstarfull=pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/AllstarFull.csv')
allstarfull21= allstarfull[allstarfull['yearID']==2021]
allstarfull21=allstarfull21.fillna(0)


appearances=pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/Appearances.csv')
appearances21= appearances[appearances['yearID']==2021]
appearances21=appearances21.drop(columns=['lgID'])


battingpost=pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/BattingPost.csv')
battingpost21= battingpost[battingpost['yearID']==2021]
battingpost21=battingpost21.drop(columns=['lgID'])
battingpost21=battingpost21.rename(columns = {'G':'b_G', 'AB':'b_AB','R':'b_R',
                            'H':'b_H', '2B':'b_2B', '3B':'b_3B',
                            'HR':'b_HR','RBI':'b_RBI','SB':'b_SB',
                            'CS':'b_CS','BB':'b_BB','SO':'b_SO',
                            'IBB':'b_IBB','HBP':'b_HBP','SH':'b_SH',
                            'SF':'b_SF','GIDP':'b_GIDP'})



fielding=pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/Fielding.csv')
fielding21= fielding[fielding['yearID']==2021]
fielding21=fielding21.fillna(0)
fielding21=fielding21.drop(columns=['lgID'])
fielding21=fielding21.rename(columns = {'POS':'position', 'G':'f_G','GS':'f_GS',
                            'InnOuts':'f_InnOuts', 'PO':'f_PO', 'A':'f_A',
                            'E':'f_E','DP':'f_DP','PB':'f_PB',
                            'WP':'f_WP','SB':'f_SB','CS':'f_CS',
                            'ZR':'f_ZR'})





fieldingpost =pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/FieldingPost.csv')
fieldingpost21= fieldingpost[fieldingpost['yearID']==2021]
fieldingpost21=fieldingpost21.fillna(0)
fieldingpost21=fieldingpost21.drop(columns=['lgID'])
fieldingpost21=fieldingpost21.drop(columns=['TP'])
fieldingpost21=fieldingpost21.rename(columns = {'POS':'position', 'G':'f_G','GS':'f_GS',
                            'InnOuts':'f_InnOuts', 'PO':'f_PO', 'A':'f_A',
                            'E':'f_E','DP':'f_DP','PB':'f_PB',
                            'WP':'f_WP','SB':'f_SB','CS':'f_CS',
                            'ZR':'f_ZR'})


#data base missing 2020 homegames as well
homegames =pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/HomeGames.csv')
homegames21= homegames[homegames['year.key']==2021]
homegames20= homegames[homegames['year.key']==2020]
homegames21=homegames21.drop(columns=['league.key'])
homegames20=homegames20.drop(columns=['league.key'])
homegames21=homegames21.rename(columns = {'year.key':'yearID','park.key':'parkID','team.key':'teamID',
                            'span.first':'firstgame', 'span.last':'lastgame','attendance':'attendence'})

homegames20=homegames20.rename(columns = {'year.key':'yearID','park.key':'parkID','team.key':'teamID',
                            'span.first':'firstgame', 'span.last':'lastgame','attendance':'attendence'})

managers =pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/Managers.csv')
managers21= managers[managers['yearID']==2021]
managers21=managers21.drop(columns=['lgID'])
managers21=managers21.rename(columns = {'inseason':'inSeason','G':'manager_G','W':'manager_W',
                            'L':'manager_L', 'rank':'teamRank'})

# DATA BASE already has people who debuted in 2021
people =pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/People.csv')
people=people.fillna(0)
people=people.drop(columns=['retroID','bbrefID'])
people=people.rename(columns = {'debut':'debutDate','finalGame':'finalGameDate'})
people['debutDate']=pd.to_datetime(people['debutDate'])
# people who debut in 2021
people21= people[people['debutDate'].dt.year==2021]
#people who died in 2021 RIP
d21= people[people['deathYear']==2021]
d21=d21.drop(columns=['birthYear','birthMonth','birthDay','birthCountry'
                     ,'birthState','birthCity','nameFirst','nameLast',
                      'nameGiven','weight','height','bats','throws',
                      'debutDate','finalGameDate'])

pitching =pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/Pitching.csv')
pitching21= pitching[pitching['yearID']==2021]
pitching21=pitching21.fillna(0)
pitching21=pitching21.drop(columns=['lgID'])
pitching21=pitching21.rename(columns = {'W':'p_W','L':'p_L','G':'p_G', 'GS':'p_GS','CG':'p_CG',
                            'SHO':'p_SHO', 'SV':'p_SV', 'IPouts':'p_IPOuts',
                            'H':'p_H','ER':'p_ER','HR':'p_HR','BFP':'p_BFP','GF':'p_GF','R':'p_R',
                            'BAOpp':'p_BAOpp','ERA':'p_ERA','WP':'p_WP','BK':'p_BK','BB':'p_BB','SO':'p_SO',
                            'IBB':'p_IBB','HBP':'p_HBP','SH':'p_SH',
                            'SF':'p_SF','GIDP':'p_GIDP'})



pitchingpost =pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/PitchingPost.csv')
pitchingpost21= pitchingpost[pitchingpost['yearID']==2021]
pitchingpost21=pitchingpost21.drop(columns=['lgID'])
pitchingpost21=pitchingpost21.rename(columns = {'W':'p_W','L':'p_L','G':'p_G', 'GS':'p_GS','CG':'p_CG',
                            'SHO':'p_SHO', 'SV':'p_SV', 'IPouts':'p_IPOuts',
                            'H':'p_H','ER':'p_ER','HR':'p_HR','BFP':'p_BFP','GF':'p_GF','R':'p_R',
                            'BAOpp':'p_BAOpp','ERA':'p_ERA','WP':'p_WP','BK':'p_BK','BB':'p_BB','SO':'p_SO',
                            'IBB':'p_IBB','HBP':'p_HBP','SH':'p_SH',
                            'SF':'p_SF','GIDP':'p_GIDP'})

pitchingpost21 = pitchingpost21.replace([np.inf, -np.inf], np.nan)
pitchingpost21=pitchingpost21.fillna(0)

seriespost =pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/SeriesPost.csv')
seriespost21= seriespost[seriespost['yearID']==2021]
seriespost21=seriespost21.drop(columns=['lgIDwinner','lgIDloser'])
seriespost21=seriespost21.rename(columns = {'losses':'loses'})

teams =pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/Teams.csv')
teams21= teams[teams['yearID']==2021]
teams21=teams21.drop(columns=['teamIDlahman45','teamIDretro','name','park','attendance','E','DP','FP',
                             'BPF','PPF','teamIDBR'])
teams21=teams21.rename(columns = {'Rank':'teamRank','G':'team_G','Ghome':'team_G_home', 'W':'team_W',
                                  'L':'team_L','R':'team_R','AB':'team_AB','H':'team_H',
                                 '2B':'team_2B','3B':'team_3B','HR':'team_HR','BB':'team_BB',
                                 'SO':'team_SO','SB':'team_SB','CS':'team_CS','HBP':'team_HBP',
                                 'SF':'team_SF','RA':'team_RA','ER':'team_ER','ERA':'team_ERA',
                                 'CG':'team_CG','SHO':'team_SHO','SV':'team_SV','IPouts':'team_IPouts',
                                 'HA':'team_HA','HRA':'team_HRA','BBA':'team_BBA','SOA':'team_SOA'})


##managers who are not in people:
halede99 = people[people['playerID']=='halede99']
eversbi99= people[people['playerID']=='eversbi99']
schnejo99=people[people['playerID']=='schnejo99']
rowsoja99=people[people['playerID']=='rowsoja99']
jaussda99=people[people['playerID']=='jaussda99']

con=pymysql.connect(host=cfg.mysql['location'],user=cfg.mysql['user'],password=cfg.mysql['password'],db=cfg.mysql['database'])
with con:
    cur = con.cursor()
    cols = "`,`".join([str(i) for i in allstarfull21.columns.tolist()])
    for i, row in allstarfull21.iterrows():
        sql = "INSERT INTO `allstarfull` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))


    sql = "ALTER TABLE appearances ADD G_rf VARCHAR(100)  AFTER G_cf"
    cur.execute(sql)

    cols = "`,`".join([str(i) for i in halede99.columns.tolist()])
    for i, row in halede99.iterrows():
        sql = "INSERT INTO `people` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in eversbi99.columns.tolist()])
    for i, row in eversbi99.iterrows():
        sql = "INSERT INTO `people` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in schnejo99.columns.tolist()])
    for i, row in schnejo99.iterrows():
        sql = "INSERT INTO `people` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in rowsoja99.columns.tolist()])
    for i, row in rowsoja99.iterrows():
        sql = "INSERT INTO `people` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in jaussda99.columns.tolist()])
    for i, row in jaussda99.iterrows():
        sql = "INSERT INTO `people` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))



    cols = "`,`".join([str(i) for i in appearances21.columns.tolist()])
    for i, row in appearances21.iterrows():
        sql = "INSERT INTO `appearances` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in batting21.columns.tolist()])
    for i, row in batting21.iterrows():
        sql = "INSERT INTO `batting` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in battingpost21.columns.tolist()])
    for i, row in battingpost21.iterrows():
        sql = "INSERT INTO `battingpost` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in fielding21.columns.tolist()])
    for i, row in fielding21.iterrows():
        sql = "INSERT INTO `fielding` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in fieldingpost21.columns.tolist()])
    for i, row in fieldingpost21.iterrows():
        sql = "INSERT INTO `fieldingpost` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in homegames20.columns.tolist()])
    for i, row in homegames20.iterrows():
        sql = "INSERT INTO `homegames` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in homegames21.columns.tolist()])
    for i, row in homegames21.iterrows():
        sql = "INSERT INTO `homegames` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in managers21.columns.tolist()])
    for i, row in managers21.iterrows():
        sql = "INSERT INTO `manager` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in pitching21.columns.tolist()])
    for i, row in pitching21.iterrows():
        sql = "INSERT INTO `pitching` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in pitchingpost21.columns.tolist()])
    for i, row in pitchingpost21.iterrows():
        sql = "INSERT INTO `pitchingpost` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in seriespost21.columns.tolist()])
    for i, row in seriespost21.iterrows():
        sql = "INSERT INTO `seriespost` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in teams21.columns.tolist()])
    for i, row in teams21.iterrows():
        sql = "INSERT INTO `team` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    for i in range(len(d21)):
            sql="UPDATE people SET deathYear = %s, deathMonth = %s, deathDay=%s, deathCountry=%s, deathState=%s, deathCity=%s WHERE playerid =%s"
            cur.execute(sql,(d21.iloc[i, 1],d21.iloc[i, 2],d21.iloc[i, 3],d21.iloc[i, 4],d21.iloc[i, 5],d21.iloc[i, 6],d21.iloc[i, 0]))









    con.commit()


