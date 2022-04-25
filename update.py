import pymysql
import csi3335sp2022 as cfg
import pandas as pd
import numpy as np

##lxml package
## losses spelled wrong in series post
## data base was missing 2020 home games

## NAN from data frame replaced with 0
## Need to figure out how to get NAN from data frame to null in SQL
#df1 = df.where(pd.notnull(df), None)





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
## messed up allstar
bailean01=allstarfull[allstarfull['playerID']=='bailean01']
bailean01=bailean01[bailean01['yearID']==2009]


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


fieldingofsplit=pd.read_csv('/Users/youngjosh/Desktop/baseballdatabank-2022-2.2/core/FieldingOFsplit.csv')
## all values null: drop
fieldingofsplit=fieldingofsplit.drop(columns=['PB','WP','SB','CS','ZR'])


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



## Hall of fame

##2021
url = "https://www.baseball-reference.com/awards/hof_2021.shtml"
df_list = pd.read_html(url)
normal_players = df_list[0]
len(normal_players.columns)
cols = list(range(5,34))
normal_players_2021 = df_list[0]
len(normal_players.columns)
cols = list(range(5,34))

normal_players_2021.drop(normal_players_2021.columns[cols],axis=1,inplace=True)
normal_players_2021.columns = normal_players_2021.columns.droplevel(0)

normal_players_2021=normal_players_2021.drop(columns=['Rk','YoB','H','HR','BB','SO','%vote'])
normal_players_2021.drop(normal_players_2021.columns[2], axis=1,inplace=True)

yearID = [2021 for i in range(25)]
normal_players_2021['YearID']=yearID

needed =[301 for i in range(25)]
normal_players_2021['needed']=needed

ballots =[401 for i in range(25)]
normal_players_2021['ballots']=ballots

votedBy =['BBWAA' for i in range(25)]
normal_players_2021['votedBy']=votedBy

inducted=['N' for i in range(25)]
normal_players_2021['inducted']=inducted

category=['Player' for i in range(25)]
normal_players_2021['category']=category

normal_players_2021['Name']=normal_players_2021['Name'].str.replace("X-","")

normal_players_2021[['nameFirst', 'nameLast']] = normal_players_2021['Name'].str.split(' ', expand=True)

normal_players_2021.at[24,'nameFirst']='A. J.'


##2020
url = 'https://www.baseball-reference.com/awards/hof_2020.shtml#all_hof_Veterans'
df_list = pd.read_html(url)
hof20 = df_list[0]
hof20.columns = hof20.columns.droplevel(0)
cols = list(range(5,39))
hof20.drop(hof20.columns[cols],axis=1,inplace=True)
hof20=hof20.drop(columns=['Rk','YoB','%vote'])

yearID = [2020 for i in range(32)]
hof20['YearID']=yearID

needed =[298 for i in range(32)]
hof20['needed']=needed

ballots =[397 for i in range(32)]
hof20['ballots']=ballots

votedBy =['BBWAA' for i in range(32)]
hof20['votedBy']=votedBy

inducted=['N' for i in range(32)]
hof20['inducted']=inducted

category=['Player' for i in range(32)]
hof20['category']=category

hof20['Name']=hof20['Name'].str.replace("X-","")

hof20.at[0,'inducted']='Y'
hof20.at[1,'inducted']='Y'



hof20.loc[hof20.shape[0]] = ['Marvin Miller', None, 2020,None,None,'Veterans','Y','Pioneer/Executive']
hof20.loc[hof20.shape[0]] = ['Ted Simmons', None, 2020,None,None,'Veterans','Y','Player']

hof20[['nameFirst', 'nameLast']] = hof20['Name'].str.split(' ', expand=True)

hof20.at[21,'nameFirst']='J. J.'

##2019
url = "https://www.baseball-reference.com/awards/hof_2019.shtml#all_hof_Veterans"
df_list = pd.read_html(url)

hof19 = df_list[0]
len(normal_players.columns)
cols = list(range(5,34))

hof19.columns = hof19.columns.droplevel(0)
hof19.drop(hof19.columns[cols],axis=1,inplace=True)
hof19=hof19.drop(columns=['Rk','YoB','%vote','SO'])
hof19.drop(hof19.columns[2], axis=1,inplace=True)

yearID = [2019 for i in range(35)]
hof19['YearID']=yearID

needed =[319 for i in range(35)]
hof19['needed']=needed

ballots =[425 for i in range(35)]
hof19['ballots']=ballots

votedBy =['BBWAA' for i in range(35)]
hof19['votedBy']=votedBy

inducted=['N' for i in range(35)]
hof19['inducted']=inducted

category=['Player' for i in range(35)]
hof19['category']=category

hof19['Name']=hof19['Name'].str.replace("X-","")

hof19.at[0,'inducted']='Y'
hof19.at[1,'inducted']='Y'
hof19.at[2,'inducted']='Y'
hof19.at[3,'inducted']='Y'

hof19.loc[hof19.shape[0]] = ['Harold Baines', None, 2019,None,None,'Veterans','Y','Player']
hof19.loc[hof19.shape[0]] = ['Lee Smith', None, 2019,None,None,'Veterans','Y','Player']

hof19[['nameFirst', 'nameLast']] = hof19['Name'].str.split(' ', expand=True)

hof19.at[7,'nameLast']='Walker'


con=pymysql.connect(host=cfg.mysql['location'],user=cfg.mysql['user'],password=cfg.mysql['password'],db=cfg.mysql['database'])
with con:
    cur = con.cursor()
    sql='UPDATE allstarfull SET yearID =%s, gameNum=%s, gameID=%s WHERE yearID=0'
    cur.execute(sql,(bailean01.iloc[0, 1],bailean01.iloc[0, 2],bailean01.iloc[0, 3]))

    cols = "`,`".join([str(i) for i in allstarfull21.columns.tolist()])
    for i, row in allstarfull21.iterrows():
        sql = "INSERT INTO `allstarfull` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))


    sql = "ALTER TABLE appearances ADD G_rf VARCHAR(100)  AFTER G_cf"
    cur.execute(sql)

    sql='CREATE TABLE if not exists fieldingofsplit (ID int NOT NULL AUTO_INCREMENT,' \
        'playerID varchar(255) NOT NULL, yearID int NOT NULL, stint int NOT NULL, teamID varchar(255) NOT NULL,' \
        'lgID varchar (255) NOT NULL, POS varchar (255), G int, GS int, Innouts int,' \
        'PO int, A int, E int, DP int, PRIMARY KEY (ID))'
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

    #for i in range(len(appearances)):
        #sql="UPDATE appearances SET G_rf =%s WHERE playerid=%s"
        #cur.execute(sql,(appearances.iloc[i,16],appearances.iloc[i,3]))

    hof21ids=[]
    hof20ids=[]
    hof19ids=[]

    for i in range(len(normal_players_2021)):
        sql = 'SELECT playerid FROM people WHERE nameFirst=%s AND nameLast=%s;'
        cur.execute(sql,(normal_players_2021.iloc[i,8],normal_players_2021.iloc[i,9]))
        results=cur.fetchall()
        for row in results:
            for col in row:
                hof21ids.append(col)

    normal_players_2021['playerID']=hof21ids

    for i in range(len(hof20)):
        sql = 'SELECT playerid FROM people WHERE nameFirst=%s AND nameLast=%s;'
        cur.execute(sql, (hof20.iloc[i, 8], hof20.iloc[i, 9]))
        results=cur.fetchall()
        for row in results:
            for col in row:
                hof20ids.append(col)
    hof20['playerID']=hof20ids

    for i in range(len(hof19)):
        sql = 'SELECT playerid FROM people WHERE nameFirst=%s AND nameLast=%s;'
        cur.execute(sql, (hof19.iloc[i, 8], hof19.iloc[i, 9]))
        results=cur.fetchall()
        for row in results:
            for col in row:
                hof19ids.append(col)
    hof19ids.remove('garcifr01')
    hof19['playerID']=hof19ids

    hof19 = hof19.drop(columns=['Name','nameFirst','nameLast'])
    cols = "`,`".join([str(i) for i in hof19.columns.tolist()])
    for i, row in hof19.iterrows():
        sql = "INSERT INTO `halloffame` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    hof20 = hof20.drop(columns=['Name', 'nameFirst', 'nameLast'])
    cols = "`,`".join([str(i) for i in hof20.columns.tolist()])
    for i, row in hof20.iterrows():
        sql = "INSERT INTO `halloffame` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    normal_players_2021 = normal_players_2021.drop(columns=['Name', 'nameFirst', 'nameLast'])

    cols = "`,`".join([str(i) for i in normal_players_2021.columns.tolist()])
    for i, row in normal_players_2021.iterrows():
        sql = "INSERT INTO `halloffame` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

    cols = "`,`".join([str(i) for i in fieldingofsplit.columns.tolist()])
    for i, row in fieldingofsplit.iterrows():
        sql = "INSERT INTO `fieldingofsplit` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))





    con.commit()


