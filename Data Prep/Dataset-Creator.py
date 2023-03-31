import mariadb
import sys
import csv

try:
    db_conn = mariadb.connect(
        user="tf2sa",
        password="pwd",
        host="localhost",
        port=3306,
        database="tf2sadb"
    )

    print("Successfully connected to the database")
except mariadb.Error as e:
    print("Error connecting to the database: " + e)
    sys.exit()

db_cursor = db_conn.cursor()

# Connection to the mariadb database containing game statistics.

# TeamID Values --> Blue = 1, Red = 0.

gameStatsFile = open('Game_stats.csv', mode='w')
gameStatsWriter = csv.writer(
    gameStatsFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

gameStatsWriter.writerow(["RedScout1Kills", "RedScout1Assists", "RedScout1Deaths", "RedScout1Damage", "RedScout2Kills", "RedScout2Assists", "RedScout2Deaths", "RedScout2Damage", "BlueScout1Kills", "BlueScout1Assists", "BlueScout1Deaths", "BlueScout1Damage", "BlueScout2Kills", "BlueScout2Assists", "BlueScout2Deaths", "BlueScout2Damage", "RedSoldier1Kills", "RedSoldier1Assists", "RedSoldier1Deaths", "RedSoldier1Damage", "RedSoldier2Kills", "RedSoldier2Assists", "RedSoldier2Deaths", "RedSoldier2Damage",
                         "BlueSoldier1Kills", "BlueSoldier1Assists", "BlueSoldier1Deaths", "BlueSoldier1Damage", "BlueSoldier2Kills", "BlueSoldier2Assists", "BlueSoldier2Deaths", "BlueSoldier2Damage", "RedDemo1Kills", "RedDemo1Assists", "RedDemo1Deaths", "RedDemo1Damage", "BlueDemo1Kills", "BlueDemo1Assists", "BlueDemo1Deaths", "BlueDemo1Damage", "RedMedic1Assists", "RedMedic1Deaths", "RedMedic1Ubers", "RedMedic1Heals", "BlueMedic1Assists", "BlueMedic1Deaths", "BlueMedic1Ubers", "BlueMedic1Heals"])

db_cursor.execute("""
    SELECT GameID from games WHERE IsValidStats = 1
""")

GameIdValues = db_cursor.fetchall()

GameIdList = []
ProgressCounter = 0

for Game in GameIdValues:
    GameIdList.append(Game[0])

for Game in GameIdList:
    GameLog = []
    ProgressCounter += 1
    print("Processing game: " + str(ProgressCounter) +
          " of " + str(len(GameIdList)))

    db_cursor.execute("""
        SELECT
            g.GameID, cs.classID, ps.SteamID, ps.TeamID, cs.Kills, cs.Assists, cs.Deaths, cs.Damage
            FROM
                games as g INNER JOIN playerstats as ps ON g.GameID = ps.gameID
                INNER JOIN classstats as cs on  ps.PlayerStatsID = cs.PlayerStatsID
                WHERE g.IsValidStats = 1 AND cs.classID = 1 AND g.GameID = ? AND cs.Playtime > 1000
                GROUP BY g.gameID, ps.PlayerStatsID, ps.SteamID, ps.TeamID, cs.classID, cs.Kills, cs.Assists, cs.Deaths, cs.Damage
                ORDER BY g.gameID DESC, ps.TeamID, cs.ClassID, cs.Damage DESC
    """, [Game])

    ScoutGameList = db_cursor.fetchall()

    db_cursor.execute("""
        SELECT
            g.GameID, cs.classID, ps.SteamID, ps.TeamID, cs.Kills, cs.Assists, cs.Deaths, cs.Damage
            FROM
                games as g INNER JOIN playerstats as ps ON g.GameID = ps.gameID
                INNER JOIN classstats as cs on  ps.PlayerStatsID = cs.PlayerStatsID
                WHERE g.IsValidStats = 1 AND cs.classID = 2 AND g.GameID = ? AND cs.Playtime > 1000
                GROUP BY g.gameID, ps.PlayerStatsID, ps.SteamID, ps.TeamID, cs.classID, cs.Kills, cs.Assists, cs.Deaths, cs.Damage
                ORDER BY g.gameID DESC, ps.TeamID, cs.ClassID, cs.Damage DESC
""", [Game])

    SoldierGameList = db_cursor.fetchall()

    db_cursor.execute("""
        SELECT
            g.GameID, cs.classID, ps.SteamID, ps.TeamID, cs.Kills, cs.Assists, cs.Deaths, cs.Damage
            FROM
                games as g INNER JOIN playerstats as ps ON g.GameID = ps.gameID
                INNER JOIN classstats as cs on  ps.PlayerStatsID = cs.PlayerStatsID
                WHERE g.IsValidStats = 1 AND cs.classID = 4 AND g.GameID = ? AND cs.Playtime > 1000
                GROUP BY g.gameID, ps.PlayerStatsID, ps.SteamID, ps.TeamID, cs.classID, cs.Kills, cs.Assists, cs.Deaths, cs.Damage
                ORDER BY g.gameID DESC, ps.TeamID, cs.ClassID, cs.Damage DESC
""", [Game])

    DemoGameList = db_cursor.fetchall()

    db_cursor.execute("""
        SELECT
            g.GameID, cs.classID, ps.SteamID, ps.TeamID, cs.Assists, cs.Deaths, ps.Ubers, ps.Drops, ps.Heals
            FROM
                games as g INNER JOIN playerstats as ps ON g.GameID = ps.gameID
                INNER JOIN classstats as cs on  ps.PlayerStatsID = cs.PlayerStatsID
                WHERE g.IsValidStats = 1 AND cs.classID = 7 AND g.GameID = ? AND cs.Playtime > 1000
                GROUP BY g.GameID, cs.classID, ps.SteamID, ps.TeamID, cs.Assists, cs.Deaths, ps.Ubers, ps.Drops, ps.Heals
                ORDER BY g.gameID DESC, ps.TeamID, cs.ClassID, ps.Heals DESC
""", [Game])

    MedicGameList = db_cursor.fetchall()

    db_cursor.execute("""
        SELECT 
            RedScore, BlueScore
        FROM
            games
        WHERE
            GameID = ?
    """, [Game])

    GameScore = db_cursor.fetchall()

    GameScoreDifference = abs(GameScore[0][0] - GameScore[0][1])

    if (len(ScoutGameList) == 4) and (len(SoldierGameList) == 4) and (len(DemoGameList) == 2) and (len(MedicGameList) == 2):

        GameLog.extend([ScoutGameList[0][4], ScoutGameList[0][5],
                        ScoutGameList[0][6], ScoutGameList[0][7],
                        ScoutGameList[1][4], ScoutGameList[1][5],
                        ScoutGameList[1][6], ScoutGameList[1][7]])  # Red Scouts

        GameLog.extend([ScoutGameList[2][4], ScoutGameList[2][5],
                        ScoutGameList[2][6], ScoutGameList[2][7],
                        ScoutGameList[3][4], ScoutGameList[3][5],
                        ScoutGameList[3][6], ScoutGameList[3][7]])  # Blue Scouts

        GameLog.extend([SoldierGameList[0][4], SoldierGameList[0][5],
                        SoldierGameList[0][6], SoldierGameList[0][7],
                        SoldierGameList[1][4], SoldierGameList[1][5],
                        SoldierGameList[1][6], SoldierGameList[1][7]])  # Red Soldiers

        GameLog.extend([SoldierGameList[2][4], SoldierGameList[2][5],
                        SoldierGameList[2][6], SoldierGameList[2][7],
                        SoldierGameList[3][4], SoldierGameList[3][5],
                        SoldierGameList[3][6], SoldierGameList[3][7]])  # Blue Soldiers

        GameLog.extend([DemoGameList[0][4], DemoGameList[0][5],
                        DemoGameList[0][6], DemoGameList[0][7],
                        DemoGameList[1][4], DemoGameList[1][5],
                        DemoGameList[1][6], DemoGameList[1][7]])  # Red and Blue Demoman

        GameLog.extend([MedicGameList[0][4], MedicGameList[0][5],
                        MedicGameList[0][6], MedicGameList[0][7],
                        MedicGameList[0][8],
                        MedicGameList[1][4], MedicGameList[1][5],
                        # Red and Blue Medic
                        MedicGameList[1][6], MedicGameList[1][7],
                        MedicGameList[1][8]])

        GameLog.append(GameScoreDifference)
        gameStatsWriter.writerow(GameLog)
    GameLog = []

gameStatsFile.close()
