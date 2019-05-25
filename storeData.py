import json
import sqlite3

# SQLite DB Name
DB_Name = "iot_database.db"


# ===============================================================
# Database Manager Class

class DatabaseManager():
    def __init__(self):
        self.conn = sqlite3.connect(DB_Name)
        self.conn.execute('pragma foreign_keys = on')
        self.conn.commit()
        self.cur = self.conn.cursor()

    def add_del_update_db_record(self, sql_query, args=()):
        self.cur.execute(sql_query, args)
        self.conn.commit()
        return

    def __del__(self):
        self.cur.close()
        self.conn.close()


# ===============================================================
# Functions to push Sensor Data into Database

# Function to save Temperature to DB Table
def DHT22_Temp_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Temperature = json_Dict['Temperature']
    Temperature_Level = json_Dict['TemperatureLevel']

    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record(
        "insert into Temperature_Data (SensorID, Date_Time, Temperature, TemperatureLevel) values (?,?,?,?)",
        [SensorID, Data_and_Time, Temperature, Temperature_Level])
    del dbObj
    print("\nInserted Temperature Data into Database.\n")


def DHT22_Humidity_Data_Handler(jsonData):


    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Humidity = json_Dict['Humidity']
    Humidity_Level = json_Dict['HumidityLevel']
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into Humidity_Data (SensorID, Date_Time, Humidity, HumidityLevel) values (?,?,?,?)",
                                   [SensorID, Data_and_Time, Humidity,Humidity_Level])
    del dbObj
    print("\nInserted Humidity Data into Database.\n")


def DHT22_Acceleration_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    acc_X = json_Dict['accX']
    acc_Y = json_Dict['accY']
    acc_Z = json_Dict['accZ']

    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into Acceleration_Data (SensorID, Date_Time,accX, accY, accZ) values (?,?,?,?,?)",
                                   [SensorID, Data_and_Time, acc_X, acc_Y, acc_Z])
    del dbObj
    print("\nInserted Acceleration Data into Database.\n")

# ===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
    if Topic == "Home/BedRoom/Topics/Temperature":
        DHT22_Temp_Data_Handler(jsonData)
    elif Topic == "Home/BedRoom/Topics/Humidity":
        DHT22_Humidity_Data_Handler(jsonData)
    elif Topic == "Home/BedRoom/Topics/Acceleration":
        DHT22_Acceleration_Data_Handler(jsonData)

# ===============================================================