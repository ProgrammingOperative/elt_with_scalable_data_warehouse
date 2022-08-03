import pandas as pd

class ExtractCSV:
    def __init__(self) -> None:
        pass


    def load_and_restructure():
        data = pd.read_csv("~/10 Academy/week 11/repository/traffic_data_etl/data/warehousedata.csv")

        cols = data.columns[0].split(";")

    # Strip the data of empty spaces
        for i in range(len(cols)):
            cols[i] = cols[i].strip()
        
    # Start restructuring by including records with only the columns stipulated
        track_ids = []
        types = []
        traveled_d = []
        avg_speeds = []
        lat = []
        lon = []
        speed = []
        lon_acc = []
        lat_acc = []
        time = []

        for r in range(len(data)): 
            row = data.iloc[r,:][0].split(";")
            row_p1 = row[:]    
            track_ids.append(row_p1[0])
            types.append(row_p1[1])
            traveled_d.append(row_p1[2])
            avg_speeds.append(row_p1[3])
            lat.append(row_p1[4])
            lon.append(row_p1[5])
            speed.append(row_p1[6])
            lon_acc.append(row_p1[7])
            lat_acc.append(row_p1[8])
            time.append(row_p1[9])

        #Create a dictionary first before creation of pandas dataframe
        data_dict = {cols[0]:track_ids, cols[1]:types, cols[2]:traveled_d, cols[3]:avg_speeds, cols[4]:lat, cols[5]:lon, cols[6]:speed, cols[7]:lon_acc, cols[8]:lat_acc, cols[9]:time}
        
        #Create dataframe
        extract_df = pd.DataFrame(data_dict)


        #Final output
        print("-----------------Successfully Extracted data---------------------")
        return extract_df

