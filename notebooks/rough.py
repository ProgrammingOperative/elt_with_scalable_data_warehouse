import sys
import os

# sys.path.append('/home/wacira/10 Academy/week 11/repository/traffic_data_etl/scripts')
sys.path.insert(0, '/home/wacira/10 Academy/week 11/repository/traffic_data_etl/scripts')


try:
    import load_data
    print("----Success---")
except Exception as e:
    print(e)