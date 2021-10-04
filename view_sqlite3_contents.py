#! /Users/tfuku/Tools/miniconda3/bin/python3.7
#! /home/kioxpace/tools/miniconda3/envs/kioxpace/bin/python3

import sys
import os
import json
import sqlite3
import pandas as pd
from os.path import expanduser
import argparse



#     _________________________________
#____/ [*] Check Platform              \_________________________
#
import platform
cloud_type = str()
if "kxtweetana" in platform.uname():
    cloud_type = "gcp"
elif "Darwin" in platform.system():
    cloud_type = "mac"
else:
    cloud_type = "colab"



#     _________________________________
#____/ [*] Aarg parse                  \_________________________
#
parser  =   argparse.ArgumentParser(description="Example ::")
parser.add_argument('-s', '--sqlite3', type=str, help='Specify sqlite3 file path.', required=True)
args    =   parser.parse_args()




#     _________________________________
#____/ [*] Main                        \_________________________
#


conn =   sqlite3.connect(args.sqlite3)
tweet_df = pd.read_sql('SELECT * FROM pickup_tweets',conn)
conn.close()

print(tweet_df[["text","date"]])
