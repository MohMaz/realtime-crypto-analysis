#This code read a CSV file and generate a streaming of the input file
# to use for the spark streaming

import random
import os
import time
import uuid
import csv


import csv

cwd = os.getcwd()
f = open(cwd +'/data/history.csv')
csv_f = csv.reader(f)

lines=[]
for row in csv_f:
  lines.append(row)


outdir = cwd +'/data/tmp'

os.makedirs(outdir, exist_ok=True)
print("Starting file stream...")
while True:
    subset = random.sample(lines, random.randint(1,2))
    print(subset)
    uu = str(uuid.uuid4())
    filename = os.path.join(outdir, uu+'.csv')
  
    with open(filename, "w") as f:
        writer = csv.writer(f)
        writer.writerows(subset)
    time.sleep(random.random()*3)