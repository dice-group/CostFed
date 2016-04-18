#!/usr/bin/env python3

import sys
import os
import csv
import math

fedxsum = []
with open("fedx-summary.csv") as f:
	reader = csv.reader(f)
	fedxsum = list(reader)
	
semagrowsum = []
with open("semagrow-summary.csv") as f:
	reader = csv.reader(f)
	semagrowsum = list(reader)
	
quetsalsum = []
with open("quetsal-summary.csv") as f:
	reader = csv.reader(f)
	quetsalsum = list(reader)
	
#calculate average, standard deviation and standard error
summary = []
summary_row0 = ["Query", "FedX", "Semagrow", "Quetsal+"]
summary.append(summary_row0)

for row in quetsalsum[1:]:
	
	#find val for fedx
	fedxval = "-"
	for fedxrow in fedxsum[1:]:
		if fedxrow[0] == row[0]:
			fedxval = fedxrow[1]
		
	semagrowval = "-"
	for semrow in semagrowsum[1:]:
		if semrow[0] == row[0]:
			semagrowval = semrow[1]
			
	newrow = [row[0], fedxval, semagrowval, row[1]]
	summary.append(newrow)

with open("cmp-summary.csv", 'w', newline='') as csvfile:
	reportwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
	reportwriter.writerows(summary)
