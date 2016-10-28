#!/usr/bin/env python3

import sys
import os
import csv
import math

reportrows = []
with open("cfed-report.csv") as f:
	reader = csv.reader(f)
	reportrows = list(reader)
		
#calculate average, standard deviation and standard error
summary = []
summary_row0 = ["Query", "Average", "SD", "SE"]
summary.append(summary_row0)

for row in reportrows[1:]:
	n = len(row[2:])
	sum = 0
	for val in row[2:]:
		sum += int(val)
	avr = sum / n
	
	sum = 0
	for val in row[2:]:
		sum += (avr - int(val))**2
	sn = math.sqrt(sum / n)
	se = sn / math.sqrt(n)
	newrow = [row[0], round(avr), round(sn), round(se)]
	summary.append(newrow)

with open("cfed-summary.csv", 'w', newline='') as csvfile:
	reportwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
	reportwriter.writerows(summary)
