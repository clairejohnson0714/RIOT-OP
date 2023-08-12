#!/bin/python3.7
import json

inputFileName = "TAXI_sample_data_senml.csv"
outputFileName = "Modify_data_taxi.csv"

with open(inputFileName, 'r') as input_file, open(outputFileName, 'w') as output_file:
    v_values = [1, 2, 3]
    v_index = 0
    
    for line in input_file:
        length = len("1422748800000")
        firstPart = line[:length+1]
        secondPart = line[length+1:]
        
        secondPartJson = json.loads(secondPart)
        secondPartJson['e'].append({'v': v_values[v_index], 'u': 'p', 'n':'priority'})
        
        modifiedLine = str(firstPart) + str(secondPartJson)
        modifiedLine = modifiedLine.replace("'", '"').replace(" ", "")
        modifiedLine = modifiedLine.replace(f'"v":{v_values[v_index]}', f'"v":"{v_values[v_index]}"')
        
        output_file.write(modifiedLine + "\n")
        
        v_index = (v_index + 1) % len(v_values)
