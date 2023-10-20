import json
import random
import numpy as np

inputFileName1 = "SYS_sample_data_senml.csv"
outputFileName1 = "/home/cc/storm/riot-bench/modules/tasks/src/main/resources/SYS_sample_data_senml.csv"
inputFileName2 = "TAXI_sample_data_senml.csv"
outputFileName2 = "/home/cc/storm/riot-bench/modules/tasks/src/main/resources/TAXI_sample_data_senml.csv"
data_file = "/home/cc/storm/riot-bench/modules/tasks/src/main/resources/priority_sys.txt"

# Number of data points
data_size = 1000

# Load or generate your priority array
priorities = np.concatenate([
    np.full(int(0.30 * data_size), 1),  # %occurrences of priority 1
    np.full(int(0.40 * data_size), 2),  # % occurrences of priority 2
    np.full(int(0.30 * data_size), 3)   # % occurrences of priority 3
])


with open(inputFileName1, 'r') as input_file1, open(inputFileName2, 'r') as input_file2, \
     open(outputFileName1, 'w') as output_file1, open(outputFileName2, 'w') as output_file2, \
     open(data_file, 'w') as output1:

    priority_index = 0  # Index to track the current priority

    for line1, line2 in zip(input_file1, input_file2):
        current_priority = int(priorities[priority_index])  # Convert to regular Python int
        
        length = len("1422748800000")
        firstPart1 = line1[:length + 1]
        secondPart1 = line1[length + 1:]

        firstPart2 = line2[:length + 1]
        secondPart2 = line2[length + 1:]

        secondPartJson1 = json.loads(secondPart1)
        secondPartJson2 = json.loads(secondPart2)

        # Add the current priority to the tuple
        secondPartJson1['e'].append({'v': current_priority, 'u': 'p', 'n': 'priority'})
        secondPartJson2['e'].append({'v': current_priority, 'u': 'p', 'n': 'priority'})

        # Write the current priority to the output1 file
        output1.write(str(current_priority) + "\n")

        # Modify and write the modified lines to the output files
        modifiedLine1 = firstPart1 + json.dumps(secondPartJson1, separators=(',', ':'))
        modifiedLine2 = firstPart2 + json.dumps(secondPartJson2, separators=(',', ':'))

        modifiedLine1 = modifiedLine1.replace("'", '"').replace(" ", "")
        modifiedLine1 = modifiedLine1.replace(f'"v":{current_priority}', f'"v":"{current_priority}"')
        modifiedLine2 = modifiedLine2.replace("'", '"').replace(" ", "")
        modifiedLine2 = modifiedLine2.replace(f'"v":{current_priority}', f'"v":"{current_priority}"')

        output_file1.write(modifiedLine1 + "\n")
        output_file2.write(modifiedLine2 + "\n")

        priority_index += 1  # Move to the next priority in the array
