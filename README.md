# EDGAR_Streaming_Log_Processing
Streaming processing of log file

# Contents
1. [Preference of Datastructure](README.md#Preference-of-Datastructure)
2. [Preference of Algorithm](README.md#Preference-of-Algorithm)
3. [Required Libraries and Environment](README.md#Required-Libraries-and-Environment)
4. [Implementation Details](README.md#Implementation-Details)

# Preference of Datastructure

Data Structure: Due to duck typing nature of python variable data types are not specified.

For storing active sessions an associative array also known as a dictionary is used.

The dictionary structure at any instance of time will be:
```javascript
[{ip : {
        'start_time' : 'string with date and time signifying the first request time'
        'end_time' : 'string with date and time signifying the last request time'
        'count'    : 'integer'
        'start_time_stp' : 'datetime object signifying the last request time'
    }
}, {}] 
```


A dictionary makes the retrieval time very fast approximately O(1).
All the ips are appened in order of their start time.

Another more efficient way would have been to make an index on start_time_sp. 
This reduces the number of comparisons extensively to a value of 2 versus the worst case comparison of n
each time there is a change in time encountered. The distadvantage of this was that it required a sort by 
start time each time the output needs to be written to file which would wash off its gain in time complexity.
The code for this can be found in sessionization_optimized.py.

# Preference of Algorithm

1. processes each line in the document first extracts the required fields

2. checks if current_timestamp is not equal to time in current log
if not equal calls the write function

4. write function checks if the inactivity period is greater than mentioned
and writes the ip to output file

3. checks if the ip address is already present in active sessions
updates count and start_time_stp

4. creates a new ip key if ip not in active session dictionary and updates all its nested keys as required.

5. writes all the ip left after processing the last line to the file

# Required Libraries and Environment

For the purpose of simplicity only python three libraries have been used
1. sys
2. csv 
3. datetime

The environment reuired for the script to run is Python 3.6+

# Implementation Details
The code has been implemented in a modular manner in a simple file using functional programming paradigms 
There are two implementations:
1. sessionization.py
There are 6 functions
* `inactivity_period` : reads the input file and derives the inactivity period
* `parse_time` : converts a string to datetime object
* `time_difference` : calculates difference between two time periods
* `process_line` : code runs for all lines in the log file parsing the input and then processing it according to the algorithm
* `write_output` : writes the output to a text file
* `main function` : maintains the flow of execution

It can be run from root folder using ./run.sh in bash terminal

2. sessionization_optimized.py
This is only for reference as it does not print the output in the sequence as expected but takes less number of comparisons than the 1st code.

It can be run from root folder using ./run_optimized.sh in bash terminal

The code handles exceptions at each stage and is robust to errors.
