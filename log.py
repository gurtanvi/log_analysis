from os import name
import pandas as pd
import re



def create_log_df(filename):
    df = pd.DataFrame(data={
        'name': [],
        'type': [],
        'duration': [],
    })
    with open(filename, 'r') as file:
        while True:
            line: str = file.readline()
            #print(line)
            if not line:
                break
            operation = parse_logline(line)
            df = df.append(operation, ignore_index=True)
    return df

def get_operation_name(line):
    name_extract = re.search(
        r'(\| operation: ([a-zA-Z]* )\|)',
        line,
    )
    if name_extract:
        return name_extract.group(2).strip()
    return None

def get_operation_type(line):
    type_extract = re.search(
        r'(\| operationType: (query|subscription|mutation){1})',
        line,
    )
    if type_extract:
        return type_extract.group(2).strip()
    return None

def get_operation_duration(line):
    type_extract = re.search(
        r'(\| duration: ([0-9]*.[0-9]*))',
        line,
    )
    if type_extract:
        return float(type_extract.group(2).strip())
    return None

def is_valid_logline(line):
    extract = re.search(
        r'^(\[graphql\])',
        line,
    )
    if extract:
        return True
    return False

def parse_logline(line):
    if not is_valid_logline(line):
        return None
    operation = {
        'name': get_operation_name(line),
        'type': get_operation_type(line),
        'duration': get_operation_duration(line),
    }
    return operation




df_logfile = create_log_df('/home/gurleenkaur/Programming/log_analysis/data/logs.log')
print(df_logfile)


log_file_value_counts = df_logfile.type.value_counts()
print(df_logfile['name'].nunique())
# how many queries, mutations and subscriptions have been performed
print("query count:", log_file_value_counts.query)
print("mutation count:", log_file_value_counts.mutation)
print("subscription count:", log_file_value_counts.subscription)

#counts for different operations
print("operation count:", df_logfile['name'].value_counts())

#average duration type grouped by operation type
group_operation_type = df_logfile.groupby("type")
mean_operation_type = group_operation_type.mean()	
mean_operation_type = mean_operation_type.reset_index()
print(mean_operation_type) 	


#average duration type grouped by operation
group_operation_name = df_logfile.groupby("name")
mean_operation_name = group_operation_name.mean()
mean_operation_name = mean_operation_name.reset_index()
print(mean_operation_name)

#max and min duration grouped by operation
operation_max_min = df_logfile.groupby('name').agg({'duration': ['max','min']})
print(operation_max_min)

#max and min operation grouped by operation type
operation_type_max_min = df_logfile.groupby('type').agg({'duration': ['max','min']})
print(operation_type_max_min)