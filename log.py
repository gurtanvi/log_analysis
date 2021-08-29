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



print(df_logfile['name'].nunique())
# how many queries, mutations and subscriptions have been performed
print(df_logfile.type.value_counts().query)
print(df_logfile.type.value_counts().mutation)
print(df_logfile.type.value_counts().subscription)

#counts for different operations
print(df_logfile['name'].value_counts())

#average duration type grouped by operation type
group_operationtype1 = df_logfile.groupby("type")
mean_operationtype = group_operationtype1.mean()	
mean_operationtype = mean_operationtype.reset_index()
print(mean_operationtype) 	


#average duration type grouped by operation
group_operationname = df_logfile.groupby("name")
mean_operationname = group_operationname.mean()
mean_operationname = mean_operationname.reset_index()
print(mean_operationname)

#max and min duration grouped by operation
operationname_mean = df_logfile.groupby('name').agg({'duration': ['max','min']})
print(operationname_mean)

#max and min operation grouped by operation type
operationtype_mean = df_logfile.groupby('type').agg({'duration': ['max','min']})
print(operationtype_mean)