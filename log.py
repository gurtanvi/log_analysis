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




#df_logfile = create_log_df('/home/gurleenkaur/Programming/log_analysis/data/logs.log')
#print(df_logfile)

df_operation_name = get_operation_type('query')
print(df_operation_name)

