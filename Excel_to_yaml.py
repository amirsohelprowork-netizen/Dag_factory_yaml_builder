import pandas as pd
import yaml
import os

# Define the directories
INPUT_DIR = 'input_files'
OUTPUT_DIR = 'output_yamls'
EXCEL_FILE = 'Untitled spreadsheet.xlsx'

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

file_path = os.path.join(INPUT_DIR, EXCEL_FILE)

# 1. Load the data from the specific sheets in the single Excel file
dags_df = pd.read_excel(file_path, sheet_name='Dad_config')
tasks_df = pd.read_excel(file_path, sheet_name='Task_details')
deps_df = pd.read_excel(file_path, sheet_name='Dependency_flow')

# Operator mapping dictionary
OPERATOR_MAP = {
    'empty': 'airflow.operators.empty.EmptyOperator',
    'bash': 'airflow.operators.bash.BashOperator'
}

def parse_key_value_string(kv_string):
    """Converts 'key: value, key2: value2' into a dictionary."""
    if pd.isna(kv_string) or not kv_string:
        return {}
    parsed_dict = {}
    for item in str(kv_string).split(','):
        if ':' in item:
            k, v = item.split(':', 1)
            parsed_dict[k.strip()] = v.strip().replace("'", "")
    return parsed_dict

# 2. Initialize the main DAG dictionary
dag_factory_output = {}

# 3. Iterate through DAG configurations
for _, dag_row in dags_df.iterrows():
    dag_id = dag_row['DAG_ID']
    dag_ref = dag_row['dad_ref']
    
    # Handle Schedule logic
    schedule = None
    if dag_row['Schedule_type'] == 'Preset' and not pd.isna(dag_row['schedule']):
        schedule = f"@{dag_row['schedule']}"
        
    # Build DAG metadata
    dag_dict = {
        'default_args': parse_key_value_string(dag_row['Default_arg']),
        'schedule': schedule,
        'Description': dag_row['description'],
        'catchup': str(dag_row['Dag_parameters']).lower() == 'catchup:true',
        'tasks': {}
    }
    
    # 4. Fetch and attach tasks for this DAG
    dag_tasks = tasks_df[tasks_df['dag_ref'] == dag_ref]
    
    for _, task_row in dag_tasks.iterrows():
        task_id = task_row['Task_ID']
        task_type = str(task_row['Task_type']).lower()
        
        task_dict = {
            'operator': OPERATOR_MAP.get(task_type, 'airflow.operators.empty.EmptyOperator')
        }
        
        # Add operator-specific arguments (like bash_command)
        if task_type == 'bash' and not pd.isna(task_row['Task_specs']):
            task_dict['bash_command'] = task_row['Task_specs']
            
        # Add task parameters (like retries)
        task_params = parse_key_value_string(task_row['Task_parameters'])
        for k, v in task_params.items():
            task_dict[k] = int(v) if v.isdigit() else v
            
        # 5. Fetch and attach dependencies
        task_deps = deps_df[(deps_df['dag_ref'] == dag_ref) & (deps_df['Task_id'] == task_id)]
        upstreams = task_deps['Upstream_task'].dropna().tolist()
        
        if upstreams:
            # Filter out empty or self-referencing upstreams
            valid_upstreams = [u for u in upstreams if str(u) != 'nan' and u != task_id]
            if valid_upstreams:
                task_dict['dependencies'] = valid_upstreams
                
        dag_dict['tasks'][task_id] = task_dict
        
    dag_factory_output[dag_id] = dag_dict

# 6. Export to separate YAML files
class CustomDumper(yaml.SafeDumper):
    def write_line_break(self, data=None):
        super().write_line_break(data)

print(f"Generating YAML files in '{OUTPUT_DIR}' directory...")

for dag_id, dag_config in dag_factory_output.items():
    # Wrap the config back in the dag_id key for dag-factory format
    single_dag_output = {dag_id: dag_config}
    
    # Create the file name based on the DAG_ID
    output_file_path = os.path.join(OUTPUT_DIR, f"{dag_id}.yaml")
    
    with open(output_file_path, 'w') as file:
        yaml.dump(single_dag_output, file, default_flow_style=False, sort_keys=False, Dumper=CustomDumper)
        
    print(f" - Created: {output_file_path}")

print("All DAGs processed successfully!")