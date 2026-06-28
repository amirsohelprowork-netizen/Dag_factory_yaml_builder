import pandas as pd
import yaml
import os
import glob
import re

# Define the directories
INPUT_DIR = 'input_files'
OUTPUT_DIR = 'output_yamls'

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

def validate_data(dags_df, tasks_df, deps_df, filename):
    """Runs data quality checks before processing."""
    errors = []
    
    # 1. Check for missing primary keys
    if dags_df['DAG_ID'].isnull().any():
        errors.append("Found missing DAG_ID(s) in Dad_config.")
    if tasks_df['Task_ID'].isnull().any():
        errors.append("Found missing Task_ID(s) in Task_details.")
        
    # 2. Validate Airflow Naming Conventions (alphanumeric and underscores only)
    name_pattern = re.compile(r'^[a-zA-Z0-9_]+$')
    
    invalid_dags = [dag for dag in dags_df['DAG_ID'].dropna() if not name_pattern.match(str(dag))]
    if invalid_dags:
        errors.append(f"Invalid DAG_IDs (no spaces/special chars allowed): {invalid_dags}")
        
    invalid_tasks = [task for task in tasks_df['Task_ID'].dropna() if not name_pattern.match(str(task))]
    if invalid_tasks:
        errors.append(f"Invalid Task_IDs (no spaces/special chars allowed): {invalid_tasks}")

    # 3. Referential Integrity: Do tasks belong to valid dag_refs?
    valid_dag_refs = set(dags_df['dad_ref'].dropna())
    task_dag_refs = set(tasks_df['dag_ref'].dropna())
    orphan_refs = task_dag_refs - valid_dag_refs
    if orphan_refs:
        errors.append(f"Task_details contains dag_refs not found in Dad_config: {orphan_refs}")

    # 4. Dependency Validation: Do upstream tasks actually exist?
    for _, row in deps_df.dropna(subset=['Upstream_task']).iterrows():
        dag_ref = row['dag_ref']
        upstream = row['Upstream_task']
        
        # Get all valid task IDs for this specific dag_ref
        valid_tasks_for_dag = tasks_df[tasks_df['dag_ref'] == dag_ref]['Task_ID'].tolist()
        
        if upstream not in valid_tasks_for_dag:
            errors.append(f"Upstream task '{upstream}' in dag_ref '{dag_ref}' does not exist in Task_details.")

    return errors

class CustomDumper(yaml.SafeDumper):
    def write_line_break(self, data=None):
        super().write_line_break(data)

# Process ALL Excel files in the input directory, but IGNORE open/hidden files starting with ~$
all_files = glob.glob(os.path.join(INPUT_DIR, '*.xlsx'))
excel_files = [f for f in all_files if not os.path.basename(f).startswith('~$')]

if not excel_files:
    print(f"No valid Excel files found in '{INPUT_DIR}' directory.")
else:
    print(f"Found {len(excel_files)} Excel file(s). Starting batch processing...")

for file_path in excel_files:
    filename = os.path.basename(file_path)
    print(f"\n--- Processing {filename} ---")
    
    try:
        # Load sheets
        dags_df = pd.read_excel(file_path, sheet_name='Dad_config')
        tasks_df = pd.read_excel(file_path, sheet_name='Task_details')
        deps_df = pd.read_excel(file_path, sheet_name='Dependency_flow')
    except ValueError as e:
        print(f" [ERROR] Missing required sheet in {filename}. Skipping file. Details: {e}")
        continue
        
    # Run Validations
    validation_errors = validate_data(dags_df, tasks_df, deps_df, filename)
    
    if validation_errors:
        print(f" [FAILED VALIDATION] Skipping {filename} due to errors:")
        for err in validation_errors:
            print(f"   - {err}")
        continue # Skip to the next Excel file
        
    print(" [Validation Passed] Generating YAMLs...")
    
    # Process valid data
    for _, dag_row in dags_df.iterrows():
        dag_id = dag_row['DAG_ID']
        dag_ref = dag_row['dad_ref']
        
        schedule = None
        if dag_row['Schedule_type'] == 'Preset' and not pd.isna(dag_row['schedule']):
            schedule = f"@{dag_row['schedule']}"
            
        dag_dict = {
            'default_args': parse_key_value_string(dag_row['Default_arg']),
            'schedule': schedule,
            'Description': dag_row['description'],
            'catchup': str(dag_row['Dag_parameters']).lower() == 'catchup:true',
            'tasks': {}
        }
        
        dag_tasks = tasks_df[tasks_df['dag_ref'] == dag_ref]
        for _, task_row in dag_tasks.iterrows():
            task_id = task_row['Task_ID']
            task_type = str(task_row['Task_type']).lower()
            
            task_dict = {
                'operator': OPERATOR_MAP.get(task_type, 'airflow.operators.empty.EmptyOperator')
            }
            
            if task_type == 'bash' and not pd.isna(task_row['Task_specs']):
                task_dict['bash_command'] = task_row['Task_specs']
                
            task_params = parse_key_value_string(task_row['Task_parameters'])
            for k, v in task_params.items():
                task_dict[k] = int(v) if v.isdigit() else v
                
            task_deps = deps_df[(deps_df['dag_ref'] == dag_ref) & (deps_df['Task_id'] == task_id)]
            upstreams = task_deps['Upstream_task'].dropna().tolist()
            
            if upstreams:
                valid_upstreams = [u for u in upstreams if str(u) != 'nan' and u != task_id]
                if valid_upstreams:
                    task_dict['dependencies'] = valid_upstreams
                    
            dag_dict['tasks'][task_id] = task_dict
            
        # Export this specific DAG
        output_file_path = os.path.join(OUTPUT_DIR, f"{dag_id}.yaml")
        with open(output_file_path, 'w') as file:
            yaml.dump({dag_id: dag_dict}, file, default_flow_style=False, sort_keys=False, Dumper=CustomDumper)
            
        print(f"  -> Created: {dag_id}.yaml")

print("\nBatch processing complete!")