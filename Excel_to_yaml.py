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

# ---------------------------------------------------------
# GLOBAL DEFAULTS - Hardcoded parameters for all DAGs
# ---------------------------------------------------------
DEFAULT_ARGS_BASE = {
    'owner': 'airflow',
    'start_date': '2026-01-01'
}
DEFAULT_CATCHUP = False

# Valid Airflow Schedule Presets
VALID_PRESETS = {'once', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'annually'}
VALID_SCHEDULE_TYPES = {'manual', 'preset', 'cron'}

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
    
    if dags_df['DAG_ID'].isnull().any():
        errors.append("Found missing DAG_ID(s) in Dad_config.")
    if tasks_df['Task_ID'].isnull().any():
        errors.append("Found missing Task_ID(s) in Task_details.")
        
    name_pattern = re.compile(r'^[a-zA-Z0-9_]+$')
    
    invalid_dags = [dag for dag in dags_df['DAG_ID'].dropna() if not name_pattern.match(str(dag))]
    if invalid_dags:
        errors.append(f"Invalid DAG_IDs (no spaces/special chars allowed): {invalid_dags}")
        
    invalid_tasks = [task for task in tasks_df['Task_ID'].dropna() if not name_pattern.match(str(task))]
    if invalid_tasks:
        errors.append(f"Invalid Task_IDs (no spaces/special chars allowed): {invalid_tasks}")

    # --- Schedule Mismatch Validation ---
    for _, row in dags_df.iterrows():
        dag_id = row.get('DAG_ID')
        sched_type = str(row.get('Schedule_type')).strip().lower()
        sched_raw = str(row.get('schedule')).strip().lower()
        
        if sched_type not in VALID_SCHEDULE_TYPES:
            errors.append(f"Invalid Schedule_type '{row.get('Schedule_type')}' in DAG '{dag_id}'. Must be Manual, Preset, or Cron.")
            continue
            
        if sched_type == 'manual':
            if sched_raw not in ['nan', 'none', 'null', '']:
                errors.append(f"DAG '{dag_id}' is 'Manual' but has schedule '{row.get('schedule')}'. Should be blank, none, or null.")
                
        elif sched_type == 'preset':
            if sched_raw not in VALID_PRESETS:
                errors.append(f"Invalid preset '{row.get('schedule')}' for 'Preset' type in DAG '{dag_id}'. Valid options are: {', '.join(VALID_PRESETS)}")
                
        elif sched_type == 'cron':
            # Basic cron validation: must have exactly 5 space-separated fields
            original_sched_str = str(row.get('schedule')).strip()
            if sched_raw == 'nan' or len(original_sched_str.split()) != 5:
                errors.append(f"Invalid cron expression '{row.get('schedule')}' for 'Cron' type in DAG '{dag_id}'. Must contain exactly 5 space-separated fields.")

    valid_dag_refs = set(dags_df['dad_ref'].dropna())
    task_dag_refs = set(tasks_df['dag_ref'].dropna())
    orphan_refs = task_dag_refs - valid_dag_refs
    if orphan_refs:
        errors.append(f"Task_details contains dag_refs not found in Dad_config: {orphan_refs}")

    for _, row in deps_df.dropna(subset=['Upstream_task']).iterrows():
        dag_ref = row['dag_ref']
        upstream = row['Upstream_task']
        
        valid_tasks_for_dag = tasks_df[tasks_df['dag_ref'] == dag_ref]['Task_ID'].tolist()
        
        if upstream not in valid_tasks_for_dag:
            errors.append(f"Upstream task '{upstream}' in dag_ref '{dag_ref}' does not exist in Task_details.")

    return errors

class CustomDumper(yaml.SafeDumper):
    def write_line_break(self, data=None):
        super().write_line_break(data)

# Process ALL Excel files in the input directory, ignoring open/hidden files starting with ~$
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
        dags_df = pd.read_excel(file_path, sheet_name='Dad_config')
        tasks_df = pd.read_excel(file_path, sheet_name='Task_details')
        deps_df = pd.read_excel(file_path, sheet_name='Dependency_flow')
    except ValueError as e:
        print(f" [ERROR] Missing required sheet in {filename}. Skipping file. Details: {e}")
        continue
        
    validation_errors = validate_data(dags_df, tasks_df, deps_df, filename)
    
    if validation_errors:
        print(f" [FAILED VALIDATION] Skipping {filename} due to errors:")
        for err in validation_errors:
            print(f"   - {err}")
        continue
        
    print(" [Validation Passed] Generating YAMLs...")
    
    for _, dag_row in dags_df.iterrows():
        dag_id = dag_row['DAG_ID']
        dag_ref = dag_row['dad_ref']
        
        # 1. Determine final Schedule formatting based on Schedule_type
        sched_type = str(dag_row.get('Schedule_type')).strip().lower()
        sched_raw = str(dag_row.get('schedule')).strip()
        
        schedule = None # Default for 'manual'
        if sched_type == 'preset':
            schedule = f"@{sched_raw.lower()}"
        elif sched_type == 'cron':
            schedule = sched_raw # Keep original casing and spacing for cron expressions
            
        # 2. Merge default_args: Default constants first, then override with user input
        user_default_args = parse_key_value_string(dag_row['Default_arg'])
        final_default_args = {**DEFAULT_ARGS_BASE, **user_default_args}
        
        # 3. Determine catchup: Check user input first, fallback to default constant
        user_dag_params = parse_key_value_string(dag_row['Dag_parameters'])
        if 'catchup' in user_dag_params:
            final_catchup = str(user_dag_params['catchup']).lower() == 'true'
        else:
            raw_params = str(dag_row['Dag_parameters']).replace(" ", "").lower()
            if 'catchup:true' in raw_params:
                final_catchup = True
            elif 'catchup:false' in raw_params:
                final_catchup = False
            else:
                final_catchup = DEFAULT_CATCHUP
            
        dag_dict = {
            'default_args': final_default_args,
            'schedule': schedule,
            'Description': dag_row['description'],
            'catchup': final_catchup,
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
            
        output_file_path = os.path.join(OUTPUT_DIR, f"{dag_id}.yaml")
        with open(output_file_path, 'w') as file:
            yaml.dump({dag_id: dag_dict}, file, default_flow_style=False, sort_keys=False, Dumper=CustomDumper)
            
        print(f"  -> Created: {dag_id}.yaml")

print("\nBatch processing complete!")