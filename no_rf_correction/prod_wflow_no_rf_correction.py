# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
# Airflow varibales; can be specified in UI.
from airflow.models import Variable

from datetime import datetime, timedelta
from os import path, makedirs
import json

from algo_wrapper import Config
from flo2d_input_preparation.raincell.raincell import RaincellNcfIO, RaincellAlgo

wrf_results_nfs = Variable.get("WRF_RESULTS_NFS")
interim_data_nfs = Variable.get("INTERIM_DATA_NFS")
airflow_home = Variable.get("AIRFLOW_HOME")

di_pipelines_dir = path.join(airflow_home, 'dags', 'DI_Pipelines')
resources_dir = path.join(di_pipelines_dir, 'resources')
raincell_config_fp = path.join(di_pipelines_dir, 'no_rf_correction', 'prod_wflow_no_rf_correction_raincell.json')

todays_date_str = datetime.utcnow().strftime('%Y-%m-%d')
run_name = 'daily_no_correction'

run_dir_tree = path.join(interim_data_nfs, todays_date_str, run_name)


def prepare_raincell_config(run_dir, json_config_fp):
    # Prepare dir tree for the output.
    raincell_out_dir = path.join(run_dir, 'raincell')
    if not path.exists(raincell_out_dir):
        makedirs(raincell_out_dir)
    with open(json_config_fp) as f:
        configs = json.load(f)
        configs['output_config']['outflow_dat_fp'] = path.join(raincell_out_dir, 'RAINCELL.DAT')
        configs['input_config']['kelani_basin_cell_lon_lat_map_file'] = \
            path.join(resources_dir, 'flo2d', 'model-250m', 'cell-map', 'kelani_basin_points_250m.txt')
        return configs


raincell_configs = prepare_raincell_config(run_dir_tree, raincell_config_fp)
base_dt = datetime.strptime(todays_date_str, '%Y-%m-%d')
start_dt = base_dt - timedelta(days=2)
end_dt = base_dt + timedelta(days=3)


def create_raincell(configs, start_dt, base_dt, end_dt, **kwargs):
    raincell_config = Config(configs)
    raincell_io = RaincellNcfIO(raincell_config)
    outflow_algo = RaincellAlgo(raincell_io, raincell_config)

    schedule_dt = kwargs['ds']
    print(schedule_dt)

    outflow_algo.execute(
        ncfs={
            'nc_f': path.join(wrf_results_nfs, "wrf0_2018-12-11_18:00_0000/wrf/wrfout_d03_2018-12-11_18:00:00_rf"),
            'nc_f_prev_days': [
                path.join(wrf_results_nfs, "wrf0_2018-12-10_18:00_0000/wrf/wrfout_d03_2018-12-10_18:00:00_rf"),
                path.join(wrf_results_nfs, "wrf0_2018-12-09_18:00_0000/wrf/wrfout_d03_2018-12-09_18:00:00_rf")
            ]
            },
        start_dt=start_dt,
        base_dt=base_dt,
        end_dt=end_dt,
    )


default_args = {
    'owner': 'thilinamad',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 11),
    'email': ['madumalt@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

dag = DAG('prod_wflow_no_rf_correction', default_args=default_args, schedule_interval=timedelta(days=1))

task_create_raincell = PythonOperator(
    task_id='create_raincell',
    dag=dag,
    python_callable=create_raincell,
    op_args=[raincell_configs, start_dt, base_dt, end_dt]
)
