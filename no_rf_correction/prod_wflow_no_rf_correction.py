# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
# Airflow varibales; can be specified in UI.
from airflow.models import Variable

from datetime import datetime, timedelta
from os import path, makedirs
import json

from algo_wrapper import Config
from flo2d_input_preparation.raincell.raincell import RaincellNcfIO, RaincellAlgo
from flo2d_input_preparation.inflow.inflow import InflowIO, InflowAlgo
from flo2d_input_preparation.outflow.outflow import OutflowIO, OutflowAlgo
from hec_hms_input_preparation.rain_csv.rain_csv import RainCsvIO, RainCsvAlgo

DATE_FORMAT = '%Y-%m-%d'

wrf_results_nfs = Variable.get("WRF_RESULTS_NFS")
interim_data_nfs = Variable.get("INTERIM_DATA_NFS")
airflow_home = Variable.get("AIRFLOW_HOME")

di_pipelines_dir = path.join(airflow_home, 'dags', 'DI_Pipelines')
resources_dir = path.join(di_pipelines_dir, 'resources')

todays_date_str = datetime.utcnow().strftime(DATE_FORMAT)
run_name = 'daily_no_correction'
run_dir_tree = path.join(interim_data_nfs, todays_date_str, run_name)


"""
Create RAINCELL.DAT
"""


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


def create_raincell(configs, **kwargs):
    raincell_config = Config(configs)
    raincell_io = RaincellNcfIO(raincell_config)
    raincell_algo = RaincellAlgo(raincell_io, raincell_config)

    schedule_date_str = kwargs['ds']
    schedule_date = datetime.strptime(schedule_date_str, DATE_FORMAT)

    base_dt = schedule_date
    start_dt = base_dt - timedelta(days=2)
    end_dt = base_dt + timedelta(days=3)

    nc_f_format = "wrf0_{0}_18:00_0000/wrf/wrfout_d03_{0}_18:00:00_rf"
    nc_f = nc_f_format.format(schedule_date.strftime(DATE_FORMAT))
    nc_f_prev_1 = nc_f_format.format((schedule_date - timedelta(days=1)).strftime(DATE_FORMAT))
    nc_f_prev_2 = nc_f_format.format((schedule_date - timedelta(days=2)).strftime(DATE_FORMAT))

    print(nc_f)
    print(nc_f_prev_1)
    print(nc_f_prev_2)

    raincell_algo.execute(
        ncfs={
            'nc_f': path.join(wrf_results_nfs, nc_f),
            'nc_f_prev_days': [
                path.join(wrf_results_nfs, nc_f_prev_1),
                path.join(wrf_results_nfs, nc_f_prev_2)
            ]
            },
        start_dt=start_dt,
        base_dt=base_dt,
        end_dt=end_dt,
    )


"""
Create INFLOW.DAT
"""


def prepare_inflow_config(run_dir, json_config_fp):
    # Prepare dir tree for the output.
    inflow_out_dir = path.join(run_dir, 'inflow')
    if not path.exists(inflow_out_dir):
        makedirs(inflow_out_dir)
    with open(json_config_fp) as f:
        configs = json.load(f)
        configs['output_config']['inflow_dat_fp'] = path.join(inflow_out_dir, 'INFLOW.DAT')
        configs['algo_config']['init_wl_config'] = path.join(resources_dir, 'flo2d', 'model-250m', 'INITWL.CONF')
        return configs


def create_inflow(configs, **kwargs):
    inflow_config = Config(configs)
    inflow_io = InflowIO(inflow_config)
    inflow_algo = InflowAlgo(inflow_io, inflow_config)

    schedule_date_str = kwargs['ds']
    schedule_date = datetime.strptime(schedule_date_str, DATE_FORMAT)

    base_dt = schedule_date
    start_dt = base_dt - timedelta(days=2)
    end_dt = base_dt + timedelta(days=3)

    inflow_algo.execute(
        start_dt=start_dt,
        base_dt=base_dt,
        end_dt=end_dt,
    )


"""
Create OUTFLOW.DAT
"""


def prepare_outflow_config(run_dir, json_config_fp):
    # Prepare dir tree for the output.
    outflow_out_dir = path.join(run_dir, 'outflow')
    if not path.exists(outflow_out_dir):
        makedirs(outflow_out_dir)
    with open(json_config_fp) as f:
        configs = json.load(f)
        configs['output_config']['outflow_dat_fp'] = path.join(outflow_out_dir, 'OUTFLOW.DAT')
        configs['algo_config']['init_tidal_config'] = path.join(resources_dir, 'flo2d', 'model-250m', 'INITTIDAL.CONF')
        return configs


def create_outflow(configs, **kwargs):
    outflow_config = Config(configs)
    outflow_io = OutflowIO(outflow_config)
    outflow_algo = OutflowAlgo(outflow_io, outflow_config)

    schedule_date_str = kwargs['ds']
    schedule_date = datetime.strptime(schedule_date_str, DATE_FORMAT)

    base_dt = schedule_date
    start_dt = base_dt - timedelta(days=2)
    end_dt = base_dt + timedelta(days=3)

    outflow_algo.execute(
        start_dt=start_dt,
        end_dt=end_dt
    )


"""
Create DailyRain.csv
"""


def prepare_dailyraincsv_config(run_dir, json_config_fp):
    # Prepare dir tree for the output.
    dailyraincsv_out_dir = path.join(run_dir, 'dailyraincsv')
    if not path.exists(dailyraincsv_out_dir):
        makedirs(dailyraincsv_out_dir)
    with open(json_config_fp) as f:
        configs = json.load(f)
        configs['output_config']['rain_csv_fp'] = path.join(dailyraincsv_out_dir, 'DailyRain.csv')
        return configs


def create_dailyraincsv(configs, **kwargs):
    dailyraincsv_config = Config(configs)
    dailyraincsv_io = RainCsvIO(dailyraincsv_config)
    dailyraincsv_algo = RainCsvAlgo(dailyraincsv_io, dailyraincsv_config)

    schedule_date_str = kwargs['ds']
    schedule_date = datetime.strptime(schedule_date_str, DATE_FORMAT)

    base_dt = schedule_date
    start_dt = base_dt - timedelta(days=2)
    end_dt = base_dt + timedelta(days=3)

    dailyraincsv_algo.execute(
        start_dt=start_dt,
        base_dt=base_dt,
        end_dt=end_dt
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

raincell_config_fp = path.join(di_pipelines_dir, 'no_rf_correction', 'prod_wflow_no_rf_correction_raincell.json')
raincell_configs = prepare_raincell_config(run_dir_tree, raincell_config_fp)
task_create_raincell = PythonOperator(
    task_id='create_raincell',
    dag=dag,
    python_callable=create_raincell,
    op_args=[raincell_configs]
)

inflow_config_fp = path.join(di_pipelines_dir, 'no_rf_correction', 'prod_wflow_no_rf_correction_inflow.json')
inflow_configs = prepare_inflow_config(run_dir_tree, inflow_config_fp)
task_create_inflow = PythonOperator(
    task_id='create_inflow',
    dag=dag,
    python_callable=create_inflow,
    op_args=[inflow_configs]
)

outflow_config_fp = path.join(di_pipelines_dir, 'no_rf_correction', 'prod_wflow_no_rf_correction_outflow.json')
outflow_configs = prepare_outflow_config(run_dir_tree, outflow_config_fp)
task_create_outflow = PythonOperator(
    task_id='create_outflow',
    dag=dag,
    python_callable=create_outflow,
    op_args=[outflow_configs]
)

dailyraincsv_config_fp = path.join(di_pipelines_dir, 'no_rf_correction', 'prod_wflow_no_rf_correction_dailyraincsv.json')
dailyraincsv_configs = prepare_dailyraincsv_config(run_dir_tree, dailyraincsv_config_fp)
task_create_dailyraincsv = PythonOperator(
    task_id='create_dailyraincsv',
    dag=dag,
    python_callable=create_dailyraincsv,
    op_args=[dailyraincsv_configs]
)


# Trigger HEC-HMS run
task_trigger_hec_hms = SimpleHttpOperator(
    task_id='init-hec-hms-run',
    dag=dag,
    http_conn_id='hec-hms-rest-service',
    method='GET',
    endpoint='',
    response_check=lambda response: True if 'Welcome' in response.content else False
)
