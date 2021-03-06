from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
# Airflow custom plugin. Impl should be available in $AIRFLOW_HOME/plugins folder.
from airflow.hooks.file_upload_http_plugin import FileUploadHttpHook
# Airflow varibales; can be specified in UI.
from airflow.models import Variable

import json
from datetime import datetime, timedelta
from os import path, makedirs


from algo_wrapper import Config
from flo2d_input_preparation.raincell.raincell import RaincellNcfIO, RaincellAlgo
from flo2d_input_preparation.inflow.inflow import InflowIO, InflowAlgo
from flo2d_input_preparation.outflow.outflow import OutflowIO, OutflowAlgo
from hec_hms_input_preparation.rain_csv.rain_csv import RainCsvIO, RainCsvAlgo

DATE_FORMAT = '%Y-%m-%d'
DATE_TIME_FORMAT = '%Y-%m-%d_00:00:00'

wrf_results_nfs = Variable.get("WRF_RESULTS_NFS")
interim_data_nfs = Variable.get("INTERIM_DATA_NFS")
airflow_home = Variable.get("AIRFLOW_HOME")

di_pipelines_dir = path.join(airflow_home, 'dags', 'DI_Pipelines')
resources_dir = path.join(di_pipelines_dir, 'resources')

todays_date_str = datetime.utcnow().strftime(DATE_FORMAT)
run_name = 'daily_no_correction'
run_dir_tree = path.join(interim_data_nfs, todays_date_str, run_name)

nc_f_format = "wrf0_{0}_18:00_0000/wrf/wrfout_d03_{0}_18:00:00_rf"


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


"""
Init Hec-HMS Run
"""


def init_hec_hms_run(name, rain_csv_fp, init_state_fp, **kwargs):
    schedule_date_str = kwargs['ds']
    schedule_date = datetime.strptime(schedule_date_str, DATE_FORMAT)

    run_dt = schedule_date
    start_dt = run_dt - timedelta(days=2)
    end_dt = run_dt + timedelta(days=3)
    save_state_dt = start_dt + timedelta(days=1)
    data = {
        'run-name': name,
        'run-dt': run_dt.strftime(DATE_TIME_FORMAT),
        'start-dt': start_dt.strftime(DATE_TIME_FORMAT),
        'end-dt': end_dt.strftime(DATE_TIME_FORMAT),
        'save-state-dt': save_state_dt.strftime(DATE_TIME_FORMAT)
    }

    end_point = 'hec_hms/single/init-run'
    http = FileUploadHttpHook(method='POST', http_conn_id='hec-hms-rest-service')

    with open(rain_csv_fp, 'rb') as rain_csv, open(init_state_fp, 'rb') as init_state:
        files = {"rainfall": rain_csv, "init-state": init_state}
        response = http.run(end_point, params=data, files=files, extra_options={'check_response': True})
        resp_dict = response.json()
        run_id = resp_dict['run_id']
        print(resp_dict)
        return run_id


"""
Start Hec-HMS Run
"""


def start_hec_hms_run(**kwargs):
    ti = kwargs['ti']
    # get run_id
    run_id = ti.xcom_pull(task_ids='init_hec_hms_run')
    print(run_id)


default_args = {
    'owner': 'thilinamad',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
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

task_init_hec_hms_run = PythonOperator(
    task_id='init_hec_hms_run',
    dag=dag,
    python_callable=init_hec_hms_run,
    op_args=[run_name, dailyraincsv_configs['output_config']['rain_csv_fp'],
             dailyraincsv_configs['output_config']['rain_csv_fp']]
)

task_start_hec_hms_run = PythonOperator(
    task_id='start_hec_hms_run',
    dag=dag,
    python_callable=start_hec_hms_run
)

task_wait_till_wrf_output = FileSensor(
    task_id='wait_till_wrf_output',
    dag=dag,
    fs_conn_id='WRF_RESULTS_NFS',
    filepath='{{ "wrf0_{0}_18:00_0000/wrf/wrfout_d03_{0}_18:00:00_rf".format(execution_date.strftime("%Y-%m-%d")) }}'
)

task_wait_till_wrf_output >> task_create_dailyraincsv >> task_init_hec_hms_run >> task_start_hec_hms_run
task_start_hec_hms_run >> [task_create_raincell, task_create_inflow, task_create_outflow]
