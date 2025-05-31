from socket import timeout
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import time
import os

def task_failure_alert(context):
    ti = context['task_instance']
    exc = context['exception']
    job_group_name = str(ti.dag_id)
    job_name = str(ti.task_id)
    job_status = str(ti.state)
    exception_html = str(exc)
    log_url = str(ti.log_url)
    hostname = str(ti.hostname)
    os.system(f'/opt/anaconda3/envs/airflow/bin/python "/home/airflow/script/email/so2w_dragon.py" "{job_group_name}" "{job_name}" "{job_status}" "{exception_html}" "{log_url}" "{hostname}"')

## Airflow Var ##

tent = Variable.get('hostname_server')

'''
Main configuration for so2w Pipeline
'''
args = {
    'owner': 'so2w',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
   dag_id='so2w_dragon',
   description='Ingest tables from file project dragon to cloudera',
   default_args=args,
   concurrency=40,
   schedule_interval='00 21 * * *',
   tags=['HSO']
)

start = DummyOperator(
    task_id ='start',
    dag = dag,
    pool ='so2w')
#------------------------- SAP INTEGRATION -------------------------------#

integration_pssu                          = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'integration_pssu',command ='{} "{}integration_pssu.sql"'.format(command_beeline_dm_dragon, path_script),trigger_rule = 'all_success',dag = dag, pool='so2w_dragon',execution_timeout=timedelta(hours=3))
integration_ustk2                         = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'integration_ustk2',command ='{} "{}integration_ustk.sql"'.format(command_beeline_dm_dragon, path_script),trigger_rule = 'all_success',dag = dag, pool='so2w_dragon',execution_timeout=timedelta(hours=3))
sap_zpygsdi00045_pssu                     = SSHOperator(ssh_conn_id='adis2w01a000_dgx', timeout=90, task_id='sap_zpygsdi00045_pssu', command="{} {} {} {}".format(spark_submit_rfc,'zpygsdi00045_pssu','/home/adis2w01a000/cfg_rfc/PSSU/pssu.cfg',current_cluster), trigger_rule='all_done', dag=dag , pool='so2w_dragon',execution_timeout=timedelta(hours=12))
sap_ZPYGDO00004_USTK2                     = SSHOperator(ssh_conn_id='adis2w01a000_dgx', timeout=90, task_id='sap_ZPYGDO00004_USTK2', command="{} {} {} {} {} {}".format(spark_submit_rfc_rangedate,'ZPYGDO00004_USTK2','/home/adis2w01a000/cfg_rfc/USTK/ustk.cfg',current_cluster, start_ustk, end_ustk), trigger_rule='all_done', dag=dag , pool='so2w_dragon',execution_timeout=timedelta(hours=12))


start >> RFC >> sap_ZPYGDO00004_USTK2 >> integration_ustk2 >> [com_ZPYGDO00004_USTK2, inv_ZPYGDO00004_USTK2] >> SMW_reference_rfc
start >> RFC >> sap_zpygsdi00045_pssu >> integration_pssu >> [com_zpygsdi00045_pssu, inv_zpygsdi00045_pssu] >> SMW_reference_rfc

#--------------------------- DATAMART --------------------------------#

check_masterdealer_null                             = ExternalTaskSensor(task_id='check_masterdealer_null',external_dag_id='dragon',external_task_id='masterdealer_null',execution_delta = timedelta(hours=7),timeout=60*60*12,trigger_rule='all_done',dag=dag, pool='so2w_dragon') #sblmnya all_success
customerprofilesales_cdb                            = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_cdb_eva',command ='{} "{}customerprofilesales_cdb_eva.py"'.format(command_spark_dm.format('customerprofilesales_cdb',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_fovallowndealer                = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_fovallowndealer_eva',command ='{} "{}customerprofilesales_fovallowndealer_eva.py"'.format(command_spark_dm.format('customerprofilesales_fovallowndealer',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_fovalltime                     = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_fovalltime_eva',command ='{} "{}customerprofilesales_fovalltime_eva.py"'.format(command_spark_dm.format('customerprofilesales_fovalltime',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_fovlastyear                    = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_fovlastyear_eva',command ='{} "{}customerprofilesales_fovlastyear_eva.py"'.format(command_spark_dm.format('customerprofilesales_fovlastyear',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_kpb1                           = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_kpb1_eva',command ='{} "{}customerprofilesales_kpb1_eva.py"'.format(command_spark_dm.format('customerprofilesales_kpb1',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_kpb2                           = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_kpb2_eva',command ='{} "{}customerprofilesales_kpb2_eva.py"'.format(command_spark_dm.format('customerprofilesales_kpb2',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_kpb3                           = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_kpb3_eva',command ='{} "{}customerprofilesales_kpb3_eva.py"'.format(command_spark_dm.format('customerprofilesales_kpb3',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_kpb4                           = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_kpb4_eva',command ='{} "{}customerprofilesales_kpb4_eva.py"'.format(command_spark_dm.format('customerprofilesales_kpb4',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_lastservice                    = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_lastservice_eva',command ='{} "{}customerprofilesales_lastservice_eva.py"'.format(command_spark_dm.format('customerprofilesales_lastservice',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_salesorder                     = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_salesorder_eva',command ='{} "{}customerprofilesales_salesorder_eva.py"'.format(command_spark_dm.format('customerprofilesales_salesorder',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_dataservice                    = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_dataservice_eva',command ='{} "{}customerprofilesales_dataservice_eva.py"'.format(command_spark_dm.format('customerprofilesales_dataservice',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='dragon',execution_timeout=timedelta(hours=3))
AzureExtract_customerprofilesales_assemblyyearStar  = SSHOperator(ssh_conn_id='adis2w01a000_dgx', timeout=90, task_id = 'AzureExtract_customerprofilesales_assemblyyearstar_eva',command ='{} "{}AzureExtract_CustomerProfileSalesStar(Assemblyyear)_eva.py"'.format(python_edgenode_dataengineer, path_migrasi_star_dragon_dgx),trigger_rule = 'all_success',dag = dag, pool='so2w_dragon')
customerprofilesales_rofinal                        = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_rofinal_eva',command ='{} "{}customerprofilesales_rofinal_eva.py"'.format(command_spark_dm.format('customerprofilesales_rofinal',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='so2w_dragon',execution_timeout=timedelta(hours=3)) #sblmnya all_success
customerprofilesales_unitsales                      = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_unitsales_eva',command ='{} "{}customerprofilesales_unitsales_eva.sql"'.format(command_beeline_dm_dragon, path_cp),trigger_rule = 'all_success',dag = dag, pool='so2w_dragon',execution_timeout=timedelta(hours=12)) #sblmnya all_success
customerprofilesales_ring                           = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_ring_eva',command ='{} "{}customerprofilesales_ring_eva.sql" -hivevar USER=$USER'.format(command_beeline_dm_dragon, path_cp),trigger_rule = 'all_success',dag = dag, pool='so2w_dragon',execution_timeout=timedelta(hours=3)) 
customerprofilesales_servicejobtype                 = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_servicejobtype_eva',command ='{} "{}customerprofilesales_servicejobtype_eva.py"'.format(command_spark_dm.format('customerprofilesales_servicejobtype',_getPort(minport, maxport)), path_cp),trigger_rule = 'all_success',dag = dag, pool='so2w_dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_kpb                            = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_kpb_eva',command ='{} "{}customerprofilesales_kpb_eva.sql" -hivevar USER=$USER'.format(command_beeline_dm_dragon, path_cp),trigger_rule = 'all_success',dag = dag, pool='so2w_dragon',execution_timeout=timedelta(hours=3))
customerprofilesales                                = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_eva',command ='{} "{}customerprofilesales_eva.sql" -hivevar USER=$USER'.format(command_beeline_dm_dragon, path_cp),trigger_rule = 'all_success',dag = dag, pool='so2w_dragon',execution_timeout=timedelta(hours=3)) #sblmnya all_success
customerprofilesales_delta                          = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_delta_eva',command ='{} "{}customerprofilesales_delta_eva.sql" -hivevar USER=$USER'.format(command_beeline_dm_dragon, path_cp),trigger_rule = 'all_success',dag = dag, pool='so2w_dragon',execution_timeout=timedelta(hours=3))
customerprofilesales_star                           = SSHOperator(ssh_conn_id='adis2w01a000', timeout=90, task_id = 'customerprofilesales_star_eva',command ='{} "{}CustomerProfileSales_AssemblyyearStar_eva.py"'.format(command_spark_dm_local.format('customerprofilesales_Star',_getPort(minport, maxport)), path_migrasi_star_dragon),trigger_rule = 'all_success',dag = dag, pool='dragon')

#---Invalidate Metadata

inv_customerprofilesales_cdb                        = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_cdb_eva'            , command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_cdb'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))  
inv_customerprofilesales_fovallowndealer            = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_fovallowndealer_eva', command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_fovallowndealer'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))
inv_customerprofilesales_fovalltime                 = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_fovalltime_eva'     , command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_fovalltime'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))
inv_customerprofilesales_fovlastyear                = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_fovlastyear_eva'    , command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_fovlastyear'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))
inv_customerprofilesales_kpb1                       = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_kpb1_eva'           , command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_kpb1'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))
inv_customerprofilesales_kpb2                       = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_kpb2_eva'           , command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_kpb2'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))
inv_customerprofilesales_kpb3                       = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_kpb3_eva'           , command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_kpb3'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))
inv_customerprofilesales_kpb4                       = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_kpb4_eva'           , command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_kpb4'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))
inv_customerprofilesales_lastservice                = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_lastservice_eva'    , command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_lastservice'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))
inv_customerprofilesales_salesorder                 = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_salesorder_eva'     , command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_salesorder'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))
inv_customerprofilesales_dataservice                = SSHOperator(ssh_conn_id='astden01a000', timeout=90, task_id='inv_customerprofilesales_dataservice_eva'    , command=invalidate_metadata.format('eva_ast_so2w_wh_dm.customerprofilesales_dataservice'), trigger_rule='all_success', dag=dag, pool='dragon',execution_timeout=timedelta(hours=3))



## ------------------------ FLOW DATAMART ----------------------------------#



#1_customerprofilesales_kpb1
[masterdealer_null,check_ahass_SvPKBHeader,check_psshso_SvPKBHeader,check_psshso_SvPKBHeaderEquipment,check_ahass_SvPKBHeaderEquipment,check_psshso_RefEquipment,check_ahass_RefEquipment] >> customerprofilesales_kpb1 >> inv_customerprofilesales_kpb1 >> finish_depedencies_customerprofilesales_ring
#1_customerprofilesales_kpb2
[masterdealer_null,check_ahass_SvPKBHeader,check_psshso_SvPKBHeader,check_psshso_SvPKBHeaderEquipment,check_ahass_SvPKBHeaderEquipment,check_psshso_RefEquipment,check_ahass_RefEquipment] >> customerprofilesales_kpb2 >> inv_customerprofilesales_kpb2 >> finish_depedencies_customerprofilesales_ring
#1_customerprofilesales_kpb3
[masterdealer_null,check_ahass_SvPKBHeader,check_psshso_SvPKBHeader,check_psshso_SvPKBHeaderEquipment,check_ahass_SvPKBHeaderEquipment,check_psshso_RefEquipment,check_ahass_RefEquipment] >> customerprofilesales_kpb3 >> inv_customerprofilesales_kpb3 >> finish_depedencies_customerprofilesales_ring
#1_customerprofilesales_kpb4
[masterdealer_null,check_ahass_SvPKBHeader,check_psshso_SvPKBHeader,check_psshso_SvPKBHeaderEquipment,check_ahass_SvPKBHeaderEquipment,check_psshso_RefEquipment,check_ahass_RefEquipment] >> customerprofilesales_kpb4 >> inv_customerprofilesales_kpb4 >> finish_depedencies_customerprofilesales_ring
#1_customerprofilesales_fovallowndealer
[masterdealer_null,check_ahass_SvPKBHeader,check_psshso_SvPKBHeader,check_psshso_SvPKBHeaderEquipment,check_ahass_SvPKBHeaderEquipment,check_psshso_RefEquipment,check_ahass_RefEquipment] >> customerprofilesales_fovallowndealer >> inv_customerprofilesales_fovallowndealer >> finish_depedencies_customerprofilesales_ring
#1_customerprofilesales_fovalltime
[check_ahass_SvPKBHeader,check_psshso_SvPKBHeader,check_psshso_SvPKBHeaderEquipment,check_ahass_SvPKBHeaderEquipment,check_psshso_RefEquipment,check_ahass_RefEquipment] >> customerprofilesales_fovalltime >> inv_customerprofilesales_fovalltime >> finish_depedencies_customerprofilesales_ring
#1_customerprofilesales_fovlastyear
[check_ahass_SvPKBHeader,check_psshso_SvPKBHeader,check_psshso_SvPKBHeaderEquipment,check_ahass_SvPKBHeaderEquipment,check_psshso_RefEquipment,check_ahass_RefEquipment] >> customerprofilesales_fovlastyear >> inv_customerprofilesales_fovlastyear >> finish_depedencies_customerprofilesales_ring
#1_customerprofilesales_lastservice
[masterdealer_null,check_ahass_SvPKBHeader,check_psshso_SvPKBHeader,check_psshso_SvPKBHeaderEquipment,check_ahass_SvPKBHeaderEquipment,check_psshso_RefEquipment,check_ahass_RefEquipment,check_ahass_SvPKBJobDetail,check_psshso_SvPKBJobDetail] >> customerprofilesales_lastservice >> inv_customerprofilesales_lastservice >> finish_depedencies_customerprofilesales_ring

#1_customerprofilesales_kpb1,#1_customerprofilesales_kpb2,#1_customerprofilesales_kpb3,#1_customerprofilesales_kpb4,#1_customerprofilesales_fovallowndealer,#1_customerprofilesales_fovalltime,#1_customerprofilesales_fovlastyear,#1_customerprofilesales_lastservice,#1_customerprofilesales_cdb
start >> check_masterdealer_null >> [[customerprofilesales_kpb1 >> inv_customerprofilesales_kpb1 >>],[customerprofilesales_kpb1 >> inv_customerprofilesales_kpb1 >>] ]
[union_CDDBUDSTK] >> customerprofilesales_cdb >> inv_customerprofilesales_cdb >> finish_depedencies_customerprofilesales_ring
#1_customerprofilesales_salesorder
[union_invoice, union_SalesOrder, union_Customer, masterdealer_null] >> customerprofilesales_salesorder >> inv_customerprofilesales_salesorder >> finish_depedencies_customerprofilesales_ring
#1_customerprofilesales_dataservice
[union_RefCounterGroup, union_RefEquipment, union_RefJob, union_RefJobType, union_RefTaskList, union_SvPKBHeader, union_SvPKBHeaderEquipment, union_SvPKBJobDetail] >> customerprofilesales_dataservice >> inv_customerprofilesales_dataservice

#finish_depedencies_customerprofilesales_unitsales
#[avocado_Kecamatan, avocado_City, avocado_Province, avocado_Occupation, avocado_Kelurahan] >> finish_depedencies_customerprofilesales_unitsales


#2_customerprofilesales_rofinal
[integration_pssu, integration_ustk2, check_masterdealer, Check_masterunittype] >> customerprofilesales_rofinal >> inv_customerprofilesales_rofinal

#2_customerprofilesales_status
[check_masterdealer, Check_mastercustomerprofileref, Check_masterunittype, Check_mastersalesperson, integration_pssu,integration_ustk2,check_finish_depedencies_customerprofilesales_unitsales] >> customerprofilesales_unitsales >> inv_customerprofilesales_unitsales

#3_customerprofilesales_servicejobtype
[finish_depedencies_cpsales, check_masterdealer, check_customerprofilesales_dataservice, check_union_RefCounterGroup, \
check_union_RefJob, check_union_RefJobType, check_union_RefTaskList, check_union_SvPKBJobDetail] >> customerprofilesales_servicejobtype >> inv_customerprofilesales_servicejobtype

#3_customerprofilesales_ring
[finish_depedencies_cpsales, check_finish_depedencies_customerprofilesales_ring, check_masterdealer, Check_MasterMainDealer, check_union_UnitColor, customerprofilesales_rofinal, \
customerprofilesales_unitsales, check_portalhso_dealer_territory_result, \
check_customerprofilesales_cdb,check_customerprofilesales_fovallowndealer,check_customerprofilesales_fovalltime,check_customerprofilesales_fovlastyear, \
check_customerprofilesales_kpb1,check_customerprofilesales_kpb2,check_customerprofilesales_kpb3,check_customerprofilesales_kpb4, \
check_customerprofilesales_lastservice,check_customerprofilesales_salesorder] >> customerprofilesales_ring >> inv_customerprofilesales_ring 

#3_customerprofilesales_kpb
[check_masterdealer, Check_MasterMainDealer, Check_masterunittype, customerprofilesales_ring, \
customerprofilesales_servicejobtype,check_customerprofilesales_dataservice] >> customerprofilesales_kpb >> inv_customerprofilesales_kpb

#4_customerprofilesales,customerprofilestar
customerprofilesales_kpb >> [AzureExtract_customerprofilesales_assemblyyearStar,customerprofilesales] >> customerprofilesales_star >> [customerprofilesales_delta, inv_customerprofilesales,com_customerprofilesales] >> \
paralel_smw_customerprofilesales >> [SMW_customerprofilesales, SMW_CustomerProfileLeadsVirtualExhibition, SMW_CustomerProfileSalesSummary, \
                                    SMW_CustomerProfileSalesH1toH2, SMW_PSS2W_Croselling_CrosellingOwnDealer, SMW_PSS2W_Croselling_CustomerProfileRO, \
                                    SMW_CustomerProfileSalesFY2019, PBI_CustomerProfile_RO_290fd33f_9bf2_488f_8ca0_483cdc81273f, PBI_CustomerProfileSales_85b14eef_6af4_443d_b043_ee93e860e5db, \
                                    PBI_CustomerProfileSales_H1toH2_924f4a50_4474_4a86_8527_87f8f066e968, \
                                    PBI_PSS2W_Croselling_CrosellingOwnDealer_21d1c1cd_f8a3_4905_beaa_1c7fb2964a87, PBI_Summary_UnitSales_2f834b4b_f007_4ef8_b31c_c396c68d01f2, \
                                    PBI_TSDandOperation_CustomerProfileSales_FY_2019_a5da6727_4738_4c33_a19d_4ddb5c619db3] 
