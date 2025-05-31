
with dag:
    # START TASK
    start = DummyOperator (
        task_id = 'start',
        pool    = 'so2w'
    )

    # NESTED TASK GROUP - SENSOR TASK FOR DEPEDENCIES
    with TaskGroup(group_id = 'depedencies') as depedencies:
        # TASK GROUP - SENSOR TASK FOR DATA LANDING
        with TaskGroup(group_id='data_landing_sensors') as data_landing_sensors:
            check_astranet_astrapay_file = ExternalTaskSensor(
                task_id             = 'check_astranet_astrapay_file',
                external_dag_id     = 'so2w_dragon',
                external_task_id    = 'astranet_astrapay_file',
                execution_delta     = timedelta(hours = 0),
                timeout             = 60 * 60 * 6,
                allowed_states      = ['success', 'failed', 'upstream_failed'],
                pool                = 'so2w_dragon',
            )
            check_astranet_mappingso_target_upload_file = ExternalTaskSensor(
                task_id             = 'check_astranet_mappingso_target_upload_file',
                external_dag_id     = 'so2w_dragon',
                external_task_id    = 'astranet_mappingso_target_upload_file',
                execution_delta     = timedelta(hours = 0),
                timeout             = 60 * 60 * 6,
                allowed_states      = ['success', 'failed', 'upstream_failed'],
                pool                = 'so2w_dragon',
            )
        # TASK GROUP - SENSOR TASK FOR DATA MASTER
        with TaskGroup(group_id='data_master_sensors') as data_master_sensors:
            check_masterdealer = ExternalTaskSensor(
                task_id             = 'check_masterdealer',
                external_dag_id     = 'dragon',
                external_task_id    = 'masterdealer_null',
                execution_delta     = timedelta(hours = 7),
                timeout             = 60 * 60 * 6,
                trigger_rule        = 'all_success',
                pool                = 'so2w_dragon',
            )
            check_mastermaindealer = ExternalTaskSensor(
                task_id             = 'check_mastermaindealer',
                external_dag_id     = 'dragon',
                external_task_id    = 'mastermaindealer',
                execution_delta     = timedelta(hours = 7),
                timeout             = 60 * 60 * 6,
                trigger_rule        = 'all_success',
                pool                = 'so2w_dragon',
            )

    # TASK GROUP - INTEGRATION 
    with TaskGroup(group_id = 'integration') as integration:
        integration_astrapay_file = SSHOperator (
            ssh_conn_id         = 'adis2w01a000', 
            timeout             = 90, 
            task_id             = 'integration_astrapay_file',
            command             = '{} "{}astrapay_file_rank.sql" -hivevar USER=$USER'.format(beelineShell, path_astrapay),
            trigger_rule        = 'all_success',
            pool                = 'dragon',
            execution_timeout   = timedelta(hours = 6)
        )
        integration_mappingso_target_file = SSHOperator (
            ssh_conn_id         = 'adis2w01a000', 
            timeout             = 90, 
            task_id             = 'integration_mappingso_target_file',
            command             = '{} "{}mappingso_target_rank.sql" -hivevar USER=$USER'.format(beelineShell, path_astrapay),
            trigger_rule        = 'all_success',
            pool                = 'dragon',
            execution_timeout   = timedelta(hours = 6)
        )

    # TASK DATAMART
    astrapay_transformation = SSHOperator (
        ssh_conn_id         = 'adis2w01a000',
        timeout             = 90, 
        task_id             = 'astrapay_transformation',
        command             = '{} "{}astrapay_transformation.sql" -hivevar USER=$USER'.format(beelineShell, path_astrapay),
        trigger_rule        = 'all_success',
        pool                = 'dragon',
        execution_timeout   = timedelta(hours = 6)
    )

    # TASK PBI
    pbi_astrapay = SSHOperator (
        ssh_conn_id         = 'adis2w01a000', 
        timeout             = 90, 
        task_id             = 'PBI_Astrapay_878ed35c_4319_4508_8a15_9151bf81ae6b',
        command             = '{} {}Astrapay.py'.format(command_PBI, path_pbi),
        trigger_rule        = 'all_success',
        pool                = 'dragon'
    )

    # FINISH TASK
    finish = DummyOperator (
        task_id = 'finish',
        pool    = 'so2w'
    )

# FLOW DATAMART
start >> depedencies >> integration >> astrapay_transformation >> pbi_astrapay >> finish