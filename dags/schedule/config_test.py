from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from typing_extensions import final

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}


with open("/opt/bitnami/airflow/dags/schedule/config_test.py", 'r') as f:
    file_data=f.read()
    print(file_data)


dag = DAG(
    dag_id='config_test',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)


# [START howto_operator_python]
def print_context(ds, key, **kwargs):
    print("THIS IS KEY: ", key)
    print("STARTED")
    #print(dag.__class__, dag.__deepcopy__, dag.__delattr__, dag.__dict__, dag.__dir__, dag.__doc__, dag.__enter__, dag.__eq__, dag.__exit__, dag.__format__, dag.__ge__, dag.__getattribute__, dag.__gt__, dag.__hash__, dag.__init__, dag.__init_subclass__, dag.__le__, dag.__lt__, dag.__metaclass__, dag.__module__, dag.__ne__, dag.__new__, dag.__reduce__, dag.__reduce_ex__, dag.__repr__, dag.__setattr__, dag.__sizeof__, dag.__str__, dag.__subclasshook__, dag.__weakref__, dag._access_control, dag._comps, dag._concurrency, dag._dag_id, dag._default_view, dag._description, dag._full_filepath, dag._get_concurrency_reached, dag._get_is_paused, dag._get_latest_execution_date, dag._old_context_manager_dags, dag._pickle_id, dag._schedule_interval, dag._set_context, dag._test_cycle_helper, dag.access_control, dag.add_task, dag.add_tasks, dag.allow_future_exec_dates, dag.catchup, dag.clear, dag.clear_dags, dag.cli, dag.concurrency, dag.concurrency_reached, dag.create_dagrun, dag.dag_id, dag.dagrun_timeout, dag.date_range, dag.deactivate_stale_dags, dag.deactivate_unknown_dags, dag.default_args, dag.description, dag.description_unicode, dag.doc_md, dag.end_date, dag.fileloc, dag.filepath, dag.folder, dag.following_schedule, dag.full_filepath, dag.get_active_runs, dag.get_dagrun, dag.get_dagruns_between, dag.get_dagtags, dag.get_default_view, dag.get_last_dagrun, dag.get_num_active_runs, dag.get_num_task_instances, dag.get_run_dates, dag.get_serialized_fields, dag.get_task, dag.get_task_instances, dag.get_template_env, dag.handle_callback, dag.has_task, dag.is_fixed_time_schedule, dag.is_paused, dag.is_paused_upon_creation, dag.is_subdag, dag.jinja_environment_kwargs, dag.last_loaded, dag.latest_execution_date, dag.leaves, dag.log, dag.logger, dag.max_active_runs, dag.normalize_schedule, dag.on_failure_callback, dag.on_success_callback, dag.orientation, dag.owner, dag.params, dag.parent_dag, dag.partial, dag.pickle, dag.pickle_id, dag.pickle_info, dag.previous_schedule, dag.resolve_template_files, dag.roots, dag.run, dag.safe_dag_id, dag.schedule_interval, dag.set_dag_runs_state, dag.set_dependency, dag.sla_miss_callback, dag.start_date, dag.sub_dag, dag.subdags, dag.sync_to_db, dag.tags, dag.task_dict, dag.task_ids, dag.tasks, dag.template_searchpath, dag.template_undefined, dag.test_cycle, dag.timezone, dag.topological_sort, dag.tree_view, dag.user_defined_filters, dag.user_defined_macros)
    print("ENDED")
    print("THIS IS CONF", kwargs['conf'])
    # print("THIS IS CONF", kwargs['conf']['test_parameter'])
    # print("THIS IS CONF", kwargs.conf.values())
    print("THIS IS KWARGS: ", kwargs)
    print("THIS IS ds: ", ds)
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    op_kwargs={"key": "{{ dag_run.conf['key'] }}"},
    dag=dag,
)


run_this


# {"test_parameter":"test_value"}
