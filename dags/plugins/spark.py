def get_etl_step(bucket_name, script_key, dest_key):
    return [
        {
            "Name": "etl_data",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "client",
                    f"s3://{bucket_name}/{script_key}",
                    f"s3a://{bucket_name}/{dest_key}",
                    "{{ ts }}"
                ],
            },
        }
    ]


def get_emr_cluster_id(**context):
    from airflow.providers.amazon.aws.hooks.emr import EmrHook
    from airflow import AirflowException

    job_flow_name = context["job_flow_name"]
    cluster_states = context["cluster_states"]

    emr_hook = EmrHook()
    job_flow_id = emr_hook.get_cluster_id_by_name(job_flow_name, cluster_states)

    if not job_flow_id:
        raise AirflowException(f"No cluster found for name: {job_flow_name}")

    return job_flow_id;
