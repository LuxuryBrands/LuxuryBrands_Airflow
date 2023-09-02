from airflow.providers.amazon.aws.hooks.emr import EmrHook

from airflow.models import Variable


def get_etl_step(table, script_keys, dest_key, mode="client"):
    bucket_name = Variable.get("aws_bucket")
    return [
        {
            "Name": f"etl_{table}_data",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", mode,
                    "--packages", "com.amazon.deequ:deequ:2.0.3-spark-3.3",
                    "--exclude-packages", "net.sourceforge.f2j:arpack_combined_all",
                    "--py-files", f"s3://{bucket_name}/{script_keys[0]}",
                    f"s3://{bucket_name}/{script_keys[1]}",
                    f"s3a://{bucket_name}/{dest_key}",
                    "{{ ts }}",
                    table
                ],
            },
        }
    ]


def get_elt_step(bucket_name, script_key):
    return [
        {
            "Name": "elt_data",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "client",
                    f"s3://{bucket_name}/{script_key}",
                ],
            },
        }
    ]


def get_emr_cluster_id(**context):
    job_flow_name = Variable.get("aws_job_flow_name")
    cluster_states = context["cluster_states"]

    emr_hook = EmrHook()
    job_flow_id = emr_hook.get_cluster_id_by_name(job_flow_name, cluster_states)

    if not job_flow_id:
        raise Exception(f"No cluster found for name: {job_flow_name}")

    return job_flow_id
