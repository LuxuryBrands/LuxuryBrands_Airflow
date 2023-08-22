from airflow.models import Variable
from airflow.providers.slack.operators.slack import SlackAPIPostOperator


class SlackAlert:
    def __init__(self, channel):
        self.token = Variable.get("slack_token", default_var=None)
        self.channel = channel

    def _make_attachments(self, context, title="Failure", color="danger"):
        return [
            {
                "mrkdwn_in": ["text"],
                "title": title,
                "text": (
                    f'* `DAG`:  {context.get("task_instance").dag_id}'
                    f'\n* `Task`:  {context.get("task_instance").task_id}'
                    f'\n* `Run ID`:  {context.get("run_id")}'
                    f'\n* `Logical Date`:  {context.get("logical_date")}'
                ),
                "actions": [
                    {
                        "type": "button",
                        "name": "view log",
                        "text": "View log",
                        "url": context.get("task_instance").log_url,
                        "style": "danger" if color == "danger" else "default",
                    },
                ],
                "color": color,  # 'good', 'warning', 'danger', or hex ('#439FE0')
                "fallback": "details",  # Required plain-text summary of the attachment
            }
        ]

    def send_message(self, context, task_id, text, title, color):
        try:
            SlackAPIPostOperator(
                task_id=task_id,
                channel=self.channel,
                token=self.token,
                text=text,
                attachments=self._make_attachments(context, title, color),
            ).execute(context=context)
        except Exception as e:
            print(f"Error: SlackAPIPostOperator, {str(e)}")


def send_failure_alert(context: dict, channel="#airflow_alert"):
    alert = SlackAlert(channel=channel)
    alert.send_message(
        context,
        task_id="send_fail_alert",
        text="Failure",
        title="Failure",
        color="danger",
    )


def send_success_alert(context: dict, channel="#airflow_alert"):
    alert = SlackAlert(channel=channel)
    alert.send_message(
        context,
        task_id="send_success_alert",
        text="Success",
        title="Success",
        color="good",
    )
