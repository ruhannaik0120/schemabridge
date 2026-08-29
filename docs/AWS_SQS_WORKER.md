# Amazon SQS migration worker

This optional integration lets the FastAPI API notify an Amazon SQS queue when it creates a new durable migration job. A separate worker receives that notification and runs the existing SchemaBridge migration pipeline.

SQS is a notification queue, not the source of truth. PostgreSQL remains the control plane: it stores the job, its status, audit history, and final result. An SQS message contains only the job ID.

```text
FastAPI creates job in PostgreSQL
        -> sends job ID to SQS
        -> worker receives job ID
        -> worker claims that exact queued job in PostgreSQL
        -> existing migration pipeline runs
        -> worker deletes the SQS message after safe handling
```

If the worker receives a duplicate, stale, or unknown job ID, it does not run a migration and safely acknowledges the notification. PostgreSQL's exact claim prevents two workers from running the same queued job.

## What this implementation provides

- Optional SQS publishing from the job-submission service.
- A one-message local worker command for safe development and verification.
- Message acknowledgement only after the worker has handled the durable job record.
- Sanitized application errors: credentials, queue URLs, and raw AWS driver errors are not exposed.
- A hosted ECS Fargate worker deployment using an ECR worker image, a private RDS control plane, Secrets Manager, IAM task roles, and CloudWatch logs.

The hosted worker has been smoke-proven with a private RDS control-plane migration and a harmless unknown-job SQS notification. Long-running production migrations still need a visibility-timeout heartbeat.

## Prerequisites

1. A reachable and migrated SchemaBridge control-plane PostgreSQL database.
2. An SQS standard queue in the AWS region you will use.
3. AWS credentials available to the local process through the standard AWS credential chain, such as `aws configure`. Do not put access keys in `.env` or commit them.
4. `boto3`, installed through this project's `requirements.txt`.

## Least-privilege IAM permissions

Use one identity for the API publisher and one for the worker when you deploy them separately.

The publisher needs only:

```json
{
  "Effect": "Allow",
  "Action": "sqs:SendMessage",
  "Resource": "<your exact queue ARN>"
}
```

The worker needs only:

```json
{
  "Effect": "Allow",
  "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage"],
  "Resource": "<your exact queue ARN>"
}
```

Replace the placeholder with the ARN of the one SchemaBridge queue. Do not use `Resource: "*"` for a production policy.

## Local configuration

Copy `.env.example` to `.env` if needed. Set these optional values in `.env`:

```dotenv
SCHEMABRIDGE_AWS_REGION=<your AWS region, for example ap-south-1>
SCHEMABRIDGE_SQS_QUEUE_URL=<the HTTPS queue URL from the SQS console>
```

Keep `SCHEMABRIDGE_CONTROL_PLANE_DSN` configured as well. When either SQS value is blank, SchemaBridge keeps its previous local-job behavior and does not create an SQS client.

## Run one local worker cycle

From the repository root, run:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_sqs_migration_worker
```

The command receives at most one notification, processes its durable job if it can be claimed, and exits. Use module mode (`-m`) exactly as shown so Python can locate the `schemabridge` package.

To keep the local worker running until you stop it, use:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_sqs_migration_worker --forever
```

SQS uses long polling while this command waits for work, so it does not constantly hammer AWS. Press `Ctrl+C` once when you want to stop it; SchemaBridge closes its local resources cleanly. The one-cycle command remains useful for safe checks and tests.

## Run the worker with Docker Compose (Windows)

The Compose service uses the same SchemaBridge image as the API, but starts the worker command instead of Uvicorn. It connects to the Compose control-plane PostgreSQL database and mounts the local AWS CLI configuration directory as read-only. The credentials are not copied into the image, committed to Git, or placed in `.env`.

From the repository root, with the AWS CLI already configured for your Windows user, run:

```powershell
docker compose up --build -d migration-worker
docker compose logs -f migration-worker
```

The first command also starts the required `control-plane` and one-time `migrate` services. The second command shows the worker output. Expect `SQS migration worker started...`, followed by either a handled job or `No claimable SQS migration job.` after a long-poll wait.

Stop only the worker while keeping the control-plane database available:

```powershell
docker compose stop migration-worker
```

Docker sends the worker a termination signal during this command. SchemaBridge translates that signal into the same clean shutdown path used by `Ctrl+C`, then closes its local database resources before the container exits.

Use this local Compose proof before moving the worker image to Amazon ECR and ECS Fargate. Build the worker-specific image with `Dockerfile.worker`; it starts the SQS worker by default rather than the FastAPI server. The local `migration-worker` service is Windows-oriented because it mounts `%USERPROFILE%\.aws`; hosted AWS deployment replaces that local credentials mount with an IAM task role.

## Hosted ECS Fargate deployment

The hosted worker uses this boundary:

```text
FastAPI publisher -> SQS main queue -> ECS Fargate worker
                                        |        |
                                        |        -> CloudWatch logs
                                        -> private RDS control plane
```

The image is built from `Dockerfile.worker`, whose default command is the continuous worker loop. This keeps the FastAPI server image behavior separate from the hosted worker behavior.

For a small development proof, use a `0.25 vCPU` / `0.5 GB` Fargate task with one replica. The task needs no inbound rule. It needs outbound connectivity to ECR, SQS, Secrets Manager, and CloudWatch; an enabled public IP is a simple development choice. A production deployment should use private subnets with a NAT gateway or the appropriate VPC endpoints.

Use separate IAM roles:

- **Task execution role:** pull the ECR image, write CloudWatch logs, and read the `SCHEMABRIDGE_CONTROL_PLANE_DSN` secret.
- **Task role:** receive and delete messages from the main SQS queue.

Store the control-plane DSN in Secrets Manager under a JSON key named `SCHEMABRIDGE_CONTROL_PLANE_DSN`, then inject that key into the container as the same environment-variable name. Do not place the DSN in a task definition, Docker image, or source-controlled `.env` file.

Before starting the continuous service, run a one-off ECS task with this command override:

```text
python,-m,scripts.migrate_control_plane
```

An exit code of `0` and the CloudWatch message `Control-plane migrations verified: ...` prove that the task can read the secret and reach the private control-plane database.

### Hosted proof boundary

The hosted smoke proof validates ECR, ECS Fargate, IAM roles, Secrets Manager, SQS, private RDS, and CloudWatch as one system. It intentionally sends an unknown job ID, so it does not migrate business data.

For an ECS worker to run a real source-to-target migration, it also needs the required named source and target profiles (for example through an approved hosted profile configuration/secret mechanism). The current smoke proof does not claim a cloud database migration without that configuration.

Expected outcomes:

| Output | Meaning |
|---|---|
| `No claimable SQS migration job.` | The queue was empty, or the message referred to a job that was already handled, stale, or intentionally unknown. |
| `Migration job <id>: status=SUCCEEDED, ...` | A queued migration was claimed and completed. |
| `Migration job <id>: status=..., failure_category=...` | The job was handled and finished in a review/failure state; inspect its durable job history. |
| `SQS migration worker failed...` | The message was not acknowledged. Check configuration and PostgreSQL job state; SQS can make it visible for a later retry. |

## Safe live smoke proof

To prove the AWS boundary without migrating business data, publish a random job ID, then run one worker cycle. The worker will read the message, find no matching queued PostgreSQL job, and acknowledge the harmless notification. Do this only after confirming your queue receive permission.

The focused automated checks do not call AWS. They use fakes to verify publishing, configuration, receiving, acknowledgement ordering, dependency wiring, exact job claims, and the worker script:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_migration_job_queue.py tests\test_migration_job_sqs.py tests\test_migration_job_sqs_consumer.py tests\test_migration_job_sqs_settings.py tests\test_migration_job_sqs_dependencies.py tests\test_migration_job_sqs_worker.py tests\test_run_sqs_migration_worker.py -q
```

## Operational limits and next steps

- The local one-cycle script remains useful for safe checks; `Dockerfile.worker` and ECS run the same cycle in a controlled loop.
- SQS standard queues can redeliver messages, so the PostgreSQL exact claim is required even when messages appear once.
- Configure the main queue with `schemabridge-migration-jobs-dlq` as its dead-letter queue and a maximum-receives value of `5`. This gives temporary failures five chances before isolating the message for investigation.
- A job saved to PostgreSQL but not published because of a temporary AWS failure needs an outbox/recovery mechanism before production deployment.
- Monitor the dead-letter queue before continuous production processing, so repeated failures are investigated rather than ignored.
- Ensure the queue visibility timeout safely exceeds typical job duration; long migrations need a heartbeat that extends it while work continues.
