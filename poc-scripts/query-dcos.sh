#FRAMEWORK_NAME="spark"
#APP_ID="Driver"

FRAMEWORK_NAME="NotebookSpark-aggregateNotebook"
APP_ID="NotebookSpark-aggregateNotebook"
DCOS_URL="https://internal-implementation2-elb-1342557558.us-east-1.elb.amazonaws.com"
SPARK_DRIVER_NAME="driver-20201214183435-6679"

# get the mesos task state json
curl ${DCOS_URL}/mesos/state --insecure > mesos_state_macig.txt

TASK_STATE="$(jq -c ".frameworks[] | select(.name == \"${FRAMEWORK_NAME}\") | .tasks[] | select(.name | contains(\"${APP_ID}\"))" mesos_state_macig.txt)"
COMPLETED="$(jq -c ".frameworks[] | select(.name == \"${FRAMEWORK_NAME}\") | .completed_tasks[] | select(.name | contains(\"${APP_ID}\"))" mesos_state_macig.txt)"
IFS=$'\n'
TIME=$(date +%s)
for row in ${COMPLETED}; do
  AGENT_ID="$(echo "${row}" | jq -r '.slave_id')"
  TASK_ID="$(echo "${row}" | jq -r '.id')"
  FRAMEWORK_ID="$(echo "${row}" | jq -r '.framework_id')"
  if [[ "$FRAMEWORK_ID" != *"$SPARK_DRIVER_NAME"* ]]; then
    continue
  fi
  EXECUTOR_ID="$(echo "${row}" | jq -r '.executor_id')"
  CONTAINER_ID="$(echo "${row}" | jq -r '.statuses[0].container_status.container_id.value')"
  EXECUTOR_ID="${EXECUTOR_ID:-${TASK_ID}}"
  sleep 1
    curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout" >> ./macig_runs_12_14/${TIME}.stdout.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
    curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.1" >> ./macig_runs_12_14/${TIME}.stdout.1.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
    curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.2" >> ./macig_runs_12_14/${TIME}.stdout.2.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
    curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.3" >> ./macig_runs_12_14/${TIME}.stdout.3.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
    curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.4" >> ./macig_runs_12_14/${TIME}.stdout.4.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
    sleep 1
    curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.5" >> ./macig_runs_12_14/${TIME}.stdout.5.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
    curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.6" >> ./macig_runs_12_14/${TIME}.stdout.6.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
    curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.7" >> ./macig_runs_12_14/${TIME}.stdout.7.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
    curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.8" >> ./macig_runs_12_14/${TIME}.stdout.8.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
    curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.9" >> ./macig_runs_12_14/${TIME}.stdout.9.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
done

for row in ${TASK_STATE}; do
  AGENT_ID="$(echo "${row}" | jq -r '.slave_id')"
  TASK_ID="$(echo "${row}" | jq -r '.id')"
  FRAMEWORK_ID="$(echo "${row}" | jq -r '.framework_id')"
  if [[ "$FRAMEWORK_ID" != *"$SPARK_DRIVER_NAME"* ]]; then
     continue
  fi
  EXECUTOR_ID="$(echo "${row}" | jq -r '.executor_id')"
  CONTAINER_ID="$(echo "${row}" | jq -r '.statuses[0].container_status.container_id.value')"
  EXECUTOR_ID="${EXECUTOR_ID:-${TASK_ID}}"
  sleep 1
  curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout" >> ./macig_runs_12_14/${TIME}.stdout.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
  curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.1" >> ./macig_runs_12_14/${TIME}.stdout.1.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
  curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.2" >> ./macig_runs_12_14/${TIME}.stdout.2.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
  curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.3" >> ./macig_runs_12_14/${TIME}.stdout.3.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
  curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.4" >> ./macig_runs_12_14/${TIME}.stdout.4.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
  sleep 1
  curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.5" >> ./macig_runs_12_14/${TIME}.stdout.5.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
  curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.6" >> ./macig_runs_12_14/${TIME}.stdout.6.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
  curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.7" >> ./macig_runs_12_14/${TIME}.stdout.7.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
  curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.8" >> ./macig_runs_12_14/${TIME}.stdout.8.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt
  curl  --insecure "${DCOS_URL}/agent/${AGENT_ID}/files/download?path=/var/lib/mesos/slave/slaves/${AGENT_ID}/frameworks/${FRAMEWORK_ID}/executors/${EXECUTOR_ID}/runs/${CONTAINER_ID}/stdout.9" >> ./macig_runs_12_14/${TIME}.stdout.9.${AGENT_ID}.${FRAMEWORK_ID}.${EXECUTOR_ID}.txt

done
unset IFS
