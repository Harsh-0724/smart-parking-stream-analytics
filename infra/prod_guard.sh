#!/bin/bash
# Refuses to let the production stack start with default, empty or trivially short secrets.
# Runs as a one-shot container; the services that matter depend on it completing successfully.
fail=0

check() {
  local name=$1 value=${!1:-}
  if [ -z "$value" ] || [ "${#value}" -lt 12 ]; then
    echo "REFUSING TO START: $name is empty or shorter than 12 characters"
    fail=1
    return
  fi
  case "$value" in
    change-me* | changeme* | admin | password | MkU3OEVBNzcwNTJENDM2Qk)
      echo "REFUSING TO START: $name still has a default value from .env.example"
      fail=1
      ;;
  esac
}

for name in POSTGRES_PASSWORD GRAFANA_PASSWORD SIM_SALT KAFKA_CLUSTER_ID; do
  check "$name"
done

if [ "$fail" -eq 0 ]; then
  echo "prod-guard: secrets are set and none is a known default"
fi
exit "$fail"
