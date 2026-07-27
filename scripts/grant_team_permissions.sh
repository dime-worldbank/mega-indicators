#!/bin/bash
# TEMPORARY: grants the team group CAN_MANAGE on every bundle-managed job/pipeline,
# in both targets, via the permissions API directly (merge semantics — doesn't touch
# existing owners). This exists because the bundle's own `permissions:` block can't
# do this: it forces IS_OWNER = run_as on every managed resource, which fails for
# resources owned by someone other than run_as unless an admin reassigns them.
#
# Run this after any deploy that creates a new job/pipeline (recreation gets a fresh
# ACL) or right now to backfill visibility for existing resources. Safe to re-run.
#
# Delete this script (and stop running it) once RPF-ADBSvc-PROD has been granted the
# `servicePrincipal.user` role and databricks.yml's run_as/permissions are switched to
# it — see TODO in databricks.yml.
set -euo pipefail

GROUP="ITSDA-LKHS-DAP-PROD-boostprocessed"
PROFILE="adb-6102124407836814"

grant() {
  local object_type="$1" object_id="$2"
  echo "granting CAN_MANAGE on $object_type/$object_id"
  databricks permissions update "$object_type" "$object_id" --profile "$PROFILE" --json "{
    \"access_control_list\": [
      {\"group_name\": \"$GROUP\", \"permission_level\": \"CAN_MANAGE\"}
    ]
  }" >/dev/null
}

for target in staging prod; do
  echo "=== $target ==="
  jobs_and_pipelines=$(databricks bundle summary -t "$target" --profile "$PROFILE" --output json)

  for id in $(echo "$jobs_and_pipelines" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for obj in d.get('resources', {}).get('jobs', {}).values():
    print(obj['id'])
"); do
    grant jobs "$id"
  done

  for id in $(echo "$jobs_and_pipelines" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for obj in d.get('resources', {}).get('pipelines', {}).values():
    print(obj['id'])
"); do
    grant pipelines "$id"
  done
done
