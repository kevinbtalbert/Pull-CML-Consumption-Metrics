import os
import csv
import cmlapi
import json
from cmlapi.rest import ApiException
from datetime import datetime


###############################################################################
# Initialize the CML API client
###############################################################################
client = cmlapi.default_client(
    url=os.getenv("CDSW_API_URL").replace("/api/v1", ""),
    cml_api_key=os.getenv("CDSW_APIV2_KEY")
)

###############################################################################
# Helper: Convert a datetime/string to ISO8601
###############################################################################
def to_iso(dt):
    if isinstance(dt, datetime):
        return dt.isoformat()
    if isinstance(dt, str):
        return dt
    return ""

###############################################################################
# Main function to gather all job runs
###############################################################################
def gather_all_job_runs():
    """
    Gathers all job runs in all projects, for all time (no date filtering),
    returning a list of dicts where each dict is one row for the CSV.

    Each row includes every field from the JobRun object:
      - project_id
      - project_name (enriched)
      - job_id
      - job_name (enriched)
      - run_id
      - status
      - created_at
      - scheduling_at
      - starting_at
      - running_at
      - finished_at
      - kernel
      - cpu
      - memory
      - nvidia_gpu
      - arguments
      - runtime_identifier
      - userUsername, userName, userEmail (from either run or job creator)
    """
    rows = []

    # 1) Build a project lookup: project_id -> project_name
    try:
        projects_resp = client.list_projects(page_size=100000, include_all_projects=True)
        project_lookup = {}
        for proj in projects_resp.projects:
            project_lookup[proj.id] = proj.name
    except ApiException as e:
        print(f"[ERROR] listing projects: {e}")
        return rows

    # 2) For each project, list jobs, then job runs
    for project_id, project_name in project_lookup.items():
        try:
            job_list = client.list_jobs(project_id=project_id, page_size=100000)
        except ApiException as e:
            print(f"[ERROR] listing jobs for project={project_id}: {e}")
            continue

        # Build a quick job_id -> jobName, jobCreator lookups
        # so we can include them in each run's row
        job_meta = {}
        for job_obj in job_list.jobs:
            userUsername = None
            userName = None
            userEmail = None
            if job_obj.creator:
                userUsername = job_obj.creator.username
                userName = job_obj.creator.name
                userEmail = job_obj.creator.email

            job_meta[job_obj.id] = {
                "jobName": job_obj.name,
                # We'll store the job's CPU/mem/gpu if you prefer, but these
                # are job-level, not run-level. The run might override them.
                "userUsername": userUsername,
                "userName": userName,
                "userEmail": userEmail
            }

        # Now retrieve job runs for each job
        for job_obj in job_list.jobs:
            try:
                runs_resp = client.list_job_runs(
                    project_id=project_id,
                    job_id=job_obj.id,
                    page_size=1000
                )
            except ApiException as ex:
                print(f"[ERROR] listing job runs for job {job_obj.id}: {ex}")
                continue

            # For each run, capture all possible fields
            for run in runs_resp.job_runs:
                # We'll check if run has a separate .creator,
                # but doc suggests it might only exist in the job object
                # so fallback to the job's user
                run_userUsername = job_meta[job_obj.id]["userUsername"]
                run_userName = job_meta[job_obj.id]["userName"]
                run_userEmail = job_meta[job_obj.id]["userEmail"]
                
                if run.creator:
                    # If the run actually has a separate run-level .creator
                    # (not always documented), you can do:
                    run_userUsername = run.creator.username
                    run_userName = run.creator.name
                    run_userEmail = run.creator.email
                
                # Build row
                row = {
                    # We add some "enriched" fields
                    "UserUsername": run_userUsername or "",
                    "UserName": run_userName or "",
                    "UserEmail": run_userEmail or "",
                    "ProjectID": project_id,
                    "ProjectName": project_name,
                    "JobID": run.job_id,
                    "JobName": job_meta[job_obj.id]["jobName"],
                    
                    # Fields from the job run object
                    "RunID": run.id,
                    "Status": run.status or "",
                    "CreatedAt": to_iso(getattr(run, "created_at", None)),
                    "SchedulingAt": to_iso(getattr(run, "scheduling_at", None)),
                    "StartingAt": to_iso(getattr(run, "starting_at", None)),
                    "RunningAt": to_iso(getattr(run, "running_at", None)),
                    "FinishedAt": to_iso(getattr(run, "finished_at", None)),
                    "Kernel": run.kernel or "",
                    "CPU": run.cpu if run.cpu is not None else 0,
                    "Memory": run.memory if run.memory is not None else 0,
                    "NvidiaGPU": run.nvidia_gpu if run.nvidia_gpu is not None else 0,
                    "Arguments": run.arguments or "",
                    "RuntimeIdentifier": run.runtime_identifier or ""
                }
                rows.append(row)

    return rows

###############################################################################
# Write to CSV with full fields
###############################################################################
def write_all_job_runs_csv(rows, filename="all_job_runs.csv"):
    """
    Sort by:
      1) UserUsername
      2) CreatedAt
      3) ProjectName
      4) JobName
    """
    # Use the row's CreatedAt field for date sorting
    # If you want robust date parsing, parse the strings into datetime objects first.
    def sort_key(r):
        # We'll parse CreatedAt as best we can, or fallback to empty
        created_str = r["CreatedAt"]
        return (
            r["UserUsername"],
            created_str,
            r["ProjectName"],
            r["JobName"]
        )
    rows.sort(key=sort_key)

    fieldnames = [
        "UserUsername",
        "UserName",
        "UserEmail",
        "ProjectID",
        "ProjectName",
        "JobID",
        "JobName",
        "RunID",
        "Status",
        "CreatedAt",
        "SchedulingAt",
        "StartingAt",
        "RunningAt",
        "FinishedAt",
        "Kernel",
        "CPU",
        "Memory",
        "NvidiaGPU",
        "Arguments",
        "RuntimeIdentifier"
    ]
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

###############################################################################
# MAIN
###############################################################################
def main():
    all_runs = gather_all_job_runs()
    write_all_job_runs_csv(all_runs, "all_job_runs.csv")
    print("Done! CSV 'all_job_runs.csv' generated with every JobRun field, sorted by user->CreatedAt->ProjectName->JobName.")

if __name__ == "__main__":
    main()
