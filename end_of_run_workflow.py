from prefect import task, flow, get_run_logger
from data_validation import data_validation, get_run, get_run_processed

# QAS Application Specific
from xas.process import process_interpolate_bin_with_tiled

tiled_inst = "https://tiled.nsls2.bnl.gov"

@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! dry_run: {dry_run}")


@flow
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    uid = stop_doc["run_start"]
    data_validation(uid, api_key=api_key, dry_run=dry_run)
    # Processing goes here
    run = get_run(uid)
    run_processed = get_run_processed(uid)
    process_interpolate_bin_with_tiled(
        run, run_processed
    )
    log_completion(dry_run=dry_run)
    return True
