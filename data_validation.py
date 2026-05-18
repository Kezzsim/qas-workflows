from prefect import task
from tiled.client import from_uri
from dotenv import load_dotenv
import os

BEAMLINE_OR_ENDSTATION = "qas"


def get_api_key_from_env(api_key=None):
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task(retries=2, retry_delay_seconds=10)
def get_run(uid, api_key=None):
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl[f"{BEAMLINE_OR_ENDSTATION}/raw"][uid]
    return run


# SQL database-backed - remove if this does not exist on the beamline
@task(retries=2, retry_delay_seconds=10)
def get_run_migration(uid, api_key=None):  # TODO remove after migration is complete
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl[f"{BEAMLINE_OR_ENDSTATION}/migration"][uid]
    return run


@task(retries=2, retry_delay_seconds=10)
def get_run_processed(uid, api_key=None):
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl[f"{BEAMLINE_OR_ENDSTATION}/processed"][uid]
    return run
