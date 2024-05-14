"""
runs embeddingService as a FastAPI web service with 1 or more workers (uvicorn/optionally-gunicorn).
accepts various cmdline args, run with "--help" or "-h" to see args use. Some caching database
implementations may use a separate thread to handle transaction commits in BG as an optimization.
To insure that resources are properly cleaned-up, use "graceful" server shutdown when possible
"""
import argparse
import logging
import uvicorn

from contextlib import asynccontextmanager
from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from filelock import Timeout, FileLock
from os import getpid
from pathlib import Path
from sys import stderr
from typing import Annotated

#import embeddingService # importing further down (after parse_args) speeds up "help" display
                         # and args error-handling significantly (about 4x) due to PyTorch load
DEFAULT_MODEL = "sentence-transformers/distiluse-base-multilingual-cased-v2"


parser = argparse.ArgumentParser()
parser.add_argument("-c", "--cors-origin",
        nargs="*", default="*",
        help='optional: 1 or more origins for CORS requests. "*" (default) means all are allowed')
parser.add_argument("-d", "--data-dir",
        help="optional: path to data files (index & cache) per model, default: 'data' in curr_dir",
        default="data")
parser.add_argument("--host",
        default="127.0.0.1",
        help="optional: run uvicorn/gunicorn as this host, defaults to '127.0.0.1'")
parser.add_argument("-l", "--log-level",
        choices=["debug", "info", "warning", "error", "critical"],
        help="optional: log level for entire application, default: 'info'",
        default="info")
parser.add_argument("-m", "--model",
        help=f"optional: start all workers with this model, default: '{DEFAULT_MODEL}'",
        default=DEFAULT_MODEL)
parser.add_argument("-p", "--port", help="optional: default port: 8009", default=8009, type=int)
parser.add_argument("-t", "--db-type",
        choices=["duckdb", "leveldb", "sqlite"],
        help="optional: database type for all workers & models, default: 'sqlite'",
        default="sqlite")
parser.add_argument("-w", "--workers",
        help="optional: number of workers, more than 1 implies 'production' mode (no hot reload),"
             " default: 1",
        default=1, type=int)
args = parser.parse_args()


from databaseCommitProcess import DatabaseCommitProcess as dbcp
from embeddingService import ACQUIRE_LOCK_TIMEOUT, EmbeddingService

loglevel = getattr(logging, args.log_level.upper())
logging.basicConfig(format="%(asctime)s %(message)s", level=loglevel, stream=stderr)
models_cfg = EmbeddingService.get_models_cfg(args.data_dir)
supported_models = models_cfg.keys()
es = None # uninitialized embeddingService

# -----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    "worker initialization and cleanup (only w/ 'graceful' shutdown), requests handled @ 'yield'"
    global args, es, supported_models
    my_pid = getpid()
    logging.info(f"initializing worker {my_pid}, default model: '{args.model}'")

    lawk = FileLock(dbcp.WORKER_PIDS_LOCK, timeout = ACQUIRE_LOCK_TIMEOUT)
    try:
        lawk.acquire()
    except Timeout:
        logging.error(f"worker {my_pid}: acquire WORKER_PIDS_LOCK timed-out after "
                f"{ACQUIRE_LOCK_TIMEOUT}s")
        return # TODO: what else should be done?
    with open(dbcp.WORKER_PIDS_FILE, "a") as wfp:
        print(my_pid, file=wfp)
    lawk.release()
    es = EmbeddingService(args)
    yield

# -----------------------------------------------------------------------------

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=args.cors_origin,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
@app.post("/")
async def embed(
        document: Annotated[str, Form()],
        background_tasks: BackgroundTasks,
        model_name: str = args.model,
        read_cache: bool = True,
        emb_type: str = "sentence",
        write_cache: bool = True
) -> Response:
    """
    takes 1 www-x-form-urlencoded field, "document" and returns an embedding.
    takes several optional query parameters:
    * model_name: other than default or from command-line arg
    * read_cache: 0 or 1, check cache for embedding, compute on miss
    * emb_type: "sentence" or "word" (word not yet supported)
    * write_cache: cache computed emb if not already cached
    emb response sent as soon as it's available, then if write_cache is true, writes cache in BG
    """
    global es, supported_models
    if model_name not in supported_models:
        raise HTTPException(status_code=422,
                        detail=f'model_name "{model_name}" not found in list of supported models')
    if emb_type != "sentence":
        if emb_type == "word":
            raise HTTPException(status_code=501, detail="word embeddings not yet implemented")
        raise HTTPException(status_code=422,
                        detail='emb_type must be one of {"sentence","word"}, got: "{emb_type}"')

    message, to_write = es.get_embeddings(document, model_name, read_cache)

    if write_cache and isinstance(to_write, list):
        background_tasks.add_task(es.write_embeddings, *to_write)
    return Response(content=message.tobytes(), media_type="application/octet-stream")

# -----------------------------------------------------------------------------
def remove_lock_files(stale: bool = False) -> None:
    "removes old filelocks left from crash, forced server stop, or normal shutdown"
    # TODO: investigate whether ungraceful shutdown can corrupt cache files
    # TODO: investigate whether this can be done with a context manager
    lock_dir = EmbeddingService.get_lock_dirpath()
    lock_files = list(Path(lock_dir).glob("**/*.lock"))
    for lf in lock_files:
        if stale:
            logging.warning(f'removing stale lock file "{lf}" from previous run')
        else:
            logging.info(f'removing lock file "{lf}"')
        lf.unlink()

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    "main function called when script is run, parsed cmdline args, inits logging, then starts app with uvicorn"
    ull = args.log_level.lower()
    print(f"uvicorn log level: '{ull}'", file=stderr)

    EmbeddingService.setup_models_dirs(models_cfg)
    remove_lock_files(stale=True)

    dbc = dbcp(args)
    dbc.start()

    uvicorn.run(
        "server:app",
        host=args.host,
        log_level=ull,
        port=args.port,
        reload = (args.workers < 2),
        workers=args.workers,
    )
    dbc.join()
    remove_lock_files(stale=False)

