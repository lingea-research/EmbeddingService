# Embedding Service

Embedding service is a python server used for computing word or sentence embeddings.

It can also cache the computed embeddings on a disc, which can drastically speed up large computations with embeddings.

## How it works?

Right now there are several main components:

1. **HTTP server** for handling the requests and calling the embedding library
2. **Embedding library** which is responsible for delivering the embeddings using a selected *model*.
3. **Database** is a simple SQLite DB with currently a single table with two columns: *documentHash* (string) and *offset* (integer). The *offset* states the order of the embeddings within the cache. So that the 1st embedding has offset 0, 50th embedding has offset 49, etc.
4. **Cache** is a binary file for storing already computed embeddings.

### What happens when you send a request to the server?

1. User sends a sentence (single string) and wants their embeddings computed.
2. Embedding library computes a hash of the sentence and looks into the database if the hash is present in the table.
3. If it is present in the table, read the associated *offset*.
    1. Now it's crucial to findout what is the size of the embedding (current case is 512), and size of the data type (current case is 4 byte float). In our case, each embedding has 512*4=2048 bytes.
    2. The program opens the cache and performs *seek* action to the position of the embedding within the cache. So, if we're seeking the embedding with *offset*=46, we seek 46*2048 bytes forward, and read the following 2048 bytes - which is our embedding.
    3. Now you need to read those bytes into the respective data structure used for the embeddings. Usually a numpy array.
    4. Library returns the read embedding and server responds.
4. Otherwise, the library calls the model to compute the embeddings from the input sentence.
    1. Then, the embedding in a binary form is appended to the end of the cache file. The program needs to find out how many embeddings there are in the cache to determine the offset.
    2. New (*documentHash*, *offset*) row is inserted into the database.
    3. Embedding is returned and server responds.

## Installation

1. Clone the repository.\
   *python 3.12 is currently not supported, recommend 3.11*
2. Create a python virtual environment (optional).
3. Install the requirements: `pip install -r requirements.txt`
4. Run the server: `$ python3 server.py`

Running the Installation steps above installs everything except PyTorch, *which is required*. If you want to support Nvidia or AMD GPUs (and possibly Mac M1 GPUs), install your platform-specific version of PyTorch. It's also possible to run in CPU-only mode.\
Refer to [Get Started | PyTorch](https://pytorch.org/get-started/) for further instructions, as the following seems to change:

**AMD ROCm**

`./venv/bin/pip3 install torch --index-url https://download.pytorch.org/whl/rocm5.6`

**Nvidia CUDA**

`./venv/bin/pip3 install torch`

**CPU-only**

`./venv/bin/pip3 install torch --index-url https://download.pytorch.org/whl/cpu`

## <a id="ds"></a>Directory Structure - IndexDB (.db) & Cache file (.bin)
The user can specify the directory where they want index database and cached embeddings files to be stored ("./data/" by default), with the
*--data-dir* option. These are stored per model, one index and one cache:
```
data
├── sentence-transformers_distiluse-base-multilingual-cased-v2
│   ├── embeddings.bin
│   └── indexDatabase.db
├── model_2
│   ├── embeddings.bin
│   └── indexDatabase.db
└── model_n
    ├── embeddings.bin
    └── indexDatabase.db
```
*note that model names will be normalized in order not to cause issues with directory paths. Path separators "/" & "\\" will be converted into "_"*
## Server

* http://localhost:8009
* Method: POST
* application/www-x-form-urlencoded
* **document**=*This is a sentence to encode.*
* query parameters: **read_cache** & **write_cache**, 0 = false & 1 = true (default)

## Models

Supported models are described in the `models.txt` file. Each model descriptions consists of:
* the model's name (loadable from SentenceTransformers)
* embedding dimensions
* whether the model should be preloaded (1 or 0).

### Supported models

| Model | Dimension | Loads automatically |
| --- | --- | --- |
| sentence-transformers/distiluse-base-multilingual-cased-v2 | 512 | Yes |


## Use as a library

If you want to use the embedding service as a library, check out the **test.py** file to find out how.
