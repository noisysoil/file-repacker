# file-repacker
A tool to compress files from one directory to another.

If the specified files are of '.zip' or '.7z' extensions then they are recompressed to 7zip using the specified (default=9, maximum) LZMA2 compression level.

It performs in-memory processing of files thus no unnecessary intermediary disk operations are performed.

Configurable multi-processing is used to make use of available CPU threads / cores.

*Note* that if '.zip' and '.7z' files are specified to be (re)compressed and two files of the same name are present in the same directory (e.g. 'test.zip' and 'test.7z') then operating system 'first come first served' conditions apply.

## Installation
Requires Python 3.9 or above.

Create a virtual environment in the source directory and install dependencies:
```console
python3 -m venv venv
source venv/bin/activate
pip intstall --upgrade pip
pip intstall --upgrade setuptools
pip install -r requirements.txt
```

## Usage options
`python file-repacker.py --help`

## Example
Compress .zip, .7z and .txt files with extended logging information:

```console
python file-repacker.py --source_directory /<your>/<source>/<directory> --destination_directory /<your>/<destination>/<directory> --file_extensions_to_compress ".zip,.7z,.txt" --log_level INFO
```
