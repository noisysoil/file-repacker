#!/usr/bin/env python3
import argparse
from enum import Enum, auto
import os
import io
import shutil
import tempfile
import datetime
import py7zr
import zipfile
import multiprocessing
import logging


class FileOperations(Enum):
    DO_NOTHING = auto()
    COPY = auto()


compression_filters = [{"id": py7zr.FILTER_LZMA2, "preset": 9}]
max_file_size = 500000000


def human_readable_bytes(bytes_to_print):
    the_bytes = float(bytes_to_print)
    kilobytes = float(1024)
    megabytes = float(kilobytes ** 2)  # 1,048,576
    gigabytes = float(kilobytes ** 3)  # 1,073,741,824
    terabytes = float(kilobytes ** 4)  # 1,099,511,627,776

    if the_bytes < kilobytes:
        return f"{the_bytes:0.0f} Bytes"
    elif kilobytes <= the_bytes < megabytes:
        return f"{(the_bytes / kilobytes):0.2f} KB"
    elif megabytes <= the_bytes < gigabytes:
        return f"{(the_bytes / megabytes):0.2f} MB"
    elif gigabytes <= the_bytes < terabytes:
        return f"{(the_bytes / gigabytes):0.2f} GB"
    elif terabytes <= the_bytes:
        return f"{(the_bytes / terabytes):0.2f} TB"


def process_files(filename: str, current_relative_path: str, current_path: str, destination_directory: str, file_extensions_to_compress: tuple[str], the_semaphore: multiprocessing.Semaphore):

    process_pid = os.getpid()

    # Set post-processing default to copy file; this can be changed by any intermediate processing.
    # If any exceptions occur in the compression libraries, the process will default copy.
    post_file_processing = FileOperations.COPY

    file_details = os.path.splitext(filename)
    file_basename = file_details[0]
    file_extension = file_details[1].casefold()
    file_size = os.path.getsize(f"{current_path}{filename}")
    logging.info(f":PID-{process_pid}: + Processing file [{current_path}{filename}] {human_readable_bytes(file_size)}")

    # Temporary (zero-legth read) in-memory file as py7zr currently cannot add an empty file from an existing archive.
    empty_file = io.BytesIO(b'0')
    empty_file.seek(1)
 
    if file_size > max_file_size:
        logging.warning(f":PID-{process_pid}: Direct copy of file [{current_path}{filename}] as file size of {human_readable_bytes(file_size)} exceeds configuration maximum of {human_readable_bytes(max_file_size)}.")
        post_file_processing = FileOperations.COPY
    elif file_extension in file_extensions_to_compress:
        logging.info(f":PID-{process_pid}: Compressing: [{current_path}{file_basename}{file_extension}]")
        destination_archive_filename = f"{destination_directory}{current_relative_path}{file_basename}.7z"
        compressed_archive = py7zr.SevenZipFile(destination_archive_filename, mode="w", filters=compression_filters)
        # -- Check if file is 7zip.
        if file_extension == ".7z":
            try:
                source_archive = py7zr.SevenZipFile(f"{current_path}{filename}", mode="r")
                for archive_info in source_archive.list():
                    logging.info(f":PID-{process_pid}: Archive content in [{filename}]: /{archive_info.filename}")
                    # Currently py7zr doesn't support adding empty directories directly, so use tmpfs (https://github.com/miurahr/py7zr/issues/412).
                    if archive_info.is_directory is True:
                        with tempfile.TemporaryDirectory(prefix="file-repacker-") as temp_file:
                            compressed_archive.write(temp_file, f"{archive_info.filename}")
                    elif archive_info.uncompressed == 0:
                        compressed_archive.writef(empty_file, f"{archive_info.filename}")
                    else:
                        binary = source_archive.read([archive_info.filename])
                        compressed_archive.writef(binary.get(archive_info.filename), archive_info.filename)
                        source_archive.reset()
                logging.info(f":PID-{process_pid}: Recompressed 7zip: [{destination_archive_filename}]")
                post_file_processing = FileOperations.DO_NOTHING
            # Catch-all for any library quirks.
            except Exception:
                logger.exception(f":PID-{process_pid}: Exception in processing 7zip file: [{current_path}{filename}] Copying archive instead.")
                post_file_processing = FileOperations.COPY
            finally:
                source_archive.close()
                compressed_archive.close()
        # -- Check if file is Zip.
        elif file_extension == ".zip":
            try:
                source_archive = zipfile.ZipFile(f"{current_path}{filename}", mode="r")
                for archive_info in source_archive.infolist():
                    logging.info(f":PID-{process_pid}: Archive content in [{filename}]: /{archive_info.filename}")
                    # Currently py7zr doesn't support adding empty directories directly, so use tmpfs (https://github.com/miurahr/py7zr/issues/412).
                    if archive_info.is_dir() is True:
                        with tempfile.TemporaryDirectory(prefix="file-repacker-") as temp_file:
                            compressed_archive.write(temp_file, f"{archive_info.filename}")
                    elif archive_info.file_size == 0:
                        compressed_archive.writef(empty_file, f"{archive_info.filename}")
                    else:
                        binary = io.BytesIO(source_archive.read(name=archive_info.filename))
                        compressed_archive.writef(binary, archive_info.filename)
                logging.info(f":PID-{process_pid}: Recompressed zip: [{destination_archive_filename}]")
                post_file_processing = FileOperations.DO_NOTHING
            # Catch-all for any library quirks.
            except Exception:
                logger.exception(f":PID-{process_pid}: Exception in processing Zip file: [{current_path}{filename}] Copying archive instead.")
                post_file_processing = FileOperations.COPY
            finally:
                source_archive.close()
                compressed_archive.close()
        # Otherwise compress files with any specified extensions.
        else:
            compressed_archive.write(f"{current_path}{filename}", arcname=filename)
            post_file_processing = FileOperations.DO_NOTHING
            compressed_archive.close()

    # Post-processing operations.
    if post_file_processing == FileOperations.COPY:
        destination_archive_filename = f"{destination_directory}{current_relative_path}{filename}"
        logger.info(f":PID-{process_pid}: Copying from [{current_path}{filename}] to target [{destination_archive_filename}]")
        shutil.copy(f"{current_path}{filename}", destination_archive_filename, follow_symlinks=True)

    empty_file.close()
    destination_file_size = os.path.getsize(f"{destination_archive_filename}")
    logger.info(f":PID-{process_pid}: - Process completed on source file [{current_path}{filename}] {human_readable_bytes(file_size)} to destination [{destination_archive_filename}] {human_readable_bytes(destination_file_size)}")
    the_semaphore.release()


def compress_files(source_directory: str, destination_directory: str, processes: int, file_extensions_to_compress: tuple[str]):
    start_time = datetime.datetime.now().replace(microsecond=0)
    number_of_files = 0

    multiprocessing_manager = multiprocessing.Manager()
    pool_semaphore = multiprocessing_manager.BoundedSemaphore(processes)
    multiprocessing_pool = multiprocessing.Pool(processes)

    for path, directories, files in os.walk(source_directory):
        path = f"{path.rstrip('/')}/"
        logging.info(f"-> Current path: [{path}]")
        current_relative_path = path.replace(source_directory, "").lstrip("/")
        logging.info(f"-> Current relative path: [{current_relative_path}]")
        os.makedirs(f"{destination_directory}{current_relative_path}", exist_ok=True)

        for the_file in files:
            number_of_files += 1
            if pool_semaphore.acquire(blocking=True):
                logging.info(f"--> Spawning process for file [{current_relative_path}{the_file}]")
                multiprocessing_pool.apply_async(process_files, kwds={
                    "filename": the_file,
                    "current_relative_path": current_relative_path,
                    "current_path": path,
                    "destination_directory": destination_directory,
                    "file_extensions_to_compress": file_extensions_to_compress,
                    "the_semaphore": pool_semaphore})

    multiprocessing_pool.close()
    multiprocessing_pool.join()

    total_time = (datetime.datetime.now().replace(microsecond=0) - start_time)
    logging.info(f"---- Done!  Total time taken: {total_time} for {number_of_files} files. ----")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File repacker.  By https://github.com/noisysoil/file-repacker")
    parser.add_argument("-s", "--source_directory", type=str, help="Source directory.  Required.", required=True)
    parser.add_argument("-d", "--destination_directory", type=str, help="Destination directory.  Required.", required=True)
    parser.add_argument("-c", "--compression_level", type=int, help="7z compression level (default 9).", default=9, required=False)
    parser.add_argument("-p", "--processes", type=int, help="Number of process to use (default reported CPU thread cores -1)", required=False)
    parser.add_argument("-m", "--max_file_size", type=int, help="Maximum file size in bytes to consider for processing (default 500 megabytes).", default=500000000, required=False)
    parser.add_argument("-f", "--file_extensions_to_compress", type=lambda argument: tuple(str(item.lstrip().casefold()) for item in argument.split(',')), help="Comma-delimited list of file extensions to compress (e.g. \".zip,.txt\").", default=".7z,.zip,.lnx,.col,.int", required=False)
    parser.add_argument("-l", "--log_level", type=str, help="Log level ('INFO','WARN' or 'ERROR') default 'WARN'.", default="WARN", required=False)
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(format='%(asctime)s - %(levelname)s * %(message)s', datefmt="%d/%m/%Y %H:%M:%S", level=args.log_level)

    file_extensions_to_compress = args.file_extensions_to_compress
    logging.info(f"File extensions marked for compression: {args.file_extensions_to_compress}")

    if args.processes is None or args.processes == 0:
        args.processes = max(os.cpu_count() - 1, 1)
        logging.info(f"Auto-setting number of multiprocessing threads to: {args.processes}.")

    os.makedirs(args.destination_directory, exist_ok=True)
    logging.info(f"Created destination directory: [{args.destination_directory}]")

    # Sanitize trailing slashes.
    args.source_directory = f"{args.source_directory.rstrip('/')}/"
    args.destination_directory = f"{args.destination_directory.rstrip('/')}/"

    free_space_on_destination = shutil.disk_usage(args.destination_directory).free
    logging.info(f"Free space on destination: {human_readable_bytes(free_space_on_destination)}.")

    compression_filters[0]['preset'] = args.compression_level
    max_file_size = args.max_file_size
    compress_files(source_directory=args.source_directory, destination_directory=args.destination_directory, processes=args.processes, file_extensions_to_compress=file_extensions_to_compress)

    logging.info("Finished compressing files.")
