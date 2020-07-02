#!/usr/bin/env python
import os
import mmh3
import argparse
import shutil
from tqdm.auto import tqdm
import logging


logger = logging.getLogger(__name__)


def setup_logging(logger):
    logger.setLevel(logging.INFO)

    log_formatter = logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)


def start_move(task_fold, folder_list, done_fold, seed):
    logger.info('Folders in progress...')
    for folder in tqdm(folder_list):
        dir_path = os.path.join(done_fold, str(mmh3.hash(folder, seed) % 500), folder)
        if not os.path.exists(dir_path):
            shutil.move(os.path.join(task_fold, folder),
                        os.path.join(dir_path))
    logger.info('All folders copied!')


def make_dirs(fold_path):
    for i in range(500):
        if not os.path.exists(os.path.join(fold_path, str(i))):
            os.mkdir(os.path.join(fold_path, str(i)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--seed', default=42,
                        help='Seed for hash.')
    parser.add_argument('--tasks-dir', default='arxiv_tmp',
                        help='Folder where older folders are stored.')
    parser.add_argument('--done-dir', default='arxiv',
                        help='Folder with hashnamed folder.')
    args = parser.parse_args()

    setup_logging(logger)

    make_dirs(args.done_dir)

    start_move(args.tasks_dir, os.listdir(args.tasks_dir), args.done_dir, args.seed)
