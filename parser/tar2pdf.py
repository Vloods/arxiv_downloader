#!/usr/bin/env python
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import logging
import argparse
from utils import setup_logging
import os
import tarfile
from tqdm.auto import tqdm


logger = logging.getLogger(__name__)


def closed(pdf_dir):
    pdf_path = pdf_dir

    def on_created(event):
        try:
            tar = tarfile.open(event.src_path)
            for member in tar.getmembers():
                if not member.isdir():
                    try:
                        fname = member.name.split('/')[-1]
                        tar.makefile(member, pdf_path + '/' + fname)
                    except:
                        pass
            logger.info('%s extracted to %s' %(event.src_path.split('/')[-1], pdf_path))
        except:
            pass
    return on_created


def extract_old(src_dir, dst_dir, list):
    for tar in tqdm(list, desc='Extracting old files...'):
        filename = os.path.join(src_dir, tar)
        if not os.path.exists(filename):
            try:
                cur_tar = tarfile.open(filename)
                for member in cur_tar.getmembers():
                    if not member.isdir():
                        try:
                            fname = member.name.split('/')[-1]
                            cur_tar.makefile(member, dst_dir + '/' + fname)
                        except:
                            pass
            except:
                pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log',
                        help='Log filename.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Set logger mode to debug.')
    parser.add_argument('--tar-dir', default='tar',
                        help='Folder name where tars are stored for converting to pdf')
    parser.add_argument('--arxiv-dir', default='arxiv',
                        help='Folder name where the final result will be stored.')
    parser.add_argument('--done-dir', default='pdf',
                        help='Folder name where tasks will be stored for converting to txt.')
    args = parser.parse_args()

    old_files = os.listdir(args.tar_dir)

    if not os.path.exists(args.done_dir):
        os.mkdir(args.done_dir)
    if not os.path.exists(args.tar_dir):
        os.mkdir(args.tar_dir)

    setup_logging(logger, args)

    path = args.tar_dir
    patterns = "*"
    ignore_patterns = ""
    ignore_directories = True
    case_sensitive = True
    event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)

    event_handler.on_created = closed(args.done_dir)

    go_recursively = True
    observer = Observer()
    observer.schedule(event_handler, path, recursive=go_recursively)
    observer.start()

    logger.info('Waiting for tars...')

    extract_old(args.tar_dir, args.done_dir, old_files)
    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
