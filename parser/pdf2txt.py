#!/usr/bin/env python

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import logging
import argparse
import os
import shutil
from utils import setup_logging
from mmh3 import hash
import pdfparser.poppler as pdf
import ujson
from tqdm.auto import tqdm
import time


logger = logging.getLogger(__name__)


def pdf_convert(src_dir, dst_dir):
    try:
        if os.stat(src_dir).st_size == 0:
            time.sleep(5)
            if os.stat(src_dir).st_size == 0:
                return
        txt = []
        file_name = bytes(src_dir, "utf-8")
        d = pdf.Document(file_name, False)
        for p in d:
            page = {'page': []}
            for f in p:
                flow = {'flow': []}
                for b in f:
                    block = {'block': [], 'bbox': b.bbox.as_tuple()}
                    for l in b:
                        block['block'].append({'line': l.text, 'bbox': l.bbox.as_tuple()})
                    flow['flow'].append(block)
                page['page'].append(flow)
            txt.append(page)
        with open(dst_dir, 'w') as f:
            ujson.dump(txt, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error('Impossible convert %s to txt!' % filename)


def pdf_to_text(pdf, txt, seed):
    filename = (pdf.split('/')[-1])[:-4]
    if not os.path.exists(txt + '/' + str(hash(filename, seed)%500)):
        os.mkdir(txt + '/' + str(hash(filename, seed) % 500))
    try:
        txt_path = txt + '/' + str(hash(filename, seed) % 500) + '/' + filename + '/' + filename + '.json'
        if not os.path.exists(txt + '/' + str(hash(filename, seed) % 500) + '/' + filename):
            os.mkdir(txt + '/' + str(hash(filename, seed) % 500) + '/' + filename)
        pdf_convert(pdf, txt_path)
        logger.debug('%s converted.' % filename)
        return txt_path
    except Exception as e:
        logger.error('Impossible convert %s to txt!' % filename)


def closed(arx_path, seed):
    arx_dir = arx_path
    seed = seed

    def on_created(event):
        if event.src_path.split('/')[-1] not in old_files and os.path.exists(event.src_path):
            try:
                pdf_to_text(event.src_path, arx_dir, seed)
                filename = (event.src_path.split('/')[-1])[:-4]
                shutil.move(event.src_path, arx_dir + '/' + str(hash(filename, seed) % 500) + '/' + (filename))
            except Exception as e:
                logger.error(e)

    return on_created


def convert_old(src_dir, dst_dir, list, seed):
    for pdf_file in tqdm(list, desc='Converting old files'):
        filename = os.path.join(src_dir, pdf_file)
        if os.path.exists(filename):
            try:
                pdf_to_text(filename, dst_dir, seed)
                dirname = (filename.split('/')[-1])[:-4]
                shutil.move(filename, dst_dir + '/' + str(hash(dirname, seed) % 500) + '/' + (dirname))
            except Exception as e:
                print(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log',
                        help='Log filename.')
    parser.add_argument('--seed', default=42,
                        help='Seed for hash.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Set logger mode to debug.')
    parser.add_argument('--tasks-dir', default='pdf',
                        help='Folder name where tasks are stored for converting to txt')
    parser.add_argument('--arxiv-dir', default='arxiv',
                        help='Folder name where the final result will be stored.')
    args = parser.parse_args()

    setup_logging(logger, args)

    if not os.path.exists(args.tasks_dir):
        os.mkdir(args.tasks_dir)
    if not os.path.exists(args.arxiv_dir):
        os.mkdir(args.arxiv_dir)

    old_files = os.listdir(args.tasks_dir)

    path = args.tasks_dir
    patterns = "*"
    ignore_patterns = ""
    ignore_directories = True
    case_sensitive = True
    event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)

    event_handler.on_created = closed(args.arxiv_dir, args.seed)

    go_recursively = True
    observer = Observer()
    observer.schedule(event_handler, path, recursive=go_recursively)
    observer.start()

    convert_old(args.tasks_dir, args.arxiv_dir, old_files, args.seed)

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
