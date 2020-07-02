#!/usr/bin/env python
import os
import boto
import logging
import argparse
from utils import setup_logging
from lxml import etree
import shutil
import configparser
from tqdm import tqdm
import time

logger = logging.getLogger(__name__)


def read_manifest(manifest_filename):
    manifest_records = []
    tree = etree.parse(manifest_filename)
    for file_el in tree.findall('file'):
        file_info = {
            subel.tag: subel.text
            for subel in file_el
        }
        for field in ('size', 'seq_num', 'num_items'):
            file_info[field] = int(file_info[field])

        yymm = file_info.pop('yymm')
        file_info['month'] = ('19' if yymm[0] == '9' else '20') + yymm
        manifest_records.append(file_info)
    return manifest_records


def filter_archives(manifest_records, start_month, finish_month):
    filtered_records = []
    for record in manifest_records:
        month = record['month']
        if (start_month is None or month >= start_month) and (finish_month is None or month <= finish_month):
            filtered_records.append(record)
    return filtered_records


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log',
                        help='log filename')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Set logger mode to debug.')
    parser.add_argument('--xml-dir', default='xml',
                        help='Folder name where the final result will be stored.')
    parser.add_argument('--task-dir', default='task',
                        help='Folder name where tars are downloading.')
    parser.add_argument('--tar-dir', default='tar',
                        help='Folder name where tars are stored for extracting.')
    parser.add_argument('-s', '--start-month',
                        help='Date from which the download will begin.')
    parser.add_argument('-f', '--finish-month',
                        help='Date on which the download will end.')
    args = parser.parse_args()

    setup_logging(logger, args)

    if not os.path.exists(args.tar_dir):
        os.mkdir(args.tar_dir)
    if not os.path.exists(args.task_dir):
        os.mkdir(args.task_dir)

    configs = configparser.ConfigParser()
    configs.read('/run/secrets/keys')

    s3 = boto.connect_s3(configs['DEFAULT']['ACCESS_KEY'], configs['DEFAULT']['SECRET_KEY'])

    headers = {'x-amz-request-payer': 'requester'}
    arxiv_bucket = s3.get_bucket('arxiv', headers=headers)
    xml_path = os.path.join(args.xml_dir, 'arXiv_pdf_manifest.xml')

    if not os.path.exists(xml_path):
        if not os.path.exists(args.xml_dir):
            os.mkdir(args.xml_dir)

        logger.info('Downloading arxiv_pdf_manifest.xml from arxiv bucket')
        pdf_manifest_key = arxiv_bucket.get_key('pdf/arXiv_pdf_manifest.xml', headers=headers)
        pdf_manifest_key.get_contents_to_filename(xml_path, headers=headers)

    manifest_records = read_manifest(xml_path)

    selected_archives = filter_archives(manifest_records, args.start_month, args.finish_month)
    selected_archives.sort(key=lambda record: record['month'])

    last_path = None

    for archive in tqdm(selected_archives):
        archive_name = os.path.split(archive['filename'])[1]
        archive_task_path = os.path.join(args.task_dir, archive_name)
        archive_tar_path = os.path.join(args.tar_dir, archive_name)

        if not os.path.exists(archive_task_path) and not os.path.exists(archive_tar_path):
            number_of_attempts = 0
            stop = False
            while not stop:
                if number_of_attempts < 10:
                    try:
                        stop = True
                        if not (last_path is None):
                            shutil.move(last_path, args.tar_dir)
                        logger.info('%s: downloading from arXiv bucket' % archive_name)
                        archive_key = arxiv_bucket.get_key(archive['filename'], headers=headers)
                        archive_key.get_contents_to_filename(archive_task_path, headers=headers)
                        last_path = archive_task_path
                    except:
                        logger.info("Socket.timeout. Waiting 30s...")
                        time.sleep(30)
                        number_of_attempts += 1
                        stop = False
                else:
                    logger.error('Too many errors for %s' % archive_name)
                    stop = True
        else:
            logger.info('%s: local archive found' % archive_name)

    logger.info('Tar download completed successfully!')
