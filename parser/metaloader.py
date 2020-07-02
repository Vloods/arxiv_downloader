#!/usr/bin/env python

import logging
import argparse
from utils import setup_logging
from utils import download
import os
import json
from mmh3 import hash


logger = logging.getLogger(__name__)


def load_meta(arx_path, seed):
    for data in download():
        for sample in data:
            try:
                path = os.path.join(arx_path, str(hash(sample.arxiv_id, seed) % 500), sample.arxiv_id)
                if not os.path.exists(path):
                    os.makedirs(path)
                filename = os.path.join(path, "{0}".format(sample.arxiv_id))
                if not os.path.exists(filename):
                    with open(filename, "w") as f:
                        json.dump({'date': sample.date,
                                   'id': sample.arxiv_id,
                                   'title': sample.title,
                                   'abstract': sample.abstract,
                                   'categories': sample.categories}, f, indent=4)
            except Exception as e:
                logger.info('{0}'.format(e))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log',
                        help='log filename')
    parser.add_argument('--seed', default=42,
                        help='Seed for hash.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Set logger mode to debug.')
    parser.add_argument('--arxiv-dir', default='arxiv',
                        help='Folder name where the final result will be stored.')
    args = parser.parse_args()

    if not os.path.exists(args.arxiv_dir):
        os.mkdir(args.arxiv_dir)

    setup_logging(logger, args)

    logger.info('Loading meta...')
    load_meta(args.arxiv_dir, args.seed)
    logger.info('Meta download completed successfully!')
