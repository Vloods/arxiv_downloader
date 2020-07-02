#!/usr/bin/env python
import json
import os
import numpy as np
import pandas as pd
import logging
import argparse
from tqdm.auto import tqdm
from shutil import copy2
from random import sample


logger = logging.getLogger(__name__)
physics_categories = ['astro-ph', 'cond-mat', 'gr-qc', 'hep-ex', 'hep-lat', 'hep-ph',
        'hep-th', 'math-ph', 'nlin', 'nucl-ex', 'nucl-th', 'physics',
        'quant-ph']


def setup_logging(logger):
    logger.setLevel(logging.INFO)

    log_formatter = logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)


def get_meta(arx_path):
    meta = []
    errors = 0
    logger.info('Meta is loading...')
    samples = os.listdir(arx_path)
    for sample in tqdm(samples):
        if len([name for name in os.listdir(os.path.join(arx_path, sample))]) == 3:
            with open(os.path.join(arx_path, sample, sample)) as f:
                tmp = json.load(f)
                meta.append([tmp['id'], tmp['categories'].split()])
        else:
            errors += 1
    logger.info('Meta is ready! Current size is {}. Failed: {}'.format(len(meta), errors))
    return meta


def distance_between_papers(p1, p2):
    def distance_between_cats(c1, c2):
        cats1 = c1.split('.')  # Example: 'q-bio.PE' -> main:'q-bio'  subj:'PE' .
        cats2 = c2.split('.')
        if c1 == c2: # If cats equals. (hep-ph, hep-ph)
            return 0
        if cats1[0] == cats2[0]:  # If cats have equals main cat. (math.AG, math.AT)
            return len(cats1) + len(cats2) - 2
        if cats1[0] != cats2[0] and cats1[0] in physics_categories and cats2[0] in physics_categories:
                                                                        # If cat haven't equals main cat but both have
                                                                        # root 'physics_categories'.(astro-ph, hep-lat)
            return len(cats1) + len(cats2)
        if cats1[0] != cats2[0]: # If cat haven't equals cat.(q-bio, math.CO)
            return len(cats1) + len(cats2) + 1

    res_distance = 0
    """
    For each categories in target we calculate average of the sum of the distances between current category 
    and all categories in another article.
    """
    for cat1 in p1[1]:
        mean_dist = 0
        for cat2 in p2[1]:
            mean_dist += distance_between_cats(cat1, cat2)
        res_distance += mean_dist / len(p2[1])

    return res_distance / len(p1[1])


def triplet_rand(meta, samp):
    count = 0
    while True:
        p, n = np.random.randint(0, len(meta), 2)
        dist_p = distance_between_papers(samp, meta[p])
        dist_n = distance_between_papers(samp, meta[n])
        if samp[0] != meta[p][0] and samp[0] != meta[n][0] and meta[p][0] != meta[n][0] and dist_p != dist_n:
            if dist_p > dist_n:
                return {'target': samp[0], 'context': meta[n][0], 'negative': meta[p][0],
                        'dist_to_context': round(dist_n, 1), 'dist_to_negative': round(dist_p, 1)}
            else:
                return {'target': samp[0], 'context': meta[p][0], 'negative': meta[n][0],
                        'dist_to_context': round(dist_p, 1), 'dist_to_negative': round(dist_n, 1)}
        count += 1
        if count > 15:
            break


def triplet_range(low_s, low_f, high_s, high_f):
    low_s = low_s
    low_f = low_f
    high_s = high_s
    high_f = high_f

    def closed(meta, samp):
        count = 0
        pos = neg = None
        dist_p = dist_n = 0
        repeat_list = []
        while True:
            indexes = np.random.randint(0, len(meta), 1000)
            for i in indexes:
                if meta[i][0] != samp[0]:
                    dist = distance_between_papers(samp, meta[i])
                    if low_s <= dist <= low_f and pos is None:
                        pos = meta[i]
                        dist_p = dist
                    elif high_s <= dist <= high_f and neg is None:
                        neg = meta[i]
                        dist_n = dist
                    if pos is not None and neg is not None:
                        if [pos[0], neg[0]] not in repeat_list:
                            repeat_list.append([pos[0], neg[0]])
                            return {'target': samp[0], 'context': pos[0], 'negative': neg[0],
                                    'dist_to_context': round(dist_p, 1), 'dist_to_negative': round(dist_n, 1)}
            count += 1
            if count > 15:
                break

    return closed


def get_triplets(meta, repeat, size, mode):
    bar = tqdm(total=size if size is not None else len(meta) * repeat)
    triplets = []

    maker = triplet_rand
    if mode == 'range':
        maker = range_trip

    for samp in meta:
        for i in range(repeat):
            triplet = maker(meta, samp)
            if triplet is not None:
                triplets.append(triplet)
                bar.update(1)
            if len(triplets) == size:
                return triplets
    bar.close()

    return triplets


def make_triplets(repeat, low_s=None, low_f=None, high_s=None, high_f=None):
    low_s = low_s
    low_f = low_f
    high_s = high_s
    high_f = high_f
    repeat = repeat

    def closed(meta, triplet_path, size, mode):
        if mode == 'random':
            if not os.path.exists(os.path.join(triplet_path,
                                               'triplets.jsonl')):
                save_as_json(get_triplets(meta, repeat, size, mode), os.path.join(triplet_path, 'triplets.jsonl'))
            else:
                logger.info('File is already exist!')

        elif mode == 'range':
            if low_s is None or low_f is None or high_s is None or high_f is None:
                logger.info('Set all parameters!')
                return
            if not os.path.exists(os.path.join(triplet_path,
                                               'triplets_range({}-{}_{}-{}).jsonl'.format(low_s, low_f, high_s,
                                                                                          high_f))):
                save_as_json(get_triplets(meta, repeat, size, mode),
                             os.path.join(triplet_path,
                                          'triplets_range({}-{}_{}-{}).jsonl'.format(low_s, low_f, high_s, high_f)))
            else:
                logger.info('File is already exist!')

    return closed


def save_as_json(triplets, triplets_path):
    with open(triplets_path, 'w') as f:
        for triplet in triplets:
            json.dump(triplet, f)
            f.write('\n')
    logger.info('Triplets successfully saved!(Count: {})({})'.format(len(triplets), triplets_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--arxiv-dir', default='arxiv',
                        help='Folder name where stored papers(meta, txt, pdf).')
    parser.add_argument('--triplets-dir', default='triplets',
                        help='Folder name where triplets are stored in .json format.')
    parser.add_argument('--repeat', default=5, type=int,
                        help='Count of samples for each target.')
    parser.add_argument('--modes', nargs='+', default=['random', 'range'],
                        help='List of modes.')
    parser.add_argument('--pos-dist-s', default=0, type=int,
                        help='Min distance for context.')
    parser.add_argument('--pos-dist-f', default=2, type=int,
                        help='Max distance for context.')
    parser.add_argument('--neg-dist-s', default=3, type=int,
                        help='Min distance for negative sample.')
    parser.add_argument('--neg-dist-f', default=5, type=int,
                        help='Max distance for negative sample.')
    parser.add_argument('--seed', default=42, type=int,
                        help='Seed for random.')
    parser.add_argument('--shuffle', default=True, action='store_true',
                        help='Shuffle your meta.')
    parser.add_argument('--size', default=None, type=int,
                        help='Count of triplets.(May be less if meta size is smaller'
                             ' or if for some sample no pair was found on the range.)')
    args = parser.parse_args()

    setup_logging(logger)

    np.random.seed(args.seed)

    if not os.path.exists(args.arxiv_dir):
        os.mkdir(args.arxiv_dir)
    if not os.path.exists(args.triplets_dir):
        os.mkdir(args.triplets_dir)

    if not (args.pos_dist_s <= args.pos_dist_f < args.neg_dist_s <= args.neg_dist_f):
        logger.info('Pos distance must not cross with neg distance or be greater.')
        os.system(exit(0))

    modes = args.modes

    meta = get_meta(args.arxiv_dir)
    if args.shuffle:
        meta = sample(meta, len(meta))

    range_trip = triplet_range(args.pos_dist_s, args.pos_dist_f, args.neg_dist_s, args.neg_dist_f)
    start = make_triplets(args.repeat, args.pos_dist_s, args.pos_dist_f, args.neg_dist_s, args.neg_dist_f)

    for mode in modes:
        start(meta, args.triplets_dir, args.size, mode)

    logger.info('All triplets are ready!')
