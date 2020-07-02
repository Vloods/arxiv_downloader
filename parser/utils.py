import re
import time
import logging
import requests
import xml.etree.cElementTree as ET


def setup_logging(logger, args):
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    log_formatter = logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    if args.log is not None:
        log_file_handler = logging.FileHandler(args.log)
        log_file_handler.setFormatter(log_formatter)
        logger.addHandler(log_file_handler)


# Download constants
resume_re = re.compile(r".*<resumptionToken.*?>(.*?)</resumptionToken>.*")
url = "http://export.arxiv.org/oai2"

# Parse constant
record_tag = ".//{http://www.openarchives.org/OAI/2.0/}record"
format_tag = lambda t: ".//{http://arxiv.org/OAI/arXiv/}" + t
date_fmt = "%a, %d %b %Y %H:%M:%S %Z"


def download(start_date=None, max_tries=10):
    params = {"verb": "ListRecords", "metadataPrefix": "arXiv"}
    if start_date is not None:
        params["from"] = start_date

    failures = 0
    while True:
        # Send the request.
        r = requests.post(url, data=params)
        code = r.status_code

        # Asked to retry
        if code == 503:
            to = int(r.headers["retry-after"])
            logging.info("Got 503. Retrying after {0:d} seconds.".format(to))

            time.sleep(to)
            failures += 1
            if failures >= max_tries:
                logging.warning("Failed too many times...")
                break

        elif code == 200:
            failures = 0

            # Write the response to a file.
            content = r.text
            yield parse(content)

            # Look for a resumption token.
            token = resume_re.search(content)
            if token is None:
                logging.warning("Cant find resumption token in response: %s", content)
                break
            token = token.groups()[0]

            # If there isn't one, we're all done.
            if token == "":
                logging.info("All done.")
                break

            logging.debug("Resumption token: {0}.".format(token))

            # If there is a resumption token, rebuild the request.
            params = {"verb": "ListRecords",
                      "resumptionToken": token,}

            # Pause so as not to get banned.
            to = 20
            logging.debug("Sleeping for {0:d} seconds so as not to get banned."
                          .format(to))
            time.sleep(to)

        else:
            # Wha happen'?
            r.raise_for_status()


class Meta:
    def __init__(self,
                 arxiv_id,
                 date,
                 title,
                 abstract,
                 categories):
        self.arxiv_id = arxiv_id
        self.date = date
        self.title = title
        self.abstract = abstract
        self.categories = categories


def parse(xml_data):
    tree = ET.fromstring(xml_data)
    results = []
    for i, r in enumerate(tree.findall(record_tag)):
        try:
            arxiv_id = r.find(format_tag("id")).text
            date = r.find(format_tag("created")).text
            title = r.find(format_tag("title")).text
            abstract = r.find(format_tag("abstract")).text
            categories = r.find(format_tag("categories")).text
        except:
            logging.error("Parsing of record failed:\n{0}".format(r))
        else:
            results.append(Meta(''.join(arxiv_id.split('/')), date, title, abstract, categories))
    return results
