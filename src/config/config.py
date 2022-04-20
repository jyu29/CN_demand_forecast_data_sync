import os
import json
import logging.config
import argparse

# Single ref to configuration over the whole program
pgconf = None


def configure_logging(log_config_file):
    """
    Setup logging configuration (if file cannot be loaded or read, a fallback basic configuration is used instead)
    :param log_config_file: (string) path to external logging configuration file
    """
    if os.path.exists(log_config_file):
        with open(log_config_file, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.INFO)


def parse_data_ingestion_args():
    """
    Parse main program arguments to ensure everything is correctly launched
    :return: argparse object configured with mandatory and optional arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config-file', required=True,
                            help="Technical configuration file (YAML format)")
    parser.add_argument('-l', '--log-file', required=True,
                        help="External logging configuration file (JSON format)")
    parser.add_argument('-s', '--scope', required=False,
                        help="Choosing scope of the program")
    return parser.parse_args()


def basic_parse_args():
    """
    Parse main program arguments to ensure everything is correctly launched
    :return: argparse object configured with mandatory and optional arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config-file', required=True,
                        help="Technical configuration file (YAML format)")
    parser.add_argument('-l', '--log-file', required=True,
                        help="External logging configuration file (JSON format)")
    parser.add_argument('-s', '--scope', required=False,
                        help="Choosing scope of the program")
    parser.add_argument('-p', '--process', required=False,
                        help="Choosing process of the program (Cleaning / Refining / Full)")
    return parser.parse_args()