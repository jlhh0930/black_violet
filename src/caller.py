import logging
import sys
import argparse
from datetime import date
from dateutil.relativedelta import relativedelta

from black_violet.src.utils.AWSUtils import send_hst_msg
from utils.ConfigUtils import config
import utils.AWSUtils as AWSUtils

from pipelines.etl import replication
from pipelines.tableau import hyper
from pipelines.analytics import data_modeling, extracts


def main(logger):
    logger.info('Running Process {}.{} for {} environment'.format(config.pipeline, config.job, config.env))

    if not config.job or config.job in ['replication']:
        try:
            with replication.Replication(config, logger) as rep:
                rep.replication()
                logger.info('Replication is done.')
        except Exception as e:
            err_msg = 'Replication failed:\n\n{e}'.format(e=e)
            logger.error(err_msg)
            send_hst_msg(f'Replication failure in {config.env}', err_msg, config)
            raise  e

    if config.job in ['tableau', 'hyper']:
        try:
            with hyper.TableauHyper(config, logger) as t:
                t.tableau()
                logger.info('Tableau is done.')
        except Exception as e:
            err_msg = 'Tableau failed:\n\n{e}'.format(e=e)
            logger.error(err_msg)
            raise e

    if config.job == 'extracts':
        logger.info('Extracts block empty but reached.')

    if config.job == 'analytics':
        logger.info('Analytics block empty but reached.')

    if config.job == 'development':
        logger.info('Development block empty but reached.')


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s-12s - %(levelname)s-8s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(config.severity_level)

    logger.info('------------START PROCESS------------')

    parser = argparse.ArgumentParser(description='Running Process Arguments')

    try:
        parser.add_argument('-env', dest='env', help='Abbreviated environment name, i.e. local, dev, prep, prod')
        parser.add_argument('-pipeline', dest='pipeline', help='Simple pipeline name, i.e. etl, analytics, tableau.')
        parser.add_argument('-job', dest='job', help='Specific step or job to run',
                            choices=['replication', 'tableau', 'hyper', 'extracts', 'analytics', 'development'])
        parser.add_argument('-client', dest='client', help='Specific client, optional.')
        parser.add_argument('-start_date', dest='start_date', help='Optional, provide start date YYYY-MM-DD.')
        parser.add_argument('-end_date', dest='end_date', help='Optional, provide end date YYYY-MM-DD.')
        parser.add_argument('-publish', dest='publish', default=True, help='Optional, publish results to s3 and / or client SFTP.')

        args = parser.parse_args()

        config.load_config()
        logger.info('Config loaded.')

        if not args.env or args.env.lower().strip() not in ['local', 'dev', 'prep', 'prod']:
            err_msg = 'Invalid environment provided.'
            raise Exception(err_msg)
        else:
            config.set_env(args.env.lower().strip())
            logger.info('Environment provided: {env}.'.format(env=config.env))

        if args.pipeline:
            config.set_pipeline(args.pipeline.lower().strip())
            logger.info('Pipeline provided: {pipeline}.'.format(pipeline=args.pipeline))

        if args.start_date and args.end_date:
            config.set_start_date(args.start_date.strip())
            config.set_end_date(args.end_date.strip())
        elif not args.start_date and not args.end_date:
            logger.info('No start date and end date provided.')
        else:
            # Default start_date, three months back
            start_date = date.today().replace(day=1) - relativedelta(months=3)
            # Default end_date, last day of last month
            end_date = date.today().replace(day=1) - relativedelta(days=1)
            config.set_start_date(start_date)
            config.set_end_date(end_date)

        config.set_publish(args.publish)
        logger.info('Publish set to {}'.format(config.publish))

        config.set_job(args.job)
        config.get_extract_details()
        config.set_extracts()
        config.set_states()

    except Exception as e:
            logger.error('Argument parsing failed.')
            logger.error(e)
            raise e

    main(logger)

    logger.info('------------END PROCESS------------')