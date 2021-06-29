from time import sleep
from pathlib import Path
from csv import DictReader
from .utils import logger
from json import dumps, load, dump
from .connections import VaradaRest
from click import exceptions
from .configuration import Connection


def apply(con: Connection, json_path: Path = None, csv_path: Path = None):
    try:
        if json_path:
            fd = open(json_path)
            rules_file = load(fd)
        elif csv_path:
            fd = open(csv_path, mode='r')
            rules_file = DictReader(fd)
        else:
            logger.exception(f'No valid CSV or JSON to read from')
            raise exceptions.Exit(code=1)

        with VaradaRest(con=con) as varada_rest:
            if csv_path:
                for rule in rules_file:
                    rule['priority'] = int(rule['priority'])
                    if rule['predicates']:
                        # Convert predicates to list of dict(s), cleanup None str (multi predicates), convert to int if needed
                        if 'DateRangeSlidingWindow' in rule['predicates']:
                            rule['predicates'] = [{k: v for k, v in [pair.split(':') for pair in rule['predicates'].split(',')]}]
                            rule['predicates'][0]['startRangeDaysBefore'] = int(rule['predicates'][0]['startRangeDaysBefore'])
                            rule['predicates'][0]['endRangeDaysBefore'] = int(rule['predicates'][0]['endRangeDaysBefore'])
                        elif 'PartitionValue' in rule['predicates']:
                            if None in rule.keys():
                                # more than one predicate
                                rule[None].insert(0, rule['predicates'])
                                rule['predicates'] = rule[None]
                                del rule[None]
                                rule['predicates'] = [{k: v for k, v in [pair.split(':') for pair in predicate.split(',')]} for predicate in rule['predicates']]
                            else:
                                rule['predicates'] = [{k: v for k, v in [pair.split(':') for pair in rule['predicates'].split(',')]}]
                    logger.info(f'Setting rule: {rule}')
                    varada_rest.set_warmup_rule(json_data=rule)
            elif json_path:
                for rule in rules_file:
                    varada_rest.set_warmup_rule(json_data=rule)
    finally:
        fd.close()


def get(con: Connection, table, column, destination_dir):
    with VaradaRest(con=con) as varada_rest:
        all_rules_str = varada_rest.get_warmup_rules()
        if column:
            if not table:
                logger.info(f'Missing table for column {column}, please run with -t TABLE_NAME')
                exit()
            else:
                logger.info(f'Getting rules for table {table}, column {column}')
                rules_str = [rule for rule in all_rules_str if rule['table'] == table and rule['colNameId'] == column]
        elif table:
            logger.info(f'Rules for table {table}:')
            rules_str = [rule for rule in all_rules_str if rule['table'] == table]
        else:
            rules_str = all_rules_str
        if destination_dir:
            logger.info(f'Saving rules to {destination_dir}/rules.json')
            with open(f'{destination_dir}/rules.json', 'w') as fd:
                dump(rules_str, fd, indent=4)
        else:
            logger.info(dumps(rules_str, indent=4))


def delete(con: Connection, rule_ids: str = None, all_rules: bool = False):
    """
    Delete rule from the cluster
    """
    with VaradaRest(con=con) as varada_rest:
        if rule_ids:
            logger.info(f'Deleting rule(s): {rule_ids}')
            [varada_rest.del_warmup_rule(int(rule_id)) for rule_id in rule_ids.split(',')]
        elif all_rules:
            logger.info('Deleting all rules from the cluster')
            all_rules_str = varada_rest.get_warmup_rules()
            [varada_rest.del_warmup_rule(int(rule['id'])) for rule in all_rules_str]