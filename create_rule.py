from requests import Request, Session
import os
import logging
import json
import sys
import getopt
import yaml
import errno
from pprint import pformat
from datetime import datetime
import asyncio
import aiohttp
import copy

config = yaml.safe_load(open("config.yaml"))
logging.basicConfig(level=logging.INFO)

SENTRY_BASE = 'https://sentry.io'
SLUG = config["org_slug"]
KEY = config["auth"]
_deletion_format = {"rules": {}, "applications": {}}


def main():

    try:
        opts, _ = getopt.getopt(sys.argv[1:], ":n", ["new"])
    except getopt.GetoptError as err:
        logging.info(err)
        sys.exit(2)

    project_slugs = fetch_projects(SLUG)
    logging.info(f'\033[93m Found {len(project_slugs)} projects for org. \033')
    project_and_rules = asyncio.run(fetch_project_rules(project_slugs))

    # flow control
    await_confirm = True
    manual_mode = True
    cli_option = None
    for o, _ in opts:
        if o in ("-n", "--new"):
            cli_option = o
            manual_mode = False
        else:
            assert False, "unrecognized option"

    while await_confirm:

        merge_option = cli_option if cli_option is not None else input(
            '\033[93m\n confirm merge: [ info | snapshot | restore | n | some | new | all ] \n\033[0m')
        if merge_option in ['all']:
            await_confirm = False
            write_db(project_and_rules, 'all')
            confirmation = input(
                f'\n\033[93m\n Confirm merge all?  [ no | yes ]\033[0m\n')
            if confirmation in ['y', 'yes']:
                asyncio.run(proceed_with_merge(project_and_rules, config))
            else:
                await_confirm = True

        elif merge_option in ['n']:
            await_confirm = False
            logging.info('\n\033[93m\n Exiting \033[0m\n')

        elif merge_option in ['snapshot']:
            write_db(project_and_rules, 'snapshot', is_snapshot=True)

        elif merge_option in ['restore']:
            await_confirm = False
            from_snapshot = input(
                '\n\033[93m\n Restore from: [ database | [my_filename].json ] \n\033[0m')

            history = restore_from_db() if from_snapshot in [
                'database'] else restore_from_db(from_snapshot)

            history = list(
                map((lambda h: {"name": h["name"], "rules": json.loads(h["rules"])}), history))
            config_source = {"rules": json.dumps(None)}
            asyncio.run(proceed_with_merge(history, config_source))

        elif merge_option in ['new', '--new', '-n']:
            await_confirm = False
            # assumes project has never had rules or been modified
            only_new = [p for p in project_and_rules if p["rules"] is None]

            if len(only_new) != 0:
                logging.info(
                    f'\n\033[93m\n Found {len(only_new)} new projects.\n {[p["name"] for p in only_new]} \033[0m\n')
                confirmation = 'y' if not manual_mode else input(
                    f'\n\033[93m\n This will merge scrubbing rules with projects:\n\n {pformat(only_new)}\n\n Confirm: [ n | y ]\033[0m\n')
                if confirmation in ['y']:
                    # if is a new project rules will be null. Providing a format that is restorable.
                    history = list(
                        map((lambda h: {"name": h["name"], "rules": json.dumps(_deletion_format)}), only_new))
                    write_db(history, 'new')
                    asyncio.run(proceed_with_merge(only_new, config))
                else:
                    print('Exiting')

        elif merge_option in ['info']:
            exploring = True
            idx = 0
            while exploring:
                try:
                    print(
                        f'\n\033[92m project: - {project_and_rules[idx]["name"]}\n\n existing_rules:\n\n {pformat(project_and_rules[idx]["rules"])}\033[0m')
                    progress = input(
                        '\033[93m \n[ (next | [return] ) | exit]\033[0m')
                    if progress in ['exit', 'e']:
                        exploring = False
                    elif progress in ['next', 'n', '']:
                        idx += 1
                    else:
                        exploring = False
                except IndexError as e:
                    exploring = False

        elif merge_option in ['some']:
            await_confirm = False
            project_names = input(
                '\n\033[93m\n Enter project slug(s):  \033[0m\n')
            one_off_data = [
                d for d in project_and_rules if d["name"] in project_names.split()]
            if len(one_off_data) != 0:
                confirmation = input(
                    f'\n\033[93m\n This will merge scrubbing rules with existing:\n\n {pformat(one_off_data)}\n\n Confirm: [ n | y ]\033[0m\n')
                if confirmation in ['y']:
                    write_db(one_off_data, 'some')
                    asyncio.run(proceed_with_merge(one_off_data, config))
                else:
                    print('Exiting')
            else:
                print(f'No projects provided. Please provide space separated slugs.')
        else:
            await_confirm = False
            print(f"Unrecognized Option [{merge_option}]")


async def proceed_with_merge(project_and_rules, config_source):
    for unmerged_data in project_and_rules:
        # Double encoding rules satisfies payload expectation from server.

        unmerged_data["rules"] = dedupe_merged(merge_rules(
            unmerged_data["rules"], json.loads(config_source["rules"]), unmerged_data))

    request = async_client(SENTRY_BASE, 'PUT', async_create_privacy_rule, {"proj_meta": project_and_rules, "headers": {
        'Authorization': f'Bearer {KEY}', 'Content-Type': 'application/json'}})
    await request()


def merge_rules(prev_rules, for_merge, meta):
    # stringified prev_rules rules, new rules dict, meta {"name":<str>,"rules":[]}
    # returns updated prev_rules (not stringified)
    if for_merge == _deletion_format:
        logging.info(
            f'\033[91m Deletion rule detected. This will merge project scrubbing settings with existing: [{meta["name"]}]\033[0m')
        confirmation = input(f'\n\033[91m Confirm deletion [ n , y ]: \033[0m')
        if confirmation in ['y']:
            return for_merge
        elif confirmation in ['n']:
            logging.info(
                f'\033[92m Deletion skipped. Using prev scrubbing settings for [{meta["name"]}]\033[0m\n')
            return json.loads(prev_rules)
        else:
            print("Aborting entire process. No changes made.")
            sys.exit(1)
    elif for_merge is None:
        # meant for restore from database case
        return prev_rules
    elif prev_rules is None:
        return for_merge
    else:
        rules_pending_update = json.loads(prev_rules)
        if not rules_pending_update["rules"]:
            return for_merge
        else:
            # Reindex new rules. Doesn't dedupe at this stage.
            last_idx = int(
                sorted(
                    list(rules_pending_update["rules"].keys()), key=int
                )[-1]
            ) + 1
            re_idx_rules = {}
            for wrong_idx, rule_set in for_merge["rules"].items():
                # reindex new rules based on greatest idx in prev rule body
                re_idx_rules[str(last_idx)] = rule_set

                # reindex applications section where needed for new item indices
                for target, target_idxs in for_merge["applications"].items():
                    if wrong_idx in target_idxs:
                        target_idxs.append(str(last_idx))
                        target_idxs.remove(wrong_idx)

                last_idx += 1

            # We now have additions dict with re-indixed rules to be merged
            # We need to loop through prev applications and merge re-indexed additions from for_merge.
            for target, _ in for_merge["applications"].items():
                if target in rules_pending_update["applications"]:
                    # add new rule idx to target[idx_array]
                    rules_pending_update["applications"][target].extend(
                        for_merge["applications"][target])
                else:
                    rules_pending_update["applications"][target] = []
                    rules_pending_update["applications"][target].extend(
                        for_merge["applications"][target])

            rules_pending_update["rules"].update(re_idx_rules)
            return rules_pending_update


def fetch_projects(slug):
    # https://docs.sentry.io/api/organizations/list-an-organizations-projects/

    PROJECT_API = f'/api/0/organizations/{slug}/projects/'
    url = SENTRY_BASE + PROJECT_API
    logging.info(
        f'\33[93m Confirmation required before changes are made. \n\n Fetching projects from org.\033[0m')
    t = client(url, 'GET', project_name_aggregator, {
        "headers": {'Authorization': f'Bearer {KEY}'}})
    return t()


async def fetch_project_rules(project_names):

    project_and_rules = async_client(SENTRY_BASE, 'GET', async_rule_aggregator, {
        "project": project_names, "headers": {'Authorization': f'Bearer {KEY}'}})
    return await project_and_rules()


def write_db(project_state, method, is_snapshot=False):
    logging.info(
        f'Backing up current rule state for projects selected with [ {method} ]')
    file_name = 'database'
    ext = '.json'
    # cases that have never been edited (null rulestate e.g. new) need rules to be converted to the deletion format for successful restoration. Need a copy to preserve original project state for 'new' option.
    state_copy = copy.deepcopy(project_state)
    for project in state_copy:
        if project["rules"] is None:
            project["rules"] = json.dumps(_deletion_format)
    try:
        if is_snapshot:
            file_name = 'snapshot_' + datetime.now().strftime("%m_%d_%Y_%H:%M:%S")

        with open(file_name + ext, 'w+') as out:
            json.dump(state_copy, out)
        logging.info(f'Back up success. Rules written to {file_name + ext}')
    except IOError as e:
        if e.errno == errno.ENOENT:
            print('File not found')
        elif e.errno == errno.EACCES:
            print('Permission denied')
            sys.exit(1)
        else:
            print(e)


def restore_from_db(snapshot=None):

    file_name = 'database.json'

    if snapshot is not None:
        file_name = snapshot
    try:
        logging.info(f'Restoring previous projects rule state in {file_name}')
        with open(file_name, 'r') as history:
            return json.load(history)
    except IOError as e:
        if e.errno == errno.ENOENT:
            logging.info('File not found')
        elif e.errno == errno.EACCES:
            logging.info('Permission denied')
        else:
            logging.info(e)


async def async_rule_aggregator(session, url, data):

    semaphore = asyncio.Semaphore(12)
    urls = [
        f'https://sentry.io/api/0/projects/{SLUG}/{p}/' for p in data['project']]
    tasks = [asyncio.create_task(
        async_get_existing_rules(url, data, semaphore)) for url in urls]

    responses = []
    d = []
    count = 0
    new = 0
    has_rules = []
    for task in tasks:
        responses.append(await task)

    for r in responses:
        count += 1
        try:
            slug = r["slug"]
            rules = r["options"]["sentry:relay_pii_config"]
            d.append({"name": slug, "rules": rules})
            # don't overcount empty rule body
            if rules not in [json.dumps(_deletion_format)]:
                has_rules.append(slug)

        except KeyError:
            d.append({"name": slug, "rules": None})
            new += 1

    logging.info(
        f'\n\n\33[93m [ {len(has_rules)} ] out of [ {count} ] have existing rules.\n[ {new} ] out of [ {count} ] are new or have never been modified.\033[0m')

    # logging.info(
    #     f'\33[93m Projects with rules: \n {has_rules}.\033[0m')
    return d


async def async_get_existing_rules(url, data, sem):

    async with sem:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=data["headers"]) as response:
                if response.status in [200]:
                    logging.info(
                        f'\033[92m [ READ ] Get rules {url} [{response.status}]\033[0m')

                else:
                    logging.error(
                        f'\033[91m Unable to [ READ ] rule for {url} [{response.status}]\033[0m')
                return await response.json()


def get_existing_rules(session, url, data):
    # logging.info(f'checking rules for: {url}')

    response = session.get(url, headers=data["headers"])
    if response.status_code in [200]:
        logging.info(f'\033[92m Success {url} [{response.status_code}]\033[0m')
        yield response.json()
    else:
        logging.error(
            f'\033[91m Unable to fetch rule for {url} [{response.status_code}]\033[0m')


def get_projects(session, url, data):
    # no fallback for failed requests or non 200
    logging.info(f'fetching projects for {url}')
    current_page = session.get(url, headers=data["headers"])
    if current_page.status_code in [200]:

        yield current_page.json()
    else:
        logging.error(
            f'\033[91m fetching project for {url} failed [{current_page.status_code}]\033[0m')
    # modify other session gets
    while current_page.links["next"]["results"] == "true":
        current_page = session.get(
            current_page.links["next"]["url"], headers=data["headers"])
        yield current_page.json()


def project_name_aggregator(session, url, data):
    p = []
    for project in get_projects(session, url, data):
        p.extend([value['slug'] for value in project])
    return p


def scrubbing_rule_aggregator(session, url, data):
    r = []
    count_existing = 0
    logging.info(f'\33[93m Aggregating existing project rules.\033[0m')
    for project_name in data['project']:
        API = url + f'/api/0/projects/{SLUG}/{project_name}/'

        for d in get_existing_rules(session, API, data):
            try:
                rules = d["options"]["sentry:relay_pii_config"]
                r.append({"name": project_name, "rules": rules})
                # don't overcount empty rule body
                if rules != json.dumps(_deletion_format):
                    count_existing += 1
            except KeyError:
                r.append({"name": project_name, "rules": None})
    existing = [p["name"] for p in r if p["rules"] is not None]
    logging.info(
        f'\33[93m Found [{count_existing}] projects with existing rules.\033[0m\n\n{existing}')

    return r


def client(url=SENTRY_BASE, method='GET', processor=None, data={}):
    '''
    Client params: resource base, http method, request processor, generic data (body, headers, etc.)
    '''
    session = Session()

    def run():

        try:

            if method == 'GET':
                return processor(session, url, data)
            elif method == 'PUT':
                return processor(session, url, data)
            else:
                logging.info('Unsupported http method specified')

        except Exception as e:
            raise

    return run


def async_client(url=SENTRY_BASE, method='GET', processor=None, data={}):
    '''
    Client params: resource base, http method, request processor, generic data (body, headers, etc.)
    '''
    session = None

    async def run():

        try:
            if method == 'GET':
                return await processor(session, url, data)
            elif method == 'PUT':
                return await processor(session, url, data)
            else:
                logging.info('Unsupported http method specified')

        except Exception as e:
            raise

    return run


def _prepare_rule_payload(rules):
    # expected payload
    return {"options": {"sentry:relay_pii_config": json.dumps(rules)}}


async def async_create_rule(url, data, sem):

    async with sem:
        async with aiohttp.ClientSession() as session:
            async with session.put(url, data=data["body"], headers=data["headers"]) as response:
                if response.status in [200]:
                    logging.info(
                        f'\033[92m [ WRITE ] Successful merge {url} [{response.status}]\033[0m')
                else:
                    logging.error(
                        f'\033[91m [ WRITE ] Merging failed {url}  [{response.status}]\033[0m')

                return await response.json()


async def async_create_privacy_rule(session, url, data):
    prepared = []
    responses = []
    semaphore = asyncio.Semaphore(12)

    if len(data["proj_meta"]) == 0:
        logging.info(
            'No project data provided. Perhaps check project slug passed to test run.')

    for rule_data in data["proj_meta"]:
        url = f'https://sentry.io/api/0/projects/{SLUG}/{rule_data["name"]}/'
        body = _prepare_rule_payload(rule_data["rules"])
        prepared.append(
            {"url": url, "data": {"body": json.dumps(body), "headers": data["headers"]}})

    tasks = [asyncio.create_task(
        async_create_rule(p["url"], p["data"], semaphore)) for p in prepared]

    for task in tasks:
        responses.append(await task)


def dedupe_merged(merged):
   # remove duplicate rules referenced within the same applications array

    for _, idx_arr in merged["applications"].copy().items():

        deduped = {}  # local to each applications array
        for idx in idx_arr.copy():
            rule = merged["rules"][idx]
            rule_key = str(rule)
            if rule_key in deduped:

                idx_arr.remove(idx)
                del merged["rules"][idx]

            else:
                deduped[rule_key] = True
    # this can create jumps in indices 1,2,4 if duplicate removed.
    return merged


if __name__ == '__main__':
    main()
