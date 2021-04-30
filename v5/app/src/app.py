from contextlib import redirect_stdout
from csv import writer as csv_writer, QUOTE_MINIMAL
from hashlib import sha256
from json import load as json_load, dump as json_dump
from os import getenv
from sys import argv as sys_argv
from time import time

from google.cloud import bigquery

iexec_root = '/'
iexec_in = getenv('IEXEC_IN') or f'{iexec_root}iexec_in'
iexec_out = getenv('IEXEC_OUT') or f'{iexec_root}iexec_out'
dataset = 'bigquery.json'
dataset_loc = f'{iexec_in}/{dataset}'

deterministic_file = 'result.txt'  # file used for deterministic comparison

default_coins = ['BTC', 'ETH', 'DOGE', 'XRP', 'LTC', 'ADA', 'XMR', 'XLM', 'BNB', 'DOT']  # default if min user input
max_input = 20  # max number of user input/coins
min_input = 2  # min number of user input/coins


class ErrorCallback(Exception):
    messages = {
        1: "no dataset file found",
        2: "credentials file not found or corrupted",
        3: "general bigquery query error",
        4: "error creating csv",
        5: "auto callback test error",
        6: "general dapp error",
        7: "no results"
    }

    def __init__(self, code):
        self.code = code
        self.msg = self.messages[self.code]

    def __str__(self):
        return repr(self.msg)


def stdout(t):
    # used for TEE log since regular stdout is not saved (use instead of print)
    # adding try for sanity test (will try iexec_out first)
    with open(f'{iexec_out}/log.txt', 'a') as f:
        with redirect_stdout(f):
            print(f'{t}')


def create_receipt(t):
    # optional to generate receipt for records
    with open(f'{iexec_out}/receipt.txt', 'w+') as f:
        f.write(t)


def create_error_file(e):
    # create an error file so results can be filtered
    # I guess no csv would indicate an error too
    with open(f'{iexec_out}/ERROR.txt', 'w+') as f:
        f.write(e)


def create_computed_json(c):
    with open(f'{iexec_out}/computed.json', 'w+') as f:
        json_dump(c, f, indent=2)


def create_deterministic(t):
    # text string converted to sha256 hash
    # generating the deterministic file is user preference but must be consistent
    sha = sha256(str(t).encode('utf-8')).hexdigest()
    deterministic_loc = f'{iexec_out}/{deterministic_file}'

    with open(deterministic_loc, 'w+') as f:
        f.write(sha)

    computed = {"deterministic-output-path": deterministic_loc}

    create_computed_json(computed)

    return True


def create_csv(r):
    cvs_data = {"header": ["coin", "price", "cap", "date"], "coins": {}}

    for row in r:
        # can filter results here or in JS
        if row.cap != 0:
            date_key = row.date.strftime('%Y-%m-%d')
            # using date as key to filter out possible dupes
            try:
                # optional format here so it's easier to read in csv
                formatted_price = row.price
                formatted_cap = row.cap

                cvs_data['coins'][row.coin][date_key] = [formatted_price, formatted_cap]
            except KeyError:
                # easier way to create dict per coin
                cvs_data['coins'][row.coin] = {}

    try:
        with open(f'{iexec_out}/data.csv', 'w+', newline='') as csv_file:
            writer = csv_writer(csv_file, delimiter=',', quotechar='|', quoting=QUOTE_MINIMAL)
            writer.writerow(cvs_data['header'])  # write header first (optional?)

            for coin in cvs_data['coins']:
                for coin_date in cvs_data['coins'][coin]:
                    writer.writerow([coin,
                                     cvs_data['coins'][coin][coin_date][0],
                                     cvs_data['coins'][coin][coin_date][1],
                                     coin_date])
    except Exception:
        return False

    return True


def analyze_user_input(a):
    # takes user input then formats in a way that's readable for dapp logic
    # main purpose is to prevent exploits and user error

    dapp_input_valid = []
    input_count = 0

    try:
        dapp_input = a[1:]
    except IndexError:
        dapp_input = []

    for arg in dapp_input:
        if arg.isalnum():
            if len(arg) < 6:
                input_count += 1
                if input_count > max_input:
                    break
                dapp_input_valid.append(arg.upper())

    return dapp_input_valid


def get_dataset_table(j):
    # can have table hardcoded or in dataset (should dataset name be a secret?)
    try:
        with open(j, 'r') as f:
            data = json_load(f)
            return data['dataset']
    except Exception:
        return False


if __name__ == '__main__':
    error = None

    stdout(f'Start: {str(time())}')
    stdout(f'Max Input: {max_input}')
    stdout(f'Min Input: {min_input}')
    stdout(f'Default Coins: {default_coins}')
    stdout(f'Input: {sys_argv}')

    user_input_valid = analyze_user_input(sys_argv)

    stdout(f'Valid Input: {user_input_valid}')
    stdout(f'running dapp...')

    try:
        # for testing purposes
        if 'E5CB' in user_input_valid:
            raise ErrorCallback(5)

        # need a hardcoded coin list to avoid input of invalid coins

        coins = set(user_input_valid)

        while len(coins) < min_input:
            coins.add(default_coins.pop(0))

        coins = sorted(list(coins))
        coins_sql = ', '.join(f'"{c}"' for c in coins)

        table = get_dataset_table(dataset_loc)
        if not table:
            raise ErrorCallback(1)

        sql = (
            f'SELECT coin, price, cap, date '
            f'FROM `{table}` '
            f'WHERE coin '
            f'IN ({coins_sql}) '
            f'ORDER BY coin, date ASC'
        )

        try:
            # can set json location as env
            client = bigquery.Client.from_service_account_json(dataset_loc)
            q = client.query(sql)
            stdout(f'querying API...')
            q.result()  # this is where API is queried
        except FileNotFoundError:
            raise ErrorCallback(2)
        except Exception:
            raise ErrorCallback(3)

        results = list(q)
        if len(results) == 0:
            raise ErrorCallback(7)

        stdout(f'results received...')

        csv_success = create_csv(results)
        if not csv_success:
            raise ErrorCallback(4)

        stdout(f'data.csv created...')

        create_receipt(
            f'Google Cloud - BigQuery Receipt\n'
            f'Created: {q.created}\n'
            f'Job ID: {q.job_id}\n'
            f'Job Type: {q.job_type}\n'
            f'Location: {q.location}\n'
            f'Project: {q.project}\n'
            f'Query: {q.query}\n'
            f'Results: {len(results)}\n'
            f'Bytes Processed: {q.total_bytes_processed}\n'
            f'Bytes Billed: {q.total_bytes_billed}\n'
            f'ETag: {q.etag}\n'
            f'Ended: {q.ended}'
        )
        stdout(f'receipt.txt created...')

        # can use unique per task or generic
        create_deterministic(str(q.query))
        stdout(f'deterministic file created...')

    except ErrorCallback as e_callback:
        stdout(f'**ERROR**')
        error = f'{e_callback.code}) {e_callback.msg}'

    except Exception:
        stdout(f'**ERROR**')
        error = 'ERROR: (6) general dapp error'

    # without a deterministic the worker freezes
    # cant combo deterministic with callback error or replace
    # must always upload to IPFS on error?

    if error:
        deterministic = create_deterministic('error')
        stdout(f'deterministic error file created...')

        # error file for easier filtering
        create_error_file(error)
        stdout(f'error file created...')

    stdout(f'Done: {str(time())}')
