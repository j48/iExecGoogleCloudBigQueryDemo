from contextlib import redirect_stdout
from csv import writer as csv_writer, QUOTE_MINIMAL
from hashlib import sha256
from json import load as json_load, dump as json_dump
from os import getenv
from sys import argv as sys_argv
from time import time

from eth_abi import encode_single
from eth_utils import to_hex
from google.cloud import bigquery

iexec_root = '/'
iexec_in = getenv('IEXEC_IN') or f'{iexec_root}iexec_in'
iexec_out = getenv('IEXEC_OUT') or f'{iexec_root}iexec_out'
dataset = getenv("IEXEC_DATASET_FILENAME") or 'bigquery.json'
dataset_loc = f'{iexec_in}/{dataset}'

deterministic_file = 'result.txt'  # file used for deterministic comparison

default_coins = ['BTC', 'ETH']  # will always be in query
max_input = 10  # default number of user input/coins (does not include default)


class ErrorCallback(Exception):
    messages = {
        1: "no dataset file found",
        2: "credentials file not found or corrupted",
        3: "general bigquery query error",
        4: "error creating csv",
        5: "auto callback test error",
        6: "general dapp error"
    }

    def __init__(self, code):
        self.code = code
        self.msg = self.messages[self.code]

    def __str__(self):
        return repr(self.msg)


def stdout(txt):
    # used for TEE log since regular stdout is not saved (use instead of print)
    # adding try for sanity test (will try iexec_out first)
    with open(f'{iexec_out}/log.txt', 'a') as f:
        with redirect_stdout(f):
            print(f'{txt}')


def create_receipt(txt):
    # optional to generate receipt for records
    with open(f'{iexec_out}/receipt.txt', 'w+') as f:
        f.write(txt)


def create_error_file():
    # create an error file so results can be filtered
    # I guess no csv would indicate an error too
    with open(f'{iexec_out}/ERROR.txt', 'w+') as f:
        f.write('')


def create_deterministic(txt):
    # text string converted to sha256 hash
    # generating the deterministic file is user preference but must be consistent
    sha = sha256(str(txt).encode('utf-8')).hexdigest()
    deterministic_loc = f'{iexec_out}/{deterministic_file}'

    with open(deterministic_loc, 'w+') as f:
        f.write(sha)

    return deterministic_loc


def create_csv(rows):
    cvs_data = {"header": ["coin", "price", "cap", "date"], "coins": {}}

    for row in rows:
        # can filter results here or in JS
        if row.cap != 0:
            date_key = row.date.strftime('%m-%d-%Y')
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
                    writer.writerow([coin, coin_date] + cvs_data['coins'][coin][coin_date])

    except Exception:
        return False

    return True


def analyze_user_input(argv):
    # takes user input then formats in a way that's readable for dapp logic
    # main purpose is to prevent exploits and user error

    dapp_input_valid = []
    input_count = 0

    try:
        dapp_input = argv[1:]
    except IndexError:
        dapp_input = None

    # for testing purposes
    if 'E5CB' in dapp_input:
        return 5

    for arg in dapp_input:
        if arg.isalnum():
            if len(arg) < 6:
                input_count += 1
                if input_count > max_input:
                    break
                dapp_input_valid.append(arg.upper())

    return sorted(list(set(dapp_input_valid)))


def get_dataset_table(loc):
    # can have table hardcoded or in dataset (should dataset name be a secret?)
    try:
        with open(loc, 'r') as f:
            data = json_load(f)
            return data['dataset']
    except Exception:
        return False


def create_error_callback(error_code):
    # easier error logging of TEE dapp to avoid IPFS upload
    # error should be in a format easily determined by smart contract/web3
    # https://eth-abi.readthedocs.io/en/latest/encoding.html
    # The value is stored in the resultsCallback field of the Task object in the IexecProxy smart contract.
    error_args = error_code
    error_bytes = encode_single('uint8', error_args)

    return to_hex(error_bytes)


def create_computed_json(dic):
    with open(f'{iexec_out}/computed.json', 'w+') as f:
        json_dump(dic, f, indent=2)


if __name__ == '__main__':
    computed = {}
    callback = None
    deterministic = None

    stdout(f'Start: {str(time())}')
    stdout(f'Max Input: {max_input}')
    stdout(f'Default Coins: {default_coins}')
    stdout(f'Input: {sys_argv}')

    user_input_valid = analyze_user_input(sys_argv)

    stdout(f'Valid Input: {user_input_valid}')
    stdout(f'running dapp...')

    try:
        if user_input_valid == 5:
            raise ErrorCallback(5)

        coins = sorted(list(set(user_input_valid + default_coins)))
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
        except Exception as e:
            raise ErrorCallback(3)

        results = list(q)
        stdout(f'results received...')

        csv_success = create_csv(results)
        if csv_success:
            stdout(f'data.csv created...')
        else:
            raise ErrorCallback(4)

        create_receipt(
            f'Google Cloud - Big Query Receipt\n'
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
        deterministic = create_deterministic(str(q.query))
        stdout(f'deterministic file created...')

    except ErrorCallback as e_callback:
        # callback = create_error_callback(e_callback.code)
        stdout(f'ERROR: ({e_callback.code}) {e_callback.msg}')

    except Exception:
        # callback = create_error_callback(6)
        stdout(f'ERROR: (6) general dapp error')

    # without a deterministic the worker freezes
    # cant combo deterministic with callback error or replace...
    # must always upload to IPFS on error?
    # leave for to-do list
    if deterministic:
        computed["deterministic-output-path"] = deterministic
    else:
        deterministic = create_deterministic('error')
        stdout(f'deterministic error file created...')
        computed["deterministic-output-path"] = deterministic
        create_error_file()
        stdout(f'error file created...')

    if callback:
        #  computed["callback-data"] = callback
        pass

    create_computed_json(computed)

    stdout(f'Done: {str(time())}')
