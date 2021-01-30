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


iexec_in = getenv('IEXEC_IN') or './iexec_in'
iexec_out = getenv('IEXEC_OUT') or './iexec_out'
deterministic = 'result.txt'  # file used for deterministic comparison
error_callback = True  # handle errors with callback or log.txt

default_coins = ['BTC', 'ETH']  # will always be in query
max_input = 10  # default number of user input/coins (does not include default)
credentials = 'credentials.json'  # can set as env


class ErrorCallback(Exception):
    messages = {
        1: "no dataset file found",
        2: "credentials file not found or corrupted",
        3: "general bigquery query error",
        4: "error creating csv",
        5: "auto callback test error"
    }

    def __init__(self, code):
        self.code = code
        self.msg = self.messages[self.code]

    def __str__(self):
        return repr(self.msg)


def stdout(txt):
    # used for TEE log since regular stdout is not saved (use instead of print)
    with open(f'{iexec_out}/log.txt', 'a') as f:
        with redirect_stdout(f):
            print(f'{txt}')


def add_callback(cb):
    # https://docs.soliditylang.org/en/v0.5.3/abi-spec.html
    with open(f'{iexec_out}/computed.json', 'w+') as f:
        json_dump({'callback-data': cb}, f, indent=2)


def create_receipt(txt):
    # optional to generate receipt for records
    with open(f'{iexec_out}/receipt.txt', 'w+') as f:
        f.write(txt)


def create_deterministic(txt):
    # text string converted to sha256 hash
    # generating the deterministic file is user preference but must be consistent
    sha = sha256(str(txt).encode('utf-8')).hexdigest()

    with open(f'{iexec_out}/{deterministic}', 'w+') as f:
        f.write(sha)

    with open(f'{iexec_out}/computed.json', 'w+') as f:
        json_dump({'deterministic-output-path': f'{iexec_out}/{deterministic}'}, f, indent=2)


def create_csv(rows):
    cvs_data = {'header': ['coin', 'price', 'cap', 'date'], 'coins': {}}

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
        raise ErrorCallback(4)


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
        raise ErrorCallback(5)

    for arg in dapp_input:
        if arg.isalnum():
            if len(arg) < 6:
                input_count += 1
                if input_count > max_input:
                    break
                dapp_input_valid.append(arg.upper())

    return sorted(list(set(dapp_input_valid)))


def get_dataset_table():
    # can have table hardcoded or in dataset (should dataset name be a secret?)
    try:
        with open(f'{iexec_in}/bigquery.json', 'r+') as f:
            data = json_load(f)
            return data['dataset']
    except Exception:
        raise ErrorCallback(1)


def create_error_callback(error_code):
    # easier error logging of TEE dapp to avoid IPFS upload
    # error should be in a format easily determined by smart contract/web3
    # https://eth-abi.readthedocs.io/en/latest/encoding.html
    # The value is stored in the resultsCallback field of the Task object in the IexecProxy smart contract.
    error_args = error_code
    error_bytes = encode_single('uint8', error_args)

    return to_hex(error_bytes)


if __name__ == '__main__':
    stdout(f'Start: {str(time())}')
    stdout(f'Max Input: {max_input}')
    stdout(f'Default Coins: {default_coins}')
    stdout(f'Input: {sys_argv}')

    try:
        user_input_valid = analyze_user_input(sys_argv)

        stdout(f'Valid Input: {user_input_valid}')
        stdout(f'running dapp...')

        coins = sorted(list(set(user_input_valid + default_coins)))
        coins_sql = ', '.join(f'"{c}"' for c in coins)
        table = get_dataset_table()
        sql = (
            f'SELECT coin, price, cap, date '
            f'FROM `{table}` '
            f'WHERE coin '
            f'IN ({coins_sql}) '
            f'ORDER BY coin, date ASC'
        )

        try:
            if credentials:
                client = bigquery.Client.from_service_account_json(f'{iexec_in}/{credentials}')
            else:
                client = bigquery.Client()

            q = client.query(sql)
            stdout(f'querying API...')
            q.result()  # this is where API is queried

        except FileNotFoundError:
            raise ErrorCallback(2)
        except Exception:
            raise ErrorCallback(3)
        else:
            results = list(q)
            stdout(f'results received...')
            create_csv(results)
            stdout(f'data.csv created...')

            create_receipt(
                f'Google Cloud - Big Query Receipt\n'
                f'Job ID: {q.job_id}\n'
                f'Job Type: {q.job_type}\n'
                f'Created: {q.created}\n'
                f'Location: {q.location}\n'
                f'Project: {q.project}\n'
                f'Query: {q.query}\n'
                f'Results: {len(results)}\n'
                f'Bytes Processed: {q.total_bytes_processed}\n'
                f'Bytes Billed: {q.total_bytes_billed}\n'
                f'Ended: {q.ended}\n'
                f'ETag: {q.etag}'
            )
            stdout(f'receipt.txt created...')

            # can use unique per task or generic
            create_deterministic(str(q.query))
            stdout(f'deterministic file created...')

    except ErrorCallback as e_callback:
        stdout(f'*****ERROR******: ({e_callback.code}) {e_callback.msg}')
        if error_callback:
            callback = create_error_callback(e_callback.code)
            add_callback(callback)
        else:
            create_deterministic("ERROR")

    stdout(f'Done: {str(time())}')
