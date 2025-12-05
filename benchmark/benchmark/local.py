import subprocess
import json
from math import ceil
from os.path import basename, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters, ConfigError
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


# Hardcoded accounts from ledger initialization
HARDCODED_ACCOUNTS = [
    {
        "name": "LIUvl4zY4nG/TZIvlQLGaUryKflf+eqY8VDWPDRT8WM=",
        "secret": "TlHt5/RGIQyEfu8hyrPUzIpHPPoyB4hJoxhBDyUPJn8shS+XjNjicb9Nki+VAsZpSvIp+V/56pjxUNY8NFPxYw=="
    },
    {
        "name": "1HxzAJYTzgqFV8uewbFqmw0Z/vBa7P9fk/4VLWWz9NM=",
        "secret": "2C82Yf/z8xSmvv7FfgG3sdet2RtLN04dJ+G+wWs3ePfUfHMAlhPOCoVXy57BsWqbDRn+8Frs/1+T/hUtZbP00w=="
    },
    {
        "name": "tWBeZcYDe00SSTbFn5R+LyZhK3426IWiZym+LmE8K6c=",
        "secret": "MAtBualo+k1BRdR+wyHzyxOrDXqInztmbLcrP6/9hV21YF5lxgN7TRJJNsWflH4vJmErfjbohaJnKb4uYTwrpw=="
    },
    {
        "name": "Rx91kiXjP2BbfrqNspKwwJQZqxVEcZwQZlSysQ6LkI0=",
        "secret": "OfGqJckMMgOaRw3t4CAHCNpwXq1lT/2Y41pudBbQQhxHH3WSJeM/YFt+uo2ykrDAlBmrFURxnBBmVLKxDouQjQ=="
    },
]


def write_hardcoded_key_file(filename, account_index):
    """Write a key file using a hardcoded account (cycles through the 4 accounts)."""
    account = HARDCODED_ACCOUNTS[account_index % len(HARDCODED_ACCOUNTS)]
    with open(filename, 'w') as f:
        json.dump(account, f, indent=4)


class LocalBench:
    BASE_PORT = 9000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            nodes = self.nodes[0]

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            # Build from workspace root to ensure all binaries (including ledger) are built
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd='..')

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files using hardcoded accounts.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for i, filename in enumerate(key_files):
                write_hardcoded_key_file(filename, i)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Do not boot faulty nodes.
            nodes = nodes - self.faults

            # Initialize ledger for each node's store.
            Print.info('Initializing ledger for each node...')
            dbs = [PathMaker.db_path(i) for i in range(nodes)]
            for db in dbs:
                cmd = CommandMaker.init_ledger(db)
                subprocess.run(cmd.split(), check=True)

            # Run a single client that sends all transactions to all nodes.
            addresses = committee.front
            timeout = self.node_parameters.timeout_delay
            cmd = CommandMaker.run_client(
                timeout,
                self.total_txs,
                nodes=addresses,  # Send to all nodes
                account='LIUvl4zY4nG/TZIvlQLGaUryKflf+eqY8VDWPDRT8WM=',
                secret='TlHt5/RGIQyEfu8hyrPUzIpHPPoyB4hJoxhBDyUPJn8shS+XjNjicb9Nki+VAsZpSvIp+V/56pjxUNY8NFPxYw=='
            )
            self._background_run(cmd, PathMaker.client_log_file(0))

            # Run the nodes.
            node_logs = [PathMaker.node_log_file(i) for i in range(nodes)]
            for key_file, db, log_file in zip(key_files, dbs, node_logs):
                cmd = CommandMaker.run_node(
                    key_file,
                    PathMaker.committee_file(),
                    db,
                    PathMaker.parameters_file(),
                    debug=debug
                )
                self._background_run(cmd, log_file)

            # Wait for the nodes to synchronize
            Print.info('Waiting for the nodes to synchronize...')
            sleep(2 * self.node_parameters.timeout_delay / 1000)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_nodes()

            # Log final accounts and balances for each node.
            Print.heading('\nFinal accounts and balances:')
            for i, db in enumerate(dbs):
                Print.info(f'\nNode {i} (store: {db}):')
                cmd = CommandMaker.query_ledger(db)
                subprocess.run(cmd.split(), check=False)  # Don't fail if query fails

            # Parse logs and return the parser.
            Print.info('\nParsing logs...')
            return LogParser.process('./logs', faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)
