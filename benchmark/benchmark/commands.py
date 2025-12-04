from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node keys --filename {filename}'

    @staticmethod
    def run_node(keys, committee, store, parameters, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters}')

    @staticmethod
    def run_client(timeout, total_txs, nodes=[], account=None, secret=None):
        assert isinstance(timeout, int) and timeout > 0
        assert isinstance(total_txs, int) and total_txs > 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        nodes_str = ' '.join(f'--nodes {node}' for node in nodes) if nodes else ''
        account_str = f'--account {account}' if account else ''
        secret_str = f'--secret {secret}' if secret else ''
        return (f'./client --timeout {timeout} --total-txs {total_txs} {nodes_str} {account_str} {secret_str}').strip()

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'client')
        return f'rm node ; rm client ; ln -s {node} . ; ln -s {client} .'
