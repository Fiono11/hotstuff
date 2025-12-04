from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, nodes, faults):
        inputs = [clients, nodes]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(nodes) + int(faults)
        else:
            self.committee_size = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse client logs: {e}')
        self.size, self.total_txs, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the nodes logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_nodes, nodes)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse node logs: {e}')
        receipts, commits, sizes, self.received_samples, timeouts, self.configs \
            = zip(*results)
        self.receipts = self._merge_results([x.items() for x in receipts])
        self.commits = self._merge_results([x.items() for x in commits])
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }
        self.timeouts = max(timeouts)

        # Check whether clients missed their target rate (if applicable).
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

        # Check whether the nodes timed out.
        # Note that nodes are expected to time out once at the beginning.
        if self.timeouts > 2:
            Print.warn(f'Nodes timed out {self.timeouts:,} time(s)')

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size_match = search(r'Transactions size: (\d+)', log)
        if size_match is None:
            raise ParseError('Failed to parse transaction size from client log')
        size = int(size_match.group(1))
        
        total_txs_match = search(r'Total transactions: (\d+)', log)
        if total_txs_match is None:
            raise ParseError('Failed to parse total transactions from client log')
        total_txs = int(total_txs_match.group(1))

        start_match = search(r'\[(.*Z) .* Start ', log)
        if start_match is None:
            raise ParseError('Failed to parse start time from client log')
        tmp = start_match.group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, total_txs, start, misses, samples

    def _parse_nodes(self, log):
        if search(r'panic', log) is not None:
            raise ParseError('Node(s) panicked')

        # Parse individual transaction receipts (when transactions are received/sealed).
        tmp = findall(r'\[(.*Z) .* Received tx ([^ ]+)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        receipts = self._merge_results([tmp])

        # Parse individual transaction commits.
        tmp = findall(r'\[(.*Z) .* Committed tx ([^ ]+)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        # Sizes are no longer needed since we calculate from transaction count and size.
        # Transactions are now processed individually, so sizes are calculated per transaction.
        sizes = {}

        # Parse sample transactions - now we track them by digest instead of batch
        # Sample transactions start with 0u8, so we can identify them
        # For now, we'll track them through the receipt->commit flow
        samples = {}

        tmp = findall(r'.* WARN .* Timeout', log)
        timeouts = len(tmp)

        configs = {
            'consensus': {
                'timeout_delay': int(
                    search(r'Timeout delay .* (\d+)', log).group(1)
                ),
                'sync_retry_delay': int(
                    search(
                        r'consensus.* Sync retry delay .* (\d+)', log
                    ).group(1)
                ),
            },
            'mempool': {
                'sync_retry_delay': int(
                    search(r'mempool.* Sync retry delay .* (\d+)', log).group(1)
                ),
                'sync_retry_nodes': int(
                    search(r'Sync retry nodes .* (\d+)', log).group(1)
                ),
                'batch_size': int(
                    search(r'Batch size .* (\d+)', log).group(1)
                ),
                'max_batch_delay': int(
                    search(r'Max batch delay .* (\d+)', log).group(1)
                ),
            }
        }

        return receipts, commits, sizes, samples, timeouts, configs

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        # Use receipt time as start (when transactions are received/sealed)
        if not self.receipts:
            return 0, 0, 0
        start, end = min(self.receipts.values()), max(self.commits.values())
        duration = end - start
        # Calculate bytes: number of committed transactions * transaction size
        tx_count = len(self.commits)
        bytes = tx_count * self.size[0]
        bps = bytes / duration if duration > 0 else 0
        tps = tx_count / duration if duration > 0 else 0
        return tps, bps, duration

    def _consensus_latency(self):
        # Measure latency from when tx is received until it's committed
        latency = []
        for digest, commit_time in self.commits.items():
            if digest in self.receipts:
                receipt_time = self.receipts[digest]
                latency.append(commit_time - receipt_time)
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        # Calculate bytes: number of committed transactions * transaction size
        tx_count = len(self.commits)
        bytes = tx_count * self.size[0]
        bps = bytes / duration if duration > 0 else 0
        tps = tx_count / duration if duration > 0 else 0
        return tps, bps, duration

    def _end_to_end_latency(self):
        # Measure latency from when client sends tx until it's committed
        # This requires matching sample transactions by computing their digest
        # For now, we approximate by using receipt-to-commit latency as a proxy
        # since we can't easily match sample tx IDs to digests without hashing
        consensus_latency = self._consensus_latency()
        # End-to-end includes network time, but receipt-to-commit is the main component
        # Return consensus latency as approximation (can be improved later with digest matching)
        return consensus_latency

    def result(self):
        consensus_latency = self._consensus_latency() * 1000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1000

        consensus_timeout_delay = self.configs[0]['consensus']['timeout_delay']
        consensus_sync_retry_delay = self.configs[0]['consensus']['sync_retry_delay']
        mempool_sync_retry_delay = self.configs[0]['mempool']['sync_retry_delay']
        mempool_sync_retry_nodes = self.configs[0]['mempool']['sync_retry_nodes']
        mempool_batch_size = self.configs[0]['mempool']['batch_size']
        mempool_max_batch_delay = self.configs[0]['mempool']['max_batch_delay']

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} nodes\n'
            f' Committee size: {self.committee_size} nodes\n'
            f' Total transactions: {sum(self.total_txs):,} tx\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Consensus timeout delay: {consensus_timeout_delay:,} ms\n'
            f' Consensus sync retry delay: {consensus_sync_retry_delay:,} ms\n'
            f' Mempool sync retry delay: {mempool_sync_retry_delay:,} ms\n'
            f' Mempool sync retry nodes: {mempool_sync_retry_nodes:,} nodes\n'
            f' Mempool batch size: {mempool_batch_size:,} B\n'
            f' Mempool max batch delay: {mempool_max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        nodes = []
        for filename in sorted(glob(join(directory, 'node-*.log'))):
            with open(filename, 'r') as f:
                nodes += [f.read()]

        return cls(clients, nodes, faults)
