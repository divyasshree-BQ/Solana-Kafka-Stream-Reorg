import uuid
import base58
import threading
import signal
import time
import logging
import datetime
from collections import namedtuple

from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError

from solana import parsed_idl_block_message_pb2
import config

# Block metadata for chain/reorg bookkeeping. Identity is Hash (bytes), not Slot.
BlockInfo = namedtuple("BlockInfo", ("slot", "parent_hash"))

# =========================================================
# KAFKA CONFIG
# =========================================================
group_id_suffix = uuid.uuid4().hex

base_conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
    'group.id': f'{config.solana_username}-replay-{group_id_suffix}',
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_PLAINTEXT',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': config.solana_username,
    'sasl.password': config.solana_password,
    'enable.auto.commit': False,
}


topic = 'solana.transactions.proto'
# schema: https://github.com/bitquery/streaming_protobuf/blob/main/solana/parsed_idl_block_message.proto
# pb2 schema as a pypi package: https://pypi.org/project/bitquery-pb2-kafka-package/
# Reorg logic requires a single ordered stream; multiple consumers see interleaved blocks and trigger false reorgs.
NUM_CONSUMERS = 1

# Control flag for graceful shutdown
shutdown_event = threading.Event()
processed_count = 0
processed_count_lock = threading.Lock()

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# =========================================================
# REORG STATE & HELPERS
# =========================================================
# Schema reference (solana_messages / BlockMessage):
#   BlockHeader: Slot (uint64), Hash (bytes), ParentSlot (uint64), ParentHash (bytes), ...
#   BlockMessage: Header (BlockHeader), Rewards, Transactions
# Reorg logic uses only Header.Slot, Header.Hash, Header.ParentHash (identity is Hash, not Slot).
# Confirmed blocks are reversible. True identity is Hash, never Slot.
# chain: hash (bytes) -> BlockInfo(slot, parent_hash)
# tip_hash: current chain tip (bytes), or None before first block.
_chain: dict[bytes, BlockInfo] = {}
_tip_hash: bytes | None = None
_chain_lock = threading.Lock()


def _hash_bytes(header_hash) -> bytes:
    """Normalize header Hash field to bytes for use as dict key."""
    if isinstance(header_hash, bytes):
        return header_hash
    if hasattr(header_hash, "value"):
        return bytes(header_hash.value)
    return bytes(header_hash)


def is_reorg(new_parent_hash: bytes, tip_hash: bytes | None) -> bool:
    """
    Detect reorg/fork: incoming block's parent is not our current tip.
    Mandatory check on Solana; comparing only slots misses reorgs.
    """
    if tip_hash is None:
        return False
    return new_parent_hash != tip_hash


def find_fork_point(
    local_tip_hash: bytes,
    incoming_parent_hash: bytes,
    chain: dict[bytes, BlockInfo],
) -> bytes | None:
    """
    Find common ancestor: walk local chain backwards from local_tip_hash (add to visited),
    then check whether incoming block's parent is in that set. With one block per message,
    the only candidate for common ancestor is incoming_parent_hash itself.
    Returns that hash if it's on our chain, else None (gap or unknown parent).
    """
    visited: set[bytes] = set()
    h: bytes | None = local_tip_hash
    while h:
        visited.add(h)
        info = chain.get(h)
        if not info:
            break
        h = info.parent_hash
    return incoming_parent_hash if incoming_parent_hash in visited else None


def get_orphaned_hashes(
    fork_point_hash: bytes,
    tip_hash: bytes,
    chain: dict[bytes, BlockInfo],
) -> list[bytes]:
    """
    Every block after the fork point on our local chain must be reverted.
    Returns list of block hashes to roll back (tip back to, but not including, fork point).
    Rollback by Hash, not Slot.
    """
    orphaned: list[bytes] = []
    h: bytes | None = tip_hash
    while h and h != fork_point_hash:
        orphaned.append(h)
        info = chain.get(h)
        if not info:
            break
        h = info.parent_hash
    return orphaned


def rollback_orphaned(orphaned_hashes: list[bytes]) -> None:
    """
    Revert orphaned blocks: delete transactions, account diffs, logs, block rows.
    Key by Hash only. Override or replace this with your DB deletes, e.g.:
      DELETE FROM transactions WHERE block_hash IN (...);
      DELETE FROM blocks WHERE hash IN (...);
    """
    if not orphaned_hashes:
        return
    # Log in a form that can be used for DELETE ... WHERE hash IN (...)
    hashes_hex = [h.hex() for h in orphaned_hashes]
    logger.warning(
        "REORG: rollback %d orphaned block(s) by hash: %s",
        len(orphaned_hashes),
        hashes_hex[:5] if len(hashes_hex) > 5 else hashes_hex,
    )


def apply_block_to_chain(
    block_hash: bytes,
    parent_hash: bytes,
    slot: int,
) -> list[bytes] | None:
    """
    Update chain state for one new block. Detects reorg, finds fork point,
    runs rollback for orphaned blocks, then adds the new block.
    Returns list of orphaned hashes that were rolled back, or None if simple extend.
    Caller should call rollback_orphaned(orphaned) if return value is non-empty.
    """
    global _chain, _tip_hash
    with _chain_lock:
        tip = _tip_hash
        if tip is None:
            _chain[block_hash] = BlockInfo(slot=slot, parent_hash=parent_hash)
            _tip_hash = block_hash
            return None

        if not is_reorg(parent_hash, tip):
            _chain[block_hash] = BlockInfo(slot=slot, parent_hash=parent_hash)
            _tip_hash = block_hash
            return None

        # Reorg: find common ancestor, then roll back orphaned
        fork_point = find_fork_point(tip, parent_hash, _chain)
        if fork_point is None:
            logger.debug(
                "REORG: cannot find fork point (incoming parent not in chain?). Adding block anyway."
            )
            _chain[block_hash] = BlockInfo(slot=slot, parent_hash=parent_hash)
            _tip_hash = block_hash
            return None

        orphaned = get_orphaned_hashes(fork_point, tip, _chain)
        for h in orphaned:
            _chain.pop(h, None)
        _chain[block_hash] = BlockInfo(slot=slot, parent_hash=parent_hash)
        _tip_hash = block_hash
        return orphaned

# =========================================================
# PROCESS MESSAGE
# =========================================================
def process_message(buffer):
    global processed_count

    try:
        tx_block = parsed_idl_block_message_pb2.ParsedIdlBlockMessage()
        tx_block.ParseFromString(buffer)

        header = tx_block.Header
        slot = header.Slot
        block_hash = _hash_bytes(header.Hash)
        parent_hash = _hash_bytes(header.ParentHash)

        orphaned = apply_block_to_chain(block_hash, parent_hash, slot)
        if orphaned:
            rollback_orphaned(orphaned)

        print(
            f"\nBlock {slot} | "
            f"Txs {len(tx_block.Transactions)}"
        )

        with processed_count_lock:
            processed_count += 1

    except DecodeError as e:
        logger.error(f"Decode error: {e}")

# =========================================================
# CONSUMER WORKERS
# =========================================================

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()


def consumer_worker(consumer_id):
    """Worker function for each consumer thread"""
    global processed_count
    
    # Create a unique consumer for this thread (all use same group ID)
    conf = base_conf.copy()
    conf['group.id'] = f'{config.solana_username}-group-{group_id_suffix}'
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    logger.info(f"Consumer {consumer_id} started")
    
    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
                
            if msg.error():
                # Ignore end-of-partition notifications
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            try:
                process_message(msg.value())
                with processed_count_lock:
                    processed_count += 1
            except Exception as err:
                logger.exception(f"Consumer {consumer_id} failed to process message: {err}")
                
    except KeyboardInterrupt:
        logger.info(f"Consumer {consumer_id} stopping...")
    except Exception as e:
        logger.exception(f"Consumer {consumer_id} error: {e}")
    finally:
        consumer.close()
        logger.info(f"Consumer {consumer_id} closed")

# --- Main execution --- #

def main():
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start multiple consumer threads
    consumers = []
    for i in range(NUM_CONSUMERS):
        thread = threading.Thread(target=consumer_worker, args=(i,))
        thread.daemon = True
        consumers.append(thread)
        thread.start()
        logger.info(f"Started consumer thread {i}")
    
    try:
        # Keep the main thread alive
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, stopping all consumers...")
        shutdown_event.set()
    finally:
        # Wait for all consumer threads to finish
        logger.info("Waiting for all consumers to finish...")
        for thread in consumers:
            thread.join(timeout=5)
        logger.info(f"Shutdown complete. Total messages processed: {processed_count}")

if __name__ == "__main__":
    main()
