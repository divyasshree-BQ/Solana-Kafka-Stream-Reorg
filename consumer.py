import uuid
import threading
import signal
import time
import logging

from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError

from solana import parsed_idl_block_message_pb2
import config
from computations import BlockInfo, hash_bytes, apply_block_to_chain, rollback_orphaned
from buffer import ShredStreamBuffer

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
last_buffer_log_time = 0.0
buffer_log_lock = threading.Lock()

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
# REORG STATE (chain/tip owned here; logic in computations)
# =========================================================
_chain: dict[bytes, BlockInfo] = {}
_tip_hash: bytes | None = None
_chain_lock = threading.Lock()

stream_buffer = ShredStreamBuffer()


def log_buffer_stats():
    """Periodically log buffer state so live runs show progress."""
    global last_buffer_log_time
    now = time.time()
    with buffer_log_lock:
        if now - last_buffer_log_time < 10:
            return
        last_buffer_log_time = now

    stats = stream_buffer.stats()
    logger.info(
        "Buffers: max_slot=%d assembly_pending_slots=%d hash_blocks=%d "
        "assembly_emitted=%d connected_pending=%d connected_released=%d "
        "reorg_pending=%d",
        stats["assembly_max_seen_slot"],
        stats["assembly_pending_slots"],
        stats["assembly_hash_bearing_blocks"],
        stats["assembly_emitted_blocks"],
        stats["connected_pending_blocks"],
        stats["connected_released_blocks"],
        stats["reorg_pending_blocks"],
    )


def _process_batch(batch: list):
    """Apply reorg logic to parent-connected assembled blocks."""
    global _tip_hash, processed_count
    logger.info(
        "Processing reorg batch: %d connected block(s), slot span %d-%d",
        len(batch),
        batch[0][2],
        batch[-1][2],
    )
    for block_hash, parent_hash, slot, tx_block in batch:
        with _chain_lock:
            _tip_hash, orphaned = apply_block_to_chain(
                block_hash, parent_hash, slot, _chain, _tip_hash
            )
        if orphaned:
            print(f"Orphaned {orphaned}")
            rollback_orphaned(orphaned)
        print(f"\nBlock {slot} | Txs {len(tx_block.Transactions)}")
        with processed_count_lock:
            processed_count += 1


def flush_buffers():
    """Drain all stream buffers during shutdown."""
    for batch in stream_buffer.flush():
        _process_batch(batch)


# =========================================================
# PROCESS MESSAGE
# =========================================================
def process_message(buffer):
    try:
        tx_block = parsed_idl_block_message_pb2.ParsedIdlBlockMessage()
        tx_block.ParseFromString(buffer)

        header = tx_block.Header
        slot = header.Slot
        block_hash = hash_bytes(header.Hash)
        parent_hash = hash_bytes(header.ParentHash)

        batches = stream_buffer.add(
            block_hash, parent_hash, slot, tx_block
        )
        if batches:
            for batch in batches:
                _process_batch(batch)
        else:
            logger.debug(
                "Buffered shred part for slot %d: hash=%s txs=%d",
                slot,
                "yes" if block_hash else "no",
                len(tx_block.Transactions),
            )
        log_buffer_stats()

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
            except Exception as err:
                logger.exception(f"Consumer {consumer_id} failed to process message: {err}")
                
    except KeyboardInterrupt:
        logger.info(f"Consumer {consumer_id} stopping...")
    except Exception as e:
        logger.exception(f"Consumer {consumer_id} error: {e}")
    finally:
        flush_buffers()
        consumer.close()
        logger.info(f"Consumer {consumer_id} closed")


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
