"""
Buffers for Solana shred streams.

Kafka messages in a shred stream are block parts. The public entry point is
ShredStreamBuffer, which assembles parts, releases parent-connected blocks, and
returns batches ready for reorg processing.
"""
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from solana import parsed_idl_block_message_pb2
    BlockMessage = parsed_idl_block_message_pb2.ParsedIdlBlockMessage
else:
    BlockMessage = object


REORG_BUFFER_SIZE = 30
ASSEMBLY_SLOT_LAG = 30
CHAIN_BOOTSTRAP_SIZE = 30

BatchItem = tuple[bytes, bytes, int, BlockMessage]


def _copy_message(message):
    message_copy = type(message)()
    message_copy.CopyFrom(message)
    return message_copy


def _transaction_key(tx, fallback_index: int) -> tuple:
    signature = bytes(tx.Signature)
    if signature:
        return ("signature", signature)
    return ("index", int(tx.Index), fallback_index)


def _reward_key(reward) -> tuple:
    return (
        bytes(reward.Address),
        int(reward.Amount),
        int(reward.PostBalance),
        int(reward.RewardType),
        int(reward.Commission),
    )


def _slot_of(item: BatchItem) -> int:
    return item[2]


class _BlockAssembly:
    """Merged view of all shred messages seen for one eventual block."""

    __slots__ = (
        "slot",
        "block_hash",
        "parent_hash",
        "_block",
        "_transactions",
        "_rewards",
    )

    def __init__(self, tx_block: BlockMessage):
        self.slot = int(tx_block.Header.Slot)
        self.block_hash: bytes | None = None
        self.parent_hash: bytes | None = None
        self._block = type(tx_block)()
        self._block.Header.CopyFrom(tx_block.Header)
        self._transactions: dict[tuple, object] = {}
        self._rewards: dict[tuple, object] = {}

    def add_part(
        self,
        tx_block: BlockMessage,
        block_hash: bytes | None = None,
        parent_hash: bytes | None = None,
    ) -> None:
        self.slot = int(tx_block.Header.Slot)
        if block_hash and parent_hash:
            self.block_hash = block_hash
            self.parent_hash = parent_hash
            self._block.Header.CopyFrom(tx_block.Header)

        self._merge_transactions(tx_block)
        self._merge_rewards(tx_block)

    def merge(self, other: "_BlockAssembly") -> None:
        if other.block_hash and other.parent_hash:
            self.block_hash = other.block_hash
            self.parent_hash = other.parent_hash
            self._block.Header.CopyFrom(other._block.Header)

        self._transactions.update(other._transactions)
        self._rewards.update(other._rewards)

    @property
    def ready(self) -> bool:
        return bool(self.block_hash and self.parent_hash)

    def to_batch_item(self) -> BatchItem:
        if not self.block_hash or not self.parent_hash:
            raise ValueError("Cannot emit assembly without block and parent hash")

        block = type(self._block)()
        block.CopyFrom(self._block)

        del block.Transactions[:]
        for tx in sorted(self._transactions.values(), key=lambda item: int(item.Index)):
            block.Transactions.add().CopyFrom(tx)

        del block.Rewards[:]
        for reward in self._rewards.values():
            block.Rewards.add().CopyFrom(reward)

        return (self.block_hash, self.parent_hash, self.slot, block)

    def _merge_transactions(self, tx_block: BlockMessage) -> None:
        for tx in tx_block.Transactions:
            key = _transaction_key(tx, len(self._transactions))
            self._transactions[key] = _copy_message(tx)

    def _merge_rewards(self, tx_block: BlockMessage) -> None:
        for reward in tx_block.Rewards:
            self._rewards[_reward_key(reward)] = _copy_message(reward)


class BlockAssemblyBuffer:
    """
    Assemble shred-stream block parts.

    Parts without hash are staged by slot. Hash-bearing parts are keyed by
    block hash. Assemblies are emitted after a slot lag so late parts can merge.
    """

    __slots__ = (
        "_slot_lag",
        "_pending_by_slot",
        "_by_hash",
        "_hashes_by_slot",
        "_emitted_hashes",
        "_max_seen_slot",
        "_lock",
    )

    def __init__(self, slot_lag: int = ASSEMBLY_SLOT_LAG):
        self._slot_lag = slot_lag
        self._pending_by_slot: dict[int, _BlockAssembly] = {}
        self._by_hash: dict[bytes, _BlockAssembly] = {}
        self._hashes_by_slot: dict[int, set[bytes]] = {}
        self._emitted_hashes: set[bytes] = set()
        self._max_seen_slot = 0
        self._lock = threading.Lock()

    def add(
        self,
        block_hash: bytes | None,
        parent_hash: bytes | None,
        slot: int,
        tx_block: BlockMessage,
    ) -> list[BatchItem]:
        with self._lock:
            self._max_seen_slot = max(self._max_seen_slot, slot)

            if block_hash and parent_hash:
                self._add_hash_part(block_hash, parent_hash, slot, tx_block)
            else:
                self._add_unhashed_part(slot, tx_block)

            return self._pop_ready(self._max_seen_slot - self._slot_lag)

    def flush(self) -> list[BatchItem]:
        with self._lock:
            return self._pop_ready(None)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "assembly_max_seen_slot": self._max_seen_slot,
                "assembly_pending_slots": len(self._pending_by_slot),
                "assembly_hash_bearing_blocks": len(self._by_hash),
                "assembly_emitted_blocks": len(self._emitted_hashes),
            }

    def _add_hash_part(
        self,
        block_hash: bytes,
        parent_hash: bytes,
        slot: int,
        tx_block: BlockMessage,
    ) -> None:
        if block_hash in self._emitted_hashes:
            return

        assembly = self._by_hash.get(block_hash)
        if assembly is None:
            assembly = _BlockAssembly(tx_block)
            self._by_hash[block_hash] = assembly

        self._hashes_by_slot.setdefault(slot, set()).add(block_hash)
        pending = self._pending_by_slot.pop(slot, None)
        if pending:
            assembly.merge(pending)
        assembly.add_part(tx_block, block_hash, parent_hash)

    def _add_unhashed_part(self, slot: int, tx_block: BlockMessage) -> None:
        active_hashes = [
            block_hash
            for block_hash in self._hashes_by_slot.get(slot, set())
            if block_hash in self._by_hash
        ]

        if len(active_hashes) == 1:
            self._by_hash[active_hashes[0]].add_part(tx_block)
            return

        assembly = self._pending_by_slot.get(slot)
        if assembly is None:
            assembly = _BlockAssembly(tx_block)
            self._pending_by_slot[slot] = assembly
        assembly.add_part(tx_block)

    def _pop_ready(self, cutoff_slot: int | None) -> list[BatchItem]:
        ready_hashes = [
            block_hash
            for block_hash, assembly in self._by_hash.items()
            if assembly.ready and (cutoff_slot is None or assembly.slot <= cutoff_slot)
        ]

        ready: list[BatchItem] = []
        for block_hash in sorted(ready_hashes, key=lambda h: self._by_hash[h].slot):
            assembly = self._by_hash.pop(block_hash)
            pending = self._pending_by_slot.pop(assembly.slot, None)
            if pending and len(self._hashes_by_slot.get(assembly.slot, set())) == 1:
                assembly.merge(pending)

            ready.append(assembly.to_batch_item())
            self._emitted_hashes.add(block_hash)
            self._remove_slot_hash(assembly.slot, block_hash)

        self._drop_stale_unhashed(cutoff_slot)
        return ready

    def _remove_slot_hash(self, slot: int, block_hash: bytes) -> None:
        slot_hashes = self._hashes_by_slot.get(slot)
        if not slot_hashes:
            return
        slot_hashes.discard(block_hash)
        if not slot_hashes:
            self._hashes_by_slot.pop(slot, None)

    def _drop_stale_unhashed(self, cutoff_slot: int | None) -> None:
        if cutoff_slot is None:
            self._pending_by_slot.clear()
            return

        stale_slots = [slot for slot in self._pending_by_slot if slot <= cutoff_slot]
        for slot in stale_slots:
            self._pending_by_slot.pop(slot, None)


class ConnectedBlockBuffer:
    """
    Hold assembled blocks until they can be connected by parent hash.

    Delivery is out of slot order. This buffer releases a block only when its
    parent has already been released, except for initial bootstrap.
    """

    __slots__ = (
        "_bootstrap_size",
        "_pending_by_hash",
        "_children_by_parent",
        "_released_hashes",
        "_lock",
    )

    def __init__(self, bootstrap_size: int = CHAIN_BOOTSTRAP_SIZE):
        self._bootstrap_size = bootstrap_size
        self._pending_by_hash: dict[bytes, BatchItem] = {}
        self._children_by_parent: dict[bytes, set[bytes]] = {}
        self._released_hashes: set[bytes] = set()
        self._lock = threading.Lock()

    def add(self, item: BatchItem) -> list[BatchItem]:
        block_hash, parent_hash, _slot, _tx_block = item
        with self._lock:
            if block_hash in self._released_hashes or block_hash in self._pending_by_hash:
                return []

            self._store(item)
            if parent_hash in self._released_hashes:
                return self._release_component(block_hash)

            if self._should_bootstrap():
                return self._release_component(self._oldest_pending_hash())

            return []

    def flush(self) -> list[BatchItem]:
        with self._lock:
            released: list[BatchItem] = []
            while self._pending_by_hash:
                released.extend(self._release_component(self._oldest_pending_hash()))
            return released

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "connected_pending_blocks": len(self._pending_by_hash),
                "connected_released_blocks": len(self._released_hashes),
            }

    def _store(self, item: BatchItem) -> None:
        block_hash, parent_hash, _slot, _tx_block = item
        self._pending_by_hash[block_hash] = item
        self._children_by_parent.setdefault(parent_hash, set()).add(block_hash)

    def _should_bootstrap(self) -> bool:
        return (
            not self._released_hashes
            and len(self._pending_by_hash) >= self._bootstrap_size
        )

    def _oldest_pending_hash(self) -> bytes:
        return min(self._pending_by_hash, key=lambda h: _slot_of(self._pending_by_hash[h]))

    def _release_component(self, root_hash: bytes) -> list[BatchItem]:
        released: list[BatchItem] = []
        stack = [root_hash]

        while stack:
            block_hash = stack.pop()
            item = self._pending_by_hash.pop(block_hash, None)
            if item is None:
                continue

            _block_hash, parent_hash, _slot, _tx_block = item
            self._unlink_from_parent(parent_hash, block_hash)
            self._released_hashes.add(block_hash)
            released.append(item)

            children = sorted(
                self._children_by_parent.pop(block_hash, set()),
                key=lambda h: _slot_of(self._pending_by_hash[h]),
                reverse=True,
            )
            stack.extend(children)

        return released

    def _unlink_from_parent(self, parent_hash: bytes, block_hash: bytes) -> None:
        siblings = self._children_by_parent.get(parent_hash)
        if siblings is None:
            return

        siblings.discard(block_hash)
        if not siblings:
            self._children_by_parent.pop(parent_hash, None)


class ReorgBuffer:
    """Batch connected blocks for reorg processing while preserving release order."""

    __slots__ = ("_size", "_items")

    def __init__(self, size: int = REORG_BUFFER_SIZE):
        self._size = size
        self._items: list[BatchItem] = []

    def add_many(self, items: list[BatchItem]) -> list[list[BatchItem]]:
        batches: list[list[BatchItem]] = []
        for item in items:
            self._items.append(item)
            if len(self._items) >= self._size:
                batches.append(self._drain())
        return batches

    def flush(self) -> list[BatchItem]:
        if not self._items:
            return []
        return self._drain()

    def stats(self) -> dict[str, int]:
        return {"reorg_pending_blocks": len(self._items)}

    def _drain(self) -> list[BatchItem]:
        batch = self._items
        self._items = []
        return batch


class ShredStreamBuffer:
    """Facade that turns unordered shred messages into reorg-ready batches."""

    __slots__ = ("_assembly", "_connected", "_reorg")

    def __init__(
        self,
        assembly_slot_lag: int = ASSEMBLY_SLOT_LAG,
        chain_bootstrap_size: int = CHAIN_BOOTSTRAP_SIZE,
        reorg_batch_size: int = REORG_BUFFER_SIZE,
    ):
        self._assembly = BlockAssemblyBuffer(assembly_slot_lag)
        self._connected = ConnectedBlockBuffer(chain_bootstrap_size)
        self._reorg = ReorgBuffer(reorg_batch_size)

    def add(
        self,
        block_hash: bytes | None,
        parent_hash: bytes | None,
        slot: int,
        tx_block: BlockMessage,
    ) -> list[list[BatchItem]]:
        assembled = self._assembly.add(block_hash, parent_hash, slot, tx_block)
        return self._queue_assembled(assembled)

    def flush(self) -> list[list[BatchItem]]:
        batches = self._queue_assembled(self._assembly.flush())
        batches.extend(self._queue_connected(self._connected.flush()))

        final_batch = self._reorg.flush()
        if final_batch:
            batches.append(final_batch)
        return batches

    def stats(self) -> dict[str, int]:
        return {
            **self._assembly.stats(),
            **self._connected.stats(),
            **self._reorg.stats(),
        }

    def _queue_assembled(self, blocks: list[BatchItem]) -> list[list[BatchItem]]:
        connected: list[BatchItem] = []
        for block in blocks:
            connected.extend(self._connected.add(block))
        return self._queue_connected(connected)

    def _queue_connected(self, blocks: list[BatchItem]) -> list[list[BatchItem]]:
        return self._reorg.add_many(blocks)