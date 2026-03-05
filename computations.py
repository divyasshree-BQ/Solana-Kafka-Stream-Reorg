"""
Chain and reorg computation helpers.
Block identity is Hash (bytes); reorg uses longest-chain rule.
"""
import logging
from collections import namedtuple

# Block metadata for chain/reorg bookkeeping. Identity is Hash (bytes), not Slot.
# depth = chain length (block count from start); reorg only when incoming branch is longer.
BlockInfo = namedtuple("BlockInfo", ("slot", "parent_hash", "depth"))

logger = logging.getLogger(__name__)


def hash_bytes(header_hash) -> bytes:
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
    then walk backwards from incoming_parent_hash until a common hash is found.
    This handles multi-block forks where the incoming block extends an already-forked branch.
    Returns the common ancestor hash, or None if no common ancestor is found.
    """
    visited: set[bytes] = set()
    h: bytes | None = local_tip_hash
    while h:
        visited.add(h)
        info = chain.get(h)
        if not info:
            break
        h = info.parent_hash

    h = incoming_parent_hash
    while h:
        if h in visited:
            return h
        info = chain.get(h)
        if not info:
            break
        h = info.parent_hash
    return None


def get_chain_length(hash_val: bytes, chain: dict[bytes, BlockInfo]) -> int:
    """Chain length (depth) for this block; 0 if not in chain or unknown."""
    info = chain.get(hash_val)
    if not info:
        return 0
    return info.depth


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
    Log rollback of orphaned blocks (e.g. for DELETE FROM ... WHERE hash IN (...)).
    Caller is responsible for actual DB deletes.
    """
    if not orphaned_hashes:
        return
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
    chain: dict[bytes, BlockInfo],
    tip_hash: bytes | None,
) -> tuple[bytes | None, list[bytes] | None]:
    """
    Update chain state for one new block. Reorg only when incoming branch length
    is greater than current head length (longest-chain rule). Mutates chain in place.
    Returns (new_tip_hash, orphaned_hashes_or_None).
    """
    if tip_hash is None:
        chain[block_hash] = BlockInfo(slot=slot, parent_hash=parent_hash, depth=1)
        return (block_hash, None)

    if not is_reorg(parent_hash, tip_hash):
        parent_depth = get_chain_length(parent_hash, chain)
        depth = (parent_depth + 1) if parent_depth else 1
        chain[block_hash] = BlockInfo(slot=slot, parent_hash=parent_hash, depth=depth)
        return (block_hash, None)

    # Fork: compare branch lengths; only reorg when incoming branch is longer
    fork_point = find_fork_point(tip_hash, parent_hash, chain)
    if fork_point is None:
        logger.info(
            "REORG: cannot find fork point for block %s (incoming parent not in chain). Adding block anyway.",
            block_hash.hex()
        )
        parent_depth = get_chain_length(parent_hash, chain)
        depth = (parent_depth + 1) if parent_depth else 0
        chain[block_hash] = BlockInfo(slot=slot, parent_hash=parent_hash, depth=depth)
        return (block_hash, None)

    current_head_length = get_chain_length(tip_hash, chain)
    parent_depth = get_chain_length(parent_hash, chain)
    incoming_branch_length = (parent_depth + 1) if parent_depth else 0

    if incoming_branch_length <= current_head_length:
        depth = incoming_branch_length if incoming_branch_length else 0
        chain[block_hash] = BlockInfo(slot=slot, parent_hash=parent_hash, depth=depth)
        return (tip_hash, None)

    # Incoming branch is longer: roll back our tip to fork, then add new block as tip
    orphaned = get_orphaned_hashes(fork_point, tip_hash, chain)
    for h in orphaned:
        chain.pop(h, None)
    chain[block_hash] = BlockInfo(
        slot=slot, parent_hash=parent_hash, depth=incoming_branch_length
    )
    return (block_hash, orphaned)
