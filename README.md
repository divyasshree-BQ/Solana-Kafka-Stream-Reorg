# Solana Kafka consumer (reorg-aware)

Kafka consumer for Solana block messages with **reorg detection and rollback**. Uses block **hash** as identity so chain reversals are handled correctly.

---

## Project layout 

| File | Role |
|------|------|
| **`consumer.py`** | Kafka consumer, message parsing, and orchestration. Owns in-memory chain state (`_chain`, `_tip_hash`) and calls buffer + computations. |
| **`buffer.py`** | Reorg buffer: collect blocks, sort by slot, yield batches. `ReorgBuffer.add()` and `ReorgBuffer.flush()`. |
| **`computations.py`** | Chain/reorg logic: `hash_bytes`, `is_reorg`, `find_fork_point`, `get_chain_length`, `get_orphaned_hashes`, `apply_block_to_chain`, `rollback_orphaned`. |
| **`config.py`** | Credentials (e.g. `solana_username`, `solana_password`).

---

## Flow: buffer → sort by slot → reorg logic

1. **Buffer** (`buffer.py`): Single purpose — hold the **resulting tree** at the head (~30 slots) so we can determine which branch is longer. Messages are appended via `ReorgBuffer.add(block_hash, parent_hash, slot, tx_block)`. When the buffer reaches `REORG_BUFFER_SIZE`, a batch is returned **sorted by slot** and the buffer is cleared. On shutdown, `flush()` returns any remaining blocks. Parent hash is always set (no special handling for missing).

2. **Process batch** (`consumer.py`): For each batch (from `add()` or `flush()`), the consumer runs reorg logic in slot order via `apply_block_to_chain` from `computations.py`, then logs rollbacks and increments `processed_count`.

Blocks are **reordered by slot** before chain/reorg updates. A **single consumer** (`NUM_CONSUMERS = 1`) is used so one process sees one sequence; multiple consumers would see interleaved messages and can trigger false reorgs.

---

## Reorg logic (in `computations.py`)

### Why hashes, not slots?

On Solana, the same slot can be produced by different blocks (forks). **Block identity is the block hash.** Confirmed blocks can be reverted; the only stable identifier is `Hash`, never `Slot`. The consumer tracks a chain keyed by hash and detects reorgs by comparing parent hashes to the current tip.

### In-memory chain model

- **`chain`**: `dict[bytes, BlockInfo]` — map from block hash → `BlockInfo(slot, parent_hash, depth)`.
- **`tip_hash`**: hash of the current chain tip (head), or `None` before the first block.

Each block is stored once by its `Header.Hash`. `BlockInfo` holds `slot`, `parent_hash`, and `depth` (chain length from start). Depth is used to compare branch lengths on a fork.

### Longest-chain rule (when to reorg)

Reorg happens only when the **incoming branch length is greater than the current head length**. Depth is tracked per block: first block has depth 1, then `depth(block) = 1 + depth(parent)`. On a fork we compare `depth(tip)` vs `depth(incoming_block)`; we only roll back and switch tip when `depth(incoming) > depth(tip)`. If the incoming branch is not longer, we still add the block to the chain but keep the current tip.

### When is it a reorg?

A **reorg** is when the next block’s **parent** is not our current **tip**:

```
is_reorg(new_parent_hash, tip_hash)  →  (tip_hash is not None) and (new_parent_hash != tip_hash)
```

- First block (`tip_hash is None`): not a reorg; we just extend.
- Next block’s parent equals our tip: normal extend; not a reorg.
- Next block’s parent ≠ our tip: we’re on a different fork → reorg.

Comparing only slots would miss these cases.

### Reorg handling (three steps)

1. **Detect fork**  
   Incoming block’s `parent_hash` ≠ our `tip_hash` → we have a fork.

2. **Find fork point (common ancestor)**  
   Walk backwards from our tip; if the incoming block’s `parent_hash` is on that path, it’s the fork point. If not (e.g. gap or unknown parent), we add the block anyway and set it as tip.

3. **Compare branch lengths, then maybe roll back**  
   Current head length = `depth(tip)`. Incoming branch length = `1 + depth(parent)`. Only if **incoming length > current head length** do we orphan and roll back (remove blocks from tip down to fork point), add the new block, and set it as tip. Otherwise we add the block to the chain but keep the current tip. In this repo, `rollback_orphaned()` only logs the orphaned hashes; in production you would delete DB rows by those hashes.

### Flow in code

- **`apply_block_to_chain(block_hash, parent_hash, slot, chain, tip_hash)`** (in `computations.py`)  
  Mutates `chain` in place and returns `(new_tip_hash, orphaned_or_None)`.  
  - If no tip yet → add block (depth=1), return new tip, no reorg.  
  - If `parent_hash == tip` → extend chain (depth = 1 + parent depth), return new tip, no reorg.  
  - If fork → find fork point; compare current head length and incoming branch length; only if **incoming length > current head length** → roll back orphaned blocks, add new block as tip, return new tip and orphaned list; else add block to chain but keep current tip, return that tip and `None`.

- **`rollback_orphaned(orphaned_hashes)`** (in `computations.py`)  
  Called when `apply_block_to_chain` returns a non-empty orphaned list. Here it only logs; you can replace or extend it to perform DB deletes by hash.

### Walk-back algorithm (pseudo)

```
WALK_BACK(start_hash, chain):
  h := start_hash
  LOOP:
    use h  (e.g. add to visited set, or append to orphaned list)
    parent := chain[h].parent_hash
    IF parent is empty OR parent not in chain:
      STOP
    h := parent
```

Start from a hash (tip), follow `parent_hash` until parent is empty or missing. Linear walk, no binary search.

---

## Running

1. Install dependencies

   ```bash
   pip install -r requirements.txt
   ```

2. Provide credentials via a local `config` module (e.g. `config.py` in `protobuf/` with `solana_username` and `solana_password`).

3. Run the consumer:

   ```bash
   python consumer.py
   ```

**Schema:** [ParsedIdlBlockMessage](https://github.com/bitquery/streaming_protobuf/blob/main/solana/parsed_idl_block_message.proto).  
**Python pb2 package:** [bitquery-pb2-kafka-package](https://pypi.org/project/bitquery-pb2-kafka-package/).
