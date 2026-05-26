# Solana Kafka consumer (reorg-aware)

Kafka consumer for [Bitquery Solana Kafka Stream](https://docs.bitquery.io/docs/streams/protobuf/chains/Solana-protobuf/) with **reorg detection and rollback**. Uses block **hash** as identity so chain reversals are handled correctly.

## Project Layout

| File                  | Role                                                                                                                                                    |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`consumer.py`**     | Kafka consumer, message parsing, and orchestration. Owns in-memory chain state (`_chain`, `_tip_hash`) and calls buffer + computations.                 |
| **`buffer.py`**       | Shred/block assembly, parent-connectivity, and reorg batching behind `ShredStreamBuffer`.                                                               |
| **`computations.py`** | Chain/reorg logic: `hash_bytes`, `is_reorg`, `find_fork_point`, `get_chain_length`, `get_orphaned_hashes`, `apply_block_to_chain`, `rollback_orphaned`. |
| **`config.py`**       | Credentials (e.g. `solana_username`, `solana_password`).                                                                                                |

## Flow: Shred Assembly → Parent Connectivity → Reorg Logic

1. **Block assembly** (`buffer.py`): Stream messages are block parts, not complete blocks. `ShredStreamBuffer` first merges parts into block assemblies. Messages without `Header.Hash` or `Header.ParentHash` are staged by `Header.Slot`; hash-bearing messages are keyed by `Header.Hash`. Transactions are deduped by signature.

2. **Parent connectivity** (`buffer.py`): Assembled blocks can still arrive out of slot order. `ShredStreamBuffer` releases a block only after its parent hash has already been released. At startup, where no persisted checkpoint exists, it bootstraps from the lowest slot in the first window.

3. **Reorg batching** (`buffer.py`): Parent-connected blocks are batched in release order. When the buffer reaches `REORG_BUFFER_SIZE`, the batch is returned for reorg processing. On shutdown, `flush()` returns any remaining blocks.

4. **Process batch** (`consumer.py`): For each batch returned by `ShredStreamBuffer.add()` or `ShredStreamBuffer.flush()`, the consumer runs reorg logic via `apply_block_to_chain` from `computations.py`, then logs rollbacks and increments `processed_count`.

Shred delivery can be out of slot order, so blocks are assembled and released by parent-hash connectivity before chain/reorg updates. A **single consumer** (`NUM_CONSUMERS = 1`) is used so one process owns the in-memory buffers and chain state.

### Stream Pipeline

```
Kafka message
       |
       v
Parse ParsedIdlBlockMessage
       |
       v
ShredStreamBuffer.add()
       |
       v
Block assembly
  - no hash: stage by slot
  - hash-bearing: merge by block hash
  - dedupe transactions by signature
       |
       v
Parent-connectivity buffer
  - hold assembled blocks until parent hash is released
  - bootstrap from the first window if no checkpoint exists
       |
       v
Reorg batch buffer
  - batch parent-connected blocks in release order
       |
       v
consumer.py::_process_batch()
       |
       v
computations.py::apply_block_to_chain()
```

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

### Block chain tree

```
genesis
   │
   └──► [A] slot 100
            │
            ├──► [B] slot 101  ← canonical tip (depth 2)
            │         ✕ orphaned when D arrives
            │
            └──► [C] slot 102  (depth 2, fork — no reorg yet, equal depth)
                      │
                      └──► [D] slot 103  ← new tip after reorg (depth 3 > 2)
```

A reorg only fires when the incoming fork branch becomes **strictly longer** than the current canonical tip. A competing block at equal depth is stored but does not trigger a rollback.

---

### `apply_block_to_chain()` — decision flow

This flow starts after `ShredStreamBuffer` has emitted an assembled, hash-bearing, parent-connected block. Individual Kafka shred messages do not go directly into this function.

```
Assembled connected block arrives
       │
       ▼
  tip_hash == None?
  ├── yes ──► add as first block, done
  └── no
       │
       ▼
  parent_hash == tip_hash?        (is_reorg check)
  ├── yes (no fork) ──► extend canonical chain, done
  └── no (fork detected!)
       │
       ▼
  find_fork_point()
  ├── walk local chain backwards  → build visited set
  │     tip → parent → parent → ...
  └── walk fork chain backwards   → find first hash in visited
        incoming_parent → its parent → ...
               │
               ▼
        common ancestor found?
        ├── no ──► add block silently, done (gap/unknown parent)
        └── yes
               │
               ▼
        incoming branch length > current head length?
        ├── no  ──► keep current tip, store fork block, done
        └── yes
               │
               ▼
           REORG
        get_orphaned_hashes()  → walk local chain: tip → fork point
        pop orphaned blocks from chain
        add new block as tip
        return (new_tip, orphaned_list)
```

### Reorg handling (three steps)

1. **Detect fork**
   Incoming block’s `parent_hash` ≠ our `tip_hash` → we have a fork.

2. **Find fork point (common ancestor)**
   Two-pass walk: first build a visited set by walking the local chain backwards from tip, then walk backwards from `incoming_parent_hash` until a hash in the visited set is found. Returns that common ancestor hash, or `None` if no ancestor is found (gap/unknown parent), in which case the block is added silently.

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

Start from a hash (tip), follow `parent_hash` until parent is empty or missing. Uses linear walk.

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
