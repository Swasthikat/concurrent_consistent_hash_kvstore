"""
Concurrent In-Memory Key–Value Store with Consistent Hashing and Virtual Nodes

Implements:
1) Consistent hashing ring with virtual nodes (add/remove, O(log N) lookup)
2) Thread-safe key–value store interface (PUT, GET, DELETE)
3) Simulation driver that loads 10,000 key–value pairs
4) Scenario where one virtual node is removed; reports how many keys
   are remapped and their percentage.
"""

import hashlib
from bisect import bisect_right
import threading


# ----------------------------------------------------------------------
# Helper: hash function
# ----------------------------------------------------------------------
def hash_value(key: str) -> int:
    """Return a 128-bit integer hash of the given string using MD5."""
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)


# ----------------------------------------------------------------------
# Task 2: Thread-safe in-memory key-value store
# ----------------------------------------------------------------------
class KeyValueStore:
    """Thread-safe in-memory key-value store."""

    def __init__(self) -> None:
        self._data = {}
        self._lock = threading.RLock()  # internal concurrency control

    def put(self, key: str, value: str) -> None:
        """Insert or update a key."""
        with self._lock:
            self._data[key] = value

    def get(self, key: str):
        """Retrieve a value; returns None if key does not exist."""
        with self._lock:
            return self._data.get(key)

    def delete(self, key: str) -> bool:
        """Delete a key; returns True if deleted."""
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False

    def keys(self):
        """Return a snapshot list of keys (for simulation use only)."""
        with self._lock:
            return list(self._data.keys())


# ----------------------------------------------------------------------
# Node: physical node that owns a KeyValueStore
# ----------------------------------------------------------------------
class Node:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.store = KeyValueStore()

    def __repr__(self) -> str:
        return f"Node({self.node_id}, keys={len(self.store.keys())})"


# ----------------------------------------------------------------------
# Task 1: Consistent Hashing Ring with virtual nodes
# ----------------------------------------------------------------------
class ConsistentHashRing:
    """Consistent hashing ring with virtual nodes."""

    def __init__(self, replicas: int = 100) -> None:
        self.replicas = replicas
        self.ring = {}              # position -> (node_id, replica_index)
        self.sorted_positions = []  # sorted hash positions
        self.nodes = {}             # node_id -> Node object

    def add_node(self, node: Node) -> None:
        """Add a physical node and all its virtual nodes."""
        self.nodes[node.node_id] = node
        for r in range(self.replicas):
            vnode_key = f"{node.node_id}#{r}"
            pos = hash_value(vnode_key)
            if pos in self.ring:
                continue
            self.ring[pos] = (node.node_id, r)
            self.sorted_positions.append(pos)
        self.sorted_positions.sort()
        print(f"[Ring] Added node {node.node_id} with {self.replicas} virtual nodes.")

    def remove_one_virtual_node(self):
        """Remove ONE virtual node and return its details."""
        if not self.sorted_positions:
            raise RuntimeError("Ring is empty; cannot remove virtual node.")

        pos = self.sorted_positions[0]
        node_id, replica_index = self.ring[pos]

        del self.ring[pos]
        self.sorted_positions.pop(0)

        print(f"[Ring] Removed VIRTUAL node {node_id}#{replica_index} at position {pos}.")
        return node_id, replica_index, pos

    def get_node_for_key(self, key: str) -> Node:
        """Return the node responsible for a given key."""
        if not self.sorted_positions:
            raise RuntimeError("Ring is empty; add nodes first.")

        h = hash_value(key)
        idx = bisect_right(self.sorted_positions, h)

        if idx == len(self.sorted_positions):
            idx = 0

        pos = self.sorted_positions[idx]
        node_id, _ = self.ring[pos]
        return self.nodes[node_id]


# ----------------------------------------------------------------------
# Cluster wrapper: exposes PUT / GET / DELETE
# ----------------------------------------------------------------------
class Cluster:
    def __init__(self, replicas: int = 100) -> None:
        self.ring = ConsistentHashRing(replicas=replicas)
        self.all_keys = []

    def add_node(self, node_id: str) -> None:
        node = Node(node_id)
        self.ring.add_node(node)

    def put(self, key: str, value: str) -> None:
        node = self.ring.get_node_for_key(key)
        node.store.put(key, value)

    def get(self, key: str):
        node = self.ring.get_node_for_key(key)
        return node.store.get(key)

    def delete(self, key: str) -> bool:
        node = self.ring.get_node_for_key(key)
        return node.store.delete(key)

    def node_key_distribution(self):
        """Return key count per node."""
        dist = {}
        for node_id, node in self.ring.nodes.items():
            dist[node_id] = len(node.store.keys())
        return dist


# ----------------------------------------------------------------------
# Task 3 & 4: Simulation Driver
# ----------------------------------------------------------------------
def run_simulation():
    print("=== Concurrent In-Memory KV Store with Consistent Hashing ===\n")

    cluster = Cluster(replicas=50)
    cluster.add_node("Node-A")
    cluster.add_node("Node-B")
    cluster.add_node("Node-C")

    NUM_KEYS = 10_000
    print(f"[Simulation] Loading {NUM_KEYS} key-value pairs...")

    for i in range(NUM_KEYS):
        key = f"key_{i}"
        value = f"value_{i}"
        cluster.put(key, value)
        cluster.all_keys.append(key)

    print("[Simulation] Initial load complete.")
    print("Initial distribution:", cluster.node_key_distribution(), "\n")

    print("[Simulation] Removing ONE virtual node...")
    original_owner = {k: cluster.ring.get_node_for_key(k).node_id for k in cluster.all_keys}

    cluster.ring.remove_one_virtual_node()

    moved = sum(
        1 for key in cluster.all_keys
        if cluster.ring.get_node_for_key(key).node_id != original_owner[key]
    )

    percent = (moved / NUM_KEYS) * 100.0

    print("\n=== Remapping Report ===")
    print("Total keys:", NUM_KEYS)
    print("Keys remapped:", moved)
    print(f"Percentage remapped: {percent:.2f}%\n")

    print("New distribution after removal:", cluster.node_key_distribution())
    print("\n=== Simulation complete ===")


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------
if __name__ == "__main__":
    run_simulation()
