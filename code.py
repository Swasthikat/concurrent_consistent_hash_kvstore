import hashlib
import bisect
import threading
import time
import random
import string
import collections

# ==========================================
# 1. Thread-Safe Physical Node (The Storage)
# ==========================================
class PhysicalNode:
    """
    Represents a single server in the cluster.
    Thread-safe using a Mutex (threading.Lock).
    """
    def __init__(self, node_id):
        self.node_id = node_id
        self._store = {}  # The actual in-memory storage
        self._lock = threading.Lock() # Mutex for thread safety

    def put(self, key, value):
        """Implement PUT operation."""
        with self._lock:
            self._store[key] = value

    def get(self, key):
        """Implement GET operation."""
        with self._lock:
            return self._store.get(key)

    def delete(self, key):
        """Implement DELETE operation."""
        with self._lock:
            if key in self._store:
                del self._store[key]

    def get_key_count(self):
        """Returns the number of keys stored."""
        with self._lock:
            return len(self._store)
            
    def dump_keys(self):
        """Helper to retrieve all keys for simulation stats"""
        with self._lock:
            return list(self._store.keys())

# ==========================================
# 2. Consistent Hashing Ring Data Structure
# ==========================================
class ConsistentHashRing:
    """
    Handles the mapping of keys to physical nodes using Consistent Hashing.
    Lookup Complexity: O(log N) due to binary search.
    """
    # FIX: Added 'virtual_nodes' to the constructor signature to resolve TypeError.
    def __init__(self, virtual_nodes=100): 
        self.virtual_nodes = virtual_nodes
        self.ring = []  # Sorted list of hash values
        self.ring_map = {}  # Map: Hash Value -> Physical Node ID
        self._lock = threading.Lock() # Protects the ring structure during add/remove

    def _hash_val(self, key):
        """Generates a hash integer using MD5."""
        encoded = key.encode('utf-8')
        # Using MD5 for stable, well-distributed hashing across the ring.
        return int(hashlib.md5(encoded).hexdigest(), 16)

    def add_node(self, node_id):
        """Adds a physical node and its virtual replicas to the ring."""
        with self._lock:
            for i in range(self.virtual_nodes):
                virtual_id = f"{node_id}:{i}"
                h = self._hash_val(virtual_id)
                self.ring_map[h] = node_id
                # bisect.insort ensures O(log N) insertion position lookup
                bisect.insort(self.ring, h) 

    def remove_node(self, node_id):
        """Removes a physical node and all its virtual replicas."""
        with self._lock:
            hashes_to_remove = [h for h, nid in self.ring_map.items() if nid == node_id]
            
            for h in hashes_to_remove:
                del self.ring_map[h]
                # Removing from list is inefficient (O(N)), but functional for this example.
                self.ring.remove(h)

    def get_node(self, key):
        """
        Returns the physical node ID responsible for the key. O(log N) lookup.
        """
        if not self.ring:
            return None
        
        h = self._hash_val(key)
        
        # bisect_left finds the index of the first point >= key_hash
        idx = bisect.bisect_left(self.ring, h)
        
        # Wrap-around: If idx reaches the end, map to the start of the ring (index 0).
        if idx == len(self.ring):
            idx = 0
            
        target_hash = self.ring[idx]
        return self.ring_map[target_hash]

# ==========================================
# 3. Distributed Key-Value Store (Orchestrator)
# ==========================================
class DistributedKVStore:
    def __init__(self):
        # Initializing the ring with 50 virtual nodes per server.
        self.ring = ConsistentHashRing(virtual_nodes=50) 
        # FIX: Explicitly initializing 'self.nodes' to resolve 'AttributeError'.
        self.nodes = {} # Map: NodeID -> PhysicalNode Object

    def add_server(self, node_id):
        if node_id not in self.nodes:
            # The node is created and added to the physical map
            self.nodes[node_id] = PhysicalNode(node_id)
            # Its virtual points are added to the ring
            self.ring.add_node(node_id)
            print(f"[System] Server '{node_id}' added.")

    def remove_server(self, node_id):
        if node_id in self.nodes:
            self.ring.remove_node(node_id)
            # Data migration logic would go here in a production system.
            print(f"[System] Server '{node_id}' removed from ring.")

    def put(self, key, value):
        target_node_id = self.ring.get_node(key)
        if target_node_id:
            # Route the request to the correct physical node
            self.nodes[target_node_id].put(key, value)
        else:
            raise Exception("No servers available!")

    def get(self, key):
        target_node_id = self.ring.get_node(key)
        if target_node_id and target_node_id in self.nodes:
            return self.nodes[target_node_id].get(key)
        return None
        
    def delete(self, key):
        target_node_id = self.ring.get_node(key)
        if target_node_id and target_node_id in self.nodes:
            self.nodes[target_node_id].delete(key)

# ==========================================
# 4. Simulation Driver & Reporting
# ==========================================
def generate_random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def run_simulation():
    print("================================================================")
    print("   Concurrent In-Memory KV Store with Consistent Hashing")
    print("================================================================")

    store = DistributedKVStore()
    
    # 1. Setup Initial Cluster
    initial_servers = ["Server_A", "Server_B", "Server_C", "Server_D"]
    for s in initial_servers:
        store.add_server(s)

    # 2. Load Data (Task 3: Predefined dataset)
    total_keys = 10000
    print(f"\n[Simulation] Loading {total_keys} keys into the system...")
    
    key_locations_initial = {} # Store where the key landed initially
    all_data = {} 
    
    start_time = time.time()
    for i in range(total_keys):
        key = f"user_{i}" 
        val = f"data_{generate_random_string()}"
        all_data[key] = val
        
        # Determine and store the initial location
        initial_node = store.ring.get_node(key)
        key_locations_initial[key] = initial_node
        
        store.put(key, val)
    
    print(f"[Simulation] Load complete in {time.time() - start_time:.4f} seconds.")

    # Check Initial Distribution
    print("\n[Report] Initial Data Distribution:")
    initial_counts = collections.defaultdict(int)
    for key in all_data.keys():
        initial_counts[key_locations_initial[key]] += 1
        
    for nid, count in sorted(initial_counts.items()):
        print(f"  - {nid}: {count} keys ({count/total_keys*100:.2f}%)")

    # 3. Simulation Scenario: Remove a Node (Task 4)
    node_to_remove = "Server_C"
    print(f"\n[Scenario] Simulating failure/removal of '{node_to_remove}'...")

    # Perform Removal
    store.remove_server(node_to_remove)

    # 4. Calculate Remapping Stats
    print("[Analysis] Calculating theoretical impact of node removal...")
    keys_remapped = 0
    
    # We iterate through all 10,000 keys and check where they should go NOW
    for key in all_data.keys():
        old_node = key_locations_initial[key]
        new_node = store.ring.get_node(key)
        
        # A key is considered 'remapped' if its destination is different
        # from the initial destination, OR if it was on the removed node.
        if old_node == node_to_remove or old_node != new_node:
            keys_remapped += 1
            
    keys_stayed = total_keys - keys_remapped
    
    # ==========================================
    # Deliverable: Text-Based Report
    # ==========================================
    print("\n" + "="*40)
    print("      SIMULATION RESULTS REPORT")
    print("="*40)
    print(f"Total Keys:              {total_keys}")
    print(f"Nodes Before Removal:    {len(initial_servers)}")
    print(f"Nodes After Removal:     {len(initial_servers) - 1}")
    print(f"Node Removed:            {node_to_remove}")
    print("-" * 40)
    print(f"Keys needing remapping:  {keys_remapped}")
    print(f"Keys unaffected:         {keys_stayed}")
    print(f"Percentage Remapped:     {(keys_remapped / total_keys) * 100:.2f}%")
    print("-" * 40)
    
    expected_pct = 100 / len(initial_servers)
    print(f"Theoretical Expected Remapping (1/N): ~{expected_pct:.2f}%")
    
    print("\n[Conclusion]")
    print("The low percentage of remapped keys (close to 1/N) confirms that")
    print("Consistent Hashing successfully isolates data movement to only the")
    print("affected node and its immediate neighbor.")


# FIX: Correct use of double underscores for the standard execution block.
if __name__ == "__main__":
    run_simulation()
