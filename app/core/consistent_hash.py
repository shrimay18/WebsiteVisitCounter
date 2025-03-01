import hashlib
from typing import List, Dict
from bisect import bisect_left, insort
 
class ConsistentHash:
    def __init__(self, nodes: List[str]=[], virtual_nodes: int = 100):
        self.virtual_nodes = virtual_nodes
        self.hash_ring: Dict[int, str] = {}  # Maps hash values to physical nodes
        self.sorted_keys: List[int] = []  # Sorted list of hash values
 
        # Add nodes to the hash ring
        for node in nodes:
            self.add_node(node)
 
    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
 
    def add_node(self, node: str) -> None:
        for i in range(self.virtual_nodes):
            virtual_node_id = f"{node}#{i}"
            hash_val = self._hash(virtual_node_id)
 
            # Insert in sorted order using bisect.insort()
            if hash_val not in self.hash_ring:  # Avoid duplicates
                insort(self.sorted_keys, hash_val)
                self.hash_ring[hash_val] = node
 
    def remove_node(self, node: str) -> None:
        for i in range(self.virtual_nodes):
            virtual_node_id = f"{node}#{i}"
            hash_val = self._hash(virtual_node_id)
 
            # Find the index using binary search and remove efficiently
            index = bisect_left(self.sorted_keys, hash_val)
            if index < len(self.sorted_keys) and self.sorted_keys[index] == hash_val:
                del self.hash_ring[hash_val]
                del self.sorted_keys[index]
 
    def get_node(self, key: str) -> str:
        if not self.hash_ring:
            return None
 
        hash_val = self._hash(key)
        index = bisect_left(self.sorted_keys, hash_val)
 
        # If the hash value is larger than the last node, wrap around
        if index == len(self.sorted_keys):
            index = 0
 
        return self.hash_ring[self.sorted_keys[index]]
 