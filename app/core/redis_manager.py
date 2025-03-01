import os
import redis
from typing import Dict, List, Tuple, Any, Optional
from .consistent_hash import ConsistentHash
from .config import settings
from bisect import bisect_left
 
 
class RedisManager:
 
    MAX_POOL_CONNECTIONS = 200
 
    def __init__(self):
        redis_urls = os.getenv("REDIS_NODES").split(",") if os.getenv("REDIS_NODES") else ["redis://redis1:6379"]
        self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        self.redis_clients: Dict[str, redis.Redis] = {}
        self.consistent_hash = ConsistentHash()
 
        for redis_url in redis_urls:
            self.add_redis_instance(redis_url)
 
    def add_redis_instance(self, redis_url: str) -> None:
        if redis_url in self.redis_clients:
            return
 
        print(f"Adding Redis instance: {redis_url}")
 
        # Creating Redis connection pool and Redis client
        connection_pool = redis.ConnectionPool.from_url(
            redis_url, decode_responses=True, max_connections=RedisManager.MAX_POOL_CONNECTIONS
        )
        self.connection_pools[redis_url] = connection_pool
        self.redis_clients[redis_url] = redis.Redis(connection_pool=connection_pool)
 
        # Getting old state before adding node
        old_keys = self.consistent_hash.sorted_keys.copy()
        old_hash_ring = self.consistent_hash.hash_ring.copy()
 
        # Adding node to consistent hash
        self.consistent_hash.add_node(redis_url)
 
        # Getting all keys except the ones in the new node
        all_keys = list(set(self.get_all_keys()) - set(self.redis_clients[redis_url].keys()))
 
        print(f"Keys: {all_keys}")
 
        for key in all_keys:
            # Determining which node should handle this key
            node = self.consistent_hash.get_node(key)
            if node != redis_url:
                continue
 
            # Finding the old node for the key
            hash_value = self.consistent_hash._hash(key)
            idx = bisect_left(old_keys, hash_value) % len(old_keys)
            old_node = old_hash_ring[old_keys[idx]]
 
            print(f"Key: {key} is being migrated from {old_node} to {node}")
 
            # Migrating the key from old node to new node
            value = self.redis_clients[old_node].get(key)
            if value is not None:
                self.redis_clients[node].set(key, value)
                self.redis_clients[old_node].delete(key)
 
    def remove_redis_instance(self, redis_url: str) -> None:
    
        if redis_url not in self.redis_clients:
            return
 
        if len(self.redis_clients) == 1:
            print("Cannot remove the last Redis instance")
            return
 
        print(f"Removing Redis instance: {redis_url}")
 
        # Getting old state before removing node
        old_keys = self.consistent_hash.sorted_keys.copy()
        old_hash_ring = self.consistent_hash.hash_ring.copy()
 
        # Remove the node from consistent hashing
        self.consistent_hash.remove_node(redis_url)
 
        # Get all keys currently in the Redis instance being removed
        all_keys = self.redis_clients[redis_url].keys()
 
        print(f"Keys: {all_keys}")
 
        for key in all_keys:
            new_node = self.consistent_hash.get_node(key)
            print(f"Key: {key} is being migrated from {redis_url} to {new_node}")
 
            # Migrating key from old node to new node
            value = self.redis_clients[redis_url].get(key)
            if value is not None:
                self.redis_clients[new_node].set(key, value)
                self.redis_clients[redis_url].delete(key)
 
        # Remove Redis instance from manager
        self.redis_clients.pop(redis_url)
        self.connection_pools.pop(redis_url)
 
    def get_all_keys(self) -> List[str]:
        
        all_keys = []
        for redis_client in self.redis_clients.values():
            all_keys.extend(redis_client.keys())
        return all_keys
 
    def get_redis_node_from_key(self, key: str) -> str:
        
        return self.consistent_hash.get_node(key)
 
    def get_connection(self, key: str) -> redis.Redis:
        
        node = self.consistent_hash.get_node(key)
        if node is None:
            raise Exception("No Redis nodes available")
        return self.redis_clients[node]
 
    async def increment(self, key: str, amount: int = 1) -> Optional[int]:
        
        redis_client = self.get_connection(key)
        return redis_client.incr(key, amount)
 
    async def get(self, key: str) -> Optional[int]:
        
        redis_client = self.get_connection(key)
        value = redis_client.get(key)
        return int(value) if value is not None else None
 