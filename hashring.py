import hashlib
import bisect

class HashRing:
    def __init__(self, replicas=100):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []

    def _hash(self, key):
        """Return a SHA-256 based hash value (0..2^256-1)."""
        return int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16)

    def add_node(self, node_id):
        """Add a node (server) to the ring."""
        for i in range(self.replicas):
            h = self._hash(f"{node_id}:{i}")
            self.ring[h] = node_id
            bisect.insort(self.sorted_keys, h)

    def remove_node(self, node_id):
        """Remove a node (server) from the ring."""
        for i in range(self.replicas):
            h = self._hash(f"{node_id}:{i}")
            if h in self.ring:
                del self.ring[h]
                self.sorted_keys.remove(h)

    def get_node(self, key):
        """Return the node responsible for the given key."""
        if not self.ring:
            return None
        h = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, h)
        if idx == len(self.sorted_keys):
            idx = 0
        return self.ring[self.sorted_keys[idx]]