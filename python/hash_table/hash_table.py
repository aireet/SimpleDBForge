"""
Hash Table Implementation

This is a simple, educational implementation of a hash table
with separate chaining for collision resolution.

Key concepts:
- Hash function: Maps keys to bucket indices
- Collision resolution: Handles multiple keys mapping to same bucket
- Load factor: Ratio of entries to buckets, affects performance
- Resizing: Growing the table to maintain performance

Hash tables provide O(1) average-case time complexity for:
- Insert: Add a key-value pair
- Lookup: Find a value by key
- Delete: Remove a key-value pair
"""

from typing import Any, Optional, List, Tuple


class HashTable:
    """
    Hash Table with separate chaining collision resolution.
    
    This implementation uses a list of buckets, where each bucket
    is a list of (key, value) pairs. When multiple keys hash to
    the same bucket, they form a chain.
    
    Example usage:
        >>> ht = HashTable()
        >>> ht.put('name', 'Bob')
        >>> ht.put('age', 25)
        >>> print(ht.get('name'))  # 'Bob'
        >>> ht.delete('age')
        >>> print(ht.get('age'))   # None
    """
    
    DEFAULT_CAPACITY = 16
    LOAD_FACTOR_THRESHOLD = 0.75
    
    def __init__(self, initial_capacity: int = DEFAULT_CAPACITY):
        """
        Initialize the hash table.
        
        Args:
            initial_capacity: Initial number of buckets
        """
        self.capacity = initial_capacity
        self.size = 0
        self.buckets: List[List[Tuple[Any, Any]]] = [[] for _ in range(self.capacity)]
    
    def _hash(self, key: Any) -> int:
        """
        Compute the bucket index for a key.
        
        Uses Python's built-in hash function and modulo to
        map to a bucket index.
        
        Args:
            key: The key to hash
            
        Returns:
            The bucket index (0 to capacity-1)
        """
        return hash(key) % self.capacity
    
    def _get_load_factor(self) -> float:
        """
        Calculate the current load factor.
        
        Returns:
            The ratio of entries to buckets
        """
        return self.size / self.capacity
    
    def _resize(self) -> None:
        """
        Double the capacity and rehash all entries.
        
        Called when load factor exceeds threshold to maintain
        O(1) average-case performance.
        """
        old_buckets = self.buckets
        self.capacity *= 2
        self.buckets = [[] for _ in range(self.capacity)]
        self.size = 0
        
        # Rehash all existing entries
        for bucket in old_buckets:
            for key, value in bucket:
                self.put(key, value)
    
    def put(self, key: Any, value: Any) -> None:
        """
        Insert or update a key-value pair.
        
        If the key already exists, its value is updated.
        If the load factor exceeds threshold, the table is resized.
        
        Args:
            key: The key to store
            value: The value to associate with the key
            
        Time complexity: O(1) average, O(n) worst case
        """
        # Check if resize is needed
        if self._get_load_factor() >= self.LOAD_FACTOR_THRESHOLD:
            self._resize()
        
        index = self._hash(key)
        bucket = self.buckets[index]
        
        # Check if key already exists in bucket
        for i, (k, v) in enumerate(bucket):
            if k == key:
                bucket[i] = (key, value)  # Update existing
                return
        
        # Add new entry
        bucket.append((key, value))
        self.size += 1
    
    def get(self, key: Any) -> Optional[Any]:
        """
        Retrieve a value by key.
        
        Args:
            key: The key to look up
            
        Returns:
            The value if found, None otherwise
            
        Time complexity: O(1) average, O(n) worst case
        """
        index = self._hash(key)
        bucket = self.buckets[index]
        
        for k, v in bucket:
            if k == key:
                return v
        
        return None
    
    def delete(self, key: Any) -> bool:
        """
        Remove a key-value pair.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key was found and deleted, False otherwise
            
        Time complexity: O(1) average, O(n) worst case
        """
        index = self._hash(key)
        bucket = self.buckets[index]
        
        for i, (k, v) in enumerate(bucket):
            if k == key:
                del bucket[i]
                self.size -= 1
                return True
        
        return False
    
    def contains(self, key: Any) -> bool:
        """
        Check if a key exists in the hash table.
        
        Args:
            key: The key to check
            
        Returns:
            True if key exists, False otherwise
        """
        return self.get(key) is not None
    
    def keys(self) -> List[Any]:
        """
        Return all keys in the hash table.
        
        Returns:
            List of all keys
        """
        all_keys = []
        for bucket in self.buckets:
            for key, value in bucket:
                all_keys.append(key)
        return all_keys
    
    def values(self) -> List[Any]:
        """
        Return all values in the hash table.
        
        Returns:
            List of all values
        """
        all_values = []
        for bucket in self.buckets:
            for key, value in bucket:
                all_values.append(value)
        return all_values
    
    def items(self) -> List[Tuple[Any, Any]]:
        """
        Return all key-value pairs.
        
        Returns:
            List of (key, value) tuples
        """
        all_items = []
        for bucket in self.buckets:
            all_items.extend(bucket)
        return all_items
    
    def __len__(self) -> int:
        """Return the number of entries in the hash table."""
        return self.size
    
    def __str__(self) -> str:
        """Return a string representation of the hash table."""
        items = [f"{k}: {v}" for k, v in self.items()]
        return "{" + ", ".join(items) + "}"
    
    def __repr__(self) -> str:
        """Return a detailed representation of the hash table."""
        return f"HashTable(size={self.size}, capacity={self.capacity}, load_factor={self._get_load_factor():.2f})"
