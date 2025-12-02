"""
LSM-Tree (Log-Structured Merge-tree) Implementation

This is a simplified, educational implementation of an LSM-tree.
LSM-trees are used in many modern databases (like LevelDB, RocksDB, Cassandra)
for their excellent write performance.

Key concepts:
- MemTable: In-memory buffer for recent writes (implemented as a sorted structure)
- SSTables: Sorted String Tables on disk (immutable, sorted files)
- Compaction: Merging SSTables to reduce read amplification
"""

import os
import json
import bisect
from typing import Any, Optional
from collections import OrderedDict


class MemTable:
    """
    In-memory component of LSM-tree.
    Stores key-value pairs in a sorted structure for fast writes and reads.
    """
    
    def __init__(self, max_size: int = 1000):
        self.data: dict = {}
        self.max_size = max_size
    
    def put(self, key: str, value: Any) -> None:
        """Insert or update a key-value pair."""
        self.data[key] = value
    
    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value by key."""
        return self.data.get(key)
    
    def delete(self, key: str) -> None:
        """Mark a key as deleted (tombstone)."""
        self.data[key] = None  # Tombstone marker
    
    def is_full(self) -> bool:
        """Check if MemTable has reached its size limit."""
        return len(self.data) >= self.max_size
    
    def get_sorted_items(self) -> list:
        """Return all items sorted by key."""
        return sorted(self.data.items())
    
    def clear(self) -> None:
        """Clear all entries from the MemTable."""
        self.data.clear()


class SSTable:
    """
    Sorted String Table - immutable, sorted file on disk.
    Each SSTable contains sorted key-value pairs and an index for fast lookups.
    """
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.index: dict = {}  # Sparse index for fast lookups
    
    def write(self, sorted_items: list) -> None:
        """Write sorted items to disk."""
        with open(self.filepath, 'w') as f:
            for key, value in sorted_items:
                entry = json.dumps({'key': key, 'value': value})
                self.index[key] = f.tell()
                f.write(entry + '\n')
        
        # Write index file
        with open(self.filepath + '.idx', 'w') as f:
            json.dump(self.index, f)
    
    def read_index(self) -> None:
        """Load the index from disk."""
        idx_path = self.filepath + '.idx'
        if os.path.exists(idx_path):
            with open(idx_path, 'r') as f:
                self.index = json.load(f)
    
    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value by key using the index."""
        if key not in self.index:
            return None
        
        with open(self.filepath, 'r') as f:
            f.seek(self.index[key])
            line = f.readline()
            entry = json.loads(line)
            return entry['value']
    
    def get_all_items(self) -> list:
        """Read all items from the SSTable."""
        items = []
        if not os.path.exists(self.filepath):
            return items
        
        with open(self.filepath, 'r') as f:
            for line in f:
                entry = json.loads(line.strip())
                items.append((entry['key'], entry['value']))
        return items


class LSMTree:
    """
    Log-Structured Merge-tree implementation.
    
    Architecture:
    - Level 0: MemTable (in-memory)
    - Level 1+: SSTables on disk
    
    Write path: Write to MemTable -> Flush to SSTable when full
    Read path: Check MemTable -> Check SSTables from newest to oldest
    
    Example usage:
        >>> lsm = LSMTree('/tmp/mydb')
        >>> lsm.put('name', 'Alice')
        >>> lsm.put('age', 30)
        >>> print(lsm.get('name'))  # 'Alice'
    """
    
    def __init__(self, directory: str, memtable_size: int = 100):
        self.directory = directory
        self.memtable = MemTable(max_size=memtable_size)
        self.sstables: list = []
        self.sstable_counter = 0
        
        # Create directory if it doesn't exist
        os.makedirs(directory, exist_ok=True)
        
        # Load existing SSTables
        self._load_sstables()
    
    def _load_sstables(self) -> None:
        """Load existing SSTables from disk."""
        if not os.path.exists(self.directory):
            return
        
        sstable_files = sorted([
            f for f in os.listdir(self.directory) 
            if f.endswith('.sst')
        ])
        
        for filename in sstable_files:
            filepath = os.path.join(self.directory, filename)
            sstable = SSTable(filepath)
            sstable.read_index()
            self.sstables.append(sstable)
        
        if sstable_files:
            # Update counter based on existing files
            last_num = int(sstable_files[-1].split('.')[0])
            self.sstable_counter = last_num + 1
    
    def put(self, key: str, value: Any) -> None:
        """
        Insert or update a key-value pair.
        
        Args:
            key: The key to store
            value: The value to associate with the key
        """
        self.memtable.put(key, value)
        
        # Flush to disk if MemTable is full
        if self.memtable.is_full():
            self._flush_memtable()
    
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value by key.
        
        Searches in order: MemTable -> SSTables (newest first)
        
        Args:
            key: The key to look up
            
        Returns:
            The value if found, None otherwise
        """
        # First, check MemTable
        value = self.memtable.get(key)
        if value is not None:
            return value
        
        # Check if it's a tombstone in memtable
        if key in self.memtable.data:
            return None
        
        # Then, check SSTables (newest first)
        for sstable in reversed(self.sstables):
            value = sstable.get(key)
            if value is not None:
                return value
        
        return None
    
    def delete(self, key: str) -> None:
        """
        Delete a key by writing a tombstone.
        
        Args:
            key: The key to delete
        """
        self.memtable.delete(key)
        
        if self.memtable.is_full():
            self._flush_memtable()
    
    def _flush_memtable(self) -> None:
        """Flush the MemTable to a new SSTable on disk."""
        if not self.memtable.data:
            return
        
        # Create new SSTable
        filepath = os.path.join(self.directory, f'{self.sstable_counter:06d}.sst')
        sstable = SSTable(filepath)
        sstable.write(self.memtable.get_sorted_items())
        sstable.read_index()
        
        self.sstables.append(sstable)
        self.sstable_counter += 1
        self.memtable.clear()
    
    def compact(self) -> None:
        """
        Merge all SSTables into one to reduce read amplification.
        
        This is a simplified compaction - production systems use
        leveled or size-tiered compaction strategies.
        """
        if len(self.sstables) < 2:
            return
        
        # Merge all items
        merged: dict = {}
        for sstable in self.sstables:
            for key, value in sstable.get_all_items():
                merged[key] = value
        
        # Remove tombstones
        merged = {k: v for k, v in merged.items() if v is not None}
        
        # Remove old SSTable files
        for sstable in self.sstables:
            if os.path.exists(sstable.filepath):
                os.remove(sstable.filepath)
            if os.path.exists(sstable.filepath + '.idx'):
                os.remove(sstable.filepath + '.idx')
        
        # Create new compacted SSTable
        self.sstables.clear()
        if merged:
            filepath = os.path.join(self.directory, f'{self.sstable_counter:06d}.sst')
            sstable = SSTable(filepath)
            sstable.write(sorted(merged.items()))
            sstable.read_index()
            self.sstables.append(sstable)
            self.sstable_counter += 1
    
    def flush(self) -> None:
        """Force flush the MemTable to disk."""
        self._flush_memtable()
    
    def close(self) -> None:
        """Flush any pending data and close the LSM-tree."""
        self.flush()
