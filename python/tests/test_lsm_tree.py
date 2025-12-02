"""Tests for LSM-Tree implementation."""

import os
import shutil
import tempfile
import pytest
from python.lsm_tree import LSMTree


class TestLSMTree:
    """Test cases for LSM-Tree."""
    
    def setup_method(self):
        """Create a temporary directory for each test."""
        self.test_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Clean up temporary directory after each test."""
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_put_and_get(self):
        """Test basic put and get operations."""
        lsm = LSMTree(self.test_dir)
        
        lsm.put('name', 'Alice')
        lsm.put('age', 30)
        
        assert lsm.get('name') == 'Alice'
        assert lsm.get('age') == 30
        assert lsm.get('nonexistent') is None
    
    def test_update_value(self):
        """Test updating an existing key."""
        lsm = LSMTree(self.test_dir)
        
        lsm.put('key', 'value1')
        assert lsm.get('key') == 'value1'
        
        lsm.put('key', 'value2')
        assert lsm.get('key') == 'value2'
    
    def test_delete(self):
        """Test delete operation (tombstone)."""
        lsm = LSMTree(self.test_dir)
        
        lsm.put('key', 'value')
        assert lsm.get('key') == 'value'
        
        lsm.delete('key')
        assert lsm.get('key') is None
    
    def test_flush_to_sstable(self):
        """Test flushing memtable to SSTable."""
        lsm = LSMTree(self.test_dir, memtable_size=5)
        
        # Insert more than memtable size to trigger flush
        for i in range(10):
            lsm.put(f'key{i}', f'value{i}')
        
        # Should still be able to retrieve all values
        for i in range(10):
            assert lsm.get(f'key{i}') == f'value{i}'
    
    def test_persistence(self):
        """Test data persistence across restarts."""
        # Create and populate LSM-tree
        lsm = LSMTree(self.test_dir, memtable_size=5)
        for i in range(10):
            lsm.put(f'key{i}', f'value{i}')
        lsm.flush()
        
        # Create new LSM-tree instance
        lsm2 = LSMTree(self.test_dir, memtable_size=5)
        
        # Data should persist
        for i in range(10):
            assert lsm2.get(f'key{i}') == f'value{i}'
    
    def test_compaction(self):
        """Test compaction merges SSTables."""
        lsm = LSMTree(self.test_dir, memtable_size=5)
        
        # Create multiple SSTables
        for i in range(20):
            lsm.put(f'key{i}', f'value{i}')
        lsm.flush()
        
        # Compact
        lsm.compact()
        
        # Data should still be accessible
        for i in range(20):
            assert lsm.get(f'key{i}') == f'value{i}'
    
    def test_complex_values(self):
        """Test with complex value types."""
        lsm = LSMTree(self.test_dir)
        
        lsm.put('dict', {'a': 1, 'b': 2})
        lsm.put('list', [1, 2, 3])
        lsm.put('nested', {'data': [1, 2, {'x': 'y'}]})
        
        assert lsm.get('dict') == {'a': 1, 'b': 2}
        assert lsm.get('list') == [1, 2, 3]
        assert lsm.get('nested') == {'data': [1, 2, {'x': 'y'}]}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
