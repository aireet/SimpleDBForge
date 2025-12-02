"""Tests for B-Tree implementation."""

import pytest
from python.btree import BTree


class TestBTree:
    """Test cases for B-Tree."""
    
    def test_insert_and_search(self):
        """Test basic insert and search operations."""
        btree = BTree(t=2)
        
        btree.insert(10, 'ten')
        btree.insert(20, 'twenty')
        btree.insert(5, 'five')
        
        assert btree.search(10) == 'ten'
        assert btree.search(20) == 'twenty'
        assert btree.search(5) == 'five'
        assert btree.search(15) is None
    
    def test_update_value(self):
        """Test updating an existing key."""
        btree = BTree(t=2)
        
        btree.insert(10, 'ten')
        assert btree.search(10) == 'ten'
        
        btree.insert(10, 'TEN')
        assert btree.search(10) == 'TEN'
    
    def test_delete(self):
        """Test delete operation."""
        btree = BTree(t=2)
        
        btree.insert(10, 'ten')
        btree.insert(20, 'twenty')
        btree.insert(5, 'five')
        
        assert btree.delete(10) is True
        assert btree.search(10) is None
        assert btree.search(20) == 'twenty'
        assert btree.search(5) == 'five'
    
    def test_delete_nonexistent(self):
        """Test deleting a nonexistent key."""
        btree = BTree(t=2)
        
        btree.insert(10, 'ten')
        assert btree.delete(20) is False
    
    def test_many_insertions(self):
        """Test with many insertions to trigger splits."""
        btree = BTree(t=2)
        
        # Insert many values
        for i in range(100):
            btree.insert(i, f'value{i}')
        
        # All should be searchable
        for i in range(100):
            assert btree.search(i) == f'value{i}'
    
    def test_random_order_insertions(self):
        """Test insertions in random order."""
        btree = BTree(t=2)
        
        values = [50, 25, 75, 10, 30, 60, 90, 5, 15, 28, 35]
        for v in values:
            btree.insert(v, f'val{v}')
        
        for v in values:
            assert btree.search(v) == f'val{v}'
    
    def test_get_all_sorted(self):
        """Test that get_all returns sorted keys."""
        btree = BTree(t=2)
        
        btree.insert(30, 'thirty')
        btree.insert(10, 'ten')
        btree.insert(20, 'twenty')
        
        all_items = btree.get_all()
        keys = [k for k, v in all_items]
        
        assert keys == sorted(keys)
    
    def test_range_query(self):
        """Test range query."""
        btree = BTree(t=2)
        
        for i in range(1, 11):
            btree.insert(i, f'value{i}')
        
        result = btree.range_query(3, 7)
        keys = [k for k, v in result]
        
        assert keys == [3, 4, 5, 6, 7]
    
    def test_len(self):
        """Test __len__ method."""
        btree = BTree(t=2)
        
        for i in range(5):
            btree.insert(i, f'value{i}')
        
        assert len(btree) == 5
    
    def test_minimum_degree_validation(self):
        """Test that minimum degree < 2 raises error."""
        with pytest.raises(ValueError):
            BTree(t=1)
    
    def test_delete_and_rebalance(self):
        """Test that deletions maintain B-tree properties."""
        btree = BTree(t=2)
        
        # Insert many values
        for i in range(20):
            btree.insert(i, f'value{i}')
        
        # Delete half
        for i in range(0, 20, 2):
            btree.delete(i)
        
        # Remaining values should still be accessible
        for i in range(1, 20, 2):
            assert btree.search(i) == f'value{i}'
        
        # Deleted values should not be found
        for i in range(0, 20, 2):
            assert btree.search(i) is None
    
    def test_higher_order_btree(self):
        """Test with higher order (t > 2)."""
        btree = BTree(t=5)
        
        for i in range(100):
            btree.insert(i, i * 10)
        
        for i in range(100):
            assert btree.search(i) == i * 10
    
    def test_str_representation(self):
        """Test string representation."""
        btree = BTree(t=2)
        btree.insert(1, 'one')
        btree.insert(2, 'two')
        
        str_repr = str(btree)
        assert 'BTree' in str_repr
        assert '1' in str_repr
        assert '2' in str_repr


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
