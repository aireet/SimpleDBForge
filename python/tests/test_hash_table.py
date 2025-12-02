"""Tests for Hash Table implementation."""

import pytest
from python.hash_table import HashTable


class TestHashTable:
    """Test cases for Hash Table."""
    
    def test_put_and_get(self):
        """Test basic put and get operations."""
        ht = HashTable()
        
        ht.put('name', 'Bob')
        ht.put('age', 25)
        
        assert ht.get('name') == 'Bob'
        assert ht.get('age') == 25
        assert ht.get('nonexistent') is None
    
    def test_update_value(self):
        """Test updating an existing key."""
        ht = HashTable()
        
        ht.put('key', 'value1')
        assert ht.get('key') == 'value1'
        
        ht.put('key', 'value2')
        assert ht.get('key') == 'value2'
    
    def test_delete(self):
        """Test delete operation."""
        ht = HashTable()
        
        ht.put('key', 'value')
        assert ht.get('key') == 'value'
        
        assert ht.delete('key') is True
        assert ht.get('key') is None
        assert ht.delete('key') is False  # Already deleted
    
    def test_contains(self):
        """Test contains operation."""
        ht = HashTable()
        
        ht.put('key', 'value')
        
        assert ht.contains('key') is True
        assert ht.contains('nonexistent') is False
    
    def test_contains_with_none_value(self):
        """Test contains correctly handles None values."""
        ht = HashTable()
        
        ht.put('key', None)
        
        # Key exists even though value is None
        assert ht.contains('key') is True
        assert ht.get('key') is None
    
    def test_keys_values_items(self):
        """Test keys, values, and items methods."""
        ht = HashTable()
        
        ht.put('a', 1)
        ht.put('b', 2)
        ht.put('c', 3)
        
        assert set(ht.keys()) == {'a', 'b', 'c'}
        assert set(ht.values()) == {1, 2, 3}
        assert set(ht.items()) == {('a', 1), ('b', 2), ('c', 3)}
    
    def test_len(self):
        """Test __len__ method."""
        ht = HashTable()
        
        assert len(ht) == 0
        
        ht.put('a', 1)
        assert len(ht) == 1
        
        ht.put('b', 2)
        assert len(ht) == 2
        
        ht.delete('a')
        assert len(ht) == 1
    
    def test_resize(self):
        """Test automatic resizing."""
        ht = HashTable(initial_capacity=4)
        
        # Insert enough items to trigger resize
        for i in range(20):
            ht.put(f'key{i}', i)
        
        # All items should still be accessible
        for i in range(20):
            assert ht.get(f'key{i}') == i
        
        # Capacity should have increased
        assert ht.capacity > 4
    
    def test_collision_handling(self):
        """Test that collisions are handled correctly."""
        ht = HashTable(initial_capacity=2)
        
        # Multiple keys likely to collide with capacity of 2
        ht.put('key1', 'value1')
        ht.put('key2', 'value2')
        ht.put('key3', 'value3')
        ht.put('key4', 'value4')
        
        assert ht.get('key1') == 'value1'
        assert ht.get('key2') == 'value2'
        assert ht.get('key3') == 'value3'
        assert ht.get('key4') == 'value4'
    
    def test_various_key_types(self):
        """Test with various key types."""
        ht = HashTable()
        
        ht.put(42, 'integer key')
        ht.put('string', 'string key')
        ht.put((1, 2), 'tuple key')
        
        assert ht.get(42) == 'integer key'
        assert ht.get('string') == 'string key'
        assert ht.get((1, 2)) == 'tuple key'
    
    def test_str_repr(self):
        """Test string representations."""
        ht = HashTable()
        ht.put('a', 1)
        
        assert 'a: 1' in str(ht)
        assert 'HashTable' in repr(ht)
        assert 'size=1' in repr(ht)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
