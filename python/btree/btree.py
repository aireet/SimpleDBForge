"""
B-Tree Implementation

This is a simple, educational implementation of a B-tree.
B-trees are used in databases and file systems for efficient
storage and retrieval of sorted data.

Key concepts:
- Order (t): Minimum degree, defines range of keys per node
- Each node can have at most 2t-1 keys and 2t children
- Each node (except root) must have at least t-1 keys
- All leaves are at the same depth
- Keys in each node are sorted

B-trees provide O(log n) time complexity for:
- Search: Find a key
- Insert: Add a key-value pair
- Delete: Remove a key-value pair

They are optimized for disk-based storage where reading
large blocks of data is more efficient than random access.
"""

from typing import Any, Optional, List, Tuple


class BTreeNode:
    """
    A node in the B-tree.
    
    Attributes:
        keys: List of keys stored in this node
        values: List of values corresponding to keys
        children: List of child node references
        leaf: Whether this node is a leaf node
    """
    
    def __init__(self, leaf: bool = True):
        self.keys: List[Any] = []
        self.values: List[Any] = []
        self.children: List['BTreeNode'] = []
        self.leaf = leaf
    
    def __str__(self) -> str:
        return f"BTreeNode(keys={self.keys}, leaf={self.leaf})"


class BTree:
    """
    B-Tree implementation for efficient sorted data storage.
    
    A B-tree of order t (minimum degree) has the following properties:
    - Every node has at most 2t-1 keys
    - Every internal node has at most 2t children
    - Every non-root node has at least t-1 keys
    - Root has at least 1 key (if non-empty)
    - All leaves appear at the same level
    
    Example usage:
        >>> btree = BTree(t=2)  # B-tree of order 2 (2-3-4 tree)
        >>> btree.insert(10, 'ten')
        >>> btree.insert(20, 'twenty')
        >>> btree.insert(5, 'five')
        >>> print(btree.search(10))  # 'ten'
    """
    
    def __init__(self, t: int = 2):
        """
        Initialize the B-tree.
        
        Args:
            t: Minimum degree (order) of the B-tree.
               - Each node can have at most 2t-1 keys
               - Each node (except root) must have at least t-1 keys
        """
        if t < 2:
            raise ValueError("Minimum degree must be at least 2")
        
        self.t = t
        self.root = BTreeNode(leaf=True)
        self._size = 0
    
    def search(self, key: Any) -> Optional[Any]:
        """
        Search for a key in the B-tree.
        
        Args:
            key: The key to search for
            
        Returns:
            The associated value if found, None otherwise
            
        Time complexity: O(log n)
        """
        return self._search_node(self.root, key)
    
    def _search_node(self, node: BTreeNode, key: Any) -> Optional[Any]:
        """
        Recursively search for a key starting from a node.
        
        Args:
            node: The node to start searching from
            key: The key to search for
            
        Returns:
            The associated value if found, None otherwise
        """
        # Find the first key greater than or equal to k
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        
        # Check if key is found
        if i < len(node.keys) and key == node.keys[i]:
            return node.values[i]
        
        # If leaf, key is not in tree
        if node.leaf:
            return None
        
        # Recurse to appropriate child
        return self._search_node(node.children[i], key)
    
    def insert(self, key: Any, value: Any) -> None:
        """
        Insert a key-value pair into the B-tree.
        
        If the key already exists, its value is updated.
        
        Args:
            key: The key to insert
            value: The value to associate with the key
            
        Time complexity: O(log n)
        """
        # Check if key already exists (for size tracking)
        is_new_key = self.search(key) is None
        
        root = self.root
        
        # If root is full, tree grows in height
        if len(root.keys) == 2 * self.t - 1:
            new_root = BTreeNode(leaf=False)
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
            self._insert_non_full(new_root, key, value)
        else:
            self._insert_non_full(root, key, value)
        
        # Increment size only for new keys
        if is_new_key:
            self._size += 1
    
    def _insert_non_full(self, node: BTreeNode, key: Any, value: Any) -> None:
        """
        Insert a key into a node that is not full.
        
        Args:
            node: The node to insert into
            key: The key to insert
            value: The value to associate
        """
        i = len(node.keys) - 1
        
        if node.leaf:
            # Find position and insert in leaf
            while i >= 0 and key < node.keys[i]:
                i -= 1
            
            # Check if key already exists
            if i >= 0 and key == node.keys[i]:
                node.values[i] = value  # Update existing
                return
            
            node.keys.insert(i + 1, key)
            node.values.insert(i + 1, value)
        else:
            # Find child to insert into
            while i >= 0 and key < node.keys[i]:
                i -= 1
            
            # Check if key already exists in this node
            if i >= 0 and key == node.keys[i]:
                node.values[i] = value  # Update existing
                return
            
            i += 1
            
            # Split child if full
            if len(node.children[i].keys) == 2 * self.t - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
                elif key == node.keys[i]:
                    node.values[i] = value  # Update existing
                    return
            
            self._insert_non_full(node.children[i], key, value)
    
    def _split_child(self, parent: BTreeNode, index: int) -> None:
        """
        Split a full child node.
        
        A full node has 2t-1 keys. After split:
        - Left child keeps first t-1 keys
        - Middle key (at index t-1) goes to parent
        - Right child (new node) gets last t-1 keys
        
        Args:
            parent: The parent node
            index: Index of the child to split
        """
        t = self.t
        child = parent.children[index]
        
        # Create new node to hold right half
        new_node = BTreeNode(leaf=child.leaf)
        
        # Get middle key before modifying child
        middle_key = child.keys[t - 1]
        middle_value = child.values[t - 1]
        
        # New node gets the right half (keys from index t to 2t-2)
        new_node.keys = child.keys[t:]
        new_node.values = child.values[t:]
        
        # Child keeps the left half (keys from index 0 to t-2)
        child.keys = child.keys[:t - 1]
        child.values = child.values[:t - 1]
        
        # Move children if not leaf
        if not child.leaf:
            new_node.children = child.children[t:]
            child.children = child.children[:t]
        
        # Insert middle key into parent
        parent.keys.insert(index, middle_key)
        parent.values.insert(index, middle_value)
        parent.children.insert(index + 1, new_node)
    
    def delete(self, key: Any) -> bool:
        """
        Delete a key from the B-tree.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key was found and deleted, False otherwise
            
        Time complexity: O(log n)
        """
        result = self._delete(self.root, key)
        
        # If root is empty but has children, shrink tree
        if len(self.root.keys) == 0 and not self.root.leaf:
            self.root = self.root.children[0]
        
        # Decrement size if key was deleted
        if result:
            self._size -= 1
        
        return result
    
    def _delete(self, node: BTreeNode, key: Any) -> bool:
        """
        Recursively delete a key from the subtree rooted at node.
        
        Args:
            node: The root of the subtree
            key: The key to delete
            
        Returns:
            True if key was deleted, False otherwise
        """
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        
        # Case 1: Key is in this node and node is a leaf
        if i < len(node.keys) and key == node.keys[i] and node.leaf:
            node.keys.pop(i)
            node.values.pop(i)
            return True
        
        # Case 2: Key is in this node and node is internal
        if i < len(node.keys) and key == node.keys[i] and not node.leaf:
            return self._delete_internal(node, i)
        
        # Case 3: Key is not in this node
        if node.leaf:
            return False  # Key not found
        
        # Ensure child has enough keys
        if len(node.children[i].keys) < self.t:
            self._fill_child(node, i)
        
        # Recurse (index might have changed)
        if i < len(node.keys) and key == node.keys[i]:
            return self._delete_internal(node, i)
        elif i > len(node.keys):
            return self._delete(node.children[i - 1], key)
        else:
            return self._delete(node.children[i], key)
    
    def _delete_internal(self, node: BTreeNode, index: int) -> bool:
        """
        Delete key at index from an internal node.
        
        Args:
            node: The internal node
            index: Index of the key to delete
            
        Returns:
            True if deletion was successful
        """
        key = node.keys[index]
        
        # Case 2a: Left child has >= t keys, use predecessor
        if len(node.children[index].keys) >= self.t:
            pred_key, pred_value = self._get_predecessor(node.children[index])
            node.keys[index] = pred_key
            node.values[index] = pred_value
            return self._delete(node.children[index], pred_key)
        
        # Case 2b: Right child has >= t keys, use successor
        elif len(node.children[index + 1].keys) >= self.t:
            succ_key, succ_value = self._get_successor(node.children[index + 1])
            node.keys[index] = succ_key
            node.values[index] = succ_value
            return self._delete(node.children[index + 1], succ_key)
        
        # Case 2c: Both children have t-1 keys, merge them
        else:
            self._merge_children(node, index)
            return self._delete(node.children[index], key)
    
    def _get_predecessor(self, node: BTreeNode) -> Tuple[Any, Any]:
        """Get the predecessor (largest key in left subtree)."""
        while not node.leaf:
            node = node.children[-1]
        return node.keys[-1], node.values[-1]
    
    def _get_successor(self, node: BTreeNode) -> Tuple[Any, Any]:
        """Get the successor (smallest key in right subtree)."""
        while not node.leaf:
            node = node.children[0]
        return node.keys[0], node.values[0]
    
    def _fill_child(self, node: BTreeNode, index: int) -> None:
        """
        Ensure child at index has at least t keys.
        
        Args:
            node: The parent node
            index: Index of the child to fill
        """
        # Try borrowing from left sibling
        if index > 0 and len(node.children[index - 1].keys) >= self.t:
            self._borrow_from_left(node, index)
        
        # Try borrowing from right sibling
        elif index < len(node.children) - 1 and len(node.children[index + 1].keys) >= self.t:
            self._borrow_from_right(node, index)
        
        # Merge with a sibling
        else:
            if index < len(node.children) - 1:
                self._merge_children(node, index)
            else:
                self._merge_children(node, index - 1)
    
    def _borrow_from_left(self, node: BTreeNode, index: int) -> None:
        """Borrow a key from the left sibling."""
        child = node.children[index]
        left_sibling = node.children[index - 1]
        
        # Move key from parent to child
        child.keys.insert(0, node.keys[index - 1])
        child.values.insert(0, node.values[index - 1])
        
        # Move key from left sibling to parent
        node.keys[index - 1] = left_sibling.keys.pop()
        node.values[index - 1] = left_sibling.values.pop()
        
        # Move child pointer if internal node
        if not child.leaf:
            child.children.insert(0, left_sibling.children.pop())
    
    def _borrow_from_right(self, node: BTreeNode, index: int) -> None:
        """Borrow a key from the right sibling."""
        child = node.children[index]
        right_sibling = node.children[index + 1]
        
        # Move key from parent to child
        child.keys.append(node.keys[index])
        child.values.append(node.values[index])
        
        # Move key from right sibling to parent
        node.keys[index] = right_sibling.keys.pop(0)
        node.values[index] = right_sibling.values.pop(0)
        
        # Move child pointer if internal node
        if not child.leaf:
            child.children.append(right_sibling.children.pop(0))
    
    def _merge_children(self, node: BTreeNode, index: int) -> None:
        """Merge child at index with child at index+1."""
        left_child = node.children[index]
        right_child = node.children[index + 1]
        
        # Move key from parent to left child
        left_child.keys.append(node.keys[index])
        left_child.values.append(node.values[index])
        
        # Move all keys/values from right child
        left_child.keys.extend(right_child.keys)
        left_child.values.extend(right_child.values)
        
        # Move children if internal node
        if not left_child.leaf:
            left_child.children.extend(right_child.children)
        
        # Remove key and right child from parent
        node.keys.pop(index)
        node.values.pop(index)
        node.children.pop(index + 1)
    
    def range_query(self, start_key: Any, end_key: Any) -> List[Tuple[Any, Any]]:
        """
        Return all key-value pairs where start_key <= key <= end_key.
        
        Args:
            start_key: The lower bound (inclusive)
            end_key: The upper bound (inclusive)
            
        Returns:
            List of (key, value) tuples in the range
            
        Time complexity: O(log n + k) where k is the number of keys in range
        """
        result = []
        self._range_query_node(self.root, start_key, end_key, result)
        return result
    
    def _range_query_node(self, node: BTreeNode, start_key: Any, end_key: Any, 
                          result: List[Tuple[Any, Any]]) -> None:
        """Recursively collect keys in range."""
        i = 0
        
        # Find first key >= start_key
        while i < len(node.keys) and node.keys[i] < start_key:
            i += 1
        
        # Collect keys in range
        while i < len(node.keys):
            # Check left child if internal node
            if not node.leaf:
                self._range_query_node(node.children[i], start_key, end_key, result)
            
            # Add key if in range
            if node.keys[i] > end_key:
                return
            
            if start_key <= node.keys[i] <= end_key:
                result.append((node.keys[i], node.values[i]))
            
            i += 1
        
        # Check rightmost child
        if not node.leaf and i < len(node.children):
            self._range_query_node(node.children[i], start_key, end_key, result)
    
    def get_all(self) -> List[Tuple[Any, Any]]:
        """
        Return all key-value pairs in sorted order.
        
        Returns:
            List of (key, value) tuples in ascending key order
        """
        result = []
        self._inorder_traversal(self.root, result)
        return result
    
    def _inorder_traversal(self, node: BTreeNode, result: List[Tuple[Any, Any]]) -> None:
        """Collect all keys in order via in-order traversal."""
        for i in range(len(node.keys)):
            if not node.leaf:
                self._inorder_traversal(node.children[i], result)
            result.append((node.keys[i], node.values[i]))
        
        if not node.leaf and node.children:
            self._inorder_traversal(node.children[-1], result)
    
    def __len__(self) -> int:
        """Return the number of keys in the B-tree. O(1) time complexity."""
        return self._size
    
    def __str__(self) -> str:
        """Return a string representation of the B-tree."""
        return f"BTree(t={self.t}, keys={[k for k, v in self.get_all()]})"
