# SimpleDBForge

A lightweight, educational repository featuring simple implementations of core database structures in various programming languages. Explore hands-on examples of LSM-trees, hash tables, B-trees, and more, designed for learning and experimentation.

**Ideal for students, developers, and database enthusiasts looking to build foundational knowledge through minimalistic, easy-to-understand code.**

## 📚 Data Structures

### 1. LSM-Tree (Log-Structured Merge-tree)

A data structure commonly used in modern databases (LevelDB, RocksDB, Cassandra) for high write throughput.

**Key Concepts:**
- **MemTable**: In-memory buffer for recent writes
- **SSTables**: Sorted String Tables on disk (immutable, sorted files)
- **Compaction**: Merging SSTables to reduce read amplification

**Usage:**
```python
from python.lsm_tree import LSMTree

# Create an LSM-tree
lsm = LSMTree('/tmp/mydb')

# Insert data
lsm.put('user:1', {'name': 'Alice', 'age': 30})
lsm.put('user:2', {'name': 'Bob', 'age': 25})

# Retrieve data
print(lsm.get('user:1'))  # {'name': 'Alice', 'age': 30}

# Delete data
lsm.delete('user:2')

# Persist to disk
lsm.flush()

# Compact SSTables
lsm.compact()
```

### 2. Hash Table

A fundamental data structure providing O(1) average-case lookups, used for indexing in databases.

**Key Concepts:**
- **Hash Function**: Maps keys to bucket indices
- **Collision Resolution**: Separate chaining for handling collisions
- **Load Factor**: Automatic resizing to maintain performance

**Usage:**
```python
from python.hash_table import HashTable

# Create a hash table
ht = HashTable()

# Insert data
ht.put('name', 'Alice')
ht.put('age', 30)

# Retrieve data
print(ht.get('name'))  # 'Alice'

# Check existence
print(ht.contains('name'))  # True

# Delete data
ht.delete('age')

# Iterate over entries
for key, value in ht.items():
    print(f'{key}: {value}')
```

### 3. B-Tree

A self-balancing tree structure optimized for disk-based storage, commonly used in databases and file systems.

**Key Concepts:**
- **Order (t)**: Minimum degree, defines range of keys per node
- **Balanced Structure**: All leaves at the same depth
- **Range Queries**: Efficient retrieval of sorted data ranges

**Usage:**
```python
from python.btree import BTree

# Create a B-tree (order 2 = 2-3-4 tree)
btree = BTree(t=2)

# Insert data
btree.insert(10, 'ten')
btree.insert(20, 'twenty')
btree.insert(5, 'five')

# Search for a key
print(btree.search(10))  # 'ten'

# Range query
results = btree.range_query(5, 15)  # [(5, 'five'), (10, 'ten')]

# Get all entries sorted
all_entries = btree.get_all()  # [(5, 'five'), (10, 'ten'), (20, 'twenty')]

# Delete a key
btree.delete(10)
```

## 🚀 Getting Started

### Prerequisites

- Python 3.7+
- pytest (for running tests)

### Installation

```bash
# Clone the repository
git clone https://github.com/aireet/SimpleDBForge.git
cd SimpleDBForge

# Install test dependencies
pip install pytest
```

### Running Tests

```bash
# Run all tests
pytest python/tests/ -v

# Run specific test file
pytest python/tests/test_btree.py -v
```

## 📁 Project Structure

```
SimpleDBForge/
├── README.md
└── python/
    ├── __init__.py
    ├── btree/
    │   ├── __init__.py
    │   └── btree.py          # B-Tree implementation
    ├── hash_table/
    │   ├── __init__.py
    │   └── hash_table.py     # Hash Table implementation
    ├── lsm_tree/
    │   ├── __init__.py
    │   └── lsm_tree.py       # LSM-Tree implementation
    └── tests/
        ├── __init__.py
        ├── test_btree.py
        ├── test_hash_table.py
        └── test_lsm_tree.py
```

## 🎓 Learning Resources

Each implementation includes:
- **Detailed docstrings** explaining the concepts
- **Time complexity** annotations
- **Example usage** in comments
- **Comprehensive tests** demonstrating functionality

## 🤝 Contributing

Contributions are welcome! Feel free to:
- Add implementations in other programming languages
- Improve existing implementations
- Add more database data structures (Skip Lists, Red-Black Trees, etc.)
- Improve documentation and examples

## 📄 License

This project is open source and available for educational purposes.
