# Advanced Database Management System

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Platform](https://img.shields.io/badge/platform-macOS-lightgrey)

## 🚀 Introduction
This project provides a comprehensive set of functions for managing file blocks and buffer pools. It includes operations for reading, writing, and manipulating file data, as well as initializing and managing index structures like B-trees.

## 📋 Table of Contents
- [🚀 Introduction](#-introduction)
- [📋 Table of Contents](#-table-of-contents)
- [📂 Features](#-features)
- [⚙️ Installation](#️-installation)
- [📖 Usage](#-usage)
- [🛠️ Development](#️-development)
- [🤝 Contributors](#-contributors)
- [📜 License](#-license)

## 📂 Features
- File operations: Read and write blocks of data.
- Buffer management: Initialize and manage buffer pools.
- B-tree operations: Create, open, close, and delete B-trees.
- Debugging tools: Print B-tree structures for analysis.

## ⚙️ Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/Database-System.git
   ```
2. Navigate to the project directory:
   ```bash
   cd Database-System
   ```
3. Build the project using the provided Makefile:
   ```bash
   make
   ```

## 📖 Usage
Run the test cases to verify functionality:
```bash
./test_assign4_1
./test_expr
```

## 🛠️ Development
### File Operations
- `readFirstBlock`: Reads the first block of the file.
- `writeBlock`: Writes data to a specified page number.

### Buffer Manager
- `initIndexManager`: Initialize the index manager.
- `createBtree`: Create a B-tree.

### Debugging
- `printTree`: Print the B-tree structure.

## 🤝 Contributors
- **Sai Charan Reddy Perla** - Initial work

## 📜 License
This project is licensed under the MIT License. See the LICENSE file for details.
