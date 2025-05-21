# Advanced Database Management System

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Platform](https://img.shields.io/badge/platform-macOS-lightgrey)

## 🚀 Introduction
This project provides a comprehensive set of functions for managing file blocks and buffer pools. It includes operations for reading, writing, and manipulating file data, as well as initializing and managing index structures like B-trees.

## 🌟 Motivation
The Advanced Database Management System project was developed to provide a robust and efficient solution for managing data storage and retrieval. The primary motivation was to create a system that simplifies file and buffer management while offering advanced indexing capabilities like B-trees. This project aims to serve as a learning tool for database management concepts and as a foundation for building more complex database systems.

## 🏗️ Development Goals
- **Efficiency**: Optimize file and buffer operations for better performance.
- **Scalability**: Design the system to handle large datasets and complex queries.
- **Modularity**: Ensure the codebase is modular and easy to extend.
- **Reliability**: Provide a reliable system with comprehensive test coverage.

## 🌍 Use Cases
- **Educational Purposes**: Ideal for students and professionals learning about database management systems.
- **Prototyping**: Can be used as a base for developing custom database solutions.
- **Research**: Useful for experimenting with new indexing and storage techniques.

## 📋 Table of Contents
- [🚀 Introduction](#-introduction)
- [🌟 Motivation](#-motivation)
- [🏗️ Development Goals](#-development-goals)
- [🌍 Use Cases](#-use-cases)
- [📋 Table of Contents](#-table-of-contents)
- [📂 Features](#-features)
- [⚙️ Installation](#️-installation)
- [📖 Usage](#-usage)
- [🛠️ Development](#️-development)
- [🤝 Contributors](#-contributors)
- [📜 License](#-license)
- [📚 Modules and Key Functions](#-modules-and-key-functions)

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