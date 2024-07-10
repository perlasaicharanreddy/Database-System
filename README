# Advanced Database Management System

## 1. Introduction
This project provides a comprehensive set of functions for managing file blocks and buffer pools. It includes operations for reading, writing, and manipulating file data, as well as initializing and managing index structures like B-trees.

## 2. Table of Contents
- [1. Introduction](#1-introduction)
- [2. Table of Contents](#2-table-of-contents)
- [3. File Operations](#3-file-operations)
  - [Read Operations](#read-operations)
  - [Write Operations](#write-operations)
- [4. Buffer Manager](#4-buffer-manager)
- [5. Environment](#5-environment)

## 3. File Operations

### Read Operations

#### 1. `readFirstBlock`
Reads the first block of the file and stores it in the provided memory page buffer.

#### 2. `readPreviousBlock`
Reads the block immediately before the current block in the file, moving backward.

#### 3. `readCurrentBlock`
Reads the current block from the file without changing the position.

#### 4. `readNextBlock`
Reads the block immediately after the current block in the file, moving forward.

#### 5. `readLastBlock`
Reads the last block of the file and stores it in the provided memory page buffer.

### Write Operations

#### 1. `writeBlock`
Writes data from a memory page to a specified page number in the file.

#### 2. `writeCurrentBlock`
Writes data from a memory page to the current position in the file.

## 4. Buffer Manager

### Initialization and Shutdown
- `initIndexManager`: Initialize the index manager.
- `shutdownIndexManager`: Shutdown the index manager.

### B-tree Operations
- `createBtree`: Create a B-tree.
- `openBtree`: Open a B-tree.
- `closeBtree`: Close a B-tree.
- `deleteBtree`: Delete a B-tree.

### B-tree Information
- `getNumNodes`: Get the number of nodes in a B-tree.
- `getNumEntries`: Get the number of entries in a B-tree.
- `getKeyType`: Get the key type of a B-tree (dummy function).

### B-tree Access
- `findKey`: Find a key in the B-tree.
- `insertKey`: Insert a key into the B-tree.
- `deleteKey`: Delete a key from the B-tree.
- `openTreeScan`: Open a tree scan.
- `nextEntry`: Get the next entry in the tree scan.
- `closeTreeScan`: Close a tree scan.

### Debug and Test Functions
- `printTree (BTreeHandle *tree)`: Print the B-tree structure.

## 5. Environment
The entire code has been tested on **macOS** and all test cases have been successful.
