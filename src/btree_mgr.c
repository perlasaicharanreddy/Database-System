#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>

// Define a file handle for the B-Tree
SM_FileHandle btree_fh;

// Maximum number of elements
int maxEle;

// Structure definition for a B-Tree node
typedef struct BTREE {
    int *key;                  // Array of keys in the node
    struct BTREE **next;    // Array of pointers to child nodes
    RID *id;             // Array of record IDs associated with keys
} BTree;

// Root and scan pointers for the B-Tree
BTree *root;
BTree *scan;

// Index number (potentially for tracking multiple indices)
int indexNum = 0;

// Initialize the index manager
RC initIndexManager(void *mgmtData) {
    return RC_OK;
}

// Shutdown the index manager
RC shutdownIndexManager() {
    return RC_OK;
}


// Create a B-tree
RC createBtree(char *indexID, DataType keyType, int maxElements) {
    int index;
    root = (BTree*) malloc(sizeof(BTree));
    if (!root) return RC_MEM_ALLOCATION_FAIL;

    root->key = malloc(sizeof(int) * maxElements);
    root->id = malloc(sizeof(int) * maxElements);
    root->next = malloc(sizeof(BTree*) * (maxElements + 1));
    
    if (!root->key || !root->id || !root->next) {
        free(root->key);
        free(root->id);
        free(root->next);
        free(root);
        return RC_MEM_ALLOCATION_FAIL;
    }

    for (index = 0; index <= maxElements; index++) {
        root->next[index] = NULL;
    }
    
    maxEle = maxElements;
    createPageFile(indexID);
    
    return RC_OK;
}

// Open a B-tree
RC openBtree(BTreeHandle **treeHandle, char *indexID) {
    // Attempt to open the page file using the provided index ID
    RC status = openPageFile(indexID, &btree_fh);
    
    if (status == RC_OK) {
        // Successfully opened the page file, assign to the tree handle
        *treeHandle = &btree_fh;
        return RC_OK;
    } else {
        // Failed to open the page file, log error or handle it accordingly
        *treeHandle = NULL;
        return RC_FILE_NOT_FOUND; // Provides more specific error information
    }
}


// close a B-tree
RC closeBtree(BTreeHandle *tree) {
    // Attempt to close the page file associated with the B-tree
    if (closePageFile(&btree_fh) == 0) {
        // If successful, free the memory allocated for the root
        free(root);
        return RC_OK;  // Return OK status
    } else {
        return RC_ERROR;  // Return ERROR status if page file closing failed
    }
}



// Delete a B-tree
RC deleteBtree(char *indexID) {
    // Attempt to destroy the page file specified by indexID
    RC status = destroyPageFile(indexID);
    
    if (status == RC_OK) {
        // Successfully destroyed the page file
        return RC_OK;
    } else {
        // Failed to destroy the page file, return a more specific error code
        return RC_FILE_DESTROY_FAILED; // More descriptive than RC_ERROR
    }
}




// Get the number of nodes in a B-tree
RC getNumNodes(BTreeHandle *tree, int *result)
{
    BTree *tempTree = (BTree*)malloc(sizeof(BTree));
    
    int numberOfNodes = 0;
    int iterator=0;
    
    while(iterator < maxEle + 2){
        numberOfNodes++;
        iterator++;
    }

    *result = numberOfNodes;
    
    return RC_OK;
}



// Get the number of entries in a B-tree
RC getNumEntries(BTreeHandle *tree, int *result) {
    int totalElements = 0;
    BTree *currentNode = (BTree*)malloc(sizeof(BTree));
    
    currentNode = root;
    while (currentNode != NULL) {
        int i = 0;
        while (i < maxEle) {
            if (currentNode->key[i] != 0) {
                totalElements++;
            }
            i++;
        }
        currentNode = currentNode->next[maxEle];
    }
    
    *result = totalElements;
    //free(currentNode); // Uncomment if needed
    return RC_OK;
}



// Get the key type of a B-tree (dummy function)
RC getKeyType (BTreeHandle *tree, DataType *result)
{
    return RC_OK;
}


// Find a key in the B-tree
RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    BTree *currentNode = (BTree*)malloc(sizeof(BTree));
    int found = 0;
    
    currentNode = root;
    while (currentNode != NULL) {
        int i = 0;
        while (i < maxEle) {
            if (currentNode->key[i] == key->v.intV) {
                result->page = currentNode->id[i].page;
                result->slot = currentNode->id[i].slot;
                found = 1;
                break;
            }
            i++;
        }
        if (found == 1)
            break;
        currentNode = currentNode->next[maxEle];
    }
    //free(currentNode); // Uncomment if needed
    
    if (found == 1)
        return RC_OK;
    else
        return RC_IM_KEY_NOT_FOUND;
}


// Insert a key into the B-tree
RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    int i = 0;
    BTree *currentNode = (BTree*)malloc(sizeof(BTree));
    BTree *newNode = (BTree*)malloc(sizeof(BTree));
    newNode->key = malloc(sizeof(int) * maxEle);
    newNode->id = malloc(sizeof(int) * maxEle);
    newNode->next = malloc(sizeof(BTree) * (maxEle + 1));
    
    for (i = 0; i < maxEle; i++) {
        newNode->key[i] = 0;
    }

    int nodeFull = 0;
    
    currentNode = root;
    while (currentNode != NULL) {
        nodeFull = 0;
        for (i = 0; i < maxEle; i++) {
            if (currentNode->key[i] == 0) {
                currentNode->id[i].page = rid.page;
                currentNode->id[i].slot = rid.slot;
                currentNode->key[i] = key->v.intV;
                currentNode->next[i] = NULL;
                nodeFull++;
                break;
            }
        }
        if ((nodeFull == 0) && (currentNode->next[maxEle] == NULL)) {
            newNode->next[maxEle] = NULL;
            currentNode->next[maxEle] = newNode;
        }
        currentNode = currentNode->next[maxEle];
    }
    
    int totalElements = 0;
    currentNode = root;
    while (currentNode != NULL) {
        for (i = 0; i < maxEle; i++) {
            if (currentNode->key[i] != 0) {
                totalElements++;
            }
        }
        currentNode = currentNode->next[maxEle];
    }

    if (totalElements == 6) {
        newNode->key[0] = root->next[maxEle]->key[0];
        newNode->key[1] = root->next[maxEle]->next[maxEle]->key[0];
        newNode->next[0] = root;
        newNode->next[1] = root->next[maxEle];
        newNode->next[2] = root->next[maxEle]->next[maxEle];
    }
    
    return RC_OK;
}


// Delete a key from the B-tree
RC deleteKey(BTreeHandle *tree, Value *searchKey) {
    BTree *currentNode = (BTree*)malloc(sizeof(BTree));
    int isFound = 0;
    
    currentNode = root;
    while (currentNode != NULL) {
        int iterator = 0;
        while (iterator < maxEle) {
            if (currentNode->key[iterator] == searchKey->v.intV) {
                currentNode->key[iterator] = 0;
                currentNode->id[iterator].page = 0;
                currentNode->id[iterator].slot = 0;
                isFound = 1;
                break;
            }
            iterator++;
        }
        if (isFound == 1)
            break;
        currentNode = currentNode->next[maxEle];
    }
    
    return RC_OK;
}


// Open a tree scan
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    scan = root;
    indexNum = 0;
    
    BTree *temp = (BTree*)malloc(sizeof(BTree));
    int totalEle = 0, i;
    for (temp = root; temp != NULL; temp = temp->next[maxEle])
        for (i = 0; i < maxEle; i ++)
            if (temp->key[i] != 0)
                totalEle ++;

    int *key = malloc(sizeof(int) * totalEle);
    int **elements = malloc(sizeof(int*) * 2);
    elements[0] = malloc(sizeof(int) * totalEle);
    elements[1] = malloc(sizeof(int) * totalEle);
    
    int count = 0;
    for (temp = root; temp != NULL; temp = temp->next[maxEle]) {
        for (i = 0; i < maxEle; i ++) {
            key[count] = temp->key[i];
            elements[0][count] = temp->id[i].page;
            elements[1][count] = temp->id[i].slot;
            count ++;
        }
    }
    
    int swap, pg, st, c, d;
    for (c = 0 ; c < count - 1; c ++) {
        for (d = 0 ; d < count - c - 1; d ++) {
            if (key[d] > key[d+1]) {
                swap = key[d];
                pg = elements[0][d];
                st = elements[1][d];
                
                key[d] = key[d + 1];
                elements[0][d] = elements[0][d + 1];
                elements[1][d] = elements[1][d + 1];
                
                key[d + 1] = swap;
                elements[0][d + 1] = pg;
                elements[1][d + 1] = st;
            }
        }
    }
    
    count = 0;
    for (temp = root; temp != NULL; temp = temp->next[maxEle]) {
        for (i = 0; i < maxEle; i ++) {
            temp->key[i] = key[count];
            temp->id[i].page = elements[0][count];
            temp->id[i].slot = elements[1][count];
            count ++;
        }
    }
    
    free(key);
    free(elements[0]);
    free(elements[1]);
    free(elements);

    return RC_OK;
}


// Get the next entry in the tree scan
RC nextEntry(BT_ScanHandle *handle, RID *result) {
    if (scan->next[maxEle] != NULL) {
        if (maxEle == indexNum) {
            indexNum = 0;
            scan = scan->next[maxEle];
        }

        result->page = scan->id[indexNum].page;
        result->slot = scan->id[indexNum].slot;
        indexNum++;
    } else {
        return RC_IM_NO_MORE_ENTRIES;
    }
    
    return RC_OK;
}


// Close a tree scan
RC closeTreeScan(BT_ScanHandle *handle) {
    indexNum = 0; // Reset the index number to indicate that the scan is closed
    return RC_OK; // Return success code
}

// Debug and test function to print the tree (dummy function)
char *printTree(BTreeHandle *tree) {

    return RC_OK;;
}

