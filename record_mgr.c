#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "record_mgr.h"
#include "tables.h"
#include "expr.h"

/**
 * Function to initialize the record manager.
 * @param mgmtData A pointer to additional manager-specific data (not used in this implementation).
 * @return RC_OK on success, or an error code otherwise.
 */

RC initRecordManager (void *mgmtData) {
    // Print a message to indicate the initialization of the record manager
    printf("Initializing the Record Manager...\n");
    
    // Return success code indicating successful initialization
    return RC_OK;
}

/**
 * Function to shutdown the record manager and release any resources.
 * @return RC_OK on success, or an error code otherwise.
 */

RC shutdownRecordManager() {
    // Print a message to indicate the shutdown of the record manager
    printf("Shutting down the Record Manager...\n");
    
    // Return success code indicating successful shutdown
    return RC_OK;
}

/**
 * Creates a table by initializing the underlying page file and storing metadata such as the schema, free space information, etc., in the Table Information pages.
 * This function sets up a new table with its schema defined and handles the organization of data within the system's storage by establishing a base page file structure.
 *
 * @param name Pointer to the name of the table to be created.
 * @param schema Pointer to the schema structure defining table layout and types.
 * @return RC Result code representing success or failure of table creation.
 */

// Helper function to handle memory allocation and ensure cleanliness.
char* allocatePage() {
    char *page = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (!page) {
        perror("Failed to allocate memory for page");
        exit(EXIT_FAILURE);  // Adjust based on error handling strategy
    }
    return page;
}

// Simplified error handling and cleanup
RC handleFailure(SM_FileHandle *fh, char *buffer) {
    free(buffer);  // Ensure we free any allocated memory
    closePageFile(fh);  // Attempt to close the file if there is an error
    return RC_WRITE_FAILED;  // Use a general write failure code or specific, depending on your error handling strategy
}

// Writes metadata and schema to file, handling large schemas that span multiple pages.
RC writeSchemaToFile(SM_FileHandle *fh, char *schema, int totalSize) {
    char *buffer = allocatePage();
    RC status;
    int i;

    for (i = 0; i < totalSize; ++i) {
        memset(buffer, 0, PAGE_SIZE);  // Clear the buffer
        int bytesToWrite = (i < totalSize - 1) ? PAGE_SIZE : strlen(schema + i * PAGE_SIZE);
        memcpy(buffer, schema + i * PAGE_SIZE, bytesToWrite);
        status = writeBlock(i, fh, buffer);
        if (status != RC_OK) {
            return handleFailure(fh, buffer);
        }
    }
    free(buffer);
    return RC_OK;
}

// Main function to create a table
RC createTable(char *name, Schema *schema) {
    SM_FileHandle fh;
    char *buffer;
    int metadata[4];  // Array to hold metadata integers
    RC status;

    if ((status = createPageFile(name)) != RC_OK)
        return status;

    if ((status = openPageFile(name, &fh)) != RC_OK)
        return status;

    buffer = allocatePage();  // Allocating a single page
    char *schemaSerialized = serializeSchema(schema);
    int fileMetadataSize = (strlen(schemaSerialized) + 4 * sizeof(int) - 1) / PAGE_SIZE + 1;

    // Prepare buffer to write metadata
    metadata[0] = fileMetadataSize;
    metadata[1] = getRecordSize(schema) / 256;  // Assuming slotSize is always 256
    metadata[2] = 256;  // slotSize
    metadata[3] = 0;    // recordNum initially 0

    // Copy metadata to buffer
    for (int i = 0; i < 4; i++) {
        memcpy(buffer + i * sizeof(int), &metadata[i], sizeof(int));
    }
    memcpy(buffer + 4 * sizeof(int), schemaSerialized, strlen(schemaSerialized));

    // Write the first block and manage schema spanning multiple pages
    if ((status = writeBlock(0, &fh, buffer)) != RC_OK) {
        return handleFailure(&fh, buffer);
    }
    free(buffer);  // Free the initial buffer

    if (fileMetadataSize > 1) {
        if ((status = writeSchemaToFile(&fh, schemaSerialized + (PAGE_SIZE - 4 * sizeof(int)), fileMetadataSize - 1)) != RC_OK)
            return status;
    }

    if ((status = addPageMetadataBlock(&fh)) != RC_OK) {
        closePageFile(&fh);
        return status;
    }

    return closePageFile(&fh);
}

/**
 * Opens a table for manipulation.
 * This function is responsible for preparing the table for data operations by opening the associated page file and setting up necessary metadata.
 *
 * @param tableData Pointer to the RM_TableData structure to be initialized.
 * @param tableName Pointer to the name of the table to open.
 * @return RC Result code indicating the success or failure of the operation.
 */



RC openTable (RM_TableData *tableData, char *tableName) {
   RC returnCode;
    SM_FileHandle *fileHandler = (SM_FileHandle*)calloc(1, sizeof(SM_FileHandle));
    BM_BufferPool *bufferPool = MAKE_POOL();
    BM_PageHandle *pageHandler = MAKE_PAGE_HANDLE();

    
    int attributeCount;
    int *typeLengths, *keyAttributes;
    int keyCount;
    DataType *attributeTypes;
    char **attributeNames;

    int metadataSize;
    char *schemaText;
    char *cursor, *temp;
    int i, j, k;

    // Open the file and initialize the buffer pool
    returnCode = openPageFile(tableName, fileHandler);
    if (returnCode != RC_OK) {
        return returnCode;
    }

    returnCode = initBufferPool(bufferPool, tableName, 10, RS_LRU, NULL);
    if (returnCode != RC_OK) {
        return returnCode;
    }

    // Read the first page to determine the size of metadata
    metadataSize = getFileMetaDataSize(bufferPool);

    // Retrieve the schema as text
    i = 0;
    while (i < metadataSize) {
        returnCode = pinPage(bufferPool, pageHandler, i);
        if (returnCode != RC_OK) {
            return returnCode;
        }

        returnCode = unpinPage(bufferPool, pageHandler);
        if (returnCode != RC_OK) {
            return returnCode;
        }
        i++;
    }

    schemaText = pageHandler->data + 4 * sizeof(int);

    // Process the schema text and convert it to specific types

    // Determine the number of attributes
    cursor = strchr(schemaText, '<');
    if (cursor == NULL) {
        return RC_INVALID_SCHEMA;
    }
    cursor++;

    temp = (char *)calloc(40, sizeof(char));
    i = 0;
    while (*(cursor + i) != '>') {
        temp[i] = cursor[i];
        i++;
    }
    attributeCount = atoi(temp);
    free(temp);

    // Extract attribute names, data types, and type lengths
    cursor = strchr(cursor, '(');
    if (cursor == NULL) {
        return RC_INVALID_SCHEMA;
    }
    cursor++;

    attributeNames = (char **)calloc(attributeCount, sizeof(char *));
    attributeTypes = (DataType *)calloc(attributeCount, sizeof(DataType));
    typeLengths = (int *)calloc(attributeCount, sizeof(int));

    i = 0;
    while (i < attributeCount) {
        j = 0;
        while (*(cursor + j) != ':') {
            j++;
        }

        attributeNames[i] = (char *)calloc(j, sizeof(char));
        memcpy(attributeNames[i], cursor, j);

        if (*(cursor + j + 2) == 'I') {
            attributeTypes[i] = DT_INT;
            typeLengths[i] = 0;
        } else if (*(cursor + j + 2) == 'F') {
            attributeTypes[i] = DT_FLOAT;
            typeLengths[i] = 0;
        } else if (*(cursor + j + 2) == 'S') {
            attributeTypes[i] = DT_STRING;

            temp = (char *)calloc(40, sizeof(char));
            k = 0;
            while (*(cursor + k + j + 9) != ']') {
                temp[k] = cursor[k + j + 9];
                k++;
            }
            typeLengths[i] = atoi(temp);
            free(temp);
        } else if (*(cursor + j + 2) == 'B') {
            attributeTypes[i] = DT_BOOL;
            typeLengths[i] = 0;
        } else {
            return RC_RM_UNKOWN_DATATYPE;
        }

        if (i == attributeCount - 1) {
            break;
        }

        cursor = strchr(cursor, ',');
        if (cursor == NULL) {
            return RC_INVALID_SCHEMA;
        }
        cursor += 2;
        i++;
    }

    // Extract key attributes and determine key count
    cursor = strchr(cursor, '(');
    if (cursor == NULL) {
        return RC_INVALID_SCHEMA;
    }
    cursor++;

    keyCount = 0;
    while (strchr(cursor, ',') != NULL) {
        keyCount++;
    }

    keyAttributes = (int *)calloc(keyCount, sizeof(int));

   for (int i = 0; i < keyCount; ++i) {
    // Find the next delimiter (',' or ')')
    char *delimiter = strchr(cursor, ',');
    if (delimiter == NULL) {
        delimiter = strchr(cursor, ')');
    }

    // Calculate the length of the attribute name substring
    size_t attributeNameLength = delimiter - cursor;

    // Allocate memory for the attribute name substring and copy it
    char *temp = (char *)calloc(attributeNameLength + 1, sizeof(char));
    if (temp == NULL) {
        
        return RC_MEM_ALLOC_FAILED;
    }
   

    // Copy the attribute name substring using strncpy
    strncpy(temp, cursor, attributeNameLength);
    // Null-terminate the string
    temp[attributeNameLength] = '\0'; 

    // Find the index of the attribute name in attributeNames array
    for (int k = 0; k < attributeCount; ++k) {
        if (strcmp(temp, attributeNames[k]) == 0) {
            keyAttributes[i] = k; 
            break;
        }
    }

    free(temp);

    // Move cursor pointer to the next attribute name (skipping delimiter and possible space)
    cursor = delimiter + 1;
    if (*cursor == ' ') {
        cursor++;
    }
}


    // Assign the extracted information to the table data structure
    // Create schema
Schema *tableSchema = createSchema(attributeCount, attributeNames, attributeTypes, typeLengths, keyCount, keyAttributes);
if (tableSchema == NULL) {
  
    return RC_SCHEMA_CREATION_FAILED;
}

// Duplicate tableName string
char *tableNameCopy = strdup(tableName);
if (tableNameCopy == NULL) {
    
    return RC_MEM_ALLOC_FAILED;
}

// Assign values to tableData
tableData->name = tableNameCopy;
tableData->schema = tableSchema;
tableData->bm = bufferPool;
tableData->fh = fileHandler;


    return RC_OK;
}



/**
 * Closes a table after manipulation.
 * This function is responsible for finalizing operations on the table and releasing associated resources.
 *
 * @param tableData Pointer to the RM_TableData structure representing the table to close.
 * @return RC Result code indicating the success or failure of the operation.
 */


RC closeTable(RM_TableData *tableData) {
    // Check if the input pointer is not NULL
    if (tableData == NULL) {
        return RC_NULL_POINTER;
    }

    // Free the schema memory if it exists
    if (tableData->schema != NULL) {
        freeSchema(tableData->schema);
        tableData->schema = NULL;  // Good practice to set freed pointer to NULL
    }

    // Shutdown the buffer pool, if initialized
    if (tableData->bm != NULL) {
        RC shutdownStatus = shutdownBufferPool(tableData->bm);
        if (shutdownStatus != RC_OK) {
            return shutdownStatus;  // Return error if buffer pool shutdown fails
        }
        free(tableData->bm);
        tableData->bm = NULL;
    }

    // Free the file handle if it exists
    if (tableData->fh != NULL) {
        free(tableData->fh);
        tableData->fh = NULL;
    }

    return RC_OK;  // Return success
}

/**
 * Function to delete a table and its associated data.
 * @param name The name of the table to be deleted.
 * @return RC_OK on success, or an error code otherwise.
 */
RC deleteTable (char *name) {
    return destroyPageFile(name);
}


/**
 * Function to get the number of tuples (records) in the specified table.
 * @param table The table handle.
 * @return The number of tuples in the table, or an error code otherwise.
 */

int getNumTuples(RM_TableData *table) {
    if (!table || !table->bm) {
        // Check if the table data or buffer manager pointer is NULL
        return -1;  // Use appropriate error code or handling as per your system's design
    }

    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();
    if (!pageHandle) {
        // Handle memory allocation failure
        return -1;  // Memory allocation failed, return error code
    }

    // Initialize numTuples with an error code in case later operations fail
    int numTuples = -1;  // Default value in case of failure

    // Attempt to pin the first page
    RC pinStatus = pinPage(table->bm, pageHandle, 0);
    if (pinStatus != RC_OK) {
        free(pageHandle);
        return -1;  // Return error code specific to pin failure
    }

    // Check if page data is not NULL to avoid dereferencing NULL
    if (pageHandle->data != NULL) {
        // Extract the number of tuples from the metadata
        // Ensure that the data pointer plus offset is not out of bounds if applicable
        numTuples = *((int *)(pageHandle->data + 3 * sizeof(int)));
    }

    // Always attempt to unpin the page regardless of previous success
    RC unpinStatus = unpinPage(table->bm, pageHandle);
    if (unpinStatus != RC_OK) {
        free(pageHandle);
        return -1;  // Return error code specific to unpin failure
    }

    // Clean up and return the number of tuples
    free(pageHandle);
    return numTuples;
}

/**
 * Inserts a record into the specified table.
 * This function adds a new record to the table, incorporating it into the existing data set.
 *
 * @param rel Pointer to the RM_TableData structure representing the table.
 * @param record Pointer to the Record structure containing the data to insert.
 * @return RC Result code indicating the success or failure of the insertion operation.
 */

RC insertRecord (RM_TableData *rel, Record *record) {
   // Allocate memory for a page handle
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    if (h == NULL) {
        return RC_INSUFFICIENT_MEMORY;  // Memory allocation failed
    }
    
    // Get the size of a record based on the table's schema
    int r_size = getRecordSize(rel->schema);
    
    // Get the size of metadata in a page
    int p_meta_index = getFileMetaDataSize(rel->bm);
    
    // Calculate the number of slots needed for the record
    int r_slotnum = (r_size + sizeof(bool)) / 256 + 1;
    
    // Initialize offset
    int offset = 0;
    
    // Variables for record management
    int r_current_num, numTuples;
    bool r_stat = true;
       
    // Find out the target page and slot at the end.
    bool continueLoop = true;
while (continueLoop) {
    pinPage(rel->bm, h, p_meta_index);
    memcpy(&p_meta_index, h->data + PAGE_SIZE - sizeof(int), sizeof(int));
    if (p_meta_index != -1) {
        unpinPage(rel->bm, h);
    } else {
        continueLoop = false;
    }
}


    // Find out the target meta index and record number of the page.
   while (true) {
    memcpy(&r_current_num, h->data + offset + sizeof(int), sizeof(int));
    offset += 2 * sizeof(int);
    if (r_current_num != PAGE_SIZE / 256) {
        break;
    }
}

    // If no page exist, add new page.
    if(r_current_num == -1){       
        // If page mata is full, add new matadata block.
        if(offset == PAGE_SIZE){       
            memcpy(h->data + PAGE_SIZE - sizeof(int), &rel->fh->totalNumPages, sizeof(int));   // Link into new meta data page. 
            addPageMetadataBlock(rel->fh);
            markDirty(rel->bm, h);
            unpinPage(rel->bm, h);      // Unpin the last meta page.
            pinPage(rel->bm, h, rel->fh->totalNumPages-1);  // Pin the new page.
            offset = 2*sizeof(int);
        }
        memcpy(h->data + offset - 2*sizeof(int), &rel->fh->totalNumPages, sizeof(int));  // set page number.
        appendEmptyBlock(rel->fh);
        r_current_num = 0;                                  
    } 

    // Read record->id and set record number add 1 in meta data.
    memcpy(&record->id.page, h->data + offset - 2*sizeof(int), sizeof(int));   // Set record->id page number.
    record->id.slot = r_current_num * r_slotnum;                                // Set record->id slot.
    r_current_num++;                                
    memcpy(h->data + offset - sizeof(int), &r_current_num, sizeof(int));   // Set record number++ into meta data.
    markDirty(rel->bm, h);
    unpinPage(rel->bm, h);              // unpin meta page.

    // Insert record header and record data into page.
    pinPage(rel->bm, h, record->id.page);
    memcpy(h->data + 256*record->id.slot, &r_stat, sizeof(bool));   // Record header is a del_flag 
    memcpy(h->data + 256*record->id.slot + sizeof(bool), record->data, r_size); // Record body is values.
    markDirty(rel->bm, h);
    unpinPage(rel->bm, h);
    // Tuple number add 1.
    pinPage(rel->bm, h, 0);
    memcpy(&numTuples, h->data + 3 * sizeof(int), sizeof(int));
    numTuples++;       
    memcpy(h->data + 3 * sizeof(int), &numTuples, sizeof(int));
    markDirty(rel->bm, h);
    unpinPage(rel->bm, h);

    free(h);
    return RC_OK;
}

/**
 * Deletes a record from the specified table by its ID.
 * This function removes the record identified by the given ID from the table.
 *
 * @param table Pointer to the RM_TableData structure representing the table.
 * @param id The ID of the record to delete.
 * @return RC Result code indicating the success or failure of the deletion operation.
 */


RC deleteRecord(RM_TableData *table, RID id) {
    // Allocate memory for a page handle
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();
    if (pageHandle == NULL) {
        return RC_INSUFFICIENT_MEMORY;  // Memory allocation failed
    }

    int recordSize = getRecordSize(table->schema);
    int numTuples;
    
    // Mark record as deleted by setting its status to false.
    char deletedRecord[recordSize + sizeof(bool)]; // Deleted record buffer
    memset(deletedRecord, 0, sizeof(bool)); // Set status to false
    memset(deletedRecord + sizeof(bool), 0, recordSize); // Clear record data
    
    // Delete record.
    pinPage(table->bm, pageHandle, id.page);
    memcpy(pageHandle->data + PAGE_SIZE * id.slot, deletedRecord, sizeof(bool) + recordSize);
    markDirty(table->bm, pageHandle);
    unpinPage(table->bm, pageHandle);
    
    // Update total tuple count.
    pinPage(table->bm, pageHandle, 0);
    memcpy(&numTuples, pageHandle->data + 3 * sizeof(int), sizeof(int));
    numTuples--;       
    memcpy(pageHandle->data + 3 * sizeof(int), &numTuples, sizeof(int));
    markDirty(table->bm, pageHandle);
    unpinPage(table->bm, pageHandle);
    
    free(pageHandle);
    return RC_OK;
}


/**
 * Updates a record in the specified table based on its ID.
 * This function modifies the data of the record identified by its ID in the table.
 *
 * @param table Pointer to the RM_TableData structure representing the table.
 * @param newRecord Pointer to the Record structure containing the updated data.
 * @return RC Result code indicating the success or failure of the update operation.
 */


RC updateRecord(RM_TableData *table, Record *newRecord) {
    // Calculate the size of a record based on the table's schema
    int recordSize = getRecordSize(table->schema);
    
    // Pin the page where the record to be updated resides
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();
    if (pageHandle == NULL) {
        return RC_INSUFFICIENT_MEMORY;  // Memory allocation failed
    }
    pinPage(table->bm, pageHandle, newRecord->id.page);
    
    // Calculate the offset to the location of the record data within the page
    int offset = 256 * newRecord->id.slot + sizeof(bool);
    
    // Copy the updated record data into the appropriate location in the page data
    char *pageData = pageHandle->data;
    char *newRecordData = newRecord->data;
    for (int i = 0; i < recordSize; i++) {
        pageData[offset + i] = newRecordData[i];
    }
    
    // Mark the page as dirty since we modified it
    markDirty(table->bm, pageHandle);
    
    // Unpin the page after finishing the update
    unpinPage(table->bm, pageHandle);
    
    // Release the memory allocated for the page handle
    free(pageHandle);
    
    return RC_OK;  // Return success
}


/**
 * Retrieves a record from the specified table based on its ID.
 * This function retrieves the record identified by the given ID from the table.
 *
 * @param table Pointer to the RM_TableData structure representing the table.
 * @param recordID The recordID of the record to retrieve.
 * @param outputRecord Pointer to the Record structure to populate with the retrieved data.
 * @return RC Result code indicating the success or failure of the retrieval operation.
 */

RC getRecord(RM_TableData *table, RID recordID, Record *outputRecord) {
    // Allocate memory for a page handle
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();
    if (pageHandle == NULL) {
        return RC_INSUFFICIENT_MEMORY;  // Memory allocation failed
    }
    
    // Get the size of a record based on the table's schema
    int recordSize = getRecordSize(table->schema);
    
    // Variable to store the existence of the record
    bool recordExists;

    // Set the ID of the output record
    outputRecord->id = recordID;

    // Pin the page where the record resides
    pinPage(table->bm, pageHandle, recordID.page);

    // Check if the record exists by reading the status flag
    recordExists = *(bool *)(pageHandle->data + 256 * recordID.slot);
    if (!recordExists) {
        free(pageHandle);
        return RC_RM_RECORD_NOT_EXIST;  // Record doesn't exist
    } else {
        // Allocate memory for the record data
        outputRecord->data = (char*)malloc(recordSize);
        if (outputRecord->data == NULL) {
            unpinPage(table->bm, pageHandle);
            free(pageHandle);
            return RC_INSUFFICIENT_MEMORY;  // Memory allocation failed
        }
        
        // Copy the record data skipping the status flag
        char *recordDataPtr = pageHandle->data + 256 * recordID.slot + sizeof(bool);
        for (int i = 0; i < recordSize; i++) {
            outputRecord->data[i] = recordDataPtr[i];
        }
        
        // Unpin the page after reading the record
        unpinPage(table->bm, pageHandle);
        
        // Release the memory allocated for the page handle
        free(pageHandle);
        
        return RC_OK;  // Return success
    }
}


/**
 * Initializes a scan based on the specified parameters.
 * This function sets up a scan operation on the given table with the provided scan handle and optional condition.
 *
 * @param table Pointer to the RM_TableData structure representing the table to scan.
 * @param scan Pointer to the RM_ScanHandle structure to initialize for the scan.
 * @param condition Pointer to the Expr structure representing the optional condition for the scan.
 * @return RC Result code indicating the success or failure of the scan initialization.
 */

RC startScan(RM_TableData *table, RM_ScanHandle *scan, Expr *condition) {
    if (table == NULL || scan == NULL) {
        return RC_INVALID_HANDLE;
    }

    memset(scan, 0, sizeof(RM_ScanHandle)); // Initialize scan handle to zero

    scan->rel = table;
    scan->expr = condition;

    return RC_OK;
}


/**
 * Searches for the next tuple in the scan handle that satisfies the scan condition and returns it in the parameter "record".
 * This function advances the scan operation to the next tuple that meets the specified condition.
 *
 * @param scan Pointer to the RM_ScanHandle structure representing the scan operation.
 * @param record Pointer to the Record structure to populate with the next tuple.
 * @return RC Result code indicating the success or failure of the operation.
 */

RC next(RM_ScanHandle *scan, Record *record) {
    if (scan == NULL || record == NULL || scan->rel == NULL || scan->rel->bm == NULL || scan->expr == NULL) {
        return RC_NULL_POINTER;
    }

    BM_BufferPool *bufferPool = scan->rel->bm;
    int index = getFileMetaDataSize(bufferPool);
    int tupleSize = (getRecordSize(scan->rel->schema) + sizeof(bool)) / 256 + 1;

    BM_PageHandle *pageHandle = (BM_PageHandle*)calloc(1, sizeof(BM_PageHandle));
    if (pageHandle == NULL) {
        return RC_MEM_ERROR;
    }

    pinPage(bufferPool, pageHandle, index);

    while (scan->currentPage != index) {
        int rPage, maxSlot;
        memcpy(&rPage, pageHandle->data + (scan->currentPage) * 2 * sizeof(int), sizeof(int));
        memcpy(&maxSlot, pageHandle->data + ((scan->currentPage) * 2 + 1) * sizeof(int), sizeof(int));

        if (maxSlot != -1) {
            for (int i = scan->currentSlot; i < maxSlot; i++) {
                RID rid = { .page = rPage, .slot = i * tupleSize };
                Record *tempRecord = (Record *)calloc(1, sizeof(Record));
                if (tempRecord == NULL) {
                    unpinPage(bufferPool, pageHandle);
                    free(pageHandle);
                    return RC_MEM_ERROR;
                }

                RC rc = getRecord(scan->rel, rid, tempRecord);
                if (rc == RC_OK) {
                    Value *result = (Value *)calloc(1, sizeof(Value));
                    if (result == NULL) {
                        free(tempRecord);
                        unpinPage(bufferPool, pageHandle);
                        free(pageHandle);
                        return RC_MEM_ERROR;
                    }

                    evalExpr(tempRecord, scan->rel->schema, scan->expr, &result);
                    if (result->v.boolV) {
                        record->id.page = rid.page;
                        record->id.slot = rid.slot;
                        record->data = tempRecord->data;

                        if (i == maxSlot - 1) {
                            scan->currentPage++;
                            scan->currentSlot = 0;
                        } else {
                            scan->currentSlot = i + 1;
                        }

                        free(result);
                        free(tempRecord);
                        unpinPage(bufferPool, pageHandle);
                        free(pageHandle);
                        return RC_OK;
                    }

                    free(result);
                }

                free(tempRecord);
            }
        } else {
            unpinPage(bufferPool, pageHandle);
            free(pageHandle);
            return RC_RM_NO_MORE_TUPLES;
        }

        scan->currentPage++;
    }

    unpinPage(bufferPool, pageHandle);
    free(pageHandle);
    return RC_RM_NO_MORE_TUPLES;
}

/**
 * Function to close a scan handle.
 * @param scan The scan handle to be closed.
 * @return RC_OK on success, or an error code otherwise.
 */

RC closeScan(RM_ScanHandle *scan) {
    // No memory deallocation needed for scan since it was not dynamically allocated
    return RC_OK;
}

/**
 * Retrieves the size of a record described by the provided schema.
 * This function calculates the size of a record based on the layout defined by the schema.
 *
 * @param schema Pointer to the Schema structure describing the record's layout.
 * @return int Size of the record described by the schema.
 */

int getRecordSize(Schema *schema) {
    int totalSize = 0;
    for (int i = 0; i < schema->numAttr; i++) {
        if (schema->dataTypes[i] == DT_BOOL) {
            totalSize += sizeof(bool);
        } else if (schema->dataTypes[i] == DT_INT) {
            totalSize += sizeof(int);
        } else if (schema->dataTypes[i] == DT_FLOAT) {
            totalSize += sizeof(float);
        } else if (schema->dataTypes[i] == DT_STRING) {
            totalSize += schema->typeLength[i];
        }
    }
    return totalSize;
}

/**
 * Creates a new schema described by the provided parameters.
 * This function initializes a new schema with the specified attributes, data types, lengths, key size, and keys.
 *
 * @param numAttr Number of attributes in the schema.
 * @param attrNames Array of strings representing the names of attributes.
 * @param dataTypes Array of DataType values representing the data types of attributes.
 * @param typeLength Array of integers representing the lengths of data types.
 * @param keySize Size of the key.
 * @param keys Array of integers representing the keys.
 * @return Schema The created schema.
 */

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
   
   Schema *newSchema = (Schema *)malloc(sizeof(Schema));
if (newSchema != NULL) {
    *newSchema = (Schema){
        .numAttr = numAttr,
        .attrNames = attrNames,
        .dataTypes = dataTypes,
        .typeLength = typeLength,
        .keySize = keySize,
        .keyAttrs = keys
    };
}
return newSchema;

}
/**
 * Frees the memory space allocated for the provided schema.
 * This function deallocates the memory space used by the schema.
 *
 * @param schema Pointer to the Schema structure to be freed.
 * @return RC Result code indicating the success or failure of the operation.
 */


RC freeSchema(Schema *schema) {
    if (schema == NULL) {
        return RC_OK; // Schema is already NULL
    }
    free(schema->dataTypes);
	free(schema->typeLength);
    free(schema->keyAttrs);
    
    
    if (schema->attrNames != NULL) {
        int i=0;
        while(i < schema->numAttr){
            free(schema->attrNames[i]);
            i++;
        }
        free(schema->attrNames);
    }
    free(schema);

    return RC_OK;
}

/**
 * Creates a new record based on the provided schema.
 * This function initializes a new record according to the layout defined by the schema.
 *
 * @param record Pointer to the Record structure to be created.
 * @param schema Pointer to the Schema structure defining the record's layout.
 * @return RC Result code indicating the success or failure of the operation.
 */

RC createRecord(Record **record, Schema *schema) {
    if (record == NULL || schema == NULL) {
        return RC_NULL_POINTER; // Check for NULL pointers
    }

    *record = (Record *)calloc(1, sizeof(Record));
    if (*record == NULL) {
        return RC_MEM_ERROR; // Check for memory allocation failure
    }

    (*record)->data = (char *)calloc(getRecordSize(schema), sizeof(char));
    if ((*record)->data == NULL) {
        free(*record); // Free allocated memory on failure
        return RC_MEM_ERROR; // Return memory error if allocation fails
    }

    return RC_OK;
}
/**
 * Frees the memory space allocated for a record.
 * This function deallocates the memory space used by the provided record.
 *
 * @param record Pointer to the Record structure to be freed.
 * @return RC Result code indicating the success or failure of the operation.
 */

RC freeRecord(Record *record) {
    if (record == NULL) {
        return RC_OK; // Record is already NULL
    }

    free(record->data);
	
    free(record);

    return RC_OK;
}

/**
 * Retrieves the value of the specified attribute from a record.
 * This function gets the value of the attribute identified by the attribute number from the provided record.
 *
 * @param record Pointer to the Record structure from which to retrieve the attribute value.
 * @param schema Pointer to the Schema structure defining the record's layout.
 * @param attributeNumber The number of the attribute to retrieve.
 * @param value Pointer to a pointer to Value structure to store the retrieved value.
 * @return RC Result code indicating the success or failure of the operation.
 */

RC getAttr(Record *record, Schema *schema, int attributeNumber, Value **value) {
    // Check for NULL pointers
    if (record == NULL || schema == NULL || value == NULL) {
        return RC_NULL_POINTER;
    }

    // Check for invalid attribute number
    if (attributeNumber < 0 || attributeNumber >= schema->numAttr) {
        return RC_INVALID_ATTR_NUM;
    }

    int offset = 0;
    int index = 0;

    // Calculate offset
    for (; index < attributeNumber; index++) {
    if (schema->dataTypes[index] == DT_BOOL) {
        offset += sizeof(bool);
    } else if (schema->dataTypes[index] == DT_INT) {
        offset += sizeof(int);
    } else if (schema->dataTypes[index] == DT_FLOAT) {
        offset += sizeof(float);
    } else if (schema->dataTypes[index] == DT_STRING) {
        offset += schema->typeLength[index];
    }
}


    // Get value from record
    *value = (Value *)malloc(sizeof(Value));
    if (*value == NULL) {
        return RC_MEM_ERROR; // Memory allocation failure
    }

    // Assign data type
    (*value)->dt = schema->dataTypes[attributeNumber];
    if (schema->dataTypes[attributeNumber] == DT_BOOL) {
    memcpy(&((*value)->v.boolV), record->data + offset, sizeof(bool));
} else if (schema->dataTypes[attributeNumber] == DT_INT) {
    memcpy(&((*value)->v.intV), record->data + offset, sizeof(int));
} else if (schema->dataTypes[attributeNumber] == DT_FLOAT) {
    memcpy(&((*value)->v.floatV), record->data + offset, sizeof(float));
} else if (schema->dataTypes[attributeNumber] == DT_STRING) {
    // Allocate memory for string value
    (*value)->v.stringV = (char *)malloc(schema->typeLength[attributeNumber] + 1);
    if ((*value)->v.stringV == NULL) {
        free(*value); // Free allocated memory on failure
        return RC_MEM_ERROR; // Memory allocation failure
    }
    // Copy string value and append '\0'
    memcpy((*value)->v.stringV, record->data + offset, schema->typeLength[attributeNumber]);
    (*value)->v.stringV[schema->typeLength[attributeNumber]] = '\0';
}

    return RC_OK;
}


/**
 * Sets the value of the specified attribute in a record.
 * This function updates the value of the attribute identified by the attribute number in the provided record.
 *
 * @param record Pointer to the Record structure in which to set the attribute value.
 * @param schema Pointer to the Schema structure defining the record's layout.
 * @param attributeNumber The number of the attribute to set.
 * @param value Pointer to the Value structure containing the new value to set.
 * @return RC Result code indicating the success or failure of the operation.
 */

RC setAttr(Record *record, Schema *schema, int attributeNumber, Value *value) {
    // Check for NULL pointers
    if (record == NULL || schema == NULL || value == NULL) {
        return RC_NULL_POINTER;
    }

    // Check for invalid attribute number
    if (attributeNumber < 0 || attributeNumber >= schema->numAttr) {
        return RC_INVALID_ATTR_NUM;
    }

    // Calculate offset
    int offset = 0;
    int index = 0;
    for (; index < attributeNumber; index++) {
    DataType dataType = schema->dataTypes[index];
    if (dataType == DT_BOOL) {
        offset += sizeof(bool);
    } else if (dataType == DT_STRING) {
        offset += schema->typeLength[index];
    } else if (dataType == DT_FLOAT) {
        offset += sizeof(float);
    } else if (dataType == DT_INT) {
        offset += sizeof(int);
    }
}

    // Set value into record
    if (schema->dataTypes[attributeNumber] == DT_BOOL) {
    memcpy(record->data + offset, &(value->v.boolV), sizeof(bool));
} else if (schema->dataTypes[attributeNumber] == DT_INT) {
    memcpy(record->data + offset, &(value->v.intV), sizeof(int));
} else if (schema->dataTypes[attributeNumber] == DT_FLOAT) {
    memcpy(record->data + offset, &(value->v.floatV), sizeof(float));
} else if (schema->dataTypes[attributeNumber] == DT_STRING) {
    // Ensure the string fits within the specified length
    strncpy(record->data + offset, value->v.stringV, schema->typeLength[attributeNumber]);
    // Null-terminate the string
    record->data[offset + schema->typeLength[attributeNumber]] = '\0';
}


    return RC_OK;
}


/**
 * Adds a block containing pages metadata to the specified file handle.
 * This function appends a block to the file to store metadata related to pages.
 *
 * @param filehandle Pointer to the SM_FileHandle structure representing the file handle.
 * @return RC Result code indicating the success or failure of the operation.
 */

RC addPageMetadataBlock(SM_FileHandle *fileHandle) {
    // Check for NULL pointer
    if (fileHandle == NULL) {
        return RC_NULL_POINTER;
    }

    // Append an empty block to the file
    RC result = appendEmptyBlock(fileHandle);
    if (result != RC_OK) {
        // Close the file handle on error
        result = closePageFile(fileHandle);
        return result;
    }

    // Calculate the number of metadata entries that can fit in a page
    int metadataEntriesPerPage = PAGE_SIZE / (2 * sizeof(int));

    // Allocate memory for the page metadata block
    char *metadataBlock = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (metadataBlock == NULL) {
        return RC_MEM_ERROR; // Memory allocation failed
    }

    // Initialize metadata values
    int capacity = -1;
    int pageNum = fileHandle->totalNumPages;
    int metadataIndex = 0;

    // Populate the page metadata block
    char *metadataPtr = metadataBlock; // Pointer to the beginning of the metadata block

for (int i = 0; i < metadataEntriesPerPage; i++) {
    // Calculate the page number for this metadata entry
    int currentPageNum = fileHandle->totalNumPages - metadataEntriesPerPage + i;
    if (i == metadataEntriesPerPage - 1) {
        currentPageNum = fileHandle->totalNumPages - 1;
    }
    
    // Copy the page number and capacity to the metadata block using pointer arithmetic
    int *pageNumPtr = (int *)(metadataPtr + i * 2 * sizeof(int));
    int *capacityPtr = (int *)(metadataPtr + i * 2 * sizeof(int) + sizeof(int));
    *pageNumPtr = currentPageNum;
    *capacityPtr = capacity;
}

    // Write the page metadata block to the last page of the file
    result = writeBlock(fileHandle->totalNumPages - 1, fileHandle, metadataBlock);
    if (result != RC_OK) {
        free(metadataBlock);
        return result;
    }

    // Free allocated memory
    free(metadataBlock);
    return RC_OK;
}




/**
 * Retrieves the size of file metadata from the file associated with the buffer pool.
 * This function calculates and returns the size of metadata stored in the file.
 *
 * @param bufferPool Pointer to the BM_BufferPool structure representing the buffer pool.
 * @return int The size of file metadata.
 */


int getFileMetaDataSize(BM_BufferPool *bufferPool) {
    // Check for invalid buffer pool pointer
    if (bufferPool == NULL) {
        return -1;
    }

    int metaSize = -1; // Default value in case of failure

    // Create a page handle
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();

    // Try to pin the first page
    if (pinPage(bufferPool, pageHandle, 0) != RC_OK) {
        free(pageHandle);
        return -1; // Unable to pin the page
    }

    // Extract meta size from the page data without memcpy
    metaSize = *((int *)pageHandle->data);

    // Unpin the page and free memory
    if (unpinPage(bufferPool, pageHandle) != RC_OK) {
        free(pageHandle);
        return -1; // Unable to unpin the page
    }

    free(pageHandle);
    return metaSize;
}

/**
 * Retrieves the size of a record (in slots) from the file associated with the buffer pool.
 * This function calculates and returns the size of a record in terms of slots.
 *
 * @param bufferPool Pointer to the BM_BufferPool structure representing the buffer pool.
 * @return int The size of a record in slots.
 */


int getRecordCostSlot(BM_BufferPool *bufferPool) {
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();
    int costSlot = -1; // Initialize to -1 indicating failure

    // Try to pin the first page
    if (pinPage(bufferPool, pageHandle, 0) != RC_OK) {
        free(pageHandle);
        return costSlot; // Return default value
    }

    // Calculate the starting position of cost slot in the page data
    char *dataPtr = pageHandle->data + sizeof(int);

    // Extract cost slot from the page data
    memcpy(&costSlot, dataPtr, sizeof(int));

    // Unpin the page and free memory
    unpinPage(bufferPool, pageHandle);
    free(pageHandle);

    return costSlot;
}

/**
 * Retrieves the size of a slot from the file associated with the buffer pool.
 * This function calculates and returns the size of a slot.
 *
 * @param bufferPool Pointer to the BM_BufferPool structure representing the buffer pool.
 * @return int The size of a slot.
 */
int getSlotSize(BM_BufferPool *bufferPool) {
    int slotSize = -1; // Default value in case of failure
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();

    // Try to pin the first page
    if (pinPage(bufferPool, pageHandle, 0) != RC_OK) {
        free(pageHandle);
        return slotSize; // Return default value
    }

    // Calculate the starting position of slot size in the page data
    char *dataPtr = pageHandle->data + 2 * sizeof(int);

    // Extract size from the page data
    memcpy(&slotSize, dataPtr, sizeof(int));

    // Unpin the page and free memory
    unpinPage(bufferPool, pageHandle);
    free(pageHandle);

    return slotSize;
}


