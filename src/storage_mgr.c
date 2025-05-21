#include <stdio.h> 
#include <stdlib.h> 
#include <fcntl.h> 
#include <sys/types.h> 
#include <sys/stat.h> 
#include <errno.h> 
#include <string.h> 
#include <limits.h> 
#include "storage_mgr.h" 
#include "time.h"

/* manipulating page files */

/**
 * Initializes the Storage Manager.
 * This function serves as an entry point for users to engage with the Storage Manager,
 * indicating that the initialization process has started and displaying the version information.
 */


void initStorageManager(void){

    printf("--------Welcome to the Storage Manager 1.0--------\n");
    printf("Developed by: Siddhartha Varanasi |  Sai Charan Reddy Perla | Varun Chittimalla | Aditya Loya\n");
	return;
}

/**
 * Initializes a new page file named as specified.
 * This function creates a file, setting its size to equal one page. It ensures the page is filled with null ('\0') bytes,
 * effectively preparing a blank page for use.
 * 
 * @param fileName Pointer to a character array containing the name of the file to create.
 * @return A status code indicating the success or failure of the file creation process.
 */


RC createPageFile(char *fileName) {
    // Attempt to open the file for writing
    FILE *fp = fopen(fileName, "w");
    if (!fp) {
        return RC_CREATE_FILE_FAIL;       // No need to close the file as fopen failed
    }
 
    char fill[PAGE_SIZE] = {0};           // Allocate and zero-initialize a buffer of PAGE_SIZE

    size_t write_result = fwrite(fill, sizeof(fill), 1, fp);     // Write the initialized buffer to the file

    fclose(fp);       // Close the file

    // Check if the write operation was successful
    if (write_result != 1) {
        remove(fileName);         // If the write failed, remove the partially created file
        return RC_CREATE_FILE_FAIL;
    }

    return RC_OK;
}


/**
 * Opens a page file if it exists and prepares the file handle with the file's specifics.
 * This function checks for the existence of a specified file by name, opens it for reading, and updates a provided file handle structure
 * with details such as the file's name, its total number of pages, and sets the current page position to the beginning.
 *
 * @param fileName Pointer to the name of the file to open.
 * @param fHandle Pointer to the file handle structure to populate with file details.
 * @return A result code indicating the success or failure of the file opening process.
 */


RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    FILE *fp = fopen(fileName, "r");        // Attempt to open the file for reading
    if (!fp) {
        return RC_FILE_NOT_FOUND;
    }

    // Move the file pointer to the end to determine the file size
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp); // Ensure the file is closed before returning
        return RC_GET_NUMBER_OF_BYTES_FAILED;
    }

    // Obtain the size of the file
    long fileSize = ftell(fp);
    if (fileSize == -1L) {
        fclose(fp); // Ensure the file is closed before returning
        return RC_GET_NUMBER_OF_BYTES_FAILED;
    }

    // Initialize the file handle structure with the file details
    fHandle->fileName = strdup(fileName); // Make a copy of the fileName to ensure the fHandle owns the string
    fHandle->totalNumPages = fileSize / PAGE_SIZE + (fileSize % PAGE_SIZE > 0 ? 1 : 0); // Calculate total number of pages
    fHandle->curPagePos = 0; // Set the current page position to the beginning

    // Close the file as its details have been obtained
    fclose(fp);

    return RC_OK;
}

/***************************************************************
 * Function Name: closePageFile
 * 
 * Description: Close an open page file
 *
 * Parameters: SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      2016/02/07      Xiaoliang Wu                implement function
 *
***************************************************************/


RC closePageFile (SM_FileHandle *fHandle){
    fHandle->fileName = "";
    fHandle->curPagePos = 0;
    fHandle->totalNumPages = 0;
    return RC_OK;
}


/**
 * Removes a page file from the filesystem.
 * This function attempts to delete a file specified by its name. If successful, the file is removed,
 * freeing up space and removing the file's data from the system. It is designed to handle the file deletion
 * process securely and efficiently, ensuring that the specified file no longer exists in the storage.
 *
 * @param fileName The name of the file to be deleted.
 * @return A result code indicating whether the file was successfully deleted or if an error occurred.
 */


RC destroyPageFile(char *fileName) {

    // Attempt to delete the file
    if (remove(fileName) == 0) {
        return RC_OK; // File successfully deleted
    } else {
        return RC_FILE_NOT_FOUND; // File could not be deleted (might not exist)
    }
}

/* reading blocks from disc */

/**
 * Reads the specified block from a file into a memory page.
 * This function targets a specific block in the file, determined by the page number, and reads its contents into a memory buffer (memPage).
 * It ensures that the read operation is accurately performed on the correct block and updates the file handle's current page position.
 *
 * @param pageNum The page number of the block to read.
 * @param fHandle Pointer to the file handle associated with the file.
 * @param memPage Buffer where the block's data will be stored.
 * @return A status code indicating the outcome of the read operation.
 */


RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Validate that the requested page number is within the bounds of existing pages.
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Attempt to open the file for reading.
    FILE *file = fopen(fHandle->fileName, "r");
    if (!file) {
        return RC_FILE_NOT_FOUND; // Return error if the file cannot be opened.
    }

    // Calculate the offset to the beginning of the requested block.
    long offset = (long)pageNum * PAGE_SIZE;
    if (fseek(file, offset, SEEK_SET) != 0) {
        fclose(file); // Ensure the file is closed if seeking fails.
        return RC_SEEK_FAILED; // Return an error if seeking to the requested block fails.
    }

    // Read the block into the provided memory page.
    if (fread(memPage, sizeof(char), PAGE_SIZE, file) < PAGE_SIZE) {
        fclose(file); // Ensure the file is closed if read operation is incomplete.
        return RC_READ_FAILED; // Indicate failure if not all bytes were read.
    }

    // Successfully read the block, update the current page position.
    fHandle->curPagePos = pageNum;

    // Close the file after a successful read operation.
    fclose(file);
    return RC_OK;
}


/**
 * Retrieves the current block position within the file.
 * This function provides the current position of the block being read or written in the file, as indicated by the file handle.
 * It is useful for determining where in the file the next read or write operation will occur, or simply for tracking the progress of file manipulation.
 *
 * @param fHandle Pointer to the file handle structure from which to retrieve the current block position.
 * @return The current block position as an integer.
 */


int getBlockPos (SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}


/**
 * Reads the first block from the specified file.
 * This function is designed to access the beginning of a file, reading its first block and storing the contents into a provided buffer (memPage).
 * It's particularly useful for starting sequential reads or when the initial data of the file is required for processing.
 *
 * @param fHandle Handle to the file from which the first block will be read.
 * @param memPage Buffer where the contents of the first block will be stored.
 * @return A status code indicating the success or failure of the read operation.
 */


RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Attempt to open the file in read mode.
    FILE *file = fopen(fHandle->fileName, "r");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND; // Indicate if the file cannot be opened.
    }

    // Seek to the start of the file.
    if (fseek(file, 0, SEEK_SET) != 0) {
        fclose(file); // Ensure the file is closed if seeking fails.
        return RC_SEEK_FAILED; // Return an error if unable to seek to the start.
    }

    // Read the first block into the provided memory page.
    size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, file);
    if (bytesRead < PAGE_SIZE) {
        fclose(file); // Ensure the file is closed if read operation is incomplete.
        return RC_READ_FAILED; // Indicate failure if not all bytes were read.
    }

    // Successfully read the first block, update the current page position.
    fHandle->curPagePos = 0;

    // Close the file after successful operation.
    fclose(file);
    return RC_OK;
}

/**
 * Reads the block immediately before the current block in the file.
 * This function seeks to the block preceding the current position indicated by fHandle and reads its contents into the specified buffer (memPage).
 * It's useful for operations that require processing or examining data in reverse order within the file.
 *
 * @param fHandle Handle to the file from which the previous block will be read.
 * @param memPage Buffer where the contents of the previous block will be stored.
 * @return A status code indicating the outcome of attempting to read the previous block.
 */


RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Validate the current page position to ensure it's within a valid range.
	
	int isValidPagePos= fHandle->curPagePos >= 0 && fHandle->curPagePos <= fHandle->totalNumPages - 1;
    if (!isValidPagePos) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Open the file in read mode to access its contents.
    FILE *file = fopen(fHandle->fileName, "r");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND; // Indicates the file could not be opened.
    }

    // Calculate the offset for the previous block based on the current position.
    long offset = (fHandle->curPagePos - 1) * PAGE_SIZE;
    if (fseek(file, offset, SEEK_SET) != 0) {
        fclose(file); // Close the file if seeking to the position fails.
        return RC_READ_NON_EXISTING_PAGE; // Indicate failure to seek to the correct position.
    }

    // Read the block into the provided buffer.
    if (fread(memPage, sizeof(char), PAGE_SIZE, file) < PAGE_SIZE) {
        fclose(file); // Close the file if reading the block partially or fails.
        return RC_READ_FAILED; // Indicate failure to read the entire block.
    }

    // Successfully read the previous block, update the current page position.
    fHandle->curPagePos--;

    // Close the file and return success.
    fclose(file);
    return RC_OK;
}


/**
 * Reads the block at the file's current position.
 * This function accesses the file at the position currently marked by the file handle (fHandle) and reads the block found there into a buffer (memPage).
 * It is suitable for sequential access patterns or when the application logic requires processing of data from the current position without changing it.
 *
 * @param fHandle Handle to the file from which the current block will be read.
 * @param memPage Buffer where the contents of the current block will be stored.
 * @return A status code reflecting the success or failure of the operation.
 */


RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Validate the current page position is within the range of existing pages.
	int isValidPagePos = (fHandle->curPagePos >= 0) && (fHandle->curPagePos <= fHandle->totalNumPages);
	
    if (!isValidPagePos) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Open the file in read mode.
    FILE *file = fopen(fHandle->fileName, "r");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND; // Indicate failure if the file cannot be opened.
    }

    // Calculate the offset for the current block based on its position.
    long offset = (long)fHandle->curPagePos * PAGE_SIZE;
    if (fseek(file, offset, SEEK_SET) != 0) {
        fclose(file); // Ensure the file is closed if seeking fails.
        return RC_READ_NON_EXISTING_PAGE; // Return an error if unable to seek to the current block.
    }

    // Attempt to read the block into the provided memory page buffer.
    size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, file);
    fclose(file); // Close the file after reading.
    
    // Verify that the read operation read the full block size.
    if (bytesRead < PAGE_SIZE) {
        return RC_READ_FAILED; // Indicate a read failure if not all bytes were read.
    }

    // Return success if the block was read successfully.
    return RC_OK;
}


/**
 * Reads the block immediately after the current block in the file.
 * This function moves to the next block from the current position, reads its content, and stores it in the provided buffer (memPage).
 * It's ideal for sequential processing of a file's contents or when the next piece of data is required for the application's logic.
 *
 * @param fHandle Handle to the file from which the next block will be read.
 * @param memPage Buffer where the contents of the next block will be stored.
 * @return A return code indicating the success or failure of the read operation.
 */


RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Validate the current position is within the valid range of pages.
    
	// Check if curPagePos is within the valid range
    int isValidPagePos = (fHandle->curPagePos >= 0) && (fHandle->curPagePos <= fHandle->totalNumPages - 2);
	
	if (!isValidPagePos) {
        return RC_READ_NON_EXISTING_PAGE;
    }
	
    // Open the file for reading.
    FILE *file = fopen(fHandle->fileName, "r");
    if (!file) {
        return RC_FILE_NOT_FOUND; // Return error if file opening fails.
    }

    // Calculate the offset for the next block to be read.
    long offset = (fHandle->curPagePos + 1) * PAGE_SIZE;
    if (fseek(file, offset, SEEK_SET) != 0) {
        fclose(file); // Ensure file is closed if seeking fails.
        return RC_READ_NON_EXISTING_PAGE; // Return error if seeking to the next block fails.
    }

    // Read the next block into the provided memory page.
    if (fread(memPage, sizeof(char), PAGE_SIZE, file) < PAGE_SIZE) {
        fclose(file); // Ensure file is closed if read operation is incomplete.
        return RC_READ_FAILED; // Return error if reading the block fails.
    }

    // Update the current page position to the next page.
    fHandle->curPagePos++;

    // Close the file and return success.
    fclose(file);
    return RC_OK;
}


/**
 * Reads the last block of the specified file into the given memory page.
 * This function seeks to the end of the file, identifies the last block based on the file's size and the defined page size,
 * and reads this block into the memory buffer provided. It is useful for applications that need to access the final part of the file's data,
 * whether for processing file footers or for other reasons.
 *
 * @param fHandle Handle to the file from which the last block will be read.
 * @param memPage Buffer where the contents of the last block will be stored.
 * @return A status code indicating the success or failure of the operation.
 */


RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Attempt to open the file in read mode.
    FILE *file = fopen(fHandle->fileName, "r");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND; // Indicate if the file could not be opened.
    }

    // Position the file pointer to the start of the last block.
    if (fseek(file, -PAGE_SIZE, SEEK_END) != 0) {
        fclose(file); // Close the file if seeking failed.
        return RC_READ_NON_EXISTING_PAGE; // Indicate an error if unable to seek.
    }

    // Read the last block into the provided buffer.
    size_t readSize = fread(memPage, sizeof(char), PAGE_SIZE, file);
    if (readSize < PAGE_SIZE) {
        fclose(file); // Ensure the file is closed on partial read.
        return RC_READ_FAILED; // Indicate a read error.
    }

    // Update the current page position to the last page.
    fHandle->curPagePos = fHandle->totalNumPages - 1;

    // Clean up by closing the file and return success.
    fclose(file);
    return RC_OK;
}



/* writing blocks to a page file */

/**
 * Writes the data from a memory page to a specified page in the file.
 * This function allows for data stored in a memory buffer (memPage) to be written to a specific page number (pageNum) within a file.
 * It supports writing data at both absolute positions determined by pageNum and the current position within the file, as indicated by the file handle (fHandle).
 * The function ensures data is accurately and efficiently written to the correct location in the file.
 *
 * @param pageNum Page number in the file where the data should be written.
 * @param fHandle Pointer to the file handle structure representing the open file.
 * @param memPage Memory page containing the data to be written.
 * @return A return code indicating the outcome of the write operation.
 */


RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Ensure the file has enough pages to accommodate the write operation.
    RC ensureCapacityResult = ensureCapacity(pageNum + 1, fHandle);
    if (ensureCapacityResult != RC_OK) {
        return ensureCapacityResult; // Propagate the error from ensureCapacity.
    }

    FILE *filePointer = fopen(fHandle->fileName, "rb+");
    if (filePointer == NULL) {
        return RC_FILE_NOT_FOUND; // Unable to open the file for reading and writing.
    }

    // Seek to the start of the page where data is to be written.
    long offset = (long)pageNum * PAGE_SIZE;
    if (fseek(filePointer, offset, SEEK_SET) != 0) {
        fclose(filePointer); // Close the file on seek failure.
        return RC_SEEK_FAILED; // Indicate failure to seek to the correct page.
    }

    // Write the block to the file.
    if (fwrite(memPage, PAGE_SIZE, 1, filePointer) != 1) {
        fclose(filePointer); // Close the file on write failure.
        return RC_WRITE_FAILED; // Indicate failure to write the block.
    }

    // Update the file handle's current page position to reflect the write operation.
    fHandle->curPagePos = pageNum;

    fclose(filePointer); // Close the file after successful write operation.
    return RC_OK; // Indicate success.
}



/**
 * Writes data to the current block of the file.
 * This function writes the contents of a memory page to the block at the current position within the file, as tracked by the file handle.
 * It provides a convenient way to update data in the file without needing to specify the page number explicitly, using the file's current position as the target for the write operation.
 *
 * @param fHandle Pointer to the file handle structure for the open file.
 * @param memPage Memory page containing the data to be written.
 * @return A return code indicating the success of the write operation or an error.
 */


RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Ensure the file handle is valid.
    if (!fHandle) {
        return RC_FILE_HANDLE_NOT_INIT; // File handle is not initialized.
    }

    // Check for a valid current position before writing.
    if (fHandle->curPagePos < 0 || fHandle->curPagePos >= fHandle->totalNumPages) {
        return RC_WRITE_FAILED; // Current position is invalid for writing.
    }

    // Delegate the write operation to the writeBlock function.
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

/**
 * Appends an empty block to the end of the file.
 * This function increases the file's size by adding a new page at the end, initializing this page with zero bytes to ensure it's empty.
 * It's useful for expanding the file in preparation for additional data without altering existing content.
 *
 * @param fHandle Pointer to the file handle structure for the file to be expanded.
 * @return A return code indicating whether the new page was successfully added or if an error occurred.
 */


RC appendEmptyBlock(SM_FileHandle *fHandle) {
    if (!fHandle) {
        return RC_FILE_HANDLE_NOT_INIT; // File handle not initialized.
    } 

    FILE *filePointer = fopen(fHandle->fileName, "ab+");
    if (!filePointer) {
        return RC_FILE_NOT_FOUND; // File not found.
    }

    char *Data = (char *)calloc(1, PAGE_SIZE);
    if (!Data) {
        fclose(filePointer);
        return RC_MALLOC_FAILED; // Memory allocation failed.
    }

    RC returnCode;
    int value=fwrite(Data, PAGE_SIZE, 1, filePointer);
    if (value!= 1) {
        returnCode = RC_WRITE_FAILED; // Failed to write the block.
    } else {
        fHandle->totalNumPages ++; // Increment total number of pages.
        returnCode = RC_OK; // Success.
    }

    free(Data);
    fclose(filePointer); // Close the file.
    return returnCode;
}

/**
 * Ensures the file has a minimum number of pages.
 * If the file associated with the given file handle has fewer pages than specified by numberOfPages, this function will increase the file's size
 * until it meets or exceeds the specified number of pages. This is achieved by appending empty pages filled with zero bytes to the file as needed.
 *
 * @param numberOfPages The minimum number of pages the file should contain.
 * @param fHandle Pointer to the file handle structure for the file to be modified.
 * @return A return code indicating the success of the operation or an error.
 */


RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    // Check if the file handle is initialized

    if (!fHandle) {
        return RC_FILE_HANDLE_NOT_INIT;
    } 
    else if(fHandle->totalNumPages >= numberOfPages) {
        return RC_OK;
    }

    FILE *filePointer = fopen(fHandle->fileName, "ab+");
    RC returnCode;
    // Calculate the additional capacity needed
    long additionalCapacity = (numberOfPages - fHandle->totalNumPages) * PAGE_SIZE;
    char *dataBuffer = (char *)calloc(1, additionalCapacity);

    if (!filePointer) {
        return RC_FILE_NOT_FOUND; // File not found.
    }
    else{
        
        if (!dataBuffer) {
            fclose(filePointer);
            return returnCode=RC_MALLOC_FAILED; // Memory allocation failed.
        }else if (fwrite(dataBuffer, additionalCapacity, 1, filePointer) == 0) {
            returnCode = RC_WRITE_FAILED; // Failed to write the block.
        } 
        else {
            fHandle->totalNumPages = numberOfPages; // Update totalNumPages.
            returnCode = RC_OK; // Success.
        }
        }
    free(dataBuffer);
    fclose(filePointer); // Close the file.
    return returnCode;
}
