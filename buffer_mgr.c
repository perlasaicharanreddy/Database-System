#include "buffer_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include "storage_mgr.h"
#include <string.h>
#include"dt.h"

// Buffer Manager Interface Pool Handling

// initBufferPool
/**
 * Initializes the Storage Manager by creating a new buffer pool with numPages page frames using the specified page replacement strategy. This buffer pool is utilized to cache pages from the existing page file named pageFileName. Initially, all page frames are empty. The page file must already exist; this function does not generate a new page file. Additional parameters, stratData, can be utilized to pass parameters for specific page replacement strategies, such as k for LRU-k.
 * @param bm Pointer to the buffer pool structure to be initialized.
 * @param pageFileName Name of the existing page file from which pages will be cached.
 * @param numPages Number of page frames in the buffer pool.
 * @param strategy Strategy for page replacement (e.g., FIFO, LRU).
 * @param stratData Additional parameters for the page replacement strategy, if applicable.
 * @return Return code indicating success or failure of the initialization process.
 */

RC initBufferPool(BM_BufferPool *const bufferPool, const char *const fileName,
                  const int totalPages, ReplacementStrategy replStrategy,
                  void *stratData) {
    // Attempt to open the specified file to check its existence
    printf("Welcome to Buffer Manager 1.0");
    FILE *filePointer = fopen(fileName, "r");
    if (!filePointer) {
        // If the file does not exist, return an error
        return RC_FILE_NOT_FOUND;
    }
    // Close the file as we just needed to check its existence
    fclose(filePointer);

    // Assign the buffer pool attributes
    bufferPool->pageFile = (char *)fileName;
    bufferPool->numPages = totalPages;
    bufferPool->strategy = replStrategy;

    // Allocate memory for the pages in the buffer pool
    BM_PageHandle *pages = calloc(totalPages, sizeof(BM_PageHandle));
    if (!pages) {
        return RC_MEM_ALLOCATION_FAIL; // Ensure you have a return code for memory allocation failure
    }
    bufferPool->mgmtData = pages;

    // Initialize each page in the buffer
    for (int idx = 0; idx < totalPages; idx++) {
        BM_PageHandle *currentPage = &pages[idx];
        currentPage->dirty = 0;
        currentPage->fixCounts = 0;
        currentPage->data = NULL;
        currentPage->pageNum = NO_PAGE; // Assuming NO_PAGE is defined as -1 or an appropriate flag value
    }

    // Initialize IO counters and timer to 0
    bufferPool->numReadIO = 0;
    bufferPool->numWriteIO = 0;
    bufferPool->timer = 0;

    // Successful initialization
    return RC_OK;
}

// shutdownBufferPool
/**
 * Destroys a buffer pool, freeing up all associated resources. This method should release all memory allocated for page frames and ensure that any dirty pages are written back to disk before the pool is destroyed. Attempting to shut down a buffer pool with pinned pages will result in an error.
 * @param bm Pointer to the buffer pool to be shutdown.
 * @return Return code indicating success or failure of the shutdown process.
 */


RC shutdownBufferPool(BM_BufferPool *const bufferPool) {
    int pageIndex;


    // Check if any page is still fixed
    for (pageIndex = 0; pageIndex < bufferPool->numPages; ++pageIndex) {
        if (getFixCounts(bufferPool)[pageIndex]) {
            return RC_SHUTDOWN_POOL_FAILED;
        }
    }

    // Flush dirty pages to disk
    RC rc_flag = forceFlushPool(bufferPool);
    if (rc_flag != RC_OK) {
        return rc_flag;
    }

    // Free allocated memory for each page's data and strategy attribute
    for (pageIndex = 0; pageIndex < bufferPool->numPages; ++pageIndex) {
        free(bufferPool->mgmtData[pageIndex].data);
        free(bufferPool->mgmtData[pageIndex].strategyAttribute);
    }

    // Free allocated memory for buffer pool management data
    free(bufferPool->mgmtData);

    return RC_OK;
}

// forceFlushPool
/**
 * Forces all dirty pages (pages with fix count 0) from the buffer pool to be written to disk.
 * @param bm Pointer to the buffer pool.
 * @return Return code indicating success or failure of the force flush operation.
 */

RC forceFlushPool(BM_BufferPool *const bm) {
    // Get dirty flags and fix counts
    bool *dirtyFlags = getDirtyFlags(bm);
    int *fixCounts = getFixCounts(bm);


    // Check if retrieval of dirty flags or fix counts failed
    if (dirtyFlags == NULL || fixCounts == NULL) {
        free(dirtyFlags);
        free(fixCounts);
        return RC_FORCE_FLUSH_FAILED;
    }

    // Iterate through each page
    for (int i = 0; i < bm->numPages; ++i) {
        if (dirtyFlags[i] && fixCounts[i] == 0) {
            // Force the page to disk
            RC rc_flag = forcePage(bm, &bm->mgmtData[i]);
            if (rc_flag != RC_OK) {
                free(dirtyFlags);
                free(fixCounts);
                return rc_flag;
            }
            // Reset the dirty flag for the page
            bm->mgmtData[i].dirty = 0;
        }
    }

    // Free allocated memory
    free(dirtyFlags);
    free(fixCounts);
    return RC_OK;
}

// Buffer Manager Interface Access Pages

// markDirty
/**
 * Marks a page in the buffer pool as dirty, indicating that it has been modified.
 * @param bm Pointer to the buffer pool.
 * @param page Pointer to the page handle structure representing the page to be marked as dirty.
 * @return Return code indicating success or failure of the operation.
 */

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    for (int i = 0; i < bm->numPages; ++i) {
        BM_PageHandle *currentPage = &bm->mgmtData[i];
        if (currentPage->pageNum == page->pageNum) {
            currentPage->dirty = 1;
            page->dirty = 1;
            return RC_OK;
        }
    }
    return RC_PAGE_NOT_FOUND;
}

// unpinPage
/**
 * Unpins a page indicated by the BM_PageHandle structure.
 * @param bm Pointer to the buffer pool.
 * @param page Pointer to the page handle structure representing the page to be unpinned.
 * @return Return code indicating success or failure of the operation.
 */

RC unpinPage (BM_BufferPool *const bufferPool, BM_PageHandle *const targetPage)
{
     // Loop through each page in the buffer pool to find the matching page
    for (int pageIndex = 0; pageIndex < bufferPool->numPages; pageIndex++) {
        // Check if current page's number matches the target page's number
        if (bufferPool->mgmtData[pageIndex].pageNum == targetPage->pageNum) {
            // Decrease the fix count for the page, indicating it's being unpinned
            bufferPool->mgmtData[pageIndex].fixCounts--;
            // No need to adjust the targetPage fixCounts here as it's managed within bufferPool
            break; // Stop searching once the matching page is found
        }
    }
    return RC_OK; // Indicate successful operation
}
// forcePage
/**
 * Writes the current content of the page back to the page file on disk.
 * @param bm Pointer to the buffer pool.
 * @param page Pointer to the page handle structure representing the page to be written back to disk.
 * @return Return code indicating success or failure of the operation.
 */

RC forcePage(BM_BufferPool *const bufferPool, BM_PageHandle *const page) {
    FILE *file;

    // Open the page file
    file = fopen(bufferPool->pageFile, "rb+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // Seek to the position of the page in the file
    fseek(file, (page->pageNum) * PAGE_SIZE, SEEK_SET);

    // Write the page data to the file
    fwrite(page->data, PAGE_SIZE, 1, file);
    bufferPool->numWriteIO++;
    // Close the file
    fclose(file);

    // Reset the dirty flag for the page in the buffer pool
    BM_PageHandle *currentPage = bufferPool->mgmtData;
    BM_PageHandle *end = bufferPool->mgmtData + bufferPool->numPages;

    for (; currentPage != end; ++currentPage) {
        if (currentPage->pageNum == page->pageNum) {
            currentPage->dirty = 0;
            break;
        }
    }

    // Reset the dirty flag for the page handle
    page->dirty = 0;

    return RC_OK;
}

// pinPage
/**
 * Pins a page in memory. If the page is not already in memory, it is read from the file.
 * @param bm Pointer to the buffer pool.
 * @param page Pointer to the page handle structure representing the page to be pinned.
 * @param pageNum The page number of the page to be pinned.
 * @return Return code indicating success or failure of the operation.
 */


RC pinPage(BM_BufferPool *const bufferPool, BM_PageHandle *const pageHandle, const PageNumber pageNum) {
    int pageIndex;
    int flag = 0;
    int pnum = -1;

    // Loop through the buffer pool pages
    for (pageIndex = 0; pageIndex < bufferPool->numPages; pageIndex++) {
        // Check for an empty slot
        if (bufferPool->mgmtData[pageIndex].pageNum == NO_PAGE) {
            // Allocate memory for the new page
            bufferPool->mgmtData[pageIndex].data = (char*)calloc(PAGE_SIZE, sizeof(char));
            pnum = pageIndex;
            flag = 1;
            break;
        }
        // Check if the requested page is already in the buffer
        if (bufferPool->mgmtData[pageIndex].pageNum == pageNum) {
            pnum = pageIndex;
            flag = 2;
            // Update page's strategy attribute if using LRU strategy
            if (bufferPool->strategy == RS_LRU){
				if (bufferPool->strategy != RS_FIFO && bufferPool->strategy != RS_LRU) {
					return RC_STRATEGY_NOT_FOUND;
				}
				// Allocate memory for the strategy attribute if it has not been done yet.
				if (!bufferPool->mgmtData[pageIndex].strategyAttribute) {
					bufferPool->mgmtData[pageIndex].strategyAttribute = (int*)calloc(1, sizeof(int));
					if (!bufferPool->mgmtData[pageIndex].strategyAttribute) {
						return RC_MEM_ALLOCATION_FAIL; // Assuming this return code exists for memory allocation failures.
					}
				}
				// Update the strategy attribute with the current timer value and increment the timer.
				*((int*)bufferPool->mgmtData[pageIndex].strategyAttribute) = bufferPool->timer++;
			}
            break;
        }
		
        // If no empty slot and requested page not found
        if (pageIndex == bufferPool->numPages - 1) {
            flag = 1;
            // Determine page to replace based on buffer pool strategy
            if (bufferPool->strategy == RS_FIFO || bufferPool->strategy == RS_LRU) {
                pnum = strategyFIFOandLRU(bufferPool);
                if (bufferPool->mgmtData[pnum].dirty)
                    forcePage(bufferPool, &bufferPool->mgmtData[pnum]);
            }
        }
    }

    // Load page data from disk if needed
    if (flag == 1) {
        FILE* filePtr = fopen(bufferPool->pageFile, "r");
        fseek(filePtr, pageNum * PAGE_SIZE, SEEK_SET);
        fread(bufferPool->mgmtData[pnum].data, sizeof(char), PAGE_SIZE, filePtr);
        fclose(filePtr);
        bufferPool->numReadIO++;
        bufferPool->mgmtData[pnum].fixCounts++;
        bufferPool->mgmtData[pnum].pageNum = pageNum;
        pageHandle->data = bufferPool->mgmtData[pnum].data;
        pageHandle->fixCounts = bufferPool->mgmtData[pnum].fixCounts;
        pageHandle->pageNum = pageNum;
        pageHandle->dirty = bufferPool->mgmtData[pnum].dirty;
        pageHandle->strategyAttribute = bufferPool->mgmtData[pnum].strategyAttribute;

        if (bufferPool->strategy != RS_FIFO && bufferPool->strategy != RS_LRU) {
			
			return RC_STRATEGY_NOT_FOUND;
		}
		// Allocate memory for the strategy attribute if it has not been done yet.
		if (!bufferPool->mgmtData[pnum].strategyAttribute) {
			bufferPool->mgmtData[pnum].strategyAttribute = (int*)calloc(1, sizeof(int));
			if (!bufferPool->mgmtData[pnum].strategyAttribute) {
				
				return RC_MEM_ALLOCATION_FAIL; // Assuming this return code exists for memory allocation failures.
			}
		}
		// Update the strategy attribute with the current timer value and increment the timer.
		*((int*)bufferPool->mgmtData[pnum].strategyAttribute) = bufferPool->timer++;
    }
    // Update page handle if page is already in buffer
    else if (flag == 2) {
        pageHandle->data = bufferPool->mgmtData[pnum].data;
        bufferPool->mgmtData[pnum].fixCounts++;
        pageHandle->fixCounts = bufferPool->mgmtData[pnum].fixCounts;
        pageHandle->pageNum = pageNum;
        pageHandle->dirty = bufferPool->mgmtData[pnum].dirty;
        pageHandle->strategyAttribute = bufferPool->mgmtData[pnum].strategyAttribute;
    }

    return RC_OK;
}


// Statistics Interface
// getFrameContents
/**
 * Returns an array of PageNumbers where the ith element is the number of the page stored in the ith page frame.
 * @param bm Pointer to the buffer pool.
 * @return An array of PageNumbers representing the page numbers stored in each page frame.
 */

PageNumber *getFrameContents(BM_BufferPool *const bufferMgr) {
    // Allocate memory for an array to store the page numbers
    PageNumber *pageNumbers = (PageNumber*)malloc(bufferMgr->numPages * sizeof(PageNumber));

    // Iterate through each page in the buffer pool
    for (int i = 0; i < bufferMgr->numPages; i++) {
        // Get the page handle of the current page
        BM_PageHandle *pageHandle = &bufferMgr->mgmtData[i];

        // Check if the page data is set (indicating a used frame)
        if (pageHandle->data != NULL) {
            pageNumbers[i] = pageHandle->pageNum; // Store the actual page number
        } else {
            pageNumbers[i] = NO_PAGE; // Mark as NO_PAGE indicating it's unused
        }
    }

    // Return the array with the page numbers (or NO_PAGE for unused frames)
    return pageNumbers;
}

// getDirtyFlags
/**
 * Returns an array of bools where the ith element is TRUE if the page stored in the ith page frame is dirty.
 * @param bm Pointer to the buffer pool.
 * @return An array of boolean values indicating whether each page stored in the page frames is dirty.
 */
bool *getDirtyFlags(BM_BufferPool *const pool) {
    // Check for valid pool and that it contains pages
    if (pool == NULL || pool->numPages <= 0) {
        // Invalid input or empty pool
        return NULL;
    }

    // Allocate memory for the array of dirty flags
    bool *dirtyFlags = (bool *)malloc(sizeof(bool) * pool->numPages);
    if (dirtyFlags == NULL) {
        // Failed to allocate memory
        return NULL;
    }

    for (int i = 0; i < pool->numPages; ++i) {
        // Safely access each page's dirty flag
        if (pool->mgmtData[i].data != NULL) {
            dirtyFlags[i] = pool->mgmtData[i].dirty;
        } else {
            // If page data is not allocated, assume not dirty
            dirtyFlags[i] = false;
        }
    }

    return dirtyFlags;
}


// getFixCounts
/**
 * Returns an array of ints where the ith element is the fix count of the page stored in the ith page frame.
 * @param bm Pointer to the buffer pool.
 * @return An array of integers representing the fix counts of pages stored in each page frame.
 */

int *getFixCounts(BM_BufferPool *const bufferPool) {
    int *fixCountsArray = (int*)malloc(bufferPool->numPages * sizeof(int));
    int *currentFixCounts = fixCountsArray;
    BM_PageHandle *currentPage = bufferPool->mgmtData;
    int pageIndex = 0;

    // Use a do-while loop to ensure at least one iteration
    do {
        // Retrieve the fix count of the current page
        *currentFixCounts = currentPage->fixCounts;
        pageIndex++;
        currentPage++;
        currentFixCounts++;
    } while (pageIndex < bufferPool->numPages);

    return fixCountsArray;
}
// getNumReadIO
/**
 * Returns the number of read I/O operations performed by the buffer pool.
 * @param bm Pointer to the buffer pool.
 * @return The number of read I/O operations.
 */
int getNumReadIO(BM_BufferPool *const bm) {
    if (!bm) {
        // Log error and return -1 if bm is NULL
        return -1; 
    }
    return bm->numReadIO; // Return number of read IO operations
}

// getNumWriteIO
/**
 * Returns the number of write I/O operations performed by the buffer pool.
 * @param bm Pointer to the buffer pool.
 * @return The number of write I/O operations.
 */
int getNumWriteIO(BM_BufferPool *const bm) {
    if (!bm) {
        // Log error and return -1 if bm is NULL
        return -1; 
    }
    return bm->numWriteIO; // Return number of Write IO operations
}

// strategyFIFOandLRU
/**
 * Determines which frame to use to save data using the First-In-First-Out (FIFO) strategy.
 * @param bm Pointer to the buffer pool.
 * @return The index of the frame chosen by the FIFO strategy.
 */

int strategyFIFOandLRU(BM_BufferPool *bufferPool) {
    int *pageAttributes = getAttributionArray(bufferPool);
    int *fixCounts = getFixCounts(bufferPool);
    int lowestAttribute = bufferPool->timer;
    int pageToEvict = -1;

    // Enhanced logic for identifying the page to evict
    for (int i = 0; i < bufferPool->numPages; ++i) {
        // Only consider pages that are not pinned (fixCounts[i] == 0)
        if (fixCounts[i] == 0) {
            // For FIFO, we are looking for the oldest page (smallest timer value)
            // For LRU, we are also looking for the least recently used page (smallest timer value)
            // The logic for both strategies converges here as we use the timer to track recency of use
            if (pageAttributes[i] < lowestAttribute) {
                lowestAttribute = pageAttributes[i];
                pageToEvict = i;
            }
        }
    }

    // Conditional adjustment of timer and attributes based on a threshold
    if (bufferPool->timer > 32000 && pageToEvict != -1) {
        int adjustmentValue = lowestAttribute;
        // Normalize the strategy attributes to prevent overflow issues
        for (int i = 0; i < bufferPool->numPages; ++i) {
            if (bufferPool->mgmtData[i].strategyAttribute) {
                *(int*)bufferPool->mgmtData[i].strategyAttribute -= adjustmentValue;
            }
        }
        bufferPool->timer -= adjustmentValue; // Adjust the global timer accordingly
    }

    free(pageAttributes);
    free(fixCounts);

    return pageToEvict;
}


// getAttributionArray
/**
 * Returns an array that includes all page strategy attributes.
 * @param bm Pointer to the buffer pool.
 * @return Pointer to an array containing the strategy attributes of all pages.
 */
int *getAttributionArray(BM_BufferPool *bm) {
    
    
    if (!bm || bm->numPages <= 0) {
        // Invalid buffer pool pointer or no pages in the buffer pool
        return NULL;
    }
    // Ensure memory allocation for the array of strategy attributes, using the size of int for clarity.
    int *strategyAttributes = calloc(bm->numPages, sizeof(int));
    if (strategyAttributes == NULL) {
        // Handle memory allocation failure if needed
        return NULL; // Indicate failure to allocate memory
    }

    // Populate the array with strategy attributes from each page
    for (int i = 0; i < bm->numPages; ++i) {
        // Direct assignment to the array element for readability
        strategyAttributes[i] = *((int *)(bm->mgmtData + i)->strategyAttribute);
    }

    return strategyAttributes;
}

