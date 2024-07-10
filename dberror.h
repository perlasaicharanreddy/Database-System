#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_TABLE_NOT_FOUND 5
#define RC_INVALID_HANDLE 6
#define RC_INVALID_PARAM 18
#define RC_INVALID_ARGS 7
#define RC_RM_UNKNOWN_DATATYPE 8
#define RC_SCHEMA_PARSE_ERROR 9
#define RC_INSUFFICIENT_MEMORY 10
#define RC_MEM_ALLOC_FAILED 11
#define RC_SCHEMA_CREATION_FAILED 12
#define RC_INVALID_SCHEMA 13
//added by myself 
#define RC_CREATE_FILE_FAIL 5 
#define RC_GET_NUMBER_OF_BYTES_FAILED 6 
#define RC_READ_FAILED 7 
#define RC_SEEK_FAILED 8 
#define RC_SHUTDOWN_POOL_FAILED 9 
#define RC_STRATEGY_NOT_FOUND 10 
#define RC_TELL_FAILED 11
#define RC_PAGE_NOT_FOUND 12
#define RC_WRITE_ERROR 13
#define RC_SUCCESS 14
#define RC_CANNOT_SHUTDOWN_POOL 15
#define RC_BUFFER_POOL_INIT_ERROR 16
#define RC_FORCE_FLUSH_FAILED 17
#define RC_PIN_PAGE_FAILED 18
#define RC_DELIMITER_NOT_FOUND 19
#define RC_ERROR 20 
#define RC_FILE_DESTROY_FAILED 21

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205
#define RC_MEM_ALLOCATION_FAIL 206
#define RC_RM_RECORD_NOT_EXIST 207

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303


#define RC_NULL_POINTER 308
#define RC_MEM_ERROR 309
#define RC_INVALID_ATTR_NUM 310
#define RC_MALLOC_FAILED 401

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
		do {			  \
			RC_message=message;	  \
			return rc;		  \
		} while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
		do {									\
			int rc_internal = (code);						\
			if (rc_internal != RC_OK)						\
			{									\
				char *message = errorMessage(rc_internal);			\
				printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
				free(message);							\
				exit(1);							\
			}									\
		} while(0);


#endif
