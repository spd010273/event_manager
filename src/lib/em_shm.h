/*------------------------------------------------------------------------
 *
 * em_shm.h
 *     Event Manager Shared Memory function prototypes
 *     This includes an allocator and uses the underlying APIs:
 *     - System V (shm.h / ipc.h)
 *     - POSIX (mman.h)
 *     - mmap
 *     The goal of this library is to present a simplified
 *     malloc/calloc/free interface for shared memory allocation
 *
 * Copyright (c) 2021, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        src/lib/em_shm.h
 *
 *------------------------------------------------------------------------
 */

#ifndef EM_SHM_H
#define EM_SHM_H

#define SHM_USE_POSIX

#ifdef SHM_USE_POSIX
 #include <fcntl.h>
 #include <sys/stat.h>
 #include <sys/mman.h>
#endif // SHM_USE_POSIX
#ifdef SHM_USE_SYSV
 #include <sys/ipc.h>
 #include <sys/shm.h>
 #include <sys/types.h>
 #ifdef SHM_SHARE_MMU
  #define SYSV_SHM_FLAGS SHM_SHARE_MMU
 #else
  #define SYSV_SHM_FLAGS 0
 #endif // SHM_SHARE_MMU
#endif // SHM_USE_SYSV
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>

// Handle possible missing defines
#ifndef MAP_NOSYNC
#define MAP_NOSYNC 0
#endif // MAP_NOSYNC
#ifndef MAP_HASSEMAPHORE
#define MAP_HASSEMAPHORE 0
#endif // MAP_HASSEMAPHORE

#define MMAP_ADDITIONAL 0

#define ZERO_BUFFER_SIZE 8192 // Bytes
#define SHM_FILE_MMAP_DIR "em_shm"
#define SHM_FILE_MMAP_PREFIX "em_mmap_"
#define SHM_FILE_POSIX_PREFIX "em_shm_"
#define SHM_FILE_PERMS ( S_IWUSR | S_IRUSR )
#define SHM_FILE_OCTAL 0600;

typedef enum {
    SHM_CREATE,
    SHM_DESTROY,
    SHM_ATTACH,
    SHM_DETACH
} shm_op;

typedef uint64_t shm_handle;

typedef struct shm_item {
    shm_handle  handle;
    uint32_t    ref_count;
    bool        locked;
} shm_item;

typedef struct ctrl_header {
    uint32_t   id;
    pid_t      owner;
    uint32_t   entry_count;
    uint32_t   max_entries;
    shm_item * items;
} ctrl_header;

void * shm_alloc( size_t );
void * shm_calloc( size_t );
void * shm_realloc( void *, size_t );
void shm_free( void * );

#ifdef SHM_USE_POSIX
static bool shm_posix( shm_op, shm_handle, size_t, void **, size_t * );
static int shm_posix_resize( int, size_t );
#endif // SHM_USE_POSIX
#ifdef SHM_USE_SYSV
static bool shm_sysv( shm_op, shm_handle, size_t, void **, void **, size_t * );
#endif // SHM_USE_SYSV
#ifdef SHM_USE_MMAP
static bool shm_mmap( shm_op, shm_handle, size_t, void **, size_t * );
static int shm_mmap_resize( int, size_t );
#endif //SHM_USE_MMAP

static bool shm_wrapper( shm_op, shm_handle, size_t, void **, void **, size_t * );

// Helper functions
static bool _close_segment( int, char *, bool );
#endif // EM_SHM_H
