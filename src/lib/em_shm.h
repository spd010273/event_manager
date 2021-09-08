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

#define __TESTING__ // Code coverage and compilation testing

#ifdef __TESTING__
 #include <unistd.h>
 #define SHM_USE_POSIX
 #define SHM_USE_MMAP
 #define SHM_USE_SYSV
#else
 #ifdef __unix__
  #include <unistd.h>
  #if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L
   #define SHM_USE_POSIX
  #else
   #define SHM_USE_MMAP
  #endif // _POSIX_C_SOURCE
 #else
  #define SHM_USE_SYSV
 #endif // __unix__
#endif // __TESTING__

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
#include <stdlib.h>
#include <stddef.h>

// Markers and default settings
#define MAX_SEGMENTS 256
#define DEFAULT_PAGE_SIZE 8192 // bytes
#define ZERO_BUFFER_SIZE DEFAULT_PAGE_SIZE
#define CTRL_HEADER_MAGIC 0x1C3DB208B
#define CTRL_HEADER_INVALID ( ( uint32_t ) - 1 )
#define SEGMENT_HANDLE_INVALID ( ( uint64_t ) - 1 )
#define INVALID_CONTROL_INDEX ( ( uint32_t ) - 1 )
#ifndef MAX_LOCK_WAIT
#define MAX_LOCK_WAIT 1
#endif // MAX_LOCK_WAIT

// MMAP FLAGS
// Handle possible missing defines
#ifndef MAP_NOSYNC
#define MAP_NOSYNC 0
#endif // MAP_NOSYNC
#ifndef MAP_HASSEMAPHORE
#define MAP_HASSEMAPHORE 0
#endif // MAP_HASSEMAPHORE
#define MMAP_ADDITIONAL 0
#define MMAP_FLAGS ( MAP_HASSEMAPHORE | MAP_NOSYNC | MMAP_ADDITIONAL )

// File prefixes and Flags
#define SHM_FILE_MMAP_DIR "em_shm"
#define SHM_FILE_MMAP_PREFIX "em_mmap_"
#define SHM_FILE_POSIX_PREFIX "em_shm_"
#define SHM_FILE_PERMS ( S_IWUSR | S_IRUSR )
#define SHM_FILE_OCTAL 0600;

// Public functions
void shm_init( void ) __attribute__((unused));
void shm_child_init( void ) __attribute__((unused));

void * shm_alloc( size_t ) __attribute__((unused));
void * shm_calloc( size_t ) __attribute__((unused));
void * shm_realloc( void *, size_t ) __attribute__((unused));
void shm_free( void * ) __attribute__((unused));

/*
 * The structs constitute book keeping information for shared memory segments.
 * The minimal bit of information for a child to map something is the handle,
 * and knowledge about what /should/ be in that mapping, but given a handle,
 * a given PID can map the structs to local memory map.
 */
typedef enum {
    SHM_CREATE,
    SHM_DESTROY,
    SHM_ATTACH,
    SHM_DETACH
} shm_op;

typedef uint64_t shm_handle;

// 'Local' state for a given segment
// Given that the ctrl_header is initialized, we can recover the shm_item.
typedef struct shm_segment {
    shm_handle  handle;
    pid_t       owner;
    uint32_t    ctrl_index;
    void      * priv;
    void      * mapped_address;
    size_t      mapped_size;
} shm_segment;

// Shared-memory state for a given segment
typedef struct shm_item {
    shm_handle  handle;
    shm_handle  ctrl;
    uint32_t    ref_count;
    bool        locked;
    void *      mapped_address;
    size_t      mapped_size;
    void *      priv;
} shm_item;

typedef struct ctrl_header {
    uint32_t   id;
    pid_t      owner;
    uint32_t   entry_count;
    uint32_t   max_entries;
    bool       locked;
    shm_item * items;
} ctrl_header;

// Global state
static ctrl_header * control;
static size_t        control_header_sz;
static shm_handle    control_handle;
static void *        sysv_private;
static bool          is_inited;

static bool shm_check_ctrl( ctrl_header * );
static bool shm_check_owner( ctrl_header * );
static bool shm_check_seg( shm_item * ) __attribute__((unused)); // Not needed?
static bool shm_check_ctrl_by_handle( shm_handle );
static size_t get_ctrl_bytes_overhead( uint32_t );

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

static bool shm_wrapper( shm_op, shm_handle, size_t, void **, void **, size_t * ); // priv, mapped_address ( void ** )
static shm_segment * create_segment( size_t ) __attribute__((unused));
static shm_segment * attach_segment( shm_handle ) __attribute__((unused));
static void detach_segment( shm_segment * ) __attribute__((unused));

static shm_segment * _new_segment( void );
static void _free_segment( shm_segment * );
// Helper functions
static bool _close_segment_descriptor( int, char *, bool );
static size_t _get_system_page_size( void ) __attribute__((unused)); // Use by allocator later, just roughed out for now;
static bool _lock_acquire( volatile bool * );
static bool _lock_release( volatile bool * );
static bool __test_and_set( volatile bool * );
#endif // EM_SHM_H
