/*------------------------------------------------------------------------
 *
 * em_shm.c
 *     Event Manager Shared Memory function prototypes
 *     This includes an allocator and uses the underlying APIs:
 *     - System V (shm.h / ipc.h)
 *     - POSIX (mman.h)
 *     - mmap
 *     The goal of this library is to prevent a simplified
 *     malloc/calloc/free interface for shared memory allocation
 *
 * Copyright (c) 2021, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        src/lib/em_shm.c
 *
 *------------------------------------------------------------------------
 */

#include "em_shm.h"

// Allocator primitives
#ifdef SHM_USE_POSIX
static bool shm_posix(
    shm_op     op,
    shm_handle handle,
    size_t     size,
    void **    mapped_address,
    size_t *   mapped_size
)
{
    char        name[64]   = {0};
    int         flags      = 0;
    int         descriptor = 0;
    int         save_errno = 0;
    struct stat statbuff   = {0};
    char *      address    = NULL;

    snprintf( name, 64, "/%s%lu", SHM_FILE_POSIX_PREFIX, ( uint64_t ) handle );

    if( op == SHM_DESTROY || op == SHM_DETACH )
    {
        if(
                *mapped_address != NULL
             && munmap( *mapped_address, *mapped_size ) != 0
          )
        {
            return false;
        }

        *mapped_address = NULL;
        *mapped_size    = 0;

        if( op == SHM_DESTROY && shm_unlink( name ) != 0 )
        {
            return false;
        }

        return true;
    }

    flags = O_RDWR;

    if( op == SHM_CREATE )
    {
        // Generate an all-or-nothing page
        flags |= O_CREAT | O_EXCL;
    }

    save_errno = errno;
    errno      = 0;

    descriptor = shm_open( name, flags, SHM_FILE_PERMS );

    if( descriptor == -1 )
    {
        if( errno != EEXIST )
        {
            fprintf(
                stderr,
                "Failed to open shared memory segment %s: %s",
                name,
                strerror( errno )
            );
            return false;
        }
    }

    if( op == SHM_ATTACH )
    {
        if( fstat( descriptor, &statbuff ) != 0 )
        {
            _close_segment( descriptor, name, false );
            fprintf(
                stderr,
                "Failed to stat shared memory segment %s: %s",
                name,
                strerror( errno )
            );
            return false;
        }

        // Possibly handle size differences here
        size = statbuff.st_size;
    }
    else if( shm_posix_resize( descriptor, size ) != 0 )
    {
        _close_segment( descriptor, name, false );
        fprintf(
            stderr,
            "Failed to resize shared memory segment %s to %zu bytes: %s",
            name,
            size,
            strerror( errno )
        );

        return false;
    }

    address = ( char * ) mmap(
        NULL,
        size,
        PROT_READ | PROT_WRITE,
        MAP_NOSYNC | MAP_SHARED | MAP_HASSEMAPHORE,
        descriptor,
        0
    );

    if( address == MAP_FAILED )
    {
        save_errno = errno;
        _close_segment( descriptor, name, false );

        if( op == SHM_CREATE )
        {
            shm_unlink( name );
        }

        errno = save_errno;

        fprintf(
            stderr,
            "Failed to map shared memory segment %s: %s",
            name,
            strerror( errno )
        );

        return false;
    }

    *mapped_address = ( void * ) address;
    *mapped_size    = size;

    _close_segment( descriptor, name, false );

    return true;
}

static int shm_posix_resize( int descriptor, size_t size )
{
    int ret = 0;

    ret = ftruncate( descriptor, size );
    /*
     * Handle the case where shm_open is backed by tmpfs. When the size is
     * extended, a hole may occur. When this hole is accessed later - tmpfs
     * will attempt to allocate memory or page in stuff. If we run out of
     * space, this may cause a (very) unexpected SIGBUS. To prevent this,
     * we zero out the file up front, exchanging hard-to-trace SIGBUS with
     * a ENOSPC. This is considered best practice, but oddly enough, is
     * mentioned only in passing in the BSD manpages.
     */
    if( ret == 0 )
    {
        do {
            ret = posix_fallocate( descriptor, 0, size );
        } while( ret == EINTR );

        errno = ret;
    }

    return ret;
}
#endif // SHM_USE_POSIX

#ifdef SHM_USE_SYSV
static bool shm_sysv(
    shm_op     op,
    shm_handle handle,
    size_t     size,
    void **    private,
    void **    mapped_address,
    size_t *   mapped_size
)
{
    key_t           key              = 0;
    int             save_errno       = 0;
    int             flags            = 0;
    int             identifier       = 0;
    int *           identifier_cache = NULL;
    char *          address          = NULL;
    char            name[64]         = {0};
    size_t          segment_size     = 0;
    struct shmid_ds shm              = {0};

    snprintf( name, 64, "%lu", ( uint64_t ) handle );

    // Type coersion that may involve truncation - here we
    // consistently handle key truncation and the possibility
    // of restructed flag use
    key = ( key_t ) handle;

    if( key < 1 )
    {
        key = -key;
    }

    if( key == IPC_PRIVATE )
    {
        if( op != SHM_CREATE )
        {
            fprintf(
                stderr,
                "Use of restricted handle resolved to SystemV IPC_PRIVATE flag\n"
            );
        }
        // Throw page exists, tricking the user into retrying
        errno = EEXIST;
        return false;
    }

    // Use the generic private pointer to cache our ID to avoid repeated lookups
    if( *private == NULL )
    {
        flags = SHM_FILE_OCTAL;

        if( op == SHM_CREATE )
        {
            flags       |= IPC_CREAT | IPC_EXCL;
            segment_size = size;
        }

        identifier_cache = ( int * ) calloc( 1, sizeof( int ) );

        if( identifier_cache == NULL )
        {
            fprintf(
                stderr,
                "Failed to allocate identifier cache"
            );
        }

        identifier = shmget( key, segment_size, flags );

        if( identifier == -1 )
        {
            if( errno != EEXIST )
            {
                save_errno = errno;
                free( identifier_cache );
                fprintf(
                    stderr,
                    "Failed to get shared memory segment %s: %s",
                    name,
                    strerror( errno )
                );

                return false;
            }
        }

        *identifier_cache = identifier;
        *private          = ( void * ) identifier_cache;
    }
    else
    {
        identifier_cache = ( int * ) *private;
        identifier       = ( int ) *identifier_cache;
    }
    
    if( op == SHM_DESTROY || op == SHM_DETACH )
    {
        // Clean up our previously or newly allocated ID cache
        save_errno = errno;
        if( identifier_cache != NULL )
        {
            free( identifier_cache );
            *private = NULL;
        }

        if( *mapped_address != NULL && shmdt( *mapped_address ) != 0 )
        {
            fprintf(
                stderr,
                "Could not unmap shared memory segment %s: %s",
                name,
                strerror( errno )
            );
            errno = save_errno;
            return false;
        }

        *mapped_address = NULL;
        *mapped_size    = 0;

        if( op == SHM_DESTROY )
        {
            if( shmctl( identifier, IPC_RMID, NULL ) < 0 )
            {
                fprintf(
                    stderr,
                    "Could not remove shared memory segment %s: %s",
                    name,
                    strerror( errno )
                );
                errno = save_errno;
                return false;
            }
        }

        return true;
    }

    if( op == SHM_ATTACH )
    {
        if( shmctl( identifier, IPC_STAT, &shm ) != 0 )
        {
            fprintf(
                stderr,
                "Failed to stat shared memory segment %s: %s",
                name,
                strerror( errno )
            );
            return false;
        }
        
        // handle size mismatch?
        size = shm.shm_segsz;
    }

    address = shmat( identifier, NULL, SYSV_SHM_FLAGS );

    if( ( void * ) address == ( void * ) -1 )
    {
        save_errno = errno;

        if( op == SHM_CREATE )
        {
            shmctl( identifier, IPC_RMID, NULL );
        }

        errno = save_errno;
        fprintf(
            stderr,
            "Failed to map shared memory segment %s: %s",
            name,
            strerror( errno )
        );

        return false;
    }

    *mapped_address = ( void * ) address;
    *mapped_size    = size;

    return true;
}
#endif // SHM_USE_SYSV

#ifdef SHM_USE_MMAP
static bool shm_mmap(
    shm_op     op,
    shm_handle handle,
    size_t     size,
    void **    mapped_address,
    size_t *   mapped_size
)
{
    char        name[64]    = {0};
    int         flags       = 0;
    int         save_errno  = 0;
    int         descriptor  = 0;
    struct stat statbuff    = {0};
    char *      address     = NULL;

    snprintf(
        name,
        64,
        "%s/%s%lu"
        SHM_FILE_MMAP_DIR,
        SHM_FILE_MMAP_PREFIX,
        ( uint64_t ) handle
    );

    if( op == SHM_DETACH || op == SHM_DESTROY )
    {
        save_errno = errno;
        if(
               *mapped_address != NULL
            && munmap( *mapped_address, *mapped_size ) != 0
          )
        {
            fprintf(
                stderr,
                "Failed to unmap shared memory segment %s: %s",
                name,
                strerror( errno )
            );
            errno = save_errno;
            return false;
        }

        *mapped_address = NULL;
        *mapped_size    = 0;

        if( op = SHM_DESTROY && unlink( name ) != 0 )
        {
            fprintf(
                stderr,
                "Failed to remove shared memory segment %s: %s",
                name,
                strerror( errno )
            );
            errno = save_errno;
            return false;
        }

        return true;
    }

    flags = O_RDWR;

    if( op == SHM_CREATE )
    {
        flags |= O_CREAT | O_EXCL;
    }

    save_errno = errno;
    descriptor = open( name, flags, SHM_FILE_PERMS );

    if( descriptor < 0 )
    {
        fprintf(
            stderr,
            "Failed to create shared memory segment %s: %s",
            name,
            strerror( errno )
        );
        errno = save_errno;
        return false;
    }

    if( op == SHM_ATTACH )
    {
        if( fstat( descriptor, &statbuff ) != 0 )
        {
            _close_segment( descriptor, name, false );

            fprintf(
                stderr,
                "Failed to stat shared memory segment %s: %s",
                name,
                strerror( errno )
            );
            return false;
        }
    
        size = statbuff.st_size;
    }
    else if( shm_mmap_resize( descriptor, size ) != 0 )
    {
        _close_segment( descriptor, name, true );
        fprintf(
            stderr,
            "Failed to resize shared memory segment %s to %zu bytes: %s",
            name,
            size,
            strerror( errno );
        );

        return false;
    }

    address = ( char * ) mmap(
        NULL,
        size,
        PROT_READ | PROT_WRITE,
        MAP_NOSYNC | MAP_SHARED | MAP_HASSEMAPHORE,
        descriptor,
        0
    );

    if( address == MAP_FAILED )
    {
        if( op == SHM_CREATE )
            _close_segment( descriptor, name, true );
        else
            _close_segment( descriptor, name, false );

        fprintf(
            stderr,
            "Could not map shared memory segment %s: %s",
            name,
            strerror( errno );
        );

        return false;
    }

    *mapped_address = ( void * ) address;
    *mapped_size    = size;

    if( !_close_segment( descriptor, name, false ) )
    {
        return false;
    }

    return true;
}

static int shm_mmap_resize( int descriptor, size_t size )
{
    char *      zero_buffer = NULL;
    uint32_t    remaining   = 0;
    size_t      goal        = 0;
    size_t      written     = 0;
    bool        success     = false;
 
    /*
     * Fill the file with zeros. We want to do this ahead of time to ensure
     * that the space has actually been allocated. In a similare vein to the
     * prevention of SIGBUS some time after the initial allocation, this
     * page-zeroing prevents an errant SIGSEGV from occuring if an
     * unallocated portion of the mapping is accessed later.
     */
    zero_buffer = ( char * ) calloc( ZERO_BUFFER_SIZE, 1 );
    remaining   = ( uint32_t ) size;
    success     = true;

    if( zero_buffer == NULL )
    {
        errno = ENOMEM;
        return -1;
    }

    while( success && remaining > 0 )
    {
        goal = ( size_t ) remaining;

        if( goal > ZERO_BUFFER_SIZE )
            goal = ZERO_BUFFER_SIZE;
        
        do {
            written = write( descriptor, zero_buffer, goal );
            ret     = erno;
        } while( ret == EINTR );
        
        if( written == goal ) 
            remaining -= goal;
        else
            success = false;
    }

    if( !success )
    {
        if( errno == 0 )
            errno = ENOSPC;

        return -1;
    }

    return 0;
}
#endif // SHM_USE_MMAP
// Primitive wrapper
static bool shm_wrapper(
    shm_op     op,
    shm_handle handle,
    size_t     size,
    void **    private,
    void **    mapped_address,
    size_t *   mapped_size
)
{
#ifdef SHM_USE_POSIX
    return shm_posix( op, handle, size, mapped_address, mapped_size );
#endif // SHM_USE_POSIX
#ifdef SHM_USE_SYSV
    return shm_sysv( op, handle, size, private, mapped_address, mapped_size );
#endif // SHM_USE_SYSV
#ifdef SHM_USE_MMAP
    return shm_mmap( op, handle, size, mapped_address, mapped_size );
#endif // SHM_USE_MMAP
}
// End allocator primitives

// Helper functions
static bool _close_segment( int descriptor, char * name, bool do_unlink )
{
    int  save_errno = 0;

    save_errno = errno;

    if( close( descriptor ) != 0 )
    {
        fprintf(
            stderr,
            "Failed to close shared memory segment %s: %s",
            name,
            strerror( errno )
        );
        errno = save_errno;
        // Cannot unlink
        return false;
    }

    if( do_unlink && name != NULL)
    {
        errno = 0;
        if( unlink( name ) != 0 )
        {
            fprintf(
                stderr,
                "Failed to remove shared memory segment %s: %s",
                name,
                strerror( errno )
            );
            errno = save_errno;
            return false;
        }
    }

    errno = save_errno;
    return true;
}
