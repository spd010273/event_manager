/*------------------------------------------------------------------------
 *
 * em_shm.c
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
 *        src/lib/em_shm.c
 *
 *------------------------------------------------------------------------
 */

#include "em_shm.h"

static ctrl_header *     control_header    = NULL;
static size_t            control_header_sz = 0;
static shm_handle        control_handle    = ( shm_handle ) 0;
static void *            sysv_private      = NULL;
static bool              is_inited         = false;
static shm_segment_map * seg_map           = NULL;

// TODO:
//  - implement shm physical file load-in and cleanup in case of crashes or reboots
//  - Add cleanup routine for program exits
void shm_init( void )
{
    void *     ctrl_header_address = NULL;
    size_t     ctrl_header_size    = 0;
    shm_handle ctrl_handle         = 0;

    /*
     * This routine is expected to be called by the parent prior to children
     * beginning to use shared memory. This is suggested as a simple and easy
     * way to consistently initialize shared memory and accounting information.
     * Things become much easier if this is done pre-fork
     */

    // May need to cleanup physical files here
    ctrl_header_size = get_ctrl_bytes_overhead( ( uint32_t ) MAX_SEGMENTS );

    while( ctrl_header_address == NULL && ctrl_header_size == 0 )
    {
        ctrl_handle = ( shm_handle ) random();

        if( unlikely( ctrl_handle == CTRL_HEADER_INVALID ) )
            continue;

        if(
            likely(
                shm_wrapper(
                    SHM_CREATE,
                    ctrl_handle,
                    ctrl_header_size,
                    &sysv_private,
                    ( void ** ) &ctrl_header_address,
                    ( size_t * ) &control_header_sz
                )
            )
          )
        {
            break;
        }
    }

    control_header = ctrl_header_address;
    control_handle = ctrl_handle;

    control_header->items       = NULL;
    control_header->id          = ( uint32_t ) CTRL_HEADER_MAGIC;
    control_header->entry_count = 0;
    control_header->owner       = getpid();

    is_inited = true;
    return;
}

void shm_child_init( void )
{
    void *     ctrl_header_address = NULL;
    size_t     ctrl_header_size    = 0;
    void *     priv                = NULL;

    if( !is_inited )
    {
        // Maybe handle the case where parent fork()'d then inited,
        // We wont have the control handle so we'll have to search the disk
        // for the header belonging to our getppid()
        return;
    }

    if( !is_local_state_good() )
        return;

    if( unlikely( control_handle == 0 || control_handle == CTRL_HEADER_INVALID ) )
        return;

    // Proceed expecing a valid init'd control header
    if(
        likely(
            shm_wrapper(
                SHM_ATTACH,
                control_handle,
                0,
                ( void ** ) &priv,
                ( void ** ) &ctrl_header_address,
                ( size_t * ) &ctrl_header_size
            )
        )
      )
    {
        if( !shm_check_owner( ctrl_header_address ) )
        {
            fprintf(
                stderr,
                "shm_child_init: Bad control segment %lu is not valid or owned by us",
                ( uint64_t ) control_handle
            );
            shm_wrapper(
                SHM_DETACH,
                control_handle,
                0,
                &priv,
                ( void ** ) &ctrl_header_address,
                &ctrl_header_size
            );

        }

        return;
    }

    control_header    = ctrl_header_address;
    control_header_sz = ctrl_header_size;

    _map_all();
    return;
}

static void _map_segment( s )
{
    
}

static void _map_all( void )
{
    uint32_t i = 0;
    shm_segment * segment    = NULL;
    shm_handle    seg_handle = ( shm_handle ) SEGMENT_HANDLE_INVALID;

    if( unlikely( control_header == NULL ) )
    {
        fprintf(
            stderr,
            "Cannot map in SHM segments when in invalid state"
        );
    }

    if( seg_map == NULL )
    {
        seg_map = calloc( sizeof( shm_segment_map ), 1 );

        if( unlikely( seg_map == NULL ) )
        {
            fprintf(
                stderr,
                "Failed to allocate segment map in _map_all - no space"
            );
            return;
        }

        seg_map->num_mapped = 0;
    }

    // Sets up the local copy of ctrl_header->items[] to track what
    // has been mapped in and what has not
    for( i = 0; i < control_header->max_entries; i++ )
    {
        segment = seg_map->mapped_segments[i];
        
        if( segment == NULL )
        {
            // Not mapped locally
            seg_handle = control_header->items[i].handle;
            segment    = attach_segment( seg_handle );
            seg_map->mapped_segments[i] = segment;
            seg_map->num_mapped++;
        }
        else
        {
            seg_handle = segment->handle;

            if( control_header->items[i].handle == seg_handle )
                continue;

            if( control_header->items[i].handle == ( shm_handle ) SEGMENT_HANDLE_INVALID )
            {
                detach_segment( segment );
                seg_map->mapped_segments[i] = NULL;
                segment                     = NULL;
                continue;
            }

            // Remap
            detach_segment( segment );
            seg_map->mapped_segments[i] = NULL;
            segment = attach_segment( control_header->items[i].handle );
            
            if( segment == NULL )
            {
                continue;
            }

            if( segment->ctrl_index == i )
            {
                seg_map->mapped_segments[i] = segment;
            }
            else
            {
                fprintf(
                    stderr,
                    "misaligned segment index and mapping index"
                );
            }
        }
    }

    return;
}

// Segment interface functions
static void detach_segment( shm_segment * segment )
{
    if( unlikely( segment == NULL ) )
        return;

    if( segment->mapped_address != NULL )
    {
        if(
            likely(
                shm_wrapper(
                    SHM_DETACH,
                    segment->handle,
                    0,
                    ( void ** ) &segment->priv,
                    ( void ** ) &segment->mapped_address,
                    ( size_t * ) &segment->mapped_size
                )
            )
          )
        {
            // Segment is no longer in shm scope and just local allocation
            segment->priv           = NULL;
            segment->mapped_address = NULL;
            segment->mapped_size    = 0;
        }
        else
        {
            fprintf(
                stderr,
                "Detach failed"
            );
            return;
        }
    }

    // Decrement ref count
    _lock_acquire( &(control_header->items[segment->ctrl_index].locked) );
    if( control_header->items[segment->ctrl_index].ref_count > 1 )
    {
        control_header->items[segment->ctrl_index].ref_count--;
    }
    else
    {
        if( control_header->items[segment->ctrl_index].ref_count == 1 )
        {
            //TODO Need to clean up this segment in SHM
        }
        else
        {
            // Invalid state?
        }
    }
    _lock_release( &(control_header->items[segment->ctrl_index].locked) );
    _free_segment( segment );
    return;
}

static shm_segment * attach_segment( shm_handle handle )
{
    shm_segment * segment = NULL;
    uint32_t      i       = 0;

    if( unlikely( control_handle == CTRL_HEADER_INVALID ) )
    {
        fprintf(
            stderr,
            "Cannot attach a segment on an invalid control handle"
        );
        return NULL;
    }

    if( !is_inited )
        shm_child_init();

    segment = _new_segment();

    if( segment == NULL )
        return NULL;

    _lock_acquire( &(control_header->locked) );

    for( i = 0; i < control_header->entry_count; i++ )
    {
        if( control_header->items[i].handle != segment->handle )
            continue;

        if( control_header->items[i].ref_count <= 1 )
            continue;

        control_header->items[i].ref_count++;
        segment->ctrl_index = i;
    }

    _lock_release( &(control_header->locked) );

    if( unlikely( segment->ctrl_index == INVALID_CONTROL_INDEX ) )
    {
        _free_segment( segment );
        return NULL;
    }

    if(
        likely(
            shm_wrapper(
                SHM_ATTACH,
                segment->handle,
                0,
                ( void ** ) &segment->priv,
                ( void ** ) &segment->mapped_address,
                ( size_t * ) &segment->mapped_size
            )
        )
      )
    {
        return segment;
    }

    _free_segment( segment );
    return NULL;
}

static shm_segment * create_segment( size_t size )
{
    shm_segment * segment = NULL;
    uint32_t      i       = 0;
    uint32_t      count   = 0;

    if( unlikely( control_handle == CTRL_HEADER_INVALID ) )
    {
        // Maybe go through initialization in the case where we arent?
        fprintf(
            stderr,
            "Cannot create a segment on an invalid control handle"
        );
        return NULL;
    }

    if( control_header->entry_count >= control_header->max_entries )
    {
        // Maybe figure out a way to extend the number of control slots
        fprintf(
            stderr,
            "Out of slots in control segment"
        );
        return NULL;
    }

    segment = _new_segment();

    if( unlikely( segment == NULL ) )
        return NULL;

    while( segment->mapped_address == NULL && segment->mapped_size == 0 )
    {
        segment->handle = ( shm_handle ) random();
        if( unlikely( segment->handle == SEGMENT_HANDLE_INVALID ) )
            continue;
        if(
            likely(
                shm_wrapper(
                    SHM_CREATE,
                    segment->handle,
                    size,
                    ( void ** ) &(segment->priv),
                    ( void ** ) &(segment->mapped_address),
                    ( size_t * ) &(segment->mapped_size)
                )
           )
          )
        {
            break;
        }
    }

    // Get lock on control header to update arrays
    _lock_acquire( &(control_header->locked) );

    // Look for an unused slot
    for( i = 0; i < control_header->entry_count; i++ )
    {
        if( control_header->items[i].ref_count == 0 )
        {
            control_header->items[i].ref_count = 2;
            control_header->items[i].handle    = segment->handle;
            segment->ctrl_index                = i;
            _lock_release( &(control_header->locked) );
            return segment;
        }
    }

    count = control_header->entry_count;
    control_header->items[count].handle    = segment->handle;
    control_header->items[count].ref_count = 2;
    segment->ctrl_index                    = count;
    control_header->entry_count            = count + 1;
    _lock_release( &(control_header->locked) );
    return segment;
}

static void _free_segment( shm_segment * segment )
{
    uint32_t index = 0;

    if( unlikely( segment == NULL ) )
        return;

    index = segment->ctrl_index;
    if( control_header->items[index].ref_count > 1 )
    {
        control_header->items[index].ref_count--;
    }
    else
    {
        // Clean up the control slot and shm_item?
    }

    free( segment );
    return;
}

static shm_segment * _new_segment( void )
{
    shm_segment * segment = NULL;

    segment = ( shm_segment * ) calloc( sizeof( shm_segment ), 1 );

    if( unlikely( segment == NULL ) )
    {
        fprintf(
            stderr,
            "Failed to allocate shared memory segment"
        );
        return NULL;
    }

    segment->handle         = ( shm_handle ) SEGMENT_HANDLE_INVALID;
    segment->ctrl_index     = INVALID_CONTROL_INDEX;
    segment->mapped_size    = 0;
    segment->mapped_address = NULL;
    segment->priv           = NULL;
    return segment;
}


// Segment sanity checking
static bool shm_check_owner( ctrl_header * header )
{
    if( !shm_check_ctrl( header ) )
        return false;
    if( header->owner == getpid() || header->owner == getppid() )
        return true;

    return false;
}

static bool shm_check_ctrl( ctrl_header * header )
{
    if( header == NULL )
        return false;
    if( header->id != CTRL_HEADER_MAGIC )
        return false;
    if( header->entry_count > header->max_entries )
        return false;

    return true;
}

static bool shm_check_ctrl_by_handle( shm_handle ctrl )
{
    // Given an arbitrary segment, attempt to locate the
    // control header and determine if that's valid
    // and owned by us or our parent
    ctrl_header * header = NULL;
    size_t        size   = 0;
    void *        priv   = NULL;

    if( unlikely( ctrl == CTRL_HEADER_INVALID ) )
        return false;

    if(
        shm_wrapper(
            SHM_ATTACH,
            ctrl,
            0,
            &priv,
            ( void ** ) &header,
            &size
        )
      )
    {
        if( !shm_check_owner( header ) )
        {
            shm_wrapper(
                SHM_DETACH,
                ctrl,
                0,
                &priv,
                ( void ** ) &header,
                &size
            );

            return false;
        }

        return true;
    }

    return false;
}

static bool shm_check_seg( shm_item * item )
{
    if( item == NULL )
        return false;
    if( item->ctrl == CTRL_HEADER_INVALID )
        return false;
    if( !shm_check_ctrl_by_handle( item->ctrl ) )
        return false;
    return true;
}

static size_t get_ctrl_bytes_overhead( uint32_t count )
{
    uint64_t result = 0;

    result = offsetof( ctrl_header, items )
           + sizeof( shm_item ) * ( ( uint64_t ) count );

    return ( size_t ) result;
}

static bool is_local_state_good( void )
{
    if( control_header == NULL )
        return false;
    if( control_header_sz == 0 )
        return false;
    if( control_handle == ( shm_handle ) CTRL_HEADER_INVALID )
        return false;
    if( seg_map == NULL )
        return false;
    if( !is_inited )
        return false;
    return true;
}

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
            _close_segment_descriptor( descriptor, name, false );
            fprintf(
                stderr,
                "Failed to stat shared memory segment %s: %s",
                name,
                strerror( errno )
            );
            return false;
        }

        // Possibly handle size differences here
        if( size != statbuff.st_size )
        {
            fprintf(
                stderr,
                "Mismatch is shared memory segment %s, loaded %zu, expected %zu",
                name,
                statbuff.st_size,
                size
            );
        }

        size = statbuff.st_size;
    }
    else if( shm_posix_resize( descriptor, size ) != 0 )
    {
        _close_segment_descriptor( descriptor, name, false );
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
        MAP_SHARED | MMAP_FLAGS,
        descriptor,
        0
    );

    if( address == MAP_FAILED )
    {
        save_errno = errno;
        _close_segment_descriptor( descriptor, name, false );

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

    _close_segment_descriptor( descriptor, name, false );

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
    struct shmid_ds shm              = {{0}};

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

    if( unlikely( ( void * ) address == ( void * ) -1 ) )
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
        "%s/%s%lu",
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

        if( op == SHM_DESTROY && unlink( name ) != 0 )
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
            _close_segment_descriptor( descriptor, name, false );

            fprintf(
                stderr,
                "Failed to stat shared memory segment %s: %s",
                name,
                strerror( errno )
            );
            return false;
        }

        // Possibly handle size differences here
        if( statbuff.st_size < size )
        {
            fprintf(
                stderr,
                "Mismatch is shared memory segment %s, loaded %zu, expected %zu",
                name,
                statbuff.st_size,
                size
            );
            _close_segment_descriptor( descriptor, name, false );
            return false;
        }

        size = statbuff.st_size;
    }
    else if( shm_mmap_resize( descriptor, size ) != 0 )
    {
        _close_segment_descriptor( descriptor, name, true );
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
        MAP_SHARED | MMAP_FLAGS,
        descriptor,
        0
    );

    if( unlikely( address == MAP_FAILED ) )
    {
        if( op == SHM_CREATE )
            _close_segment_descriptor( descriptor, name, true );
        else
            _close_segment_descriptor( descriptor, name, false );

        fprintf(
            stderr,
            "Could not map shared memory segment %s: %s",
            name,
            strerror( errno )
        );

        return false;
    }

    *mapped_address = ( void * ) address;
    *mapped_size    = size;

    if( !_close_segment_descriptor( descriptor, name, false ) )
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

    if( unlikely( zero_buffer == NULL ) )
    {
        errno = ENOMEM;
        return -1;
    }

    while( success && remaining > 0 )
    {
        goal = ( size_t ) remaining;

        if( goal > ZERO_BUFFER_SIZE )
            goal = ZERO_BUFFER_SIZE;
        errno = 0;
        do {
            written = write( descriptor, zero_buffer, goal );
        } while( errno == EINTR );

        if( written == goal )
            remaining -= goal;
        else
            success = false;
    }

    free( zero_buffer );

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
static bool _close_segment_descriptor( int descriptor, char * name, bool do_unlink )
{
    int save_errno = 0;

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

static bool _lock_acquire( volatile bool * mutex )
{
    double random_backoff = 0.0;
    double last_backoff   = 0.0;
    double total_backoff  = 0.0;

    if( unlikely( mutex == NULL ) )
        return false;

    while( *mutex == true || __test_and_set( mutex ) == true )
    {
        if( total_backoff >= MAX_LOCK_WAIT )
            return false;
        sleep( last_backoff + random_backoff );
        last_backoff    = last_backoff + random_backoff;
        total_backoff  += last_backoff;
        random_backoff  = 2 * ( ( double ) rand() / ( double ) RAND_MAX );
    }

    *mutex = true;
    return true;
}

static bool _lock_release( volatile bool * mutex )
{
    if( unlikely( mutex == NULL ) )
        return false;

    *mutex = false;
    return true;
}

static bool __test_and_set( volatile bool * mutex )
{
    bool initial = true;
    initial      = *mutex;
    *mutex       = true;
    return initial;
}

static size_t _get_system_page_size( void )
{
#ifdef __linux__
    return sysconf( _SC_PAGESIZE );
#endif // __linux__
#if defined( __FreeBSD__ ) || defined( __APPLE__ ) || defined( __unix__ )
    return ( size_t ) getpagesize();
#endif // __FreeBSD__ || __APPLE__ || __unix__
    return ( size_t ) DEFAULT_PAGE_SIZE;
}

static inline size_t _align( size_t size )
{
    // Align an arbitrary size to the wordsize of the system
    return ( size + sizeof( void * ) - 1 ) & ~( sizeof( void * ) - 1 );
}

void * shm_alloc( size_t size )
{
    size_t        aligned_size = 0;
    shm_segment * segment      = NULL;
    shmblock    * block        = NULL;

    aligned_size = _align( size );



    return NULL;
}

void * shm_calloc( size_t elem_size, uint32_t num_elem )
{
    // Stub
    size_t aligned_size = 0;

    aligned_size = _align( elem_size * num_elem );

    return NULL;
}

void * shm_realloc( void * ptr, size_t new_size )
{
    // Stub
    size_t aligned_size = 0;

    aligned_size = _align( new_size );

    return NULL;
}

void shm_free( void * ptr )
{
    // Stub
    return;
}
