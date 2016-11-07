/* allocator.c
 *
 * Copyright 2012-2016 AOL Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this Software except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "moloch.h"

extern MolochConfig_t        config;

typedef struct mal_t {
    struct mal_t       *mal_next, *mal_prev;
} MolochAllocatorList_t;

typedef struct {
    struct mal_t       *mal_next, *mal_prev;
    int                 mal_count;
    MOLOCH_LOCK_EXTERN(lock);
} MolochAllocatorListHead_t;

typedef struct molochallocator_t {
    int aThreads;
    int fThreads;
    int size;
    int fPos;
    MolochAllocatorListHead_t *aLists;
    MolochAllocatorListHead_t *fLists;
    MOLOCH_LOCK_EXTERN(lock);
} MolochAllocator_t;

/******************************************************************************/
void moloch_allocator_make(MolochAllocator_t *allocator, int aThread)
{
    LOG("ALW make %d", aThread);
    int i;
    uint8_t *buf = malloc(allocator->size*4096);
    memset(buf, 0, allocator->size*4096);
    for (i = 0; i < 4096; i++) {
        MolochAllocatorList_t *item = (MolochAllocatorList_t *)(buf + (allocator->size*i));
        DLL_PUSH_TAIL(mal_, &allocator->aLists[aThread], item);
    }
}

/******************************************************************************/
MolochAllocator_t *moloch_allocator_create(int aThreads, int fThreads, int size)
{
    LOG("ALW aThreads: %d fThreads: %d size: %d", aThreads, fThreads, size);
    MolochAllocator_t *allocator = MOLOCH_TYPE_ALLOC0(MolochAllocator_t);
    allocator->aThreads = aThreads;
    allocator->fThreads = fThreads;
    allocator->size = size;
    MOLOCH_LOCK_INIT(allocator->lock);

    int i;


    allocator->aLists = malloc(aThreads*sizeof(MolochAllocatorListHead_t));
    for (i = 0; i < aThreads; i++) {
        DLL_INIT(mal_, &allocator->aLists[i]);
        MOLOCH_LOCK_INIT(allocator->aLists[i].lock);
    }

    allocator->fLists = malloc(fThreads*sizeof(MolochAllocatorListHead_t));
    for (i = 0; i < fThreads; i++) {
        DLL_INIT(mal_, &allocator->fLists[i]);
        MOLOCH_LOCK_INIT(allocator->fLists[i].lock);
    }

    return allocator;
}

/******************************************************************************/
void *moloch_allocator_alloc(MolochAllocator_t *allocator, int aThread)
{
    MolochAllocatorList_t *mal;
    DLL_POP_HEAD(mal_, &allocator->aLists[aThread], mal);
    if (mal)
        return mal;

    MOLOCH_LOCK(allocator->lock);
    if (DLL_COUNT(mal_, &allocator->fLists[allocator->fPos])) {
        LOG("ALW shuffle %d %d %d", aThread, allocator->fPos, DLL_COUNT(mal_, &allocator->fLists[allocator->fPos]));
        MOLOCH_LOCK(allocator->fLists[allocator->fPos].lock);
        DLL_PUSH_TAIL_DLL(mal_, &allocator->aLists[aThread], &allocator->fLists[allocator->fPos]);
        MOLOCH_UNLOCK(allocator->fLists[allocator->fPos].lock);
        allocator->fPos = (allocator->fPos + 1) % allocator->fThreads;
        MOLOCH_UNLOCK(allocator->lock);
    } else {
        allocator->fPos = (allocator->fPos + 1) % allocator->fThreads;
        MOLOCH_UNLOCK(allocator->lock);
        moloch_allocator_make(allocator, aThread);
    }
    DLL_POP_HEAD(mal_, &allocator->aLists[aThread], mal);
    return mal;
}

/******************************************************************************/
void moloch_allocator_free(MolochAllocator_t *allocator, int fThread, void *item)
{
    memset(item, 0, allocator->size);
    MOLOCH_LOCK(allocator->fLists[fThread].lock);
    DLL_PUSH_TAIL(mal_, &allocator->fLists[fThread], (MolochAllocatorList_t*)item);
    MOLOCH_UNLOCK(allocator->fLists[fThread].lock);
}

