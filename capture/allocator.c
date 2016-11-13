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

/******************************************************************************/
void moloch_allocator_make(MolochAllocator_t *allocator, int aThread, int number)
{
    int i;
    LOG("ALW make %d %d %d", aThread, allocator->size, number);
    uint8_t *buf = malloc(allocator->size*number);
    if (allocator->zero)
        memset(buf, 0, allocator->size*number);
    for (i = 0; i < number; i++) {
        MolochAllocatorList_t *item = (MolochAllocatorList_t *)(buf + (allocator->size*i));
        SLL_PUSH_HEAD(mal_, allocator->aLists[aThread].mal_head, item);
    }
}

/******************************************************************************/
MolochAllocator_t *moloch_allocator_create(int aThreads, int fThreads, int size, int initial, gboolean zero)
{
    LOG("ALW aThreads: %d fThreads: %d size: %d", aThreads, fThreads, size);
    MolochAllocator_t *allocator = MOLOCH_TYPE_ALLOC0(MolochAllocator_t);
    allocator->aThreads = aThreads;
    allocator->fThreads = fThreads;
    allocator->size = size;
    allocator->zero = zero;
    MOLOCH_LOCK_INIT(allocator->lock);

    int i;

    allocator->aLists = malloc(aThreads*sizeof(MolochAllocatorListHead_t));
    for (i = 0; i < aThreads; i++) {
        allocator->aLists[i].mal_head = NULL;
        MOLOCH_LOCK_INIT(allocator->aLists[i].lock);
        moloch_allocator_make(allocator, i, initial);
    }

    allocator->fLists = malloc(fThreads*sizeof(MolochAllocatorListHead_t));
    for (i = 0; i < fThreads; i++) {
        allocator->fLists[i].mal_head = NULL;
        MOLOCH_LOCK_INIT(allocator->fLists[i].lock);
    }

    return allocator;
}

/******************************************************************************/
void *moloch_allocator_alloc(MolochAllocator_t *allocator, int aThread)
{
    MolochAllocatorList_t *mal;
    SLL_POP_HEAD(mal_, allocator->aLists[aThread].mal_head, mal);
    if (mal)
        return mal;

    MOLOCH_LOCK(allocator->lock);
    if (allocator->fLists[allocator->fPos].mal_head) {
        MOLOCH_LOCK(allocator->fLists[allocator->fPos].lock);
        allocator->aLists[aThread].mal_head = allocator->fLists[allocator->fPos].mal_head;
        allocator->fLists[allocator->fPos].mal_head = NULL;
        allocator->fPos = (allocator->fPos + 1) % allocator->fThreads;
        MOLOCH_UNLOCK(allocator->lock);
    } else {
        allocator->fPos = (allocator->fPos + 1) % allocator->fThreads;
        MOLOCH_UNLOCK(allocator->lock);
        moloch_allocator_make(allocator, aThread, 65536);
    }
    SLL_POP_HEAD(mal_, allocator->aLists[aThread].mal_head, mal);
    return mal;
}

/******************************************************************************/
void moloch_allocator_free(MolochAllocator_t *allocator, int fThread, void *item)
{
    if (allocator->zero)
        memset(item, 0, allocator->size);
    MOLOCH_LOCK(allocator->fLists[fThread].lock);
    SLL_PUSH_HEAD(mal_, allocator->fLists[fThread].mal_head, (MolochAllocatorList_t*)item);
    MOLOCH_UNLOCK(allocator->fLists[fThread].lock);
}

