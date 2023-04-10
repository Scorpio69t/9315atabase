// bufpool.c ... buffer pool implementation

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "bufpool.h"




// static unsigned int clock = 0;


// Helper Functions (private)


// pageInBuf(pool,slot)
// - return the name of the page current stored in specified slot

static
char *pageInBuf(BufPool pool, int slot)
{
	char *pname;
	pname = pool->bufs[slot].id;
	if (pname[0] == '\0')
		return "_";
	else
		return pname;
}

static
void freePageBuffer(BufPool pool, char * rel, int page){
	// pin == 0	 free page buffer and add it to freelist
	int pidx = pageInPool(pool,rel,page);
	if(pidx < 0 ) return;
	if(pool->bufs[pidx].data!=NULL){
		free(pool->bufs[pidx]);
		memset(pool->bufs->id, '\0' , MAXID);
		pool->bufs[pidx].dirty=0;
		pool->bufs[pidx].pin=0;
	}

	assert(pool->nfree>0 && pool->nfree<pool->nbufs);
	pool->freeList[pool->nfree];
}

static
int removeFirstFree(BufPool pool)
{
	int v, i;
	assert(pool->nfree > 0);
	v = pool->freeList[0];
	for (i = 0; i < pool->nfree-1; i++)
		pool->freeList[i] = pool->freeList[i+1];
	pool->nfree--;
	return v;
}

// pageInPool(BufPool pool, char rel, int page)
// - check whether page from rel is already in the pool
// - returns the slot containing this page, else returns -1

int pageInPool(BufPool pool, char *rel, int page)
{
	int i;  char id[MAXID];
	sprintf(id,"%s%d",rel,page);
	fprintf(stderr,"Buffer Pool : check table %s page %d\n",rel,page);
	for (i = 0; i < pool->nbufs; i++) {
		if (strcmp(id,pool->bufs[i].id) == 0) {
			return i;
		}
	}
	return -1;
}

// getNextSlot(pool)
// - finds the "best" unused buffer pool slot
// - "best" is determined by the replacement strategy
// - if the replaced page is dirty, write it out
// - initialise the chosen slot to hold the new page
// - if there are no available slots, return -1

static
int grabNextSlot(BufPool pool)
{
	int slot = -1;
	switch (pool->strategy) {
	case 'L':
		// get least recently used slot from used list
		break;
	case 'M':
		// get most recently used slot from used list
		break;
	case 'C':
		// get next available according to cycle counter
		int i = pool->currSlot;
		while (1)
		{
			if (pool->bufs[i % pool->nbufs].pin == 0){
				break;
			}
			else {
				pool->bufs[i%pool->nbufs].pin--;
				i++;
			}
		}
		slot = i%pool->nbufs;
		
		break;
	}

	if (slot >= 0 && pool->bufs[slot].dirty) {
		pool->nwrites++;
		pool->bufs[slot].dirty = 0;
	}
	return slot;
}


// Interface Functions


// initBufPool(nbufs,strategy)
// - initialise a buffer pool with nbufs
// - buffer pool uses supplied replacement strategy

BufPool initBufPool(int nbufs, char strategy)
{
	BufPool newPool;
	// struct buffer *bufs;

	newPool = malloc(sizeof(struct bufPool));
	assert(newPool != NULL);
	newPool->nbufs = nbufs;
	newPool->strategy = strategy;
	newPool->nrequests = 0;
	newPool->nreleases = 0;
	newPool->nreads = 0;
	newPool->nwrites = 0;
	newPool->nfree = nbufs; // all solts are free
	newPool->freeList = malloc(nbufs * sizeof(int));
	assert(newPool->freeList != NULL);
	newPool->nused = 0;
	newPool->bufs = malloc(nbufs * sizeof(struct buffer));
	assert(newPool->bufs != NULL);

	int i;
	for (i = 0; i < nbufs; i++) {
		newPool->bufs[i].id[0] = '\0';
		newPool->bufs[i].pin = 0;
		newPool->bufs[i].dirty = 0;
		newPool->bufs[i].data=NULL;
		newPool->freeList[i] = i;
	}
	return newPool;
}

void releaseBufpool(BufPool pool){
	// release free list
	if(pool->freeList!=NULL){
		free(pool->freeList);
	}
	// release pages buffer
	for(int i=0; i<pool->nbufs; i++){
		if(pool->bufs[i].data != NULL){
			free(pool->bufs[i].data);
		}
	}
	// release buffer slots
	if(pool->bufs != NULL){
		free(pool->bufs);
	}

	// finally release buffer pool
	free(pool);
	printf("releaseBufpool() is invoked!\n");
}

// request a page
int request_page(BufPool pool, char* rel, int page)
{
	int slot;
	printf("Request %s%d\n", rel, page);
	pool->nrequests++;
	slot = pageInPool(pool,rel,page);
	if (slot < 0) { // page is not already in pool
		if (pool->nfree > 0) {
			// first request from free list
			slot = removeFirstFree(pool);
		}
		else {
			// need to release
			slot = grabNextSlot(pool);
		}
		if (slot < 0) {
			fprintf(stderr, "Failed to find slot for %s%d\n",rel,page);
			exit(1);
		}
		pool->nreads++;
		sprintf(pool->bufs[slot].id,"%s%d",rel,page);
		pool->bufs[slot].pin = 0;
		pool->bufs[slot].dirty = 0;
	}
	// have a slot
	pool->bufs[slot].pin++; // used
	removeFromUsedList(pool,slot); // update slot state 
	showPoolState(pool);  // for debugging
	return slot;
}

void release_page(BufPool pool, char* rel, int page)
{
	printf("Release %s%d\n", rel, page);
	pool->nreleases++;

	int i;
	i = pageInPool(pool,rel,page);
	assert(i >= 0);
	// last user of page is about to release
	if (pool->bufs[i].pin == 1) {
		makeAvailable(pool, i);
	}
	pool->bufs[i].pin--;
	showPoolState(pool);  // for debugging
}

// showPoolUsage(pool)
// - prints statistics counters for buffer pool

void showPoolUsage(BufPool pool)
{
	assert(pool != NULL);
	printf("#requests: %d\n",pool->nrequests);
	printf("#releases: %d\n",pool->nreleases);
	printf("#reads   : %d\n",pool->nreads);
	printf("#writes  : %d\n",pool->nwrites);
}

// showPoolState(pool)
// - display printable representation of buffer pool on stdout

void showPoolState(BufPool pool)
{
	int i, j; char *p; struct buffer b;

	printf("%4s %6s %6s %6s\n","Slot","Page","Pin","Dirty");
	for (i = 0; i < pool->nbufs; i++) {
		b = pool->bufs[i];
		p = pageInBuf(pool,i);
		printf("[%02d] %6s %6d %6d\n", i, p, b.pin, b.dirty);
	}
	printf("FreeList:");
	for (i = 0; i < pool->nfree; i++) {
		j = pool->freeList[i];
		printf(" [%02d]%s", j, pageInBuf(pool,j));
	}
	printf("\n");
	printf("UsedList:");
	for (i = 0; i < pool->nused; i++) {
		j = pool->usedList[i];
		printf(" [%02d]%s", j, pageInBuf(pool,j));
	}
	printf("\n");
}
