// bufpool.h ... buffer pool interface

#define MAXID 1024

// for Cycle strategy, we simply re-use the nused counter
// since we don't actually need to maintain a usedList
#define currSlot nused

typedef struct bufPool *BufPool;

// one buffer
struct buffer {
	char  id[MAXID];
	int   pin;
	int   dirty;
	char  *data;
};

// collection of buffers + stats
struct bufPool {
	int   nbufs;         // how many buffers
	char  strategy;      // LRU, MRU, Cycle
	int   nrequests;     // stats counters
	int   nreleases;
	int   nreads;
	int   nwrites;
	int   *freeList;     // list of completely unused bufs
	int   nfree;
	int   nused;
	struct buffer *bufs;
};

BufPool initBufPool(int, char);
void    releaseBufpool(BufPool);
int		pageInPool(BufPool pool, char *rel, int page);
int     request_page(BufPool, char*, int);
void    release_page(BufPool, char*, int);
void    showPoolUsage(BufPool);
void    showPoolState(BufPool);

