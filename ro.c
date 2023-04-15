#include <stdio.h>
#include <stdlib.h>
#include "ro.h"
#include "db.h"
#include "bufpool.h"
#include <string.h>
#include <stdbool.h>
#include <assert.h>


typedef struct page
{   
    UINT64 page_id;
    UINT* data;
    UINT* len; // 
} *Page;

typedef struct fileDesc{
    FILE * file;
    UINT oid;
    bool used;
    char file_name[256];
} *FileDesc;

typedef struct _ref
{
    BufPool buffers;
    FileDesc *files;
} Rel;

Rel *rel = NULL;


void init(){
    // do some initialization here.
    // example to get the Conf pointer
    Conf* cf = get_conf();
    rel =(Rel*) malloc(sizeof(Rel)); 

    rel->files = malloc(sizeof (FileDesc) * cf->file_limit);
    for(int i=0;i<cf->file_limit;i++){
        rel->files[i] = malloc(sizeof(struct fileDesc));
        rel->files[i]->file = NULL;
        memset(rel->files[i]->file_name,0,256);
        rel->files[i]->oid = -1;
        rel->files[i]->used = false;
    }

    // initialize buffer
    rel->buffers = initBufPool(cf->buf_slots,'C');

    // example to get the Database pointer
    // Database* db = get_db();
    
    printf("init() is invoked.\n");
}

static
void print_table(_Table* table){
    for(int i=0; i<table->ntuples;i++){
        for (UINT j = 0; j < table->nattrs; j++){
            fprintf(stdout,"%d ", table->tuples[i][j]);
        }
        fprintf(stdout,"\n");
    }
}

void release(){
    // optional
    // do some end tasks here.
    // free space to avoid memory leak

    Conf* cf = get_conf();

    for(int i=0; i<cf->file_limit;i++){
        if(rel->files[i]->file != NULL){
            fclose(rel->files[i]->file);
            log_close_file(rel->files[i]->oid);
        }
        free(rel->files[i]);
    }
    // release buffer 
    releaseBufpool(rel->buffers);
    free(rel);
    printf("release() is invoked.\n");
}

static int 
get_fd(FileDesc *files, int flimit)
{
    int i = 0;
    for(;i<flimit;i++){
        if(files[i]->file == NULL){
            return i;
        }
    }
    return -1;
}

static int
is_opend(FileDesc *files, int flimit, const char * file_name){
    int i = 0;
    for(; i<flimit; i++){
        if(files[i]->file != NULL && strcmp(files[i]->file_name,file_name)==0){
            return i;
        }
    }
    return -1;
}

static int
evict_fd(FileDesc *files, int flimit)
{
    int i = 0;
    // clock algorithm select a victim fd
    while (true)
    {
        if(files[i%flimit]->used == false){
            break;
        } else{
            files[i%flimit]->used = false;
            i++;
        }
    }

    // close files[i%flimit]->file
    int n = i % flimit;
    fclose(files[n]->file);
    files[n]->file = NULL;
    memset(files[n]->file_name,0,256);
    log_close_file(files[n]->oid);
    files[n]->oid = -1;
    files[n]->used = false;

    return n;
}



_Table* sel(const UINT idx, const INT cond_val, const char* table_name){
    
    printf("sel() is invoked.\n");

    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    UINT oid=0;
    INT ntuples_per_page = 0;
    Table table;
    // Table File
    FILE * data_file = NULL;

    Database* db = get_db();
    Conf* cf = get_conf();

    for(int i=0; i<db->ntables;i++){
        table = db->tables[i];
        if (strcmp(table.name,table_name) == 0){
            oid = table.oid;
            break;
        }
    }
   
   // first we allocate enough slot for return result, must free it to avoid memory leaking. 
    _Table *result = malloc(sizeof(_Table) * table.ntuples*sizeof(Tuple));
    memset(result,0,sizeof(Table));
    result->nattrs = table.nattrs;
    result->ntuples = 0;

    char table_path[256];
    memset(table_path,0,256);
    if(strlen(table_name)>250){
        fprintf(stderr,"ERROR: the table name: %s is too long\n", table_name);
        exit(-1);
    }
    sprintf(table_path,"%s/%u", db->path,oid);
    fprintf(stderr,"DEBUG-one table name: %s\n",table_path);
    // open file pointer for the table
    // first checking is this table opend?
    int ret = is_opend(rel->files,cf->file_limit,table_path);
    if(ret == -1){
        // not opend, allocate a slot
        int sidx=get_fd(rel->files,cf->file_limit);
        if(sidx == -1){
            // no free slot log file close in evict_fd function
            sidx = evict_fd(rel->files,cf->file_limit);
        }
        // for file clock 
        rel->files[sidx]->used = true;
        rel->files[sidx]->file = fopen(table_path,"r");
        log_open_file(oid);
        strcpy(rel->files[sidx]->file_name,table_path);
        rel->files[sidx]->oid = oid;
        ret = sidx;
    }
    rel->files[ret]->used = true;
    data_file = rel->files[ret]->file;
    // calculate number of tuples per page
    ntuples_per_page = (cf->page_size-sizeof(UINT64))/sizeof(INT)/table.nattrs;
    fprintf(stderr,"DEBUG-one page contains %d tuples\n", ntuples_per_page);
    UINT npages =  (table.ntuples+(ntuples_per_page-1))/ntuples_per_page;
    fprintf(stderr,"DEBUG-the table contains %d pages\n", npages);

    fseek(data_file,0,SEEK_SET);

    UINT64 tpages;

    for(tpages=0; tpages < npages ; tpages++){
        // allocate a page
        // release_page()
        int slot = pageInPool(rel->buffers,table.name,tpages);
        if(slot < 0 ){
            // miss 
            slot = request_page(rel->buffers,table.name,tpages);
            if(rel->buffers->bufs[slot].data != NULL){
                // call release
                UINT64 r_pid = *((UINT64*)rel->buffers->bufs[slot].data);
                fprintf(stderr,"DEBUG- release relation %s page id %lu \n", rel->buffers->bufs[slot].id,r_pid);
                log_release_page(r_pid);
                freeSlot(rel->buffers,slot);
            }

            if(rel->buffers->bufs[slot].data == NULL){
                rel->buffers->bufs[slot].data = malloc(cf->page_size);
                // read a page from table file 
                size_t ret_code = fread(rel->buffers->bufs[slot].data,1,cf->page_size,data_file);
                fprintf(stderr,"DEBUG- read relation %s page id: %lu \n", table.name, tpages);
                log_read_page(tpages);
                if(ret_code < cf->page_size){
                // error handling
                    if(feof(data_file)){
                        printf("Error read page: unexpected end of file\n");
                        break;
                    }
                    else if(ferror(data_file)) {
                        perror("Error reading page");
                        exit(-1);
                    }
        
                }
            }
            
        } // else hit the page cache
        UINT64 pid = *(UINT64*)rel->buffers->bufs[slot].data; // page id UINT64 8Bytes
        assert(pid == tpages);
        for(int tuple_i = 0; tuple_i<ntuples_per_page;tuple_i++){
            // iterate every tuples
            // Tuple tuple = (Tuple)(page+sizeof(UINT64)+tuple_i*sizeof(INT)*table.nattrs); 
            Tuple tuple = (Tuple)(rel->buffers->bufs[slot].data+sizeof(UINT64)+tuple_i*sizeof(INT)*table.nattrs); 
            if(tuple[idx] == cond_val){
                // add to result 
                Tuple rtuple =(Tuple)malloc(result->nattrs * sizeof(INT));
                memcpy(rtuple,tuple, result->nattrs*sizeof(INT));
                result->tuples[result->ntuples] = rtuple;
                result->ntuples +=1;
            }
        }
    }
    // bree buffer
    // free(page);

    return result;
}

static
void swap(Tuple x, Tuple y, int nattr) {

    Tuple temp = malloc(sizeof(INT)*nattr);
    memcpy(temp,x,nattr*sizeof(INT));
    memcpy(x,y,sizeof(INT)*nattr);
    memcpy(y,temp,sizeof(INT)*nattr);
    free(temp);
}

void bubble_sort_table(Tuple arr[], int len, int idx, int nattr)
{
	int i, j;
	bool exchanged = true;
	
	for (i=0; exchanged && i<len-1; i++){
        exchanged = false;
		for (j=0; j<len-1-i; j++) 
		{ 
			if (arr[j][idx] > arr[j+1][idx])
			{ 
				swap(arr[j],arr[j+1],nattr);
				exchanged = true;
		    }
       }
    }
}

/** sort page*/
static
void bubble_sort_page(Tuple arr, int len, int idx, int nattr) {
    int i, j;
    for (i = 0; i < len - 1; i++)
        for (j = 0; j < len - 1 - i; j++)
            if (arr[j*nattr+idx] > arr[(j + 1)*nattr+idx]) {
                swap(arr+j*nattr,arr+(j+1)*nattr,nattr);
            }
}


_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name){

    printf("join() is invoked.\n");
    // write your code to join two tables
    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    UINT oid1=0, oid2=0;
    INT t1_ntuples_per_page = 0;
    INT t2_ntuples_per_page = 0;
    Table table1;
    Table table2;
    // Table File
    FILE * data_file1 = NULL;
    FILE * data_file2 = NULL;

    Database* db = get_db();
    Conf* cf = get_conf();

    for(int i=0; i<db->ntables;i++){
        table1 = db->tables[i];
        if (strcmp(table1.name,table1_name) == 0){
            oid1 = table1.oid;
            break;
        }
    }
    for(int i=0; i<db->ntables;i++){
        table2 = db->tables[i];
        if (strcmp(table2.name,table2_name) == 0){
            oid2 = table2.oid;
            break;
        }
    }
   
   // first we allocate enough slot for return result, must free it to avoid memory leaking. 
    _Table *result = malloc(sizeof(_Table) * (table1.ntuples*table2.ntuples)*sizeof(Tuple));
    memset(result,0,sizeof(Table));
    result->nattrs = table1.nattrs+table2.nattrs; // join table
    result->ntuples = 0;

    char table1_path[256];
    char table2_path[256];
    memset(table1_path,0,256);
    memset(table2_path,0,256);

    if(strlen(table1_name)>250){
        fprintf(stderr,"ERROR: the table name: %s is too long\n", table1_name);
        exit(-1);
    }
    sprintf(table1_path,"%s/%u", db->path,oid1);
    fprintf(stderr,"DEBUG-one table name: %s\n",table1_path);
    // open file pointer for the table
    int ret1 = is_opend(rel->files,cf->file_limit,table1_path);
    if(ret1 == -1){
        // not opend, allocate a slot
        int sidx=get_fd(rel->files,cf->file_limit);
        if(sidx == -1){
            // no free slot log file close in evict_fd function
            sidx = evict_fd(rel->files,cf->file_limit);
        }
        rel->files[sidx]->file = fopen(table1_path,"r");
        log_open_file(oid1);
        strcpy(rel->files[sidx]->file_name,table1_path);
        rel->files[sidx]->oid = oid1;
        ret1 = sidx;
    }

    rel->files[ret1]->used = true;
    data_file1 = rel->files[ret1]->file;

    // calculate number of tuples per page
    t1_ntuples_per_page = (cf->page_size-sizeof(UINT64))/sizeof(INT)/table1.nattrs;
    fprintf(stderr,"DEBUG-one page contains %d tuples\n", t1_ntuples_per_page);
    UINT t1_npages =  (table1.ntuples+(t1_ntuples_per_page-1))/t1_ntuples_per_page;
    fprintf(stderr,"DEBUG-the table contains %d pages\n", t1_npages);

    if(strlen(table2_name)>250){
        fprintf(stderr,"ERROR: the table name: %s is too long\n", table2_name);
        exit(-1);
    }
    sprintf(table2_path,"%s/%u", db->path,oid2);
    fprintf(stderr,"DEBUG-one table name: %s\n",table2_path);
    // open file pointer for the table
    int ret2 = is_opend(rel->files,cf->file_limit,table2_path);
    if(ret2 == -1){
        // not opend, allocate a slot
        int sidx=get_fd(rel->files,cf->file_limit);
        if(sidx == -1){
            // no free slot log file close in evict_fd function
            sidx = evict_fd(rel->files,cf->file_limit);
        }
        rel->files[sidx]->file = fopen(table2_path,"r");
        log_open_file(oid2);
        strcpy(rel->files[sidx]->file_name,table2_path);
        rel->files[sidx]->oid = oid2;
        ret2 = sidx;
    }
    rel->files[ret2]->used = true;
    data_file2 = rel->files[ret2]->file;

    // calculate number of tuples per page
    t2_ntuples_per_page = (cf->page_size-sizeof(UINT64))/sizeof(INT)/table2.nattrs;
    fprintf(stderr,"DEBUG-one page contains %d tuples\n", t2_ntuples_per_page);
    UINT t2_npages =  (table2.ntuples+(t2_ntuples_per_page-1))/t2_ntuples_per_page;
    fprintf(stderr,"DEBUG-the table contains %d pages\n", t2_npages);



    // need to free
    // char* page1 = (char*)malloc(cf->page_size);
    // char* page2 = (char*)malloc(cf->page_size);

    fseek(data_file1,0,SEEK_SET);
    fseek(data_file2,0,SEEK_SET);

    UINT64 t1p,t2p;

    for(t1p=0; t1p < t1_npages; t1p++){
        int slot = pageInPool(rel->buffers, table1.name,t1p);
        if(slot < 0){
            // miss
            slot = request_page(rel->buffers,table1.name,t1p);
            if(rel->buffers->bufs[slot].data != NULL){
                // call release
                UINT64 r_pid = *((UINT64*)rel->buffers->bufs[slot].data);
                fprintf(stderr,"DEBUG- release relation %s page id %lu \n", rel->buffers->bufs[slot].id,r_pid);
                log_release_page(r_pid);
                freeSlot(rel->buffers,slot);
            }

            if(rel->buffers->bufs[slot].data == NULL){
                rel->buffers->bufs[slot].data = malloc(cf->page_size);
                // read a page from table file 
                fseek(data_file1,t1p*cf->page_size,SEEK_SET);
                size_t ret_code = fread(rel->buffers->bufs[slot].data,1,cf->page_size,data_file1);
                fprintf(stderr,"DEBUG- read relation %s page id: %lu \n", table1.name, t1p);
                log_read_page(t1p);
                if(ret_code < cf->page_size){
                // error handling
                    if(feof(data_file1)){
                        printf("Error read page: unexpected end of file\n");
                        break;
                    }
                    else if(ferror(data_file1)) {
                        perror("Error reading page");
                        exit(-1);
                    }
                }
             }
           } // else hit the page cache

        // size_t ret_code = fread(page1,1,cf->page_size,data_file1);
        // if(ret_code < cf->page_size){ // error handling
        //      if(feof(data_file1)){
        //         printf("Error read page: unexpected end of file\n");
        //         break;
        //     }
        //     else if(ferror(data_file1)) {
        //         fprintf(stderr,"bad file1 discraptor %p\n",data_file1);
        //         perror("Error reading page");
        //         exit(-1);
        //     }
        // }
        UINT64 t1pid = *(UINT64*)rel->buffers->bufs[slot].data; // page id UINT64 8Bytes
        char * page1 = rel->buffers->bufs[slot].data;
            // log_read_page(t1pid);
            // fprintf(stderr,"DEBUG- read page id: %lu \n", t1pid);
        for(int t1tuple_i=0; t1tuple_i<t1_ntuples_per_page&&(t1tuple_i+t1p*t1_ntuples_per_page)<table1.ntuples;
                                 t1tuple_i++){
            Tuple t1_tuple = (Tuple)(page1+sizeof(UINT64)+t1tuple_i*sizeof(INT)*table1.nattrs);

            for(t2p=0;t2p<t2_npages;t2p++){
                
                int slot = pageInPool(rel->buffers, table2.name,t2p);
                if(slot < 0){
                    // miss
                    slot = request_page(rel->buffers,table2.name,t2p);
                    if(rel->buffers->bufs[slot].data != NULL){
                        // call release
                        UINT64 r_pid = *((UINT64*)rel->buffers->bufs[slot].data);
                        fprintf(stderr,"DEBUG- release relation %s page id %lu \n", rel->buffers->bufs[slot].id,r_pid);
                        log_release_page(r_pid);
                        freeSlot(rel->buffers,slot);
                    }

                    if(rel->buffers->bufs[slot].data == NULL){
                        rel->buffers->bufs[slot].data = malloc(cf->page_size);
                        // read a page from table file 
                        fseek(data_file2,t2p*cf->page_size,SEEK_SET);
                        size_t ret_code = fread(rel->buffers->bufs[slot].data,1,cf->page_size,data_file2);
                        fprintf(stderr,"DEBUG- read relation %s page id: %lu \n", table2.name, t2p);
                        log_read_page(t2p);
                    
                        if(ret_code < cf->page_size){
                        // error handling
                            if(feof(data_file2)){
                                printf("Error read page: unexpected end of file\n");
                                break;
                            }   
                            else if(ferror(data_file2)) {
                                perror("Error reading page");
                                exit(-1);
                            }
                        }
                    }
                } // else hit the page cache

                char * page2 = rel->buffers->bufs[slot].data;

                for(int t2tuple_j=0;t2tuple_j<t2_ntuples_per_page&&(t2tuple_j+t2p*t2_ntuples_per_page)
                                <table2.ntuples;t2tuple_j++)
                {
                    Tuple t2_tuple = (Tuple)(page2+sizeof(UINT64)+t2tuple_j*sizeof(INT)*table2.nattrs); 
                    if(t1_tuple[idx1] == t2_tuple[idx2])
                    {
                        // add to result 
                        Tuple rtuple =(Tuple)malloc(result->nattrs * sizeof(INT));
                        memcpy(rtuple,t1_tuple,table1.nattrs*sizeof(INT));
                        memcpy(rtuple+table1.nattrs,t2_tuple,table2.nattrs*sizeof(INT));
                        result->tuples[result->ntuples] = rtuple;
                        result->ntuples +=1;
                    }
                }
            }
        }

    }
    // bree buffer
    // free(page1);
    // free(page2);
    // printf("before sort:\n");
    // print_table(result);
    bubble_sort_table(result->tuples,result->ntuples,idx1,result->nattrs);
    // printf("after sort:\n");
    // print_table(result);
    return result;
}