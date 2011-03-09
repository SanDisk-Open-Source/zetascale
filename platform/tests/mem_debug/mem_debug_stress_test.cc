/*
 * File:   mem_debug_stress_test.c
 * Author: Haowei Yao
 *
 * Created on April 20th, 2008, 3:40 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */

#include <vector>
#include <algorithm>

#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <setjmp.h>

#include "platform/assert.h"
#include "platform/stdlib.h"
#include "platform/mem_debug.h"
#include "platform/mman.h"
#include "platform/signal.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _mem_debug_stresstest
#include "platform/opts.h"

using namespace std;

#define PLAT_OPTS_ITEMS_mem_debug_stresstest()                                 \
    item("plat/test/mem_debug/total", "Total number of test cases",            \
         PLAT_MEM_DEBUG_STRESSTEST_TOTAL,                                      \
         parse_size(&config->total, optarg, NULL),                             \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("plat/test/mem_debug/replay", "Replay a test case by its ID",         \
         PLAT_MEM_DEBUG_STRESSTEST_REPLAY,                                     \
         parse_size(&config->replay, optarg, NULL),                            \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("plat/test/mem_debug/replaynum", "Number of replays, default 1",      \
         PLAT_MEM_DEBUG_STRESSTEST_REPLAYNUM,                                  \
         parse_size(&config->replaynum, optarg, NULL),                         \
         PLAT_OPTS_ARG_REQUIRED)

struct plat_opts_config_mem_debug_stresstest {
    int64_t total;
    int64_t replay;
    int64_t replaynum;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST_MEM_DEBUG, "mem_debug_stresstest");

// DEBUG Flag
// Set debug number to compile conresponding debug code.
#define DEBUG 0
#define RANDOM_FLOAT ((float)plat_random() / (float)RAND_MAX)

// Test type definition
#define TEST_TYPE_NUM 7

typedef enum{
    test_type_normal = 0,
    test_type_reference_invalid_region,
    test_type_too_much_release,
    test_type_too_few_release,
    test_type_write_flag,
    test_type_valid_mem_access,
    test_type_invalid_mem_access,
};

char *test_type_str[] = {
        "Valid case",
        "Reference invalid region",
        "Too much release",
        "Too few release",
        "Write flags do not match",
        "Valid memory access",
        "Invalid memory access",
};

//Test error definition
typedef enum{
    test_err_normal = 0,
    test_err_internal,
    test_err_add_unused,
    test_err_reference,
    test_err_release,
    test_err_check_free,
    test_err_check_ref,
    test_err_sigsegv,
};

char *test_error_str[] = {
        "No exception detected",
        "Internal error, test program failed",
        "Exception when add unused memory pool",
        "Exception when reference",
        "Exception when release",
        "Exception when checking free",
        "Exception when checking reference",
        "Segment fault detected",
};

/*
 * Test type and test error mapping function
 * For example, if the test type is invalid memory access,
 * we expect the test process will report an error: SIGSEGV error.
 */
int
expected_test_return(int test_type)
{
    switch(test_type){
    case test_type_normal: return test_err_normal;
    case test_type_reference_invalid_region: return test_err_reference;
    case test_type_too_much_release: return test_err_release;
    case test_type_too_few_release: return test_err_check_free;
    case test_type_write_flag: return test_err_release;
    case test_type_valid_mem_access: return test_err_normal;
    case test_type_invalid_mem_access: return test_err_sigsegv;
    default: return test_err_normal;
    }
}

// Signal SIGSEGV handle code
static struct sigaction action;
static void
sighandler(int signo)
{
    if(signo == SIGSEGV)
        _exit(test_err_sigsegv);
    else
        _exit(test_err_internal);
}

// Thread related global variables
int thread_counter = 0;
int master_wakeup = 0;
pthread_mutex_t counter_mutex;
pthread_mutex_t sleeper_mutex;
pthread_cond_t sleep_threshhold;
pthread_cond_t count_threshhold;

pthread_mutex_t reference_mutex;

/*
 * Reference type
 */
typedef struct{
    char *start;
    size_t len;
    int ref_count;
    // mutex did not consider by mem_pool
    pthread_mutex_t mutex;
}ref_t;

/*
 * Memory pool class
 * @Brief Actual memory allocation and reference region generating.
 */
class mem_pool{
private:
    // pointer of memory piece
    // acquired by calling new
    char *ptr;
    // start pointer of memory pool. Equals to round_up(ptr)
    char *start;
    // Actual memory size
    size_t mem_size;
    // number of pages in the memory pool
    int page_num;
    // number of reference regions
    int ref_num;
    // reference region table
    vector<ref_t> ref_table;
    // size of a single memory page
    size_t page_size;
    // if pool is added to memory debugger
    bool pool_added_flag;
    // mutex used by check_and_add()
    pthread_mutex_t pool_add_mutex;
    // pointer round up and round down
    char *round_up(char *ptr) {return (char *)(((long)ptr + page_size - 1) & ~(page_size - 1));}
    char *round_down(char *ptr) {return ((char *)(((long)ptr & ~(page_size - 1))));}

public:
    /*
     * @Brief initialization, actual memory allocation and reference region generating.
     * @param size the actual memory pool size in bytes
     * @param density the density of reference regions to be generate in the memory pool.
     * If density = 1, every page will be occupied by regions
     * If density = 0, no region will be generated.
     * @param max_size_factor max size factor of reference region
     * the size in bytes of reference regions will be at most 2 ^ max_size_factor.
     */
    mem_pool() {}
    mem_pool(size_t size, float density, int max_size_factor);
    // Destroy function
    ~mem_pool(){}
    // release ptr and clear ref_table
    void release();
    // print information of memory pool and the reference region table
    void print();
    // get a random reference region from the table
    ref_t get_random_ref();
    // get a reference region indexed by i
    ref_t get_ref(int i);
    // get the number of reference regions
    int get_ref_num() {if(ref_num == 0) _exit(test_err_internal); return ref_num;}
    // get the start pointer
    char *get_start() {return start;}
    // get the end pointer
    char *get_end() {return start + page_num * page_size;}
    // get the number of pages
    int get_page_num() {return page_num;}
    // check if the memory pool is added to the shared memory debugger,
    // if not, add it.
    // return 0 if success or it's already added.
    // return 1 on failure.
    bool check_and_add(struct plat_mem_debug *debug);
};

mem_pool::mem_pool(size_t size, float density, int max_size_factor)
{
    // initialization
    page_size = getpagesize();
    mem_size = size;
    ref_num = 0;
    ptr = new char[mem_size + page_size];
    start = round_up(ptr);
    page_num = mem_size / page_size;
    pool_added_flag = false;
    pthread_mutex_init(&pool_add_mutex, NULL);

    // reference regions generating
    int i;
    for(i = 0; i < page_num;){
        int len = 1 << (plat_random() % max_size_factor);
        len = plat_random() % len + 1;
        int page_used = len % page_size == 0 ? len / page_size : len / page_size + 1;
        if(RANDOM_FLOAT < density){
            ref_t tmp;
            tmp.ref_count = 0;
            tmp.len = len;
            tmp.start = start + (i + page_used) * page_size - len;
            if( (i + page_used) * page_size > mem_size)
                break;
            ref_table.push_back(tmp);
            ref_num ++;
        }
        i += page_used;
    }

    // if no region is generated, the whole memory pool will
    // be treated as a single region.
    if(ref_num == 0){
        ref_t tmp;
        tmp.ref_count = 0;
        tmp.start = start;
        tmp.len = page_size * page_num;
        ref_table.push_back(tmp);
        ref_num ++;
    }
}

void
mem_pool::release()
{
    if(ptr == NULL)
        _exit(test_err_internal);
    delete [] ptr;
    ref_table.clear();
    (void)pthread_mutex_destroy(&pool_add_mutex);
}

void
mem_pool::print()
{
    int i;
    for(i = 0; i < (int)(ref_table.size()); i ++){
        printf("REF %d\t<%u,\t%u%c>\n", i, ref_table[i].start,
                ref_table[i].start + ref_table[i].len,
                (long)(ref_table[i].start + ref_table[i].len) % page_size == 0 ? 'P' : 'F');
    }
    printf("\nREF COUNT:\t%d\tPAGE NUM\t%d\n", ref_num, page_num);
    printf("MEM\t<%u,\t%u>\n", ptr, ptr + mem_size);
    printf("POOL\t<%u%c,\t%u>\n", start, (long)(start) % page_size == 0 ? 'P' : 'F',
            start + page_num * page_size);
}

ref_t
mem_pool::get_random_ref()
{
    if(ref_num == 0){
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_PLATFORM_TEST_MEM_DEBUG,
                     PLAT_LOG_LEVEL_ERROR,
                     "Error @ mem_pool::get_random_ref(): ref_num = 0.");
        _exit(test_err_internal);
    }
    return ref_table[plat_random() % ref_num];
}

ref_t
mem_pool::get_ref(int i)
{
    if(i < 0 || i >= ref_num){
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_PLATFORM_TEST_MEM_DEBUG,
                     PLAT_LOG_LEVEL_ERROR,
                     "Error @ mem_pool::get_ref(): i = %d, ref_num = %d.\n", i, ref_num);
        _exit(test_err_internal);
    }
    return ref_table[i];
}

bool
mem_pool::check_and_add(struct plat_mem_debug *debug)
{
    if(pool_added_flag)
        return true;
    pthread_mutex_lock(&pool_add_mutex);
    if(pool_added_flag){
        pthread_mutex_unlock(&pool_add_mutex);
        return true;
    }

    int ret = plat_mem_debug_add_unused(debug, (void *)start, mem_size);

    if(ret == 0)
        pool_added_flag = !pool_added_flag;
    pthread_mutex_unlock(&pool_add_mutex);
    if(ret == 0)
        return true;
    else
        return false;
}

// context type of thread
typedef struct {
    struct plat_mem_debug *debug;
    int thread_num;
    mem_pool *pools;
    int pool_num;
    int test_type;
}context_t;

// job type in job sequence
enum job_type{
    job_empty = 0,
    job_ref,
    job_release,
    job_valid_access,
    job_invalid_access,
    job_status,
};

/*
 * job item type
 * @Brief the actual test procedure will use "job" to commit every operation,
 * such as reference, and release.
 */
typedef struct{
    int job_type;
    int pool_id;
    char *start;
    size_t len;
    int writeable;
    int access;  /* 0: no access 1: valid, 2: invalid*/
}job_t;

// get a random job from job sequence: jobs.
job_t
get_random_job(job_t *jobs, int job_num)
{
    return jobs[plat_random() % job_num];
}

// get a random reference job from job sequence: jobs.
job_t
get_reference_job(job_t *jobs, int job_num)
{
    job_t ret;
    do
        ret = get_random_job(jobs, job_num);
    while(ret.job_type != job_ref);
    ret.writeable = plat_random() % 2;
#if DEBUG == 10
    if(ret.start == NULL){
        printf("start == NULL !\n");
        _exit(test_err_internal);
    }
#endif
    return ret;
}

// change one job from job sequence to
void
get_valid_access_job(job_t *jobs, int job_num)
{
    int job_id;
    int job_type = job_empty;
    do{
        job_id = plat_random() % job_num;
        job_type = jobs[job_id].job_type;
    }while(job_type != job_ref);
    jobs[job_id].access = 1;
}

// get a random release job from job sequence: jobs.
job_t
get_release_job(job_t *jobs, int job_num)
{
    job_t ret;
    do
        ret = get_random_job(jobs, job_num);
    while(ret.job_type != job_release);
    ret.writeable = plat_random() % 2;
    return ret;
}

// check whether the input region <start, len> is invalid or not
// If invalid, return 1, else return 0;
bool
check_invalid_region(job_t *jobs, int job_num,
        mem_pool *pools, int pool_num,
        char *start, size_t len)
{
    int i;
    for(i = 0; i < pool_num + 1; i ++)
        if( (i == pool_num || start < pools[i].get_start())
                && ( i == 0 || start + len > pools[i-1].get_end()))
            return true;

    for(i = 0; i < job_num; i ++){
        job_t job = jobs[i];
        if(job.job_type != job_ref)
            continue;
        if(start > job.start && start < job.start + job.len)
            return true;
        if(start + len > job.start && start + len < job.start + job.len)
            return true;
        if(start == job.start && start + len != job.start + job.len)
            return true;
        if(start != job.start && start + len == job.start + job.len)
            return true;
    }
    return false;
}

// get a job with a invalid region.
job_t
get_invalid_region_job(job_t *jobs, int job_num, mem_pool *pools, int pool_num, int &release_job_id)
{
    job_t ret;
    do{
        release_job_id = plat_random() % job_num;
        ret = jobs[release_job_id];
    }while(ret.job_type != job_release);
    size_t page_size = getpagesize();
    mem_pool tmp_pool = pools[ret.pool_id];

    do{
        long int invalid_len = 1 << (plat_random() % 20);
        invalid_len = plat_random() % invalid_len + 1;
        if(plat_random() % 2 == 0 && ret.len != 1)
            invalid_len = - (plat_random() % (ret.len - 1) + 1);
        long int invalid_start = plat_random() % (page_size - 1) + 1;
        int invalid_type = plat_random() % 4;
        switch(invalid_type){
        case 0: ret.len += invalid_len;break;
        case 1: ret.start += (int)invalid_start;break;
        case 2: ret.start = tmp_pool.get_start() - invalid_start;break;
        case 3: ret.start = tmp_pool.get_end() + invalid_start;break;
        case 4: ret.len -= invalid_len; break;
        default: break;
        }
    }while(!check_invalid_region(jobs, job_num, pools, pool_num, ret.start, ret.len));
    ret.writeable = plat_random() % 2;
    ret.job_type = job_ref;
    return ret;
}

// change the writeable flag of a random job from job sequence.
void
get_invalid_write_flag_job(job_t *jobs, int job_num)
{
    int job_id;
    int job_type = job_empty;
    do{
        job_id = plat_random() % job_num;
        job_type = jobs[job_id].job_type;
    }while(job_type != job_ref && job_type != job_release);
    jobs[job_id].writeable = !jobs[job_id].writeable;
}

/*
 * Task of a single thread.
 * @Brief Job sequence generating and executing.
 *
 * First, we generate a legal job sequence, including reference, release,
 * get status, etc. Then if the test type specified, we do something with the
 * job sequence. For example, if test type is test_type_too_much_release,
 * we add some release job in the sequence, so that we can test how the
 * memory debugger will react on this invalid operation.
 *
 * Second, we do the jobs of the sequence, one by one. Every time we do the
 * job, we check the return value of corresponding interface of the memory
 * debugger. If the debugger reports a error, the test process exits with
 * a proper error number. For example, if plat_mem_debug_release returns 1,
 * the test process will exits with test_err_release.
 *
 * @param p pointer of the thread context.
 */
void
*single_task(void *p)
{
    // thread concurrent
    pthread_mutex_lock(&sleeper_mutex);
    while(master_wakeup){
        pthread_cond_wait(&sleep_threshhold, &sleeper_mutex);
     }
    pthread_mutex_unlock(&sleeper_mutex);

    // get thread context from the context pointer
    context_t context = *(context_t *)p;
    int i;

#if DEBUG == 6
    int n = context.thread_num;
    printf("thread %d runned. \n", n);
#endif

    // get test type
    int test_type = context.test_type;

    // get the reference region number of the memory pool
    int ref_num = 0;
    for(i = 0; i < context.pool_num; i ++)
        ref_num += context.pools[i].get_ref_num();

    // get a proper number of jobs. Not too much and not too few
    // for each test case.
    int job_num = ((ref_num * 4) >> (plat_random() % 4)) + 2;
    job_num = plat_random() % job_num;
    job_num += plat_random() % 500 + 4;

    // Job sequence generating. //////////////////////////////////////

    // Allocate an array to store job sequence.
    job_t *jobs = new job_t[job_num];

    // Get an empty slot array, and fill it iterately with 1, 2, ... job_num.
    // So that we can get an empty slot pointing to the emtpy job,
    // use the slot, and erase it from the slot arry.
    vector<int> empty_slot;
    for(i = 0; i < job_num; i ++){
        empty_slot.push_back(i);
        jobs[i].job_type = job_empty;
    }

    // Fill the job sequence
    for(i = 0; i < job_num / 2 - 1 && ref_num != 0; i ++){
        job_t a, b;
        int writeable = plat_random() % 2;
        int pool_id = plat_random() % context.pool_num;
        ref_t ref = context.pools[pool_id].get_random_ref();

        // First, we get two job which are identical except
        // the job_type: one is reference job, the other is release job.
        a.job_type = job_ref;
        a.pool_id = pool_id;
        a.start = ref.start;
        a.len = ref.len;
        a.writeable = writeable;
        a.access = 0;
        b = a;
        b.job_type = job_release;

        // Get two random slot from the empty slot array for the two new job.
        // The reference job is guaranteed to be placed before the release job.
        int a_pos = plat_random() % (empty_slot.size() - 1);
        int slot_a = empty_slot[a_pos];
        int b_pos = plat_random() % (empty_slot.size() - a_pos - 1) + a_pos + 1;
        int slot_b = empty_slot[b_pos];

        // Place the two new job.
        jobs[slot_a] = a;
        jobs[slot_b] = b;

        // Remove the two used slot.
        vector<int>::iterator pos;
        pos = find(empty_slot.begin(), empty_slot.end(), slot_a);
        if(pos != empty_slot.end())
            empty_slot.erase(pos);
        pos = find(empty_slot.begin(), empty_slot.end(), slot_b);
        if(pos != empty_slot.end())
            empty_slot.erase(pos);
    }
#if DEBUG == 8
    printf("Job Sequence Test.\n");
    /*
    for(i = 0; i < job_num; i ++){
        job_t tmp = jobs[i];
        printf("Job %d <%d, %u, %d, %d>\n", i, tmp.job_type, tmp.start, tmp.len, tmp.writeable);
    }
    */
    for(i = 0; i < job_num; i ++){
        job_t tmp = jobs[i];
        if(tmp.job_type == job_release){
            printf("Invalid Job Sequence: Release First at Job %d.\n", i);
            _exit(test_err_internal);
        }
        if(tmp.job_type == job_ref){
            int j;
            for(j = i + 1; j < job_num; j ++){
                job_t tmp2 = jobs[j];
                if(tmp2.job_type == job_release &&
                        tmp2.start == tmp.start &&
                        tmp2.len == tmp.len &&
                        tmp2.writeable == tmp.writeable){
                    jobs[j].job_type = job_empty;
                    break;
                }
            }
            if(j == job_num){
                printf("Invalid Job Sequence: Reference but no release at Job %d.\n", i);
                _exit(test_err_internal);
            }
        }
    }
    printf("Valid Job Sequence.\n");
    _exit(test_err_normal);
#endif

    // Now we insert some other jobs to the job sequence.
    for(i = 0; i < (int)empty_slot.size(); i ++){
        int slot = empty_slot[i];
        int release_job_id;
        job_t job;

        // Insert proper jobs to the job sequence according the test type.
        switch(test_type){
        // If the test type is test_type_reference_invalid_region,
        // we get a job with invalid reference region.
        case test_type_reference_invalid_region:
            job = get_invalid_region_job(jobs, job_num, context.pools, context.pool_num, release_job_id);
            break;

            // If the test type is test_type_too_much_release,
            // we get an additional release job.
        case test_type_too_much_release:
            job = get_release_job(jobs, job_num);
            break;

            // If the test type is test_type_too_few_release,
            // we get an additional reference job.
        case test_type_too_few_release:
            job = get_reference_job(jobs, job_num);
            break;

            // If none of the above, we get an empty job.
        default:
            job.job_type = job_empty;
            job.access = 0;
        }

        // Place the new job to the job sequence.
        jobs[slot] = job;
        if(test_type == test_type_reference_invalid_region && slot > release_job_id){
            job_t tmp = jobs[release_job_id];
            jobs[release_job_id] = jobs[slot];
            jobs[slot] = tmp;
        }
    }

    // Now we make a proper change on a job from the job sequence.

    // if test_type is test_type_write_flag,
    // we randomly get a reference or release job, and inverse the writeable flag.
    if(test_type == test_type_write_flag)
        get_invalid_write_flag_job(jobs, job_num);
    //
    else if(test_type == test_type_valid_mem_access)
        get_valid_access_job(jobs, job_num);
    empty_slot.clear();

    // Job sequence generating finished.

    // Job sequence executing //////////////////////////////////////////////////////

    // Executing jobs one bye one, and do corresponding operation.
    // Check the return value of each memory debugger interface is 0
    // If yes, terminate the test process with corresponding error number.
    for(i = 0; i < job_num && ref_num != 0; i ++){
        job_t tmp_job = jobs[i];
        int job_type = tmp_job.job_type;

        // return value
        int ret;

        // check if the memory pool of the job is added to the debugger
        // if not, add it.
        if(job_type == job_ref || job_type == job_release){
            if(!( context.pools[tmp_job.pool_id].check_and_add(context.debug) ))
                _exit(test_err_add_unused);
        }
        // If reference job, do reference and then check it.
        if(job_type == job_ref){
            ret = plat_mem_debug_reference(context.debug,
                    (void **)(tmp_job.start),
                    tmp_job.start,
                    tmp_job.len,
                    tmp_job.writeable, 0);
            if(ret != 0)
                _exit(test_err_reference);
            if(tmp_job.access == 1){
                memset(tmp_job.start, 0, tmp_job.len);
                char c = tmp_job.start[0];
                c = tmp_job.start[tmp_job.len-1];
            }
        }
        // if release job, do release.
        else if(job_type == job_release){
            ret = plat_mem_debug_release(context.debug,
                    (void **)(tmp_job.start),
                    tmp_job.start,
                    tmp_job.len,
                    tmp_job.writeable);
            if(ret != 0)
                _exit(test_err_release);
        }
    }
    // Job executing finished.

#if DEBUG == 9
    if(test_type == test_type_write_flag){
        int write_count = 0;
        int read_count = 0;
        for(i = 0; i < job_num; i ++){
            job_t job = jobs[i];
            if(job.job_type == job_release && job.writeable == 1)
                write_count --;
            if(job.job_type == job_ref && job.writeable == 1)
                write_count ++;
            if(job.job_type == job_release && job.writeable == 0)
                read_count --;
            if(job.job_type == job_ref && job.writeable == 0)
                read_count ++;
        }
        printf("\nwrite count %d, read count %d\n", write_count, read_count);
        _exit(test_err_internal);
    }
#endif

#if DEBUG == 6
    printf("thread %d finished. \n", n);
#endif

    delete [] jobs;

    pthread_mutex_lock(&counter_mutex);
    thread_counter--;
    pthread_cond_signal(&count_threshhold);
    pthread_mutex_unlock(&counter_mutex);

    return NULL;
}

/*
 * Thread Scheduler
 *
 * @Brief create several thread to run.
 * Each thread is given a distinct thread id.
 * And the scheduler will specify test type according to
 * the test type given by test_case() for each thread.
 * For some kinds of test, only one thread is doing actual test.
 *
 * @param context context of thread scheduler.
 */
int
scheduler(context_t context)
{
    pthread_t mainthread;
    pthread_attr_t attr;
    int i;
    context_t *th_context =
        (context_t *)plat_calloc(context.thread_num, sizeof(context));

    /*
     * ID of the thread that commit the actual test case.
     * If modified, only one thread indexed by commit_thread_id will
     * do the actual test, and other thread will do normal valid case.
     */
    int commit_thread_id = -1;
    if(context.test_type == test_type_write_flag ||
            context.test_type == test_type_too_much_release){
        commit_thread_id = plat_random() % context.thread_num;
    }

    // Thread Concurrent
    pthread_mutex_init(&reference_mutex, NULL);

    pthread_mutex_init(&counter_mutex, NULL);
    pthread_cond_init(&count_threshhold, NULL);
    pthread_mutex_init(&sleeper_mutex, NULL);
    pthread_cond_init(&sleep_threshhold, NULL);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    pthread_mutex_lock(&counter_mutex);
    thread_counter= 0;
    pthread_mutex_lock(&sleeper_mutex);
    master_wakeup= 1;
    pthread_mutex_unlock(&sleeper_mutex);

#if DEBUG == 1
    printf("Main started.\n");
#endif

    // Specify contexts for each thread,
    // and create threads with single_task() as entrance function
    for(i = 0; i < context.thread_num; i ++){
        // context specification
        th_context[i] = context;
        th_context[i].thread_num = i;
        // if test type is test_type_write_flag or test_type_too_much_release
        // only one thread will do the actual test procedure.
        // The other threads will just repeat normal procedure.
        if(  i != commit_thread_id &&
                ( context.test_type == test_type_write_flag ||
                        context.test_type == test_type_too_much_release ))
            th_context[i].test_type = test_type_normal;
        // create threads with single_task() as entrance function and
        // contexts specified respectively for them.
        if(pthread_create(&mainthread, &attr, single_task, (void *)&(th_context[i])) != 0){
            printf("Can not create thread %d!\n", i);
            _exit(test_err_internal);
        }
#if DEBUG == 1
        printf("Thread %d started.\n", i);
#endif
        thread_counter++;
    }
    pthread_mutex_unlock(&counter_mutex);
    pthread_attr_destroy(&attr);

    pthread_mutex_lock(&sleeper_mutex);
    master_wakeup= 0;
    pthread_mutex_unlock(&sleeper_mutex);
    pthread_cond_broadcast(&sleep_threshhold);

    pthread_mutex_lock(&counter_mutex);

    while(thread_counter){
        struct timespec abstime;
        memset(&abstime, 0, sizeof(struct timespec));
        abstime.tv_sec= 1;
        pthread_cond_timedwait(&count_threshhold, &counter_mutex, &abstime);
    }
    pthread_mutex_unlock(&counter_mutex);

    (void)pthread_mutex_destroy(&counter_mutex);
    (void)pthread_cond_destroy(&count_threshhold);
    (void)pthread_mutex_destroy(&sleeper_mutex);
    (void)pthread_cond_destroy(&sleep_threshhold);

    (void)pthread_mutex_destroy(&reference_mutex);

    return 0;
}

// Macro Definitions
#define MAX_THREAD_NUM 25
#define MAX_MEM_SIZE_FACTOR 14
#define MAX_REG_SIZE_FACTOR 14
#define MAX_MEM_POOL_NUM 5

/*
 * Run test
 * @Brief run random test with given random seed and test type.
 * A single dependent test case. Run in a child process.
 * When the process stop, it will exit with a proper test error number,
 * so the main process knows the exit status of each test case.
 * Generate test enviroment, start shared memory debugger,
 * and start thread scheduler to run test.
 *
 * @param test_type test type of this test case.
 */
int
test_case(int test_type)
{
    // Generate random test environment including
    // number of threads, memory pools.
    int page_size = getpagesize();
    int thread_num = plat_random() % MAX_THREAD_NUM + 1;

    int pool_num = 1;
    if(plat_random() % 8 == 0)
        pool_num = plat_random() % MAX_MEM_POOL_NUM + 1;
    mem_pool *pools = new mem_pool[pool_num];

    int i;
    for(i = 0; i < pool_num; i ++){
        int mem_size_factor = plat_random() % MAX_MEM_SIZE_FACTOR;
        size_t mem_size = page_size << mem_size_factor;
        size_t region_size_factor = plat_random() % (MAX_REG_SIZE_FACTOR - 7) + 8;
        float density = (float)(plat_random() % 21) / 20.0;
        mem_pool tmp_pool(mem_size, density, region_size_factor);
        pools[i] = tmp_pool;
    }

#if DEBUG == 5
    printf("Test Type\t\t\t%d\n"
            "Mem Size\t\t%d\n"
            "Thread Num\t\t%d\n"
            "Region Size Factor\t%d\n"
            "Density\t\t\t%f\n"
            "Region Num\t\t%d\n"
            "Page Num\t\t%d\n",
            test_type, mem_size, thread_num, region_size_factor, density,
            pool.get_ref_num(), pool.get_page_num());
#endif

#if DEBUG == 4
    pool.print();
#endif

    // start shared memory debugger
    struct plat_mem_debug_config config;
    struct plat_mem_debug *debug;
    config.backtrace_depth = 10;
    config.subobject = PLAT_MEM_SUBOBJECT_DENY;
    config.log_category = PLAT_LOG_CAT_PLATFORM_MEM_DEBUG;
    debug = plat_mem_debug_alloc(&config);

    // create thread context for the scheduler.
    context_t context;
    context.debug = debug;
    context.thread_num = thread_num;
    context.pool_num = pool_num;
    context.pools = pools;
    context.test_type = test_type;

    // call scheduler to run test
    scheduler(context);

    // check free status for each reference region from memory pool
    // if check failed, exit with error nubmer: test_err_check_free.
    int j;
    for(j = 0; j < pool_num; j ++){
        mem_pool tmp_pool = pools[j];
        for(i = 0; i < tmp_pool.get_ref_num(); i ++)
            if(plat_mem_debug_check_free(debug,
                    tmp_pool.get_ref(i).start,
                    tmp_pool.get_ref(i).len) == 0)
                _exit(test_err_check_free);
    }

    /* Test overrun */
    memset(&action, 0, sizeof(action));
    action.sa_handler = &sighandler;
    plat_sigaction(SIGSEGV, &action, NULL);


    if(test_type == test_type_invalid_mem_access){
        mem_pool tmp_pool = pools[plat_random() % pool_num];
        ref_t ref = tmp_pool.get_random_ref();
        if(plat_random() % 2 == 0)
            memset(ref.start, 0, ref.len);
        else{
            char c = ref.start[0];
            c = ref.start[ref.len - 1];
        }
    }
#if DEBUG == 7
    printf("free begin $$$$$$$$$$$\n");
#endif
    plat_mem_debug_free(debug);
#if DEBUG == 7
    printf("free end *************\n");
#endif

    for(i = 0; i < pool_num; i ++)
        pools[i].release();

    // if test_case() is finished, return with status: test_err_normal
    return test_err_normal;
}

// option values with initialization
//static int opt_log = 0;
static int opt_test_num = 0;
static int opt_replay = -1;
static int opt_num_of_replay = 0;

/*
 * Main Function
 * @Brief random stress test main function
 * @param argc command line argument count
 * @param argv command line argument strings
 */
int
main(int argc, char *argv[])
{
    int i, pid, status;

    int aborted_num = 0, failed_num = 0, passed_num = 0;
    int test_type;

    // container for IDs of failed and aborted test cases
    vector<int> failed_case_id;
    vector<int> aborted_case_id;

    struct plat_opts_config_mem_debug_stresstest config;
    memset(&config, 0, sizeof(struct plat_opts_config_mem_debug_stresstest));

    if (plat_opts_parse_mem_debug_stresstest(&config, argc, argv))
        return 1;

    // parse option and get proper option values.
    if (config.total > 0)
        opt_test_num = config.total;
    if (config.replay > 0)
        opt_replay = config.replay;
    if (config.replaynum > 0)
        opt_num_of_replay = config.replaynum;

    if(opt_test_num == 0)
        opt_test_num = TEST_TYPE_NUM * 200;
    else if(opt_test_num < TEST_TYPE_NUM)
        opt_test_num = TEST_TYPE_NUM;
    if(opt_num_of_replay == 0)
        opt_num_of_replay = 1;

    int case_num = opt_test_num;
    if(opt_replay != -1)
        case_num = opt_num_of_replay;
    if(opt_replay > opt_test_num){
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_PLATFORM_TEST_MEM_DEBUG,
                     PLAT_LOG_LEVEL_ERROR,
                     "Replayed case id should not be greater than total number of test cases.");
        return 1;
    }

    // start each test case
    int test_id;
    for(i = 0; i < case_num; i ++){
        if(opt_replay != -1)
            test_id = opt_replay;
        else
            test_id = i;
        // initialize random seed for each test case
        plat_srandom(test_id);
        int number_of_cases_per_type = opt_test_num / TEST_TYPE_NUM;
        if(opt_replay != -1)
            test_type = opt_replay / number_of_cases_per_type;
        else
            test_type = test_id / number_of_cases_per_type;
        if(test_type >= TEST_TYPE_NUM)
            test_type = TEST_TYPE_NUM - 1;

        // create child process to run test case
        // each process will run exactly one test with specified random seed and test type.
        pid = fork();

        if(pid == -1){
            // error handling
            plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_PLATFORM_TEST_MEM_DEBUG,
                         PLAT_LOG_LEVEL_ERROR,
                         "Create Tese Case %d failed.\n", i);
            return -1;
        }else if(pid == 0){
            // child process, run test
            test_case(test_type);
            return 0;
        }else{
            // main process
            printf("$%d\tTest %s\t", test_id, test_type_str[test_type]);
            // wait the process stop, and get the exit status
            waitpid(pid, &status, 0);
            if(!WIFEXITED(status)){
                // if the test terminates abnormally
                aborted_num ++;
                printf("[ABORTED]\n");
                aborted_case_id.push_back(i);
            }else{
                // get the error number of test case
                int errno = WEXITSTATUS(status);
                if(errno == expected_test_return(test_type)){
                    // if the child process exit status is expected,
                    // the test case is passed.
                    printf("[PASSED]\n");
                    passed_num ++;
                }else if(errno == test_err_internal){
                    // if the child process exit with error number test_err_internal,
                    // error occurs somewhere in the program and test failed.
                    printf("\nInternal Error Occured. Test program failed.\n");
                    return 1;
                }else{
                    // if the child process exit status is beyond our expectation,
                    // the test case is failed.
                    // Print the expected test type and exit status string.
                    printf("[FAILED]\n    Reason: ");
                    if(test_type != test_type_normal){
                        printf("Failed to detect: ");
                    }
                    printf("%s.%s\n\n", test_error_str[expected_test_return(test_type)],
                            test_error_str[errno]);
                    failed_num ++;
                    failed_case_id.push_back(i);
                }
            }
        }
    }

    // print the conclusion information
    printf("Total\t%d\nRunned\t%d\nAborted\t%d\nFailed\t%d\nPassed\t%d\n",
            opt_test_num, case_num, aborted_num, failed_num, passed_num);

    // if there is any test case aborted or failed, print them
    if(aborted_case_id.size() != 0){
        printf("Aborted Cases: ");
        for(i = 0; i < (int)aborted_case_id.size(); i ++)
            printf(" %d", aborted_case_id[i]);
        printf("\n");
    }
    if(failed_case_id.size() != 0){
        printf("Failed Cases: ");
        for(i = 0; i < (int)failed_case_id.size(); i ++)
            printf(" %d", failed_case_id[i]);
        printf("\n");
    }

    // if no test case is failed or aborted, return success on 0,
    // else, return failure on 1.
    if(failed_num == 0 && aborted_num == 0)
        return 0;
    else
        return (failed_num + aborted_num);
}

#include "platform/opts_c.h"
#undef DEBUG
