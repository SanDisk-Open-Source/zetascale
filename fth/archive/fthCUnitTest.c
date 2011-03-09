/* author: mac
 *
 * Created on Apr 21, 2008, 10:21 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 */

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "CUnit/Basic.h"
#define _FTH_H
#include "fthSpinLock.h"
#undef _FTH_H
#include "fthlllUndef.h"
#define LLL_NAME(suffix) schoonerQ ## suffix
#define LLL_EL_TYPE  struct schooner
#define LLL_EL_FIELD schoonerQ
#include "fthlll.h"

#define LEN 30
typedef struct schooner {
   int id;
   char name[LEN];
   schoonerQ_lll_el_t schoonerQ;
}schooner_t;


#include "fthlll_c.h"


#define main MAIN
#include "fthTest.c"
#undef main


// =====================================================================================================================
void
fthSum(uint64_t arg) {
   int i, sum = 0;
   for(i = 1; i <= arg; i++) {
      sum += i;
   }
}

void
testlll_c()
{
   schoonerQ_lll_t sch;
   schooner_t *sch_temp;
   schoonerQ_lll_init(&sch);
   CU_ASSERT(schoonerQ_is_empty(&sch) == 1);

   /*initial three schooner_t variable*/
   schooner_t sch_china, sch_english, sch_german;
   sch_china.id = 1;
   strcpy(sch_china.name, "China");

   sch_english.id = 2;
   strcpy(sch_english.name, "English");
   
   sch_german.id = 3;
   strcpy(sch_german.name, "German");
   /*
    * Test Result:
    * Push: put the element into list from the head of it.
    * Pop : get the element from the head of the list.
    */
   schoonerQ_push(&sch, &sch_china);
   schoonerQ_push(&sch, &sch_english);
   CU_ASSERT(schoonerQ_is_empty(&sch) == 0);	
   sch_temp = schoonerQ_pop(&sch);
   CU_ASSERT(sch_temp->id == 2);
   sch_temp = schoonerQ_pop(&sch);
   CU_ASSERT(sch_temp->id == 1);
   CU_ASSERT(schoonerQ_is_empty(&sch) == 1);
   /*
    * Test Result:
    * Unshift: put the element into list from the tail of it.
    * Pop:     get the element from the head of the list 
    */
   schoonerQ_unshift(&sch, &sch_china);
   schoonerQ_unshift(&sch, &sch_english);
   CU_ASSERT(schoonerQ_is_empty(&sch) == 0);
   sch_temp = schoonerQ_pop(&sch);
   CU_ASSERT(sch_temp->id == 1);
   sch_temp = schoonerQ_pop(&sch);
   CU_ASSERT(sch_temp->id == 2);
   CU_ASSERT(schoonerQ_is_empty(&sch) == 1);
   
   /*
    * Test Result:
    * Shift: get the element from the tail of the list.
    */
   schoonerQ_push(&sch, &sch_china);
   schoonerQ_push(&sch, &sch_english);
   schoonerQ_push(&sch, &sch_german);
   CU_ASSERT(schoonerQ_is_empty(&sch) == 0);
   sch_temp = schoonerQ_shift(&sch);
   CU_ASSERT(sch_temp->id == 1);
   sch_temp = schoonerQ_shift(&sch);
   CU_ASSERT(sch_temp->id == 2);

   /*
    * Test Result:
    * Remove: if the element is in the list,delete it.
    */
   CU_ASSERT(schoonerQ_is_empty(&sch) == 0);
   schoonerQ_remove(&sch, &sch_china);
   CU_ASSERT(schoonerQ_is_empty(&sch) == 0);
   schoonerQ_remove(&sch, &sch_german);
   CU_ASSERT(schoonerQ_is_empty(&sch) == 1);
}

void
testfth() {

   /*
    * Test Result:
    * fthInit: init the scheduler
    */

   fthInit();

   /*
    * Test Result:
    * fthSpawn: create a new fth~
    */
   fthThread_t *thread = XSpawn(&fthSum, 4096);
   XResume(thread, 10);
   CU_ASSERT(thread != NULL);

   /*
    * Test Result:
    * fthBase: get the global structure fth_t
    */

   fth_t *b = fthBase();
   CU_ASSERT(b != NULL);
   /*
    * Test Result:
    * fthSelf: get the current fth,because scheduler has not start ,so current fth is null 
    */
   fthThread_t *cur = fthSelf();
   CU_ASSERT(cur == NULL);
   /*
    * Test Result:
    * fthGetWailEl: this function will always return a non-null fthWaitEl_t.
    */

   CU_ASSERT(fthGetWaitEl() != NULL);

   fthMboxInit(&mbox);
   fthMboxPost(&mbox,345);
   CU_ASSERT(fthMboxWait(&mbox) == 345);
   CU_ASSERT(fthMboxTry(&mbox) == 0);
   

   fthLockInit(&lock);
   CU_ASSERT(lock.readLock == 0);
   CU_ASSERT(lock.writeLock == 0);
   CU_ASSERT(lock.spin == 0);
   CU_ASSERT(lock.waitQ.head == LLL_NULL);
   CU_ASSERT(lock.holdQ.tail == NULL);


   CU_ASSERT(fthLock(&lock, 0) != NULL); 

     
   lock.readLock = 1;
   CU_ASSERT(fthTryLock(&lock, 1, NULL) == NULL);  
   
   /* cover all possibles  in fthTryLock function*/
   lock.readLock = 0;
   lock.writeLock = 1;
   CU_ASSERT(fthTryLock(&lock, 1, NULL) == NULL);
   CU_ASSERT(lock.writeLock == 1);
   CU_ASSERT(fthTryLock(&lock, 0, NULL) == NULL);
   

   lock.readLock = 1;
   lock.writeLock = 1;
   CU_ASSERT(fthTryLock(&lock, 1, NULL) == NULL);
   CU_ASSERT(lock.writeLock == 1);
   CU_ASSERT(fthTryLock(&lock, 0, NULL) == NULL);

 
   lock.readLock = 0;
   lock.writeLock = 0;
   CU_ASSERT(fthTryLock(&lock, 0, NULL) != NULL); 
   CU_ASSERT(lock.readLock == 1);
   
} 

#define SHM_SIZE 8 * 1024 *1024

int 
init_suite1(void)
{
    struct plat_shmem_config *shmem_config = plat_alloc(sizeof(struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    shmem_config->size = SHM_SIZE;
    plat_shmem_prototype_init(shmem_config);
    int tmp = plat_shmem_attach(shmem_config->mmap);
    if (tmp) {
        plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                     PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                     shmem_config->mmap, plat_strerror(-tmp));
        plat_abort();
    }

   return (0);
}

int 
clean_suite1(void)
{
   return (0);
}


int
main(int argc, char **argv) {

 CU_pSuite pSuite = NULL;

   /* initialize the CUnit test registry */
   if (CUE_SUCCESS != CU_initialize_registry())
      return CU_get_error();

   /* add a suite to the registry */
   pSuite = CU_add_suite("Suite_1", init_suite1, clean_suite1);
   if (NULL == pSuite) {
      CU_cleanup_registry();
      return CU_get_error();
   }

   /* add the tests to the suite */
   /* NOTE - ORDER IS IMPORTANT */
   if (NULL == CU_add_test(pSuite, "test some functions in lll_c.h", testlll_c) || NULL == CU_add_test(pSuite,"test other functions in sdf/fth", testfth)) {
      CU_cleanup_registry();
      return CU_get_error();
   }

   /* Run all tests using the CUnit Basic interface */
   CU_basic_set_mode(CU_BRM_VERBOSE);
   CU_basic_run_tests();
   CU_cleanup_registry();
   return CU_get_error();
}

