/* author: Mac
 *
 * Created on Apr 30, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */
#define _FTH_H
#include "fthSpinLock.h"
#undef _FTH_H
#include "fthlllUndef.h"
#define LLL_NAME(suffix) sportQ ## suffix
#define LLL_EL_TYPE  struct sport
#define LLL_EL_FIELD sportQ
#include "fthlll.h"

#define LEN 30

typedef struct sport {
   int id;
   char name[LEN];
   sportQ_lll_el_t sportQ;
}sport_t;


#include "fthlll_c.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static int failed_count = 0;
#define ASSERT(expr, str) \
     if (expr) { \
         printf("*** PASSED, TEST:%s *** \n", str); \
     } \
     else { \
         printf("*** FAILED, TEST:%s *** \n", str); \
         failed_count ++; \
     }

static sportQ_lll_t s;

static char *table[] = { \
"Basketball", \
"Football", \
"Archery", \
"Swimming", \
"Skeet Shooting", \
"Wrestling", \
"Pool Playing", \
"Synchronized Swimming", \
"Volleyball", \
"Badminton", \
"Baseball", \
"Boxing", \
"Canoeing", \
"Cycling", \
"Tennis", \
"Gymnastics", \
"Hockey", \
"Karate", \
"Hang Gliding", \
"Parachuting", \
"Water Skiing", \
"Down Hill Skiing", \
"Cross Country Skiing", \
"Water Polo" \
}; 
int sportQ_size;
#define SPORTNUM 10

int get_sportQ_size(sport_t *p) {
    sportQ_size = 0;
    while(p) {
        sportQ_size ++;
        p = p->sportQ.next;

    }
    return sportQ_size;
}
void fth_lll_test() {
    int index;
    sport_t *temp, *last = NULL;

    //will be used very frequently
    struct sport basketball;
    basketball.id = 0;
    strcpy(basketball.name, table[0]);
    struct sport football;
    football.id = 1;
    strcpy(football.name, table[1]);
    struct sport archery;
    archery.id = 2;
    strcpy(archery.name, table[2]);
    struct sport swimming;
    swimming.id = 3;
    strcpy(swimming.name, table[3]);


    sportQ_lll_init(&s);
    ASSERT(s.head == NULL, "the head of lll is null. ")
    ASSERT(s.tail == NULL, "the tail of lll is null. ")
    ASSERT(s.spin == 0, "spin of lll is 0. ")    
    ASSERT(sportQ_is_empty(&s) == 1, "lll is empty. ")

    printf("!<Note>: insert one element into empty lll. \n");
    sportQ_insert_nospin(&s, NULL, &basketball);
    ASSERT(sportQ_is_empty(&s) == 0, "insert one element. ")
    ASSERT(get_sportQ_size(s.head) == 1, "lll only have one element.")
    printf("!<Note>: insert one element into lll which already have one element. \n");
    sportQ_insert_nospin(&s, s.head, &football);
    ASSERT(get_sportQ_size(s.head) == 2, "lll only have two element.")
    sportQ_lll_init(&s);
    ASSERT(sportQ_is_empty(&s) == 1, "lll was empty. ")
    printf("!<Note>: insert %d elements into lll. \n", SPORTNUM);
    for(index = 0; index < SPORTNUM; index ++) {
        temp = (sport_t *)malloc(sizeof(sport_t));
        temp->id = index;
        strcpy(temp->name, table[index]);
        sportQ_insert_nospin(&s, last, temp);
        last = temp;
    }
    ASSERT(get_sportQ_size(s.head) == SPORTNUM, "lll have SPORTNUM element.")

    sportQ_lll_init(&s);
    ASSERT(sportQ_is_empty(&s) == 1, "lll was empty. ")
    printf("!<Note>: push one element into empty lll. \n");
    sportQ_push(&s, &basketball);
    ASSERT(get_sportQ_size(s.head) == 1, "lll only have one element.")
    printf("!<Note>:push is put a element into lll from the tail of it. \n");
    sportQ_push(&s, &football);
    ASSERT(get_sportQ_size(s.head) == 2, "lll have two element.")
    ASSERT(s.tail->id == 1 && !strcmp(s.tail->name, "Football"), "push element from lll tail. ")
   
    sportQ_lll_init(&s);
    ASSERT(sportQ_is_empty(&s) == 1, "lll was empty. ")

    printf("!<Note>: push %d elements into lll. \n", SPORTNUM);
    for(index = 0; index < SPORTNUM; index ++) {
        temp = (sport_t *)malloc(sizeof(sport_t));
        temp->id = index;
        strcpy(temp->name, table[index]);
        sportQ_push(&s, temp);
    }
    ASSERT(get_sportQ_size(s.head) == SPORTNUM, "lll have SPORTNUM element.")

    sportQ_lll_init(&s);
    ASSERT(sportQ_is_empty(&s) == 1, "lll was empty. ")
    printf("!<Note>: unshift one element into empty lll. \n");
    sportQ_unshift(&s, &basketball);
    ASSERT(get_sportQ_size(s.head) == 1, "lll only have one element.")
    printf("!<Note>:unshift is put a element into lll from the head of it. \n");
    sportQ_unshift(&s, &football);
    ASSERT(get_sportQ_size(s.head) == 2, "lll have two element.")
    ASSERT(s.head->id == 1 && !strcmp(s.head->name, "Football"), "unshift element from lll head. ")

    sportQ_lll_init(&s);
    ASSERT(sportQ_is_empty(&s) == 1, "lll was empty. ")

    printf("!<Note>: unshift %d elements into lll. \n", SPORTNUM);
    for(index = 0; index < SPORTNUM; index ++) {
        temp = (sport_t *)malloc(sizeof(sport_t));
        temp->id = index;
        strcpy(temp->name, table[index]);
        sportQ_unshift(&s, temp);
    }
    ASSERT(get_sportQ_size(s.head) == SPORTNUM, "lll have SPORTNUM element.")
   
    /**************************************************************************************/
    sportQ_lll_init(&s);
    ASSERT(sportQ_is_empty(&s) == 1, "lll was empty. ")
    printf("!<Note>: pop one element from empty lll. \n");
    temp = sportQ_pop(&s);
    ASSERT(temp == NULL, "the pop element is null.")
   
    printf("!<Note>: pop is get a element from the tail of lll. \n");
    sportQ_push(&s, &basketball);
    sportQ_push(&s, &football);
    ASSERT(s.tail->id == 1, "the tail of lll is football. ")
    temp = sportQ_pop(&s);
    ASSERT(temp->id == 1, "the pop element is tail. ") 
 
    sportQ_lll_init(&s); 
    printf("!<Note>: push %d element and pop them. \n", SPORTNUM);
    printf("Start pushing ....\n");
    for(index = 0; index < SPORTNUM; index ++) {
        temp = (sport_t *)malloc(sizeof(sport_t));
        temp->id = index;
        strcpy(temp->name, table[index]);
        printf("the %d times push element name is %s \n", index, temp->name);
        sportQ_push(&s, temp);
    }
    printf("Finish pushing ....\n"); 

    printf("Start poping ....\n");
    for(index = 0; index < SPORTNUM; index ++) {     
        temp = sportQ_pop(&s);
        printf("the %d times pop element name is %s \n", index, temp->name);
        
    }
    printf("Finish poping ....\n");

    printf("!<Note>: unshift %d element and pop them. \n", SPORTNUM);
    printf("Start unshifting ....\n");
    for(index = 0; index < SPORTNUM; index ++) {
        temp = (sport_t *)malloc(sizeof(sport_t));
        temp->id = index;
        strcpy(temp->name, table[index]);
        printf("the %d times unshift element name is %s \n", index, temp->name);
        sportQ_unshift(&s, temp);
    }
    printf("Finish unshifting ....\n");

    printf("Start poping ....\n");
    for(index = 0; index < SPORTNUM; index ++) {     
        temp = sportQ_pop(&s);
        printf("the %d times pop element name is %s \n", index, temp->name);

    }
    printf("Finish poping ....\n");


    sportQ_lll_init(&s);
    ASSERT(sportQ_is_empty(&s) == 1, "lll was empty. ")
    printf("!<Note>: shift one element from empty lll. \n");
    temp = sportQ_shift(&s);
    ASSERT(temp == NULL, "the shift element is null.")

    printf("!<Note>: shift is get a element from the head of lll. \n");
    sportQ_push(&s, &basketball);
    sportQ_push(&s, &football);
    ASSERT(s.head->id == 0, "the head of lll is basketball. ")
    ASSERT(s.tail->id == 1, "the tail of lll is football. ")
    temp = sportQ_shift(&s);
    ASSERT(temp->id == 0, "the shift element is head. ")
    
    sportQ_lll_init(&s);
    printf("!<Note>: push %d element and shift them. \n", SPORTNUM);
    printf("Start pushing ....\n");
    for(index = 0; index < SPORTNUM; index ++) {
        temp = (sport_t *)malloc(sizeof(sport_t));
        temp->id = index;
        strcpy(temp->name, table[index]);
        printf("the %d times push element name is %s \n", index, temp->name);
        sportQ_push(&s, temp);
    }
    printf("Finish pushing ....\n");
    printf("Start shifting ....\n");
    for(index = 0; index < SPORTNUM; index ++) {
        temp = sportQ_shift(&s);
        printf("the %d times shift element name is %s \n", index, temp->name);

    }
    printf("Finish shifting ....\n");

    printf("!<Note>: unshift %d element and shift them. \n", SPORTNUM);
    printf("Start unshifting ....\n");
    for(index = 0; index < SPORTNUM; index ++) {
        temp = (sport_t *)malloc(sizeof(sport_t));
        temp->id = index;
        strcpy(temp->name, table[index]);
        printf("the %d times unshift element name is %s \n", index, temp->name);
        sportQ_unshift(&s, temp);
    }
    printf("Finish unshifting ....\n");
    printf("Start shifting ....\n");
    for(index = 0; index < SPORTNUM; index ++) {
        temp = sportQ_shift(&s);
        printf("the %d times shift element name is %s \n", index, temp->name);

    }
    printf("Finish shifting ....\n");

    sportQ_lll_init(&s);
    ASSERT(sportQ_is_empty(&s) == 1, "lll was empty. ")
    printf("push four elements into lll. \n");
    sportQ_push(&s, &basketball);
    sportQ_push(&s, &football);
    sportQ_push(&s, &archery);
    sportQ_push(&s, &swimming);

    ASSERT(s.head->id == 0, "the head of lll is basketball. ")
    ASSERT(s.head->sportQ.next->id == 1 ,"the second of lll is football. ")
    ASSERT(s.tail->sportQ.prev->id == 2, "the third of lll is archery. " )
    ASSERT(s.tail->id == 3, "the tail of lll is swimming. ")
    ASSERT(get_sportQ_size(s.head) == 4, "the size of lll is 4. ")


    printf("!<Note>: remove element(not head and tail) of lll. \n");
    sportQ_remove(&s, s.head->sportQ.next);
    ASSERT(s.head->id == 0, "the head of lll is basketball. ")
    ASSERT(s.head->sportQ.next->id == 2 ,"the second of lll is archery. ")
    ASSERT(s.tail->id == 3, "the tail of lll is swimming. ")
    ASSERT(get_sportQ_size(s.head) == 3, "the size of lll is 3. ")


    printf("!<Note>: remove the head of lll. \n");
    sportQ_remove(&s, s.head);
    ASSERT(s.head->id == 2, "the head of lll is archery. ")
    ASSERT(s.tail->id == 3, "the tail of lll is swimming. ")
    ASSERT(get_sportQ_size(s.head) == 2, "the size of lll is 2. ")

    printf("!<Note>: remove the tail of lll. \n");
    sportQ_remove(&s, s.tail);
    ASSERT(s.head->id == 2, "the tail of lll is archery. ")
    ASSERT(s.tail->id == 2, "the tail of lll is archery. ")
    ASSERT(get_sportQ_size(s.head) == 1, "the size of lll is 1. ")


}
int main(void) {
    printf("*** === TEST THE BASIC STRUCTURE IN FTH === ***\n");
    fth_lll_test();
    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

                   
}

