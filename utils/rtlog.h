/*
 *  Light-Weight Logging
 */

#include <stdint.h>

#ifdef rt_enable_logging

extern void rtlog(const char *, uint64_t);
extern void rt_dump(void);
extern void rt_init(void);
extern void rt_set_size(int);

#else /* ! rt_enable_logging */

#define rtlog(...)
#define rt_dump(...)
#define rt_init(...)
#define rt_set_size(...)

#endif

#define rt_log(f, v)  rtlog(f, (uint64_t) v)
