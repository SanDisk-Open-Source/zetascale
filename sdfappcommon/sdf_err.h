
#ifndef SDF_ERR_H
#define SDF_ERR_H



/* Define the error base at some convenient number outside
   of unix errors range. Currently arbitrarily setup at 1<<16.
   Salt to taste later.
*/
#define SDF_ERR_BASE 1<<16
#define ERR_MAX      (SDF_ERR_BASE + 1000)
/* This can be made more useful as need arises  */
#define ERR_DEF(err_name, err_num)    \
    int err_name = (err_num);

ERR_DEF(SDF_ERR_FAILED_AGENT_RENDEZVOUS, (SDF_ERR_BASE)+2)

#endif  /* SDF_ERR_H */
