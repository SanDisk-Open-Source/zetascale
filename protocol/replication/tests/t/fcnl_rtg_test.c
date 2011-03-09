#include "protocol/init_protocol.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"
#include "protocol/home/home_flash.h"

#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/replicator_adapter.h"

#include "platform/string.h"
#include "../test_framework.h"
#include "../test_model.h"
#include "../test_common.h"
#include "../test_generator.h"

#define PLAT_OPTS_NAME(name) name ## _rtg_test
#include "platform/opts.h"
#include "misc/misc.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/case");

#define PLAT_OPTS_ITEMS_rtg_test()                                                  \
    item("mode", "running mode of generator: async or sync", MODE,                \
         parse_string_alloc(&config->str_test_mode, optarg, PATH_MAX),              \
         PLAT_OPTS_ARG_REQUIRED)                                                    \
    item("seed", "the random seed", PRNG_SEED,                                            \
         parse_int(&config->prng, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)            \
    item("iterations", "how many operations", ITERATIONS,                                  \
         parse_int(&config->iterations, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)     \
    item("parallel", "max parallel to run at once", PARALLEL,                            \
         parse_int(&config->max_parallel, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)    \
    item("workset", "work set size", WORKSET,                                           \
         parse_int(&config->work_set_size, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)

struct plat_opts_config_rtg_test {
    char *str_test_mode;
    rtg_test_mode test_mode;
    int prng;
    int iterations;
    int max_parallel;
    int work_set_size;
};

int
main(int argc, char *argv[])
{
    SDF_status_t status;

    struct replication_test_generator_config *rtg_config;
    plat_calloc_struct(&rtg_config);
    plat_assert(rtg_config != NULL);

    struct plat_opts_config_rtg_test opts_config;
    memset(&opts_config, 0, sizeof(opts_config));
    int opts_status = plat_opts_parse_rtg_test(&opts_config, argc, argv);
    if (opts_status) {
        plat_opts_usage_rtg_test();
        return (1);
    }

    if (!opts_config.prng) { opts_config.prng = 0; }
    if (!opts_config.str_test_mode) { opts_config.str_test_mode = "async"; }
    if (!opts_config.iterations) { opts_config.iterations = 1000; }
    if (!opts_config.max_parallel) { opts_config.max_parallel = 30; }
    if (!opts_config.work_set_size) { opts_config.work_set_size = 100; }

    if (strcmp(opts_config.str_test_mode, "async") == 0) {
        opts_config.test_mode = RTG_TM_ASYNC;
    } else if (strcmp(opts_config.str_test_mode, "sync") == 0) {
        opts_config.test_mode = RTG_TM_SYNC;
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR, "test mode should be \"async\" or \"sync\"");
        return (1);
    }

    plat_log_parse_arg("sdf/prot=info");
    struct plat_opts_config_replication_test_framework_sm *sm_config;
    plat_calloc_struct(&sm_config);
    plat_assert(sm_config != NULL);
    rtg_config_init(rtg_config, opts_config.test_mode, opts_config.prng,
                    opts_config.max_parallel, opts_config.iterations, opts_config.work_set_size);

    /* start shared memory */
    status = framework_sm_init(0, NULL, sm_config);

    /* start fthread library */
    fthInit();

    struct replication_test_generator *rtg =
        replication_test_generator_alloc(rtg_config, 0, NULL);

    /* Assure test_framework is started?! */
    XResume(fthSpawn(&rtg_run, 40960), (uint64_t)rtg);
    fthSchedulerPthread(0);
    replication_test_generator_destroy(rtg);
    plat_free(rtg);
    plat_free(rtg_config);
    framework_sm_destroy(sm_config);
    return (0);
}

#include "platform/opts_c.h"
