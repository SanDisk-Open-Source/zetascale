/*
 * ZS License Daemon
 *
 * Author: Niranjan Neelakanta
 *
 * Copyright (c) 2013 SanDisk Corporation.  All rights reserved.
 */

#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include "sdf.h"
#include "sdf_internal.h"
#include "zs.h"
#include "utils/properties.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"
#include "protocol/action/async_puts.h"
#include "protocol/home/home_flash.h"
#include "protocol/action/async_puts.h"
#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/replicator_adapter.h"
#include "ssd/fifo/mcd_ipf.h"
#include "ssd/fifo/mcd_osd.h"
#include "ssd/fifo/mcd_bak.h"
#include "shared/init_sdf.h"
#include "shared/private.h"
#include "shared/open_container_mgr.h"
#include "shared/container_meta.h"
#include "shared/name_service.h"
#include "shared/shard_compute.h"
#include "shared/internal_blk_obj_api.h"
#include "agent/agent_common.h"
#include "agent/agent_helper.h"
#include "fdf_internal.h"

#include "license/interface.h"

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_NAMING
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_TRACE PLAT_LOG_LEVEL_TRACE
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL

#define	MINUTE	(60)
#define HOUR	(3600)
#define DAY	(86400)

#define ZS_INVAL_GPRD		(0 * DAY)
#define ZS_EXP_GPRD		(30 * DAY)

/*
 * Period for checking the license, once in an hour.
 */
double 		zs_chk_prd = HOUR;
int		zs_chk_prd_option;

char 		*zs_version;
char		*license_path;	/* License file path */
char		*ld_prod;	/* Product Name */
double		ld_frm_diff;	/* Current time - Start of license */
double		ld_to_diff;	/* End of license - Current time */
double		ld_vtime;	/* Time stamp at which we found valid license */
double		ld_cktime;	/* Time stamp at which we make last check */
enum lic_state	ld_state;	/* Current license state */
char		ld_type;	/* Type of license found */
bool		ld_valid = false;	/* Is license valid or not? */
bool		licd_init = false;	/* Is license state initialized */
bool		licd_running = false;	/* Is license daemon running? */
bool		licstate_updating = false;	
/*
 * This CV serves the purpose of blocking wait for a
 * thread awaiting completion of license checking operation.
 */
pthread_cond_t	licd_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t	licd_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t	licstate_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t	licstate_mutex = PTHREAD_MUTEX_INITIALIZER;;

static struct ZS_state *licd_zs_state = NULL;

static void licd_handler_thread(uint64_t);
void update_lic_info(lic_data_t *, bool);
void check_time_left(double, double, bool);
void check_validity_left(lic_data_t *, lic_inst_type, lic_type, bool);
void adjust_chk_prd(double);
void free_details(lic_data_t *);


/*
 * licd_start - Start the license daemon.
 * 
 * This routine just spans a pthread which does regular check  of license.
 * The license is read from the path passed in as argument (lic_path).
 */
bool
licd_start(const char *lic_path, int period, struct ZS_state *zs_state)
{
	plat_assert(lic_path);
	plat_assert(zs_state);

#ifdef ZS_REVISION
	zs_version = malloc(strlen(ZS_REVISION) + 1);
	if (zs_version) {
		bzero(zs_version, strlen(ZS_REVISION) + 1);
		strncpy(zs_version, ZS_REVISION, strlen(ZS_REVISION));
	} else {
		plat_log_msg(160071, LOG_CAT, LOG_WARN,
				"Memory allocation failed");
		goto out;
	}
#endif
	//Checking period cannot be beyond an hour. Atleast once an hour needed
	if ((period > 0) && (period < HOUR)) {
		zs_chk_prd_option = period;
		zs_chk_prd = zs_chk_prd_option;
	} else {
		zs_chk_prd_option = 0;
	}

	if (!lic_path) {
		plat_log_msg(160148, LOG_CAT,
				LOG_ERR, "License path not specified");
		goto out;
	}

	licd_zs_state = zs_state;

	if( zs_state == NULL ) {
		plat_log_msg(160072,LOG_CAT, LOG_ERR, "Invalid ZS state");
		goto out;
	}
	/* Start the thread */
	fthResume( fthSpawn( &licd_handler_thread, MCD_FTH_STACKSIZE ),
				(uint64_t)lic_path);
	return true;
out:
	plat_log_msg(160149, LOG_CAT, LOG_WARN,
				"Starting Licensing daemon failed.");
	pthread_mutex_lock(&licd_mutex);
	licd_init = true;
	licd_running = false;
	pthread_cond_broadcast(&licd_cv);
	pthread_mutex_unlock(&licd_mutex);
	return true;
}

/*
 * This is the main license handler thread. Following is the control flow:
 *
 * 1. Get the license details.
 * 2. Update the in house license information.
 * 3. Wake up any thread waiting for license to get initialized.
 * 4. Sleep for the period (zs_chk_prd).
 * 5. Goto (1).
 */
static void
licd_handler_thread(uint64_t arg)
{
	struct timespec abstime;
	lic_data_t	data;

	license_path = (char *)arg;

	plat_log_msg(80065, LOG_INFO, LOG_DBG,
			"LIC: Starting Licensing Daemon (license path: %s)...",
			license_path);
	memset(&abstime, 0, sizeof(struct timespec));
	
	clock_gettime(CLOCK_REALTIME, &abstime);
	ld_vtime = abstime.tv_sec;
	licd_running = true;
	while(1) {
		/*
		 * Get license details and update in-house info.
		 * This will get info as array of pointers.
		 */
		pthread_mutex_lock(&licstate_mutex);

		/*
		 * Some other thread is updating the status. Let us wait.
		 */
		if (licstate_updating == true) {
			plat_log_msg(80066, LOG_CAT, LOG_DBG,
				"LIC: Daemon waiting.");
			pthread_cond_wait(&licstate_cv, &licstate_mutex);
		}
		plat_log_msg(80067, LOG_CAT, LOG_DBG,
				"LIC: Daemon updating license info.");

		licstate_updating = true;
		pthread_mutex_unlock(&licstate_mutex); 

		/*
		 * Even though recently some other thread had updated
		 * license information, we shall do the check and update
		 * the time period daemon has to wait.
		 */
		bzero(&data, sizeof(lic_data_t));
		get_license_details(license_path, &data);
		update_lic_info(&data, true);

		pthread_mutex_lock(&licstate_mutex);
		licstate_updating = false;
		pthread_cond_broadcast(&licstate_cv);
		pthread_mutex_unlock(&licstate_mutex); 

		if (licd_init == false) {
			// If running for first time, wake up waiting threads.
			if (ld_state != LS_INTERNAL_ERR) { 
				plat_log_msg(80068, LOG_CAT, LOG_DBG,
						"LIC: License daemon initialized.");
				pthread_mutex_lock(&licd_mutex);
				licd_init = true;
				pthread_cond_broadcast(&licd_cv);
				pthread_mutex_unlock(&licd_mutex);
			} else {
				plat_log_msg(160255, LOG_CAT, LOG_WARN,
						"LIC: License daemon initialization failed, restarting.");
			}

		}

		free_details(&data);
		// Sleep for zs_chk_prd time
		pthread_mutex_lock(&licd_mutex);
		clock_gettime(CLOCK_REALTIME, &abstime);
		abstime.tv_sec += zs_chk_prd;
		plat_log_msg(80069, LOG_CAT, LOG_DBG,
				"LIC: Daemon sleeping for %lf seconds.", zs_chk_prd);
		pthread_cond_timedwait(&licd_cv, &licd_mutex, &abstime);
		pthread_mutex_unlock(&licd_mutex);
	}

}

/*
 * Wait for license daemon to start. Threads will sleep till the daemon
 * reads license file and updates its in-house information.
 * Threads sleep only if license information is not initialized yet by
 * the daemon.
 */
void
wait_for_licd_start()
{
	pthread_mutex_lock(&licd_mutex);

	if (licd_init == false) {
		pthread_cond_wait(&licd_cv, &licd_mutex);
	}

	pthread_mutex_unlock(&licd_mutex);
}

#define getptr(__data, __indx) 		((__data)->fld_data[(__indx)])
#define getas(__ptr, __type) 		(*(__type *)(__ptr))
#define getasstr(__ptr) 		((char *)(__ptr))

/*
 * This routine will update the in-house information using the data
 * read from license file.
 */
void
update_lic_info(lic_data_t *data, bool daemon)
{
	void		*p, *p1;
	struct timespec abstime;
	double		exptime;
	char		*prod;
	char		*maj = NULL;
	lic_type	type = 0;
	lic_inst_type inst_type = 0;

	clock_gettime(CLOCK_REALTIME, &abstime);

	/*
	 * If we couldn't read license details due to internall error, say
	 * insufficient memory, let us not fail or decide license status.
	 * Instead, read the license details again as soon as possible.
	 */
	ld_state = data->fld_state;
	if (daemon && (data->fld_state == LS_INTERNAL_ERR)) {
		ld_valid = true;
		adjust_chk_prd(-1);
		if (is_btree_loaded()) {
			ext_cbs->zs_lic_cb((ld_valid == true) ? 1 : 0);
		}
		return;
	}

	/*
	 * Only if license is valid or expired, let us check whether the license
	 * is for ZS. Else, let us mark the license as invalid and fail the
	 * application.
	 */
	if ((ld_state == LS_VALID) || (ld_state == LS_EXPIRED)) {
		// We always expect license type and product to be set.
		p = getptr(data, LDI_LIC_TYPE);
		plat_assert(p);
		if (p) {
			type = getas(p, lic_type);
		} else {
			ld_state = LS_INVALID;
			goto print;
		}

		p = getptr(data, LDI_INST_TYPE);
		plat_assert(p);
		if (p) {
			inst_type = getas(p, lic_inst_type);
		} else {
			ld_state = LS_INVALID;
			goto print;
		}

		p = getptr(data, LDI_PROD_NAME);
		plat_assert(p);
		if (p) {
			prod = getasstr(p);
			p1 = getptr(data, LDI_PROD_MAJ);
			if (p1) {
				maj = getasstr(p1);
			}
			/*
			 * If product matches, check whether the version
			 * matches. If both, then this is a valid license.
			 */
			if ( (!strcmp(prod, ZS_PRODUCT_NAME)) || (!strcmp(prod, "Flash Data Fabric")) ) {
#ifdef ZS_REVISION
				if (p1) {
					if (strncmp(p1, "all", strlen(p1))) {
						char	*tmp;
						if (!zs_version) {
							zs_version = malloc(strlen(ZS_REVISION) + 1);
						}

						if (zs_version) {
							bzero(zs_version, strlen(ZS_REVISION) + 1);
							strncpy(zs_version, ZS_REVISION, strlen(ZS_REVISION));
							tmp = strstr(zs_version, ".");

							if (tmp) {
								int len = tmp - zs_version;
								if (len) {
									if (strncmp(zs_version, maj, len)){
										ld_state = LS_VER_MISMATCH;
									}
								} else {
									if (strncmp(zs_version, maj, strlen(zs_version))){
										ld_state = LS_VER_MISMATCH;
									}
								}
							}
						} else {
							plat_log_msg(160256, LOG_CAT, LOG_WARN,
									"Internal error, skipped version check");
						}
					}
				} else {
					ld_state = LS_PROD_MISMATCH;
				}
#endif
			} else {
				ld_state = LS_PROD_MISMATCH;
			}
		} else {
			ld_state = LS_INVALID;
		}

		//If license is valid, update ld_vtime with current time.
		if (ld_state == LS_VALID) {
			ld_vtime = abstime.tv_sec;
		}
	}

	/*
	 * Print any info/warning messages based on status of license.
	 */
print:
	ld_cktime = abstime.tv_sec; 

	if (ld_state == LS_VALID) {
		// Print any warning, if we are near to expiry.
		check_validity_left(data, inst_type, type, daemon);
	} else if (ld_state == LS_EXPIRED) {
		// If license has expired, then it has to be periodic.
		plat_assert(type != LPT_PERPETUAL);
		if (daemon) {
			plat_log_msg(160155, LOG_CAT, LOG_WARN, 
				"License has expired. Renew the license.");
		}
		p = getptr(data, LDI_DIFF_TO);
		plat_assert(p);
		if (p) {
			exptime = getas(p, double);
			plat_assert(exptime < 0);
			exptime = -exptime;

			//Get the grce period of license file supports
			//Print warning & find period we need to make next check
			p = getptr(data, LDI_GRACE_PRD);
			if (p) {
				check_time_left(exptime, getas(p, double), daemon);
			} else {
				check_time_left(exptime, ZS_EXP_GPRD, daemon);
			}
		}
	} else {
		//All other cases, license is invalid.
		if (daemon) {
			plat_log_msg(80070, LOG_CAT, LOG_WARN, 
			      "License is invalid (%s). Install valid license.",
				lic_state_msg[ld_state]);
		}
		//Print warning and find period we need to make next check.
		check_time_left(abstime.tv_sec - ld_vtime, ZS_INVAL_GPRD,
				daemon);

	}
	if (is_btree_loaded()) {
		ext_cbs->zs_lic_cb((ld_valid == true) ? 1 : 0);
	}
}

/*
 * This routie to be used only if license is not valid.
 * This just prints warning message and updates the time we need to make
 * next check based on time left for end of grace period.
 * 
 * INPUT
 *	time	Seconds since the license had expired/invalid.
 *	grace	Grace period in which license is considered valid 
 *		eventhough it was expired/invalid.
 */
void
check_time_left(double time, double grace, bool warn)
{
	int 		days, hrs, mins, secs;
	
	plat_assert(ld_state != LS_VALID);

	if (grace == -1) {
		/*
		 * Grace period is unlimited. So, warn that license has expired.
		 */
		plat_assert(ld_state == LS_EXPIRED);
		if (warn) {
			plat_log_msg( 160257, LOG_CAT, LOG_WARN, 
			"License has expired, however ZS will continue to run. Renew the license.");
		}
		ld_valid = true;
		adjust_chk_prd(0);
	} else if (time >= grace) {
		/*
		 * If we are beyond grace period, mark license validity as
		 * false and increase the rate at which we check the 
		 * validity of license.
		 */
		if (warn) {
			plat_log_msg( 160258, LOG_CAT, LOG_WARN, 
					"License %s beyond grace period. ZS will fail. Renew the license.", 
						(ld_state == LS_EXPIRED) ? "expired" : "invalid");
		}
		ld_valid = false;
		adjust_chk_prd(0);
	} else {
		/*
		 * Just print the warning message and update the license
		 * check period.
		 */ 
		secs = grace - time;
		mins = secs / 60;
		hrs = mins / 60; mins = mins - hrs * 60; 
		days = hrs / 24; hrs = hrs - days * 24;
		if (warn) {
			plat_log_msg( 160158, LOG_CAT, LOG_WARN, 
				"ZS will be functional for next %d days, %d "
				"hours and %d minutes only.", days, hrs, mins);
		}
		ld_valid = true;
		adjust_chk_prd(secs);
	}
}

/*
 * This routie to be used only if license is valid.
 * This just prints warning message and updates the time we need to make
 * next check based on time left for end of validity of license.
 * 
 * INPUT
 *	time	Seconds of validity left.
 *	grace	Period in which user needs to be warned about expiry.
 */
void
check_validity_left(lic_data_t *data, lic_inst_type inst_type, lic_type type, bool warn)
{
	void		*p;
	double		exptime;
	int 		days, hrs, mins, secs;

	plat_assert(ld_state == LS_VALID);
	if (ld_valid == false) {
		plat_log_msg(160159, LOG_CAT, LOG_INFO, 
				"Valid license found (%s/%s).",
				lic_installation_type[inst_type],
				lic_period_type[type]);
		plat_log_msg(160259, LOG_CAT, LOG_INFO,
				"Customer details: %s, %s, %s.",
				data->fld_data[LDI_CUST_NAME] ? (char *)data->fld_data[LDI_CUST_NAME]: "(null)",
				data->fld_data[LDI_CUST_COMPANY] ? (char *)data->fld_data[LDI_CUST_COMPANY]: "(null)",
				data->fld_data[LDI_CUST_MAIL] ? (char *)data->fld_data[LDI_CUST_MAIL]: "(null)");
	}
	ld_valid = true;

	p = getptr(data, LDI_DIFF_TO);
	if (p) {
		exptime = getas(p, double);
		plat_assert(exptime > 0);
		if (exptime > ZS_EXP_GPRD) {
			return;
		}
		secs = exptime;
		mins = secs / 60;
		hrs = mins / 60; mins = mins - hrs * 60; 
		days = hrs / 24; hrs = hrs - days * 24;
		if (warn) {
			plat_log_msg(160160, LOG_CAT, LOG_WARN, 
				"License will expire in next %d days, %d "
				"hours and %d minutes.", days, hrs, mins);
		}
		adjust_chk_prd(secs);
	}
}

/*
 * Adjust the time stamp at which we need to make next license check.
 *
 * INPUT
 *	secs	Seconds left for license to become invalid/expire.
 *
 * Description:
 *	1. If time left is more than an hour, check once an hour.
 *	2. In last one hour, check every 15 minutes.
 *	3. In last 15 minutes, check every minute.
 */

void
adjust_chk_prd(double secs)
{

	if (secs == -1) {
		zs_chk_prd = 1;
		return;
	}

	if (zs_chk_prd_option > 0) {
		zs_chk_prd = zs_chk_prd_option;
	} else {
		if (secs <= 15 * MINUTE) {
			zs_chk_prd = MINUTE;
		} else if (secs <= HOUR) {
			zs_chk_prd = 15 * MINUTE;
		} else {
			zs_chk_prd = HOUR;
		}
	}
}

/*
 * Returns state of license to calling thread/APIs.
 */
bool
is_license_valid(bool btree_loaded)
{
	lic_data_t	data;
	struct timespec abstime;
	int		flag;

	// If license start had failed, we shall based on when last check made.
	flag = 0;
	if (licd_running == false) {
		clock_gettime(CLOCK_REALTIME, &abstime);
		if ((abstime.tv_sec - ld_cktime) > zs_chk_prd) {
			flag = 1;
		}
	}
start:
	/*
	 * If license is invalid, the license file might have got updated
	 * while daemon is sleeping. Let us check the state now.
	 */
	if ((ld_valid == false) || flag) {
		plat_log_msg(80071, LOG_CAT, LOG_DBG,
				"LIC: API doing the checking.");
		pthread_mutex_lock(&licstate_mutex);

		/*
		 * If daemon or some other thread is already checking the
		 * state, lets wait. No need for multiple threads to do
		 * the same check.
		 */
		if (licstate_updating == true) {
			plat_log_msg(80072, LOG_CAT, LOG_DBG,
				"LIC: API waiting as update in progress.");
			pthread_cond_wait(&licstate_cv, &licstate_mutex);
			/*
			 * The thread checking the status has hit an internal
			 * error, we shall do the check now and wakr up daemon
			 * too. 
			 * Else, the state might have not changed in this small
			 * time slot. We shall use the state returned by previo
			 * thread.
			 */
			if (ld_state == LS_INTERNAL_ERR) {
				plat_log_msg(80073, LOG_CAT, LOG_DBG,
					"LIC: API rechecking license state.");
				pthread_mutex_unlock(&licstate_mutex);
				pthread_cond_signal(&licd_cv);
				goto start;
			}
			plat_log_msg(80074, LOG_CAT, LOG_DBG,
				"LIC: API using state set by previous thread.");
		} else {
			licstate_updating = true;
			plat_log_msg(80075, LOG_CAT, LOG_DBG,
					"LIC: API updating license in progress.");
			pthread_mutex_unlock(&licstate_mutex); 

			bzero(&data, sizeof(lic_data_t));
			get_license_details(license_path, &data);
			update_lic_info(&data, false);
			free_details(&data);

			/*
			 * Wake-up daemon and other threads waiting for 
			 * license status.
			 */
			pthread_mutex_lock(&licstate_mutex);
			plat_log_msg(80076, LOG_CAT, LOG_DBG,
					"LIC: API updating license done.");
			licstate_updating = false;
			pthread_cond_broadcast(&licstate_cv);
		}
		pthread_mutex_unlock(&licstate_mutex); 

	}

	if (btree_loaded) {
		return true;
	}

	return ld_valid;
}

void
free_details(lic_data_t *data)
{
	int	i;
	data->fld_state = LS_VALID;

	for (i = 0; i < LDI_MAX_INDX; i++) {
		if (data->fld_data[i]) {
			free(data->fld_data[i]);
			data->fld_data[i] = NULL;
		}
	}
}
