//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 * File:   sdf/platform/opts_c.h
 * Author: drew
 *
 * Created on February 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: opts_c.h 12881 2010-04-13 22:43:38Z drew $
 */

#ifndef PLAT_OPTS_NAME
#error "PLAT_OPTS_NAME() must be defined"
#endif

#ifndef PLATFORM_OPTS_H
#error "platform/opts.h must be included earlier"
#endif

/* Parse command line arguments */
PLAT_OPTS_STATIC int
PLAT_OPTS_NAME(plat_opts_parse)(PLAT_OPTS_CONFIG int argc, char **argv) {
    enum {
        PLAT_OPTS_NAME(PLAT_OPTS_LAST_SHORT) = CHAR_MAX,
#define item(opt, arg, caps, parse, required)                                  \
        PLAT_OPTS_NAME(PLAT_OPTS_ ## caps),
        PLAT_OPTS_NAME(PLAT_OPTS_ITEMS)()
        PLAT_OPTS_COMMON_ITEMS()
#undef item
        PLAT_OPTS_NAME(PLAT_OPTS_LAST)
    };

    const struct option options[] = {
#define item(opt, arg, caps, parse, required)                                  \
        { opt, (required) & PLAT_OPTS_ARG_BITS, NULL,                          \
            PLAT_OPTS_NAME(PLAT_OPTS_ ## caps) },
        PLAT_OPTS_NAME(PLAT_OPTS_ITEMS)()
        PLAT_OPTS_COMMON_ITEMS()
#undef item
        /* terminate with zero-filled option */
        {} };

    int got[PLAT_OPTS_NAME(PLAT_OPTS_LAST) -
        PLAT_OPTS_NAME(PLAT_OPTS_LAST_SHORT) - 1] = {0, };

    int ret = 0;
    int c;
    int tmp;

    optind = 1;
    while ((c = getopt_long(argc, argv,
#ifdef PLAT_OPTS_OPTSTRING
                            PLAT_OPTS_OPTSTRING,
#else
                            "",
#endif
                            options, NULL)) != -1) {
        switch (c) {
#define item(opt, arg, caps, parse, required)                                  \
            case PLAT_OPTS_NAME(PLAT_OPTS_ ## caps):                           \
                tmp = (parse);                                                 \
                if (tmp) {                                                     \
                    fprintf(stderr, "--%s bad argument %s: %s\n",              \
                            (arg), optarg, plat_strerror(-tmp));               \
                    ret = 1;                                                   \
                } else {                                                       \
                    ++got[PLAT_OPTS_NAME(PLAT_OPTS_ ## caps) -                 \
                        PLAT_OPTS_NAME(PLAT_OPTS_LAST_SHORT) - 1];             \
                }                                                              \
                break;
        PLAT_OPTS_NAME(PLAT_OPTS_ITEMS)()
        PLAT_OPTS_COMMON_ITEMS()
#undef item
        default:
#ifdef PLAT_OPTS_OPTSTRING
            ret = PLAT_OPTS_NAME(plat_opts_short)(PLAT_OPTS_CONFIG_PASS c);
            break;
#endif
        case '?':
            fprintf(stderr, "unrecognized option: %c\n", c);
            ret = 1;
            break;
        }
    }

#define item(opt, arg, caps, parse, required)                                  \
    if (((required) & PLAT_OPTS_OPT_BITS) == PLAT_OPTS_OPT_REQUIRED &&         \
        !got[PLAT_OPTS_NAME(PLAT_OPTS_ ##  caps) -                             \
        PLAT_OPTS_NAME(PLAT_OPTS_LAST_SHORT) - 1]) {                           \
        fprintf(stderr, "required argument %s missing\n", (arg));              \
        ret = 1;                                                               \
    }
    PLAT_OPTS_NAME(PLAT_OPTS_ITEMS)()
    PLAT_OPTS_COMMON_ITEMS()
#undef item

    if (ret) {
        PLAT_OPTS_NAME(plat_opts_usage)();
    }

    return (ret);
}

/*
 * Parse arguments and remove long options.  Makes retrofitting simpler.
 */
PLAT_OPTS_STATIC int
PLAT_OPTS_NAME(plat_opts_parse_strip)(PLAT_OPTS_CONFIG int *argc, char **argv) {
    int ret;
    /* optind doesn't show up in GDB */
    int our_optind;

    ret = PLAT_OPTS_NAME(plat_opts_parse)(PLAT_OPTS_CONFIG_PASS *argc, argv);
    our_optind = optind;
    if (!ret && our_optind > 1) {
        memmove(argv + 1, argv + our_optind, (*argc - optind) * sizeof (*argv));
        *argc = 1 + *argc - our_optind;
        argv[*argc] = NULL;
    }

    return (ret);
}

/* Print program usage message */
PLAT_OPTS_STATIC void
PLAT_OPTS_NAME(plat_opts_usage)(void) {
    fprintf(stderr, "usage:\n");
#define item(opt, arg, caps, parse, required) \
    plat_opts_usage_one(opt, arg, required);
    PLAT_OPTS_NAME(PLAT_OPTS_ITEMS)()
    PLAT_OPTS_COMMON_ITEMS()
#undef item
    plat_log_usage();
}
