#!/usr/bin/perl
#
# File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/ffdc_parser.pl $
# Author: dkhoe
#
# Created on December 11, 2009
#
# Copyright (c) 2009-2013, SanDisk Corporation.  All rights reserved.
#
# $Id: ffdc_parser.pl 13896 2010-05-28 01:55:47Z johann $
#

#
# DESCRIPTION:
# This script parses the .c and .h files in the specified directory and 
# subdirectories.  For each file, the script will look for pre-defined
# log functions.  If the message in the log function is not yet assigned 
# an appropriate message ID, the script will assign the message an ID and
# modify the log function in the source code appropriately.
# 
# Before modifying the source code, the script will save the original 
# file with a ".ffdc_orig" file extension
#
# In addition, after parsing the entire directory and sub-directories, if 
# new log messages where found, the script will generate the following 
# files:
#
#    1. ffdc_log.mc: defines msg ID ranges for developers and serves as
#                    a catalog for all messages in the codebase
#       * Note: you *need* to check this file in if it has been changed
#               by this script
#
#    2. ffdc_log.h: defines one structure for each log msg in the code
#       * Note: you don't need to check this file in
#
#    3. ffdc_log.c: defines one function fo reach log msg in the code
#       * Note: you don't need to check this file in
#
#    4. ffdc_log.dbg: debug log for script
#       * Note: you don't need to check this file in
#
#
# HOW TO USE:
# When adding new messages to the code base, only use the following
# log functions:
#
#    - plat_log_msg()
#    - mcd_log_msg()
#
# Do not introduce new log functions to the codebase unless completely
# necessary. This script will need to be modified to recognize those 
# new log functions.
#
# When adding new log messages, just specify 'PLAT_LOG_ID_INITIAL' 
# for the message ID and this script will replace it with the
# appropriate message ID.  Do not specify a numeric ID, unless it's
# exacctly the same msgID-to-msg mapping.
#

use strict;
use File::Basename;

#--------------------------------------------------------------------
# Global Variable Definitions
#--------------------------------------------------------------------
our $FFDC_LOG_N = shift @ARGV or
    die "Must specify NCPU on command line";

our $PATH = shift @ARGV or
    die "Must specify path on command line";

#$PATH.="/" unless ($PATH =~ /\/$/);

our $TIMESTAMP = "";

our $FFDC_DIR = ".";
our $FFDC_CONFIG_FILE = "ffdc.conf";
our $FFDC_MSGCAT_FILE = "ffdc_log.mc";
our $FFDC_SOURCE_FILE = "ffdc_log.c";
our $FFDC_HEADER_FILE = "ffdc_log.h";
our $FFDC_READER_FILE = "ffdc_reader.c";
our $FFDC_DEBUG_FILE = "ffdc_log.dbg";

our $PREFIX = "ffdc";
our $U_PREFIX = uc($PREFIX);
our $MAX_STRING_SIZE = 64;

our $ESC_DBL_QUOTE = "{{DBL_QUOTE}}";
our $ESC_PRID64 = "{{PRID64}}";
our $ESC_PRIU64 = "{{PRIU64}}";
our $ESC_PRIU32 = "{{PRIU32}}";

our $PREV_DIR = "";

our %FFDC_MSGS = ();
our %FFDC_MSGIDS = ();
our %FFDC_RANGES = ();

our $FFDC_RANGE_START_ID = 0;
our $FFDC_RANGE_END_ID = 0;
our $MSG_ID_START = 0;
our $CURR_MSGID = 0;
our $NEW_MSG_CNT = 0;
our $FFDC_LOG_CUR = 1;
our @FFDC_C_ARR;

our $WHOAMI = $ENV{'USER'} or 
    die "Can't determine user";

our @DIRS = qw(
    agent
    api
    applib
    common
    dll
    ecc
    flash
    fth
    misc
    physmem
    platform
    protocol
    sdfappcommon
    sdfmsg
    sdftcp
    shared
    ssd
    sys
    utils
);

our @SKIPFILES = qw(
    sdfmsg/old/
    sdfmsg/oldtests/
    platform/logging.h
    platform/ffdc_log.h
    platform/ffdc_log.c
    platform/ffdc_reader.c
    protocol/replication/tests/
);

our @VALID_PLACEHOLDERS = qw(
    PLAT_LOG_ID_INITIAL
    LOG_ID
    LOGID
);

our %LEVELS =(
    'd' => 'PLAT_LOG_LEVEL_DEBUG',
    'e' => 'PLAT_LOG_LEVEL_ERROR',
    'f' => 'PLAT_LOG_LEVEL_FATAL',
    'i' => 'PLAT_LOG_LEVEL_INFO',
    't' => 'PLAT_LOG_LEVEL_TRACE',
    'w' => 'PLAT_LOG_LEVEL_WARN',
);

#--------------------------------------------------------------------
# Function Definitions
#--------------------------------------------------------------------
sub _sitReadConfig
{
    my ($cf, $mode, $line);

    $cf = "$PATH/$FFDC_CONFIG_FILE";
    unless ( -e $cf ) {
        return;
    }

    _sitLogInfo("Loading config file '$cf'");
    open FILE, "< $cf" or return;
    while (<FILE>) {
        $line = $_;
        chomp($line);
        if ($line eq "[IGNORE]") {
            $mode = 0;
            next;
        } elsif (!$line || $line =~ /^#.*/) {
            # skip comments and blank lines
            next;
        }

        if ($mode == 0) {
            _sitLogInfo("Adding '$line' to ignore list...");
            push(@SKIPFILES, $line);
        }
    }
}

sub _sitGetTimestamp
{
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,
        $yday,$isdst) = localtime(time);

    return sprintf "%4d-%02d-%02d %02d:%02d:%02d",
        $year+1900,$mon+1,$mday,$hour,$min,$sec;
}

sub _sitNeedToCreateFiles()
{
    if ($NEW_MSG_CNT > 0) {
        _sitLogInfo("Found new messages. Re-generating source files...");   
        return 1;
    }

    unless (-e "$FFDC_DIR/$FFDC_SOURCE_FILE") {
        _sitLogInfo("$FFDC_DIR/$FFDC_SOURCE_FILE doesn't exist. Re-generating source files...");    
        return 1;
    }

    unless (-e "$FFDC_DIR/$FFDC_HEADER_FILE") {
        _sitLogInfo("$FFDC_DIR/$FFDC_HEADER_FILE doesn't exist. Re-generating source files...");    
        return 1;
    }

    unless (-e "$FFDC_DIR/$FFDC_READER_FILE") {
        _sitLogInfo("$FFDC_DIR/$FFDC_READER_FILE doesn't exist. Re-generating source files...");    
        return 1;
    }

    _sitLogInfo("Regenerate always. Re-generating source files...");    
    return 1;
}

sub _sitIsNumber
{
    my ($val) = (@_);
    if ($val =~ /^\d+$/ ) {
        return 1;
    } else {
        return 0;
    }
}

sub _sitExistsInList 
{
    my($s, @arr) = (@_);
    my($n);
    
    for $n(0..$#arr) { 
        return 1 if ($s eq $arr[$n]); 
    }
    
    return 0;
}

sub _sitMatchesInList 
{
    my($s, @arr) = (@_);
    my($n);
    
    for $n(0..$#arr) { 
        return 1 if ($s =~ /($arr[$n])/); 
    }
    
    return 0;
}

sub _sitLogMsg
{ 
    my($sev, $file, $line_num, $msg) = (@_);
    my($d, $f);

    if ($msg) { 
        $d = dirname($file);
        $f = basename($file);

        if ($PREV_DIR ne $d) {  
            print "\nDirectory: $d\n";
            $PREV_DIR = $d;
        }
        if ($line_num) {
            print "    FFDC $sev: $f:$line_num: $msg\n"; 
        } else {
            print "    FFDC $sev: $f: $msg\n"; 
        }
    } else {
        print "FFDC $sev: $file\n";
    }
}

sub _sitLogError 
{
    my($file, $line_num, $msg) = (@_);
    _sitLogMsg("ERROR", $file, $line_num, $msg);
}

sub _sitLogErrorAndExit
{
    my($file, $line_num, $msg) = (@_);
    _sitLogError($file, $line_num, "$msg\n");
    exit 1;
}

sub _sitLogWarn
{ 
    my($file, $line_num, $msg) = (@_);
    _sitLogMsg("WARN", $file, $line_num, $msg);
}

sub _sitLogInfo 
{ 
    my($file, $line_num, $msg) = (@_);
    _sitLogMsg("INFO", $file, $line_num, $msg);
}

sub _sitLogDebug
{ 
    print FFDC_DBG "$_[0]\n"; 
}

sub _sitRTrim 
{ 
    my ($s) = (@_); 
    $s =~ s/\s+$//; 
    return "$s"; 
}

sub _sitLTrim 
{ 
    my ($s) = (@_); 
    $s =~ s/^\s+//; 
    return "$s"; 
}

sub _sitTrim 
{ 
    my ($s) = (@_); 
    $s = _sitLTrim($s);
    $s = _sitRTrim($s); 
    return "$s"; 
}

sub _sitRunCmd 
{
    my($cmd) = (@_);
    my($out);
    
    $cmd .= " 2>&1" unless ($cmd =~ />/);
    $out = `$cmd`;
    chomp($out);
    
    return "$out";
}

sub _sitWrapLogMsgComment
{
    my($line) = (@_);
    my($rest, $sline, $ret, $ndx);
    
    $ret = "";
    chomp($line);
    $rest = "$line ";
    $ndx = 0;
    
    while (_sitTrim($rest) ne "") {
        $rest =~ /(.{1,60}\W)/ms;
        $sline = $1;
        
        if ($ndx++ > 0) {
            $ret .= " *          ";
        }
        
        if (_sitTrim($sline)) {
            $ret .= "$sline";
            $rest = $'; 
        } else {
            $ret .= "$rest";
            $rest = ""; 
        }
        
        if ($rest) {
            $ret .= "\n";
        }
        
        # safeguard
        if ($ndx > 20) {
            exit;
        }
    }
    
    return _sitRTrim($ret);
}

sub _sitCleanUpLogLevel
{
    my($level) = (@_);
    my(@arr, $n, $ret);
    
    @arr = (
        "LOG_DBG",
        "LOG_DIAG",
        "LOG_ERR",
        "LOG_FAT",
        "LOG_FATAL",
        "LOG_INF",
        "LOG_INFO",
        "LOG_TRACE",
        "LOG_TRC",
        "LOG_WARN",
        "PLAT_LOG_LEVEL_DEVEL",
        "PLAT_LOG_LEVEL_TRACE",
        "PLAT_LOG_LEVEL_DEBUG",
        "PLAT_LOG_LEVEL_DIAGNOSTIC",
        "PLAT_LOG_LEVEL_INFO",
        "PLAT_LOG_LEVEL_WARN",
        "PLAT_LOG_LEVEL_ERROR",
        "PLAT_LOG_LEVEL_FATAL"
    );
    
    $ret = "";
    for $n (0..$#arr) {
        if ($level =~ /\w*$arr[$n]\w*/) {
            if ($ret ne "") {
                $ret .= ", " . $arr[$n];
            } else {
                $ret .= $arr[$n];
            }
        }
    }
    
    if ($ret eq "") {
        $ret = "unknown: $level";
    }
    
    return $ret;
}

sub _sitReadMC
{
    my($file) = (@_);
    my($range_mode, $line, $line_num, $msgid, $msg);

    _sitLogInfo("Reading existing message catalog file: $file");
    
    open(FILE, $file) ||
        die("_sitParseFile: Unable to open $file\n");

    $range_mode = 1;
    $line_num = 0;
    while ($line = <FILE>) {
        chomp($line);
        $line_num++;
        
        if ($line eq "[MESSAGES]") {
            $range_mode = 0;
            next;
        } elsif ($line eq "[RANGES]") {
            $range_mode = 1;
            next;
        } elsif (!$line || $line =~ /^#.*/) {
            # skip comments and blank lines
            next;
        }

        ($msgid,$msg) = split(/ /, $line, 2);
        $msg = _sitEscape($msg);
        
        if ($range_mode) {
            $FFDC_RANGES{$msgid} = $msg;
        } else {
            if ($FFDC_MSGS{$msg} && ($FFDC_MSGS{$msg} == $msgid)) {
                _sitLogErrorAndExit($file, $line_num, 
                    "Error while reading message catalog file. Duplicate message entries found:\n"
                    . "        ID: $msgid\n"
                    . "        Msg: $msg");
                    
            } elsif ($FFDC_MSGS{$msg}) {
                _sitLogErrorAndExit($file, $line_num,
                    "Error while reading message catalog file. Message has multiple message IDs:\n"
                    . "        Msg: $msg\n"
                    . "        ID #1: $msgid\n"
                    . "        ID #2: $FFDC_MSGS{$msg}");
                    
            } elsif ($FFDC_MSGIDS{$msgid}) {
                _sitLogErrorAndExit($file, $line_num,
                    "Error while reading message catalog file. Same message ID used for different messages:\n"
                    . "        Msg ID: $msgid\n"
                    . "        Msg #1: $msg\n"
                    . "        Msg #2: $FFDC_MSGIDS{$msgid}");
            } else {
                $FFDC_MSGS{$msg} = $msgid;
                $FFDC_MSGIDS{$msgid} = $msg;
            }
        }
    }
}

sub _sitInitRangeForUser
{
    my($user) = (@_);
    my ($key, $curr_max);

    _sitLogInfo("Searching for valid message ID range for user '$user'...");
    
    # find the range that belongs to this user
    foreach $key (sort { $a <=> $b } keys %FFDC_RANGES) {
        if ($MSG_ID_START == 0) {
            $MSG_ID_START = $key;
        }

        if ($FFDC_RANGES{$key} eq $user) {
            $FFDC_RANGE_START_ID = $key;
        } elsif ($FFDC_RANGE_START_ID > 0) {
            $FFDC_RANGE_END_ID = $key;
            last;
        }
    }
 
    if ($FFDC_RANGE_START_ID == 0 && $FFDC_RANGE_END_ID == 0) {
        _sitLogWarn(
            "Unable to find a valid range for user '$user'.  This user will not be able\n"
            . "   to add new messages until there is a corresponding range for that user defined\n"
            . "   in '$FFDC_DIR/$FFDC_MSGCAT_FILE'\n\n"
            . "   **See instructions at the top of $FFDC_MSGCAT_FILE for more details\n");
        $CURR_MSGID = 0;
        return;
    }

    # find the max ID in the user's range
    foreach $key (sort { $a <=> $b } keys %FFDC_MSGIDS) {
        if ($key < $FFDC_RANGE_END_ID) {
            $curr_max = $key;
        } else {
            last;
        }
    }

    # validate the max ID
    if ($curr_max < $FFDC_RANGE_START_ID) {
        $CURR_MSGID = $FFDC_RANGE_START_ID;
    } else {
        # start at next ID
        $CURR_MSGID = $curr_max + 1;
    }
    
    _sitLogInfo("Message ID Range for user '$user':\n"
        . "\tstart   : $FFDC_RANGE_START_ID\n"
        . "\tend     : $FFDC_RANGE_END_ID\n"
        . "\tcurrent : $CURR_MSGID\n");
}

sub _sitAddMessageWithID
{
    my ($file, $line_num, $log_func, $catid, $level, $msgid, $msg) = (@_);
    my $unesc_msg = _sitUnescape($msg);
    
    _sitLogDebug("$file:$line_num:$log_func:$level:$catid:$msgid:$unesc_msg");
    
    $FFDC_MSGS{$msg} = $msgid;
    $FFDC_MSGIDS{$msgid} = $msg;
    $NEW_MSG_CNT++;
}

sub _sitAddMessage
{
    my ($file, $line_num, $log_func, $catid, $level, $msg) = (@_);
    my $msgid = $CURR_MSGID;

    if ($FFDC_RANGE_START_ID == 0 && $FFDC_RANGE_END_ID == 0) {
        _sitLogErrorAndExit("User '$WHOAMI' doesn't have an assigned range and therefore is unable to add\n"
            . "    a new message. Please add a corresponding range for user '$WHOAMI' in "
            . "'$FFDC_MSGCAT_FILE'\n\n"
            . "   **See instructions at the top of $FFDC_MSGCAT_FILE for more details");
    }

    if ($FFDC_MSGS{$msg}) {
        # Message already exists.  Return existing ID
        $msgid = $FFDC_MSGS{$msg};
    } else {
        # calculate next available message ID
        while ($FFDC_MSGIDS{$CURR_MSGID} && 
            ($CURR_MSGID < $FFDC_RANGE_END_ID)) {
            $CURR_MSGID++;
        }
        
        # see if we've exhausted this range
        if ($CURR_MSGID >= $FFDC_RANGE_END_ID) {
            _sitLogErrorAndExit("Unable to add more log messages. You have exceeded your assigned message ID range.");
        }
        
        # This is a new message
        $msgid = $CURR_MSGID;
        _sitAddMessageWithID($file, $line_num, $log_func, $catid, 
            $level, $msgid, $msg);
            
        $CURR_MSGID++;
    }
    
    # return the message ID we used for this message
    return $msgid;
}

sub _sitEscape
{
    my ($fmt) = (@_);
    
    $fmt=~s/\\"/$ESC_DBL_QUOTE/g;
    $fmt=~s/%\"PRId64\"/%$ESC_PRID64/g;
    $fmt=~s/%\"PRId64/%$ESC_PRID64\"/g;
    $fmt=~s/%\"PRIu64\"/%$ESC_PRIU64/g;
    $fmt=~s/%\"PRIu64/%$ESC_PRIU64\"/g;
    $fmt=~s/%\"PRIu32\"/%$ESC_PRIU32/g;
    $fmt=~s/%\"PRIu32/%$ESC_PRIU32\"/g;
    
    $fmt=~s/%\"FFDC_LONG_STRING\((\d+)\)\"/%{{FFDC_LONG_STRING($1)}}/g;

    return $fmt;    
}

sub _sitUnescape
{
    my ($fmt) = (@_);

    $fmt=~s/$ESC_DBL_QUOTE/\\"/g;
    $fmt=~s/%$ESC_PRID64/%\"PRId64\"/g;
    $fmt=~s/%$ESC_PRIU64/%\"PRIu64\"/g;
    $fmt=~s/%$ESC_PRIU32/%\"PRIu32\"/g;
    $fmt=~s/%{{FFDC_LONG_STRING\((\d+)\)}}/%\"FFDC_LONG_STRING($1)\"/g;

    return $fmt;    
}

sub _sitGetDataTypeForSpecifier
{
    my ($spec) = (@_);
    my ($dt);

    if ($spec =~ /^{{FFDC_LONG_STRING\((\d+)\)}}/) {
        $dt = "string_long_$1";
    } elsif ($spec =~ /^[\-\.0-9]*s/) {
        $dt = "string";
    } elsif ($spec =~ /^[\-\.0-9]*(c|d|u|x|X)/) {
        $dt = "int";
    } elsif ($spec =~ /^[\-\.0-9]*(f|F|g|G)/) {
        $dt = "double";
    } elsif ($spec =~ /^[\-\.0-9]*ll/) {
        $dt = "long long";
    } elsif ($spec =~ /^[\-\.0-9]*l/) {
        $dt = "long";
    } elsif ($spec =~ /^[\-\.0-9]*p/) {
        $dt = "pointer";
    } elsif ($spec =~ /^[\-\.0-9]*hu/) {
        $dt = "short";
    } elsif ($spec =~ /^[\-\.0-9]*\*\.*s/) {
        # to handle '%*.s' and '%*s'
        $dt = "string1";
    } elsif ($spec =~ /^[\-\.0-9]*\*\.\*s/) {
        # to handle '%*.*s'
        $dt = "string2";
    } elsif ($spec =~ /^$ESC_PRID64/) {
        # to handle '%"PRId64"'
        $dt = "long";
    } elsif ($spec =~ /^$ESC_PRIU64/) {
        # to handle '%"PRIu64"'
        $dt = "long";
    } elsif ($spec =~ /^$ESC_PRIU32/) {
        # to handle '%"PRIu64"'
        $dt = "int";
    } else {
        $dt = "";
    }
    
    return $dt;
}

sub _sitIsValidFormatString
{
    my($fmt) = (@_);
    my(@fmt_arr, $n, $dt);

    $fmt=~s/%%/__DBL_PERCENT__/g;
    
    @fmt_arr = split(/%/, $fmt);
    for $n(1..$#fmt_arr) {
        $dt = _sitGetDataTypeForSpecifier($fmt_arr[$n]);
        if (!$dt) {
            if ($fmt_arr[$n]) {
                return $fmt_arr[$n];
            } else {
                return "--unknown--";   
            }
        }
    }
    
    return "";
}

sub _sitArgsToStruct 
{
    my($msgid, $format) = (@_);
    
    my($arg_id, $n, $dt, @fmt_arr, $esc_format, $arg_offset);
    my($struct_buf, $func_buf, $func_body, $printf_buf);
    my($width);
    
    $arg_offset = -1;
    
    # escape the format specifiers and escape sequences from the
    # format string
    $esc_format = _sitUnescape($format);
    $esc_format =~ s/%/%%/g;
    
    $struct_buf = "struct ${PREFIX}_log_struct_${msgid}\n{\n";
    $struct_buf .= "    int magic;\n";
    $struct_buf .= "    int msgid;\n";
    $struct_buf .= "    int catid;\n";
    $struct_buf .= "    int level;\n";
    $struct_buf .= "    unsigned long th_id;\n";
    $struct_buf .= "    unsigned long fth_id;\n";
    $struct_buf .= "    int lineno;\n";
    $struct_buf .= "    struct timeval timestamp;\n\n";
    
    $func_buf = "void\n${PREFIX}_log_func_${msgid}(";
    $func_buf .= "int lineno, ";
    $func_buf .= "int msgid, ";
    $func_buf .= "int catid, ";
    $func_buf .= "int level, ";
    $func_buf .= "const char* fmt";
    
    $printf_buf = "char* \n${PREFIX}_printf_func_${msgid}(char* shmem, struct plat_opts_config_ffdc_reader* config)\n{\n";
    $printf_buf .= "    struct tm* ltimep = NULL;\n";
    $printf_buf .= "    char buf[256];\n";
    $printf_buf .= "    struct ffdc_info *infop = get_ffdc_info();\n";

    $printf_buf .= "    struct ${PREFIX}_log_struct_${msgid} *lms =\n";
    $printf_buf .= "        (struct ${PREFIX}_log_struct_${msgid}*) shmem;\n\n";
    $printf_buf .= "    if (!config->filter || plat_log_enabled(lms->catid, (enum plat_log_level)lms->level)) {\n";


    $printf_buf .= "    ltimep = localtime(&(lms->timestamp.tv_sec));\n\n";
    $printf_buf .= "    if (ltimep) {\n";
    $printf_buf .= "        strftime(buf, sizeof(buf),\n";
    $printf_buf .= "            \"%%Y-%%m-%%d %%H:%%M:%%S\", ltimep);\n";
    $printf_buf .= "    } else {\n";
    $printf_buf .= "        sprintf(buf, \"%%lu\", (unsigned long)lms->timestamp.tv_sec);\n";
    $printf_buf .= "    }\n\n";
    $printf_buf .= "    printf(\"%%lu %%06lu %%s %%s%%spth[%%lx] fth[%%lx] lvl[%%d] line[%%d] %%s%%s%%d-%%d $esc_format\\n\",\n";
    $printf_buf .= "        lms->timestamp.tv_sec, lms->timestamp.tv_usec, buf,\n";
    $printf_buf .= "        config->hostname ? infop->hostname : \"\",\n";
    $printf_buf .= "        config->hostname ? \" \" : \"\",";

    $printf_buf .= "        lms->th_id,\n";
    $printf_buf .= "        lms->fth_id, lms->level, lms->lineno,\n";
    $printf_buf .= "        config->strings ? plat_log_cat_to_string(lms->catid) : \"\",\n";
    $printf_buf .= "        config->strings ? \" \" : \"\", lms->catid, lms->msgid";
        
    $func_body = "    ${U_PREFIX}_START(${msgid}, catid, level, lineno);\n";
    
    # do we have any message args?
    $esc_format = $format;
    $esc_format =~ s/%%/__DBL_PERCENT__/g;
    @fmt_arr = split(/%/, $esc_format);
    if ($#fmt_arr > 0) {
        $func_buf .= ",\n    ";
        $printf_buf .= ",\n        ";
    }
    
    # generate the code for this message
    $arg_id = 0;
    for $n(1..$#fmt_arr) {

        $dt = _sitGetDataTypeForSpecifier($fmt_arr[$n]);
        if (!$dt) {
            if ($n > 0) {
                # unhandled format specifier
                _sitLogWarn("Unhandled format specifier: [" . $fmt_arr[$n] . "]:\n        $format");
                return 1;
            } else {
                next;
            }
        }
        
        # we have what we need, generate the code for this message
        # argument
        $arg_id++;
        if ($dt =~ /^string_long_(\d+)$/) {
            $width = $1; 
            $struct_buf .= "    char arg" . $arg_id . "[$width];";
            $func_buf .= "const char* arg" . $arg_id;
            $func_body .= "    ${U_PREFIX}_ADD_STR(arg" . $arg_id . ");\n";
            $printf_buf .= "lms->arg" . $arg_id;

        } elsif ($dt eq "string") {
            $struct_buf .= "    char arg" . $arg_id . "[FFDC_MAX_STR_SIZE];";
            $func_buf .= "const char* arg" . $arg_id;
            $func_body .= "    ${U_PREFIX}_ADD_STR(arg" . $arg_id . ");\n";
            $printf_buf .= "lms->arg" . $arg_id;
            
        } elsif ($dt eq "string1") {
            $struct_buf .= "\n    /* width/precision arg */\n";
            $struct_buf .= "    int arg" . $arg_id . ";\n";
            $func_buf .= "int arg" . $arg_id . ", ";
            $func_body .= "    ${U_PREFIX}_ADD_NUM(arg" . $arg_id . ");\n";
            $printf_buf .= "lms->arg" . $arg_id . ", ";
            
            $arg_id++;
            $arg_offset++;
            $struct_buf .= "    char arg" . $arg_id . "[FFDC_MAX_STR_SIZE];";
            $func_buf .= "const char* arg" . $arg_id;
            $func_body .= "    ${U_PREFIX}_ADD_STR(arg" . $arg_id . ");\n";
            $printf_buf .= "lms->arg" . $arg_id;
            
        } elsif ($dt eq "string2") {
            $struct_buf .= "\n    /* width/precision arg */\n";
            $struct_buf .= "    int arg" . $arg_id . ";\n";
            $func_buf .= "int arg" . $arg_id . ", ";
            $func_body .= "    ${U_PREFIX}_ADD_NUM(arg" . $arg_id . ");\n";
            $printf_buf .= "lms->arg" . $arg_id . ", ";
            
            $arg_id++;
            $arg_offset++;
            $struct_buf .= "\n    /* width/precision arg */\n";
            $struct_buf .= "    int arg" . $arg_id . ";\n";
            $func_buf .= "int arg" . $arg_id . ", ";
            $func_body .= "    ${U_PREFIX}_ADD_NUM(arg" . $arg_id . ");\n";
            $printf_buf .= "lms->arg" . $arg_id . ", ";
            
            $arg_id++;
            $arg_offset++;
            $struct_buf .= "    char arg" . $arg_id . "[FFDC_MAX_STR_SIZE];";
            $func_buf .= "const char* arg" . $arg_id;
            $func_body .= "    ${U_PREFIX}_ADD_STR(arg" . $arg_id . ");\n";
            $printf_buf .= "lms->arg" . $arg_id;
            
        } elsif ($dt eq "pointer") {
            $struct_buf .= "    long arg" . $arg_id . ";";
            $func_buf .= "void* arg" . $arg_id;
            $func_body .= "    ${U_PREFIX}_ADD_PTR(arg" . $arg_id . ");\n";
            $printf_buf .= "(void*)lms->arg" . $arg_id;
            
        } else {
            $struct_buf .= "    $dt arg" . $arg_id . ";";
            $func_buf .= "$dt arg" . $arg_id;
            $func_body .= "    ${U_PREFIX}_ADD_NUM(arg" . $arg_id . ");\n";
            $printf_buf .= "lms->arg" . $arg_id;
        }
        
        if ($n < $#fmt_arr) {
            $struct_buf .= "\n";
            $func_buf .= ", ";
            $printf_buf .= ", ";
            
            # try to stay within 80 chars
            if ($n % 5 == 0) {
                $func_buf .= "\n    ";
                $printf_buf .= "\n        ";
            }
        }
    }
    
    # print the generated code for this message
    print FFDC_H "$struct_buf\n} __attribute__ ((aligned (64)));\n\n";
    print FFDC_H "__inline__ $func_buf);\n\n";

    $func_body .= "    ${U_PREFIX}_END();\n";

    my $FF = $FFDC_C_ARR[$FFDC_LOG_CUR++];

    print $FF "__inline__ $func_buf)\n{\n$func_body}\n\n";

    if ($FFDC_LOG_CUR > $FFDC_LOG_N) {
        $FFDC_LOG_CUR = 1;
    }

    $printf_buf .= ");\n\n    }\n    return (char*) (shmem + sizeof(struct ${PREFIX}_log_struct_${msgid}));\n";
    printf READER "$printf_buf}\n\n";

    return 0;
}

sub _sitParseFile
{
    my($file) = (@_);
    my($line, $multi_source_line, $real_line, $multi_real_line, $line_num);
    my($msgid, $catid, $level, $ma, $args, $fmt, $j, $ret, $msgcnt, $spaces);
    my($new_file, $cmt, $place_holder, $log_func, $bad_spec, $unesc_msg);

    $new_file = "${file}.ffdc_modified";
    open(NEW_FILE, ">$new_file") || 
        die("_sitParseFile: Unable to open ${file}.ffdc_modified\n");
        
    open(FILE, $file) || 
        die("_sitParseFile: Unable to open $file\n");

    $multi_source_line = "";
    $multi_real_line = "";
    $line_num = 0;
    
    # just display the latter half of the path
    $file =~ s/^$PATH\///g;
    
    $msgcnt = 0;
    while ($line = <FILE>) {
        $real_line = $line;
        
        chomp($line);
        $line_num++;
        
        $line = _sitTrim($line);
        
        # clean up quotes
        $line =~ s/"\\"|\\""/"/g;
                
        # handle line continuations
        $line=~s/\s\\$//;
        
        $multi_source_line .= ($multi_source_line) ? " $line" : $line;
        $multi_real_line .= $real_line;

        if ($multi_source_line =~ /plat_log_msg\((.*?)\)\s*;\s*\\?$/) {
            ($msgid,$catid,$level,$ma)=split(/,/,$1,4);
            
            # remove junk before the log function
            $multi_source_line =~ s/.*plat_log_msg/plat_log_msg/;
            
            $log_func = "plat_log_msg";
            
        } elsif ($multi_source_line =~ /mcd_log_msg\((.*?)\)\s*;\s*\\?$/) {
            ($msgid,$level,$ma)=split(/,/,$1,3);
            
            # remove junk before the log function
            $multi_source_line =~ s/.*mcd_log_msg/mcd_log_msg/;
            
            $log_func = "mcd_log_msg";

        } elsif ($multi_source_line =~ /mcd_rlg_msg\((.*?)\)\s*;\s*\\?$/) {
            ($msgid,$level,$ma)=split(/,/,$1,3);
            
            # remove junk before the log function
            $multi_source_line =~ s/.*mcd_rlg_msg/mcd_rlg_msg/;
            
            $log_func = "mcd_rlg_msg";
            
        } elsif ($multi_source_line =~ /mcd_bak_msg\((.*?)\)\s*;\s*\\?$/) {
            ($msgid,$level,$ma)=split(/,/,$1,3);
            
            # remove junk before the log function
            $multi_source_line =~ s/.*mcd_bak_msg/mcd_bak_msg/;
            
            $log_func = "mcd_bak_msg";
            
        } elsif ($multi_source_line =~ /UTWarning\((.*?)\)\s*;\s*\\?$/) {
            ($msgid,$ma)=split(/,/,$1,2);
            
            # remove junk before the log function
            $multi_source_line =~ s/.*UTWarning/UTWarning/;
            
            $log_func = "UTWarning";
        
        } elsif ($multi_source_line =~ /UTMessage\((.*?)\)\s*;\s*\\?$/) {
            ($msgid,$ma)=split(/,/,$1,2);
            
            # remove junk before the log function
            $multi_source_line =~ s/.*UTMessage/UTMessage/;
            
            $log_func = "UTMessage";
        } elsif ($multi_source_line =~ /sdf_log([defitw])(_sys)?\(((?:
                                        LOG_ID|LOGID|PLAT_LOG_ID_INITIAL|
                                        [1-9]\d*,).*?)\)\s*;\s*\\?$/x) {
            $log_func = "sdf_log$1$2";
            ($msgid, $ma) = split(/,/, $3, 2);
            $multi_source_line =~ s/.*sdf_log/sdf_log/;
            $catid = 'LOG_CAT';
            $level = $LEVELS{$1};
        } elsif ($multi_source_line =~
                 /\s(t_[a-z]{4})\(((?:LOG_ID|[1-9]\d*,).*?)\)\s*;\s*\\?$/) {
            $log_func = "$1";
            ($msgid, $ma) = split(/,/, $2, 2);
            $multi_source_line =~ s/.*t_/t_/;
            $catid = 'LOG_CAT';
            $level = 'PLAT_LOG_LEVEL_TRACE';
        } else {
            $log_func = "";
        }
        
        if ($log_func) {
            # skip #defines
            if ($multi_source_line =~ /#define/) {
                $multi_source_line =~ s/\s+/ /g;
#               _sitLogInfo($file, $line_num, "Found #define:\n        $multi_source_line");
                print NEW_FILE "$multi_real_line";
                $multi_source_line = "";
                $multi_real_line = "";
                $log_func = "";
                next;
            }

#           $j = split(/ /, $msgid);
#           if ($j > 1) {
#               $multi_source_line =~ s/\s+/ /g;
#               _sitLogErrorAndExit($file, $line_num, 
#                   "Unable to find message ID in log function call. Please specify PLAT_LOG_INITIAL_ID as the "
#                   . "first parameter to $log_func:\n        $multi_source_line");
#           }
            
            $ma = _sitEscape($ma);
            $ma=~s/" "//g; # connect multi-line messages
            $ma=~s/"  "//g; # connect multi-line messages
            $ma=~s/"\s*PLAT_SP_FMT\s*"/%p/g; # handle ("fthIdlecontrol " PLAT_SP_FMT " at %p allocated")
            $ma=~s/"\s*PLAT_SP_FMT/%p"/g; # handle ("Created shmem_alloc at " PLAT_SP_FMT)
            
            ($j,$fmt,$args)=split(/"/,$ma,3);

            # skip messages that have variables for format strings
            if (!$fmt) {
                $multi_source_line =~ s/\s+/ /g;
#               _sitLogWarn($file, $line_num, "Found empty format string:\n        $multi_source_line");
                print NEW_FILE "$multi_real_line";
                $multi_source_line = "";
                $multi_real_line = "";
                $log_func = "";
                next;
            }

            $catid = _sitTrim($catid);
            $level = _sitTrim($level);

            if ($multi_real_line =~ /$log_func\s*\((\s*)([a-zA-Z0-9_]+)\s*,/) {
                $spaces = $1;
                $place_holder = $2;
                if (_sitIsNumber($place_holder)) {
                    if (!$FFDC_MSGS{$fmt} && !$FFDC_MSGIDS{$place_holder}) {
                        _sitLogInfo($file, $line_num,
                            "Message has an assigned message ID in the source code but not in the message catalog. Adding message to catalog:\n"
                            . "        Msg: $fmt\n"
                            . "        ID: $place_holder");
                            
                        $bad_spec = _sitIsValidFormatString($fmt);
                        if ($bad_spec) {
                            # unhandled format specifier
                            _sitLogErrorAndExit($file, $line_num, 
                                "Invalid format specifier: [" . $bad_spec . "]:\n        $fmt");    
                        } else {
                            _sitAddMessageWithID($file, $line_num, $log_func, $catid, $level, 
                                $place_holder, $fmt);
                            $msgcnt++;
                        }
                        
                    } elsif ($FFDC_MSGIDS{$place_holder} && $FFDC_MSGIDS{$place_holder} ne $fmt) {
                        _sitLogErrorAndExit($file, $line_num, 
                            "Same message ID used for different messages:\n"
                            . "        Msg ID: $place_holder\n"
                            . "        Msg #1: $fmt\n"
                            . "        Msg #2: $FFDC_MSGIDS{$place_holder}\n\n"
                            . "        ** Resolution: Use 'PLAT_LOG_ID_INITIAL' instead of a numeric ID when adding new log messages");
                            
                    } elsif ($FFDC_MSGS{$fmt} != $place_holder) {
                        _sitLogErrorAndExit($file, $line_num,
                            "Message has multiple message IDs:\n"
                            . "        Msg: $fmt\n"
                            . "        ID #1: $place_holder\n"
                            . "        ID #2: $FFDC_MSGS{$fmt}\n\n"
                            . "        ** Resolution: Use the same numeric ID for messages with the same text, or use 'PLAT_LOG_ID_INITIAL'\n"
                            . "                       in your new log call and leave it to the FFDC parser script to handle things for you");
                            
                    } else {
                        $unesc_msg = _sitUnescape($fmt);
                        _sitLogDebug("$file:$line_num:$log_func:$level:${catid}:${place_holder}:$unesc_msg");
                    }
                } elsif (_sitExistsInList($place_holder, @VALID_PLACEHOLDERS)) {
                    
                    $bad_spec = _sitIsValidFormatString($fmt);
                    if ($bad_spec) {
                        # unhandled format specifier
                        _sitLogWarn($file, $line_num, 
                            "Unhandled format specifier: [" . $bad_spec . "]:\n        $fmt");  
                    } else {
                        # add the message to the message catalog
                        $msgid = _sitAddMessage($file, $line_num, $log_func, $catid, $level, $fmt);
                        
                        _sitLogInfo($file, $line_num,
                            "Adding new message to catalog:\n"
                            . "        Msg: $fmt\n"
                            . "        ID: $msgid");
                            
                        # replace the msgID place holder with a valid msgID
                        $multi_real_line =~ s/$log_func\s*\(\s*$place_holder\s*,/$log_func\($spaces$msgid,/;
                        
                        $msgcnt++;
                    } 
                } else {
                    # error in parsing
                    $multi_source_line =~ s/\s+/ /g;
#                   _sitLogWarn($file, $line_num, "Invalid message ID placeholder found: $place_holder\n        $multi_source_line");
                }
            } else {
                # error in parsing
                $multi_source_line =~ s/\s+/ /g;
                _sitLogErrorAndExit($file, $line_num, 
                    "Unable to find message ID in log function call. Please specify PLAT_LOG_INITIAL_ID as the "
                    . "first parameter to $log_func:\n        $multi_source_line");
            }

            print NEW_FILE $multi_real_line;
            
            $multi_source_line = "";
            $multi_real_line = "";
            $log_func = "";
            
        } elsif ($multi_source_line =~ /;\s*$/) {
            # source line without a log call
            print NEW_FILE "$multi_real_line";
            
            $multi_source_line = "";
            $multi_real_line = "";
        }
    }
    
    if ($msgcnt == 0) {
        close NEW_FILE;
        close FILE;
        
        # we didn't find any log msgs to parse so delete the
        # generated file
        unlink($new_file);
    } else {
        print NEW_FILE "$multi_real_line";
        
        close NEW_FILE;
        close FILE;
        
        system("mv", "$PATH/$file", "$PATH/${file}.ffdc_orig");
        system("mv", $new_file, "$PATH/$file");
    }
}

sub _sitMsgCatalogAddHeader
{
    my ($key);

print FFDC_MC << "EOF";
#
# File: \$HeadURL\$
# Last Changed By: \$LastChangedBy\$
#
# Copyright (c) 2009-2013, SanDisk Corporation.  All rights reserved.
#
# \$Id\$
#

#------------------------------------------------------------------------
# To add a new message ID range for a user:
# 1. Take the range specified as "always_last" and add the new user with 
#    that range
# 2. Increment the range for "always_last" by specifying the next range
#    in the series for "always_last"
#------------------------------------------------------------------------

EOF
    
    print FFDC_MC "[RANGES]\n";
    foreach $key (sort { $a <=> $b } keys %FFDC_RANGES) {
        print FFDC_MC "$key $FFDC_RANGES{$key}\n";
    }
    print FFDC_MC "\n\n";
    print FFDC_MC "[MESSAGES]\n";
}

sub _sitCreateFFDCSourceFiles
{
    my($ret, $key, $comment, $range_ndx, @msgid_ranges, $msg, $n);

    print "\n";
    _sitLogInfo("Creating FFDC source files...");
    
    open (FFDC_C, ">$FFDC_DIR/$FFDC_SOURCE_FILE") || die ("Unable to open $FFDC_DIR/$FFDC_SOURCE_FILE\n");
    open (FFDC_H, ">$FFDC_DIR/$FFDC_HEADER_FILE") || die ("Unable to open $FFDC_DIR/$FFDC_HEADER_FILE\n");
    open (FFDC_MC, ">$FFDC_DIR/${FFDC_MSGCAT_FILE}.ffdc_modified") || die ("Unable to open $FFDC_DIR/${FFDC_MSGCAT_FILE}.ffdc_modified\n");
    open (READER, ">ffdc_reader.c") || die ("Unable to open ffdc_reader.c\n");
    
    _sitHeaderAddHeader();
    _sitSourceAddHeader();
    _sitReaderAddHeader();
    _sitMsgCatalogAddHeader();

    for $n(1..$FFDC_LOG_N) { 
        open ($FFDC_C_ARR[$n], ">$FFDC_DIR/ffdc_log.$n.c") || die ("Unable to open $FFDC_DIR/ffdc_log.$n.c\n");
        _sitSourceAddHeader_short($FFDC_C_ARR[$n]);
    }
    
    @msgid_ranges = sort { $a <=> $b } keys %FFDC_RANGES;
    
    $range_ndx = 0;
    foreach $key (sort { $a <=> $b } keys %FFDC_MSGIDS) {
        $msg = _sitUnescape($FFDC_MSGIDS{$key});
        
#        $comment = "/*---------------------------------------------------------------------------\n";
#        $comment .= " * MsgID : $key\n";
#        $comment .= " * Msg   : \"" . _sitWrapLogMsgComment($msg) . "\"\n";
#        $comment .= " *-------------------------------------------------------------------------*/\n";
        
#       print FFDC_C $comment;
#       print FFDC_H $comment;
#       print READER $comment;
        
        $ret = _sitArgsToStruct($key, $FFDC_MSGIDS{$key});
        if ($ret > 0) {
            # error in parsing
        }
        
        # write to message catalog
        while ($key >= $msgid_ranges[$range_ndx]) {
            print FFDC_MC "\n\n";
            print FFDC_MC "# $FFDC_RANGES{$msgid_ranges[$range_ndx]}\n";
            $range_ndx++;
        }
        
        print FFDC_MC "$key $msg\n";
    }
    
    _sitHeaderAddFooter();
    _sitReaderAddFooter();
    
    close FFDC_C;
    close FFDC_H;
    close FFDC_MC;
    close READER;

    for $n(1..$FFDC_LOG_N) { 
        close $FFDC_C_ARR[$n];
    }
    
    system("mv", "$FFDC_DIR/${FFDC_MSGCAT_FILE}.ffdc_modified", "$FFDC_DIR/${FFDC_MSGCAT_FILE}");
}

sub _sitSearchDir 
{
    my($dir)=(@_);
    my($out, $file, @file_list);

    unless (-e $dir) { 
        _sitLogError("Cannot find $dir"); 
        return; 
    }

    if (-d "$dir") {
        if (-l "$dir") {
            return;
        }
        
        $out = _sitRunCmd("ls $dir 2> /dev/null | grep -v '^Make'");
        @file_list = split(/\s+/, $out);
        for $file(@file_list) { 
            if ($file eq "." || $file eq "..") {
                next;
            } elsif (-d "$dir/$file") {
                if (! -l "$dir/$file") {
                    _sitSearchDir("$dir/$file");
                }
            } else {
                unless ($file =~ /\.[ch]$/) {
                    next;
                }
                
                if (_sitMatchesInList("$dir/$file", @SKIPFILES) == 1) {
                    _sitLogDebug("Skipping $dir/$file");
                    next;
                }
    
                _sitParseFile("$dir/$file"); 
            }
        }
    } else {
        _sitParseFile("$dir");
    }
}

sub _sitHeaderAddHeader
{
print FFDC_H << "EOF";
/*------------------------------------------------------------------------
 * AUTO GENERATED: DO NOT CHECK INTO SVN
 *
 * This file is automatically generated by ffdc_parser.pl.  Please do not
 * modify this file since your changes will be lost once ffdc_parser.pl
 * is run.
 *
 * This file defines the corresponding log structure for each unique
 * log message in the codebase.  The structure contains one field for
 * each message parameter in the log message, along with other meta data
 * 
 * Generated On: $TIMESTAMP
 *------------------------------------------------------------------------*/

#ifndef PLATFORM_FFDC_H
#define PLATFORM_FFDC_H 1

#define FFDC_VERSION 1

/*
 * FIXME: drew 2010-01-20 This needs to beome a runtime option which 
 * may be separate from _ffdc_info which is in shared memory.
 */
#ifndef FFDC_DEFAULT_MIN_LOG_LEVEL
#define FFDC_DEFAULT_MIN_LOG_LEVEL PLAT_LOG_LEVEL_TRACE
#endif

#define FFDC_MAX_BUFFERS 32
#define FFDC_THREAD_BUFSIZE (8 << 20) /* MBs */
#define FFDC_STR_TRAILER "..."
#define FFDC_MAX_STR_SIZE $MAX_STRING_SIZE 
#define FFDC_MAGIC_NUMBER (0xF3A5EC9C)
#define FFDC_LONG_STRING(len) "s"

#define ffdc_log(lineno, mid, cid, lvl, fmt, args...)                  \\
    if (lvl >= FFDC_DEFAULT_MIN_LOG_LEVEL) {                           \\
        ffdc_log_func_ ## mid(lineno, mid, cid, lvl, fmt, ##args);     \\
    }

#ifdef notyet
#define FFDC_START_FAIL() return
#else
#define FFDC_START_FAIL() return
#endif

#define FFDC_START(m, c, l, lineno)                                    \\
    struct ffdc_log_struct_ ## m *lms = NULL;                          \\
    uint64_t offset = 0;                                               \\
    plat_shmem_ptr_base_t shared_ptr;                                  \\
                                                                       \\
    if (_ffdc_info == NULL || _ffdc_info->enabled == 0) {              \\
        /* Not yet initialized or not enabled */                       \\
        return;                                                        \\
    }                                                                  \\
                                                                       \\
    if (_ffdc_thread_ndx == -1 &&                                      \\
       (_ffdc_info->count < FFDC_MAX_BUFFERS)) {                       \\
        _ffdc_thread_ndx =                                             \\
            __sync_fetch_and_add(&(_ffdc_info->count), 1);             \\
                                                                       \\
        if (_ffdc_thread_ndx >= FFDC_MAX_BUFFERS) {                    \\
            /* Too many buffers allocated */                           \\
            fprintf(stderr,                                            \\
                "ffdc: pth[%lx] Too many FFDC buffers allocated "      \\
                "(%d of %d buffers)\\n",                                \\
                pthread_self(), _ffdc_thread_ndx, FFDC_MAX_BUFFERS);   \\
            _ffdc_thread_ndx = -1;                                     \\
            FFDC_START_FAIL();                                         \\
        }                                                              \\
                                                                       \\
        shared_ptr =                                                   \\
            plat_shmem_steal_from_heap(_ffdc_info->buffer_len);        \\
        if (shared_ptr.int_base == 0) {                                \\
            struct plat_shmem_alloc_stats stats;                       \\
            fprintf(stderr,                                            \\
                "ffdc: pth[%lx] Cannot allocate %ld bytes (%d of %d)\\n", \\
                pthread_self(), _ffdc_info->buffer_len,                \\
                _ffdc_thread_ndx, FFDC_MAX_BUFFERS);                   \\
            if (!plat_shmem_alloc_get_stats(&stats)) {                 \\
                fprintf(stderr,                                        \\
                        "ffdc: pth[%lx] %ld of %ld shmem bytes used\\n",\\
                        pthread_self(), (long)stats.allocated_bytes +  \\
                        stats.stolen_bytes, (long)stats.total_bytes);  \\
            }                                                          \\
            _ffdc_thread_ndx = -1;                                     \\
            FFDC_START_FAIL();                                         \\
        }                                                              \\
                                                                       \\
        _ffdc_info->shm[_ffdc_thread_ndx] = (char *)                   \\
            plat_shmem_ptr_base_to_ptr(shared_ptr);                    \\
                                                                       \\
        _ffdc_thread_offset = 0;                                       \\
                                                                       \\
    } else if (_ffdc_thread_ndx == -1) {                               \\
        /* We've exceeded max FFDC buffers or we've exhausted  */      \\
        /* available space in the shared memory backing file;  */      \\
        /* the case where plat_shmem_steal_from_heap() fails   */      \\
        return;                                                        \\
    }                                                                  \\
                                                                       \\
    offset = _ffdc_thread_offset;                                      \\
    _ffdc_thread_offset += sizeof(struct ffdc_log_struct_ ## m);       \\
                                                                       \\
    offset %= _ffdc_info->buffer_len;                                  \\
    if ((offset + sizeof(struct ffdc_log_struct_ ## m)) >              \\
        _ffdc_info->buffer_len) {                                      \\
                                                                       \\
        /* FIXME:this msg can't fit in the remaining space */          \\
        /* so just skip msg and return                     */          \\
                                                                       \\
        /* clear out any magic numbers from the struct     */          \\
        /* remainder portion at the start of the buffer    */          \\
        memset(&(_ffdc_info->shm[_ffdc_thread_ndx][0]), 0,             \\
            sizeof(struct ffdc_log_struct_ ## m) -                     \\
            (_ffdc_info->buffer_len - offset));                        \\
                                                                       \\
        return;                                                        \\
    }                                                                  \\
                                                                       \\
    lms = (struct ffdc_log_struct_ ## m *)                             \\
        &(_ffdc_info->shm[_ffdc_thread_ndx][offset]);                  \\
    lms->magic = FFDC_MAGIC_NUMBER;                                    \\
    lms->msgid = m;                                                    \\
    lms->catid = c;                                                    \\
    lms->level = l;                                                    \\
    lms->lineno = lineno;                                              \\
    lms->th_id = (unsigned long) pthread_self();                       \\
    lms->fth_id = (unsigned long) fthSelf();                           \\
    plat_log_gettimeofday(&(lms->timestamp))
    
#define FFDC_ADD_STR(arg)                                              \\
    strncpy(lms->arg, arg, sizeof(lms->arg));                          \\
    memcpy(&(lms->arg[sizeof(lms->arg) - sizeof(FFDC_STR_TRAILER)]),   \\
        FFDC_STR_TRAILER, sizeof(FFDC_STR_TRAILER))

#define FFDC_ADD_NUM(arg)                                              \\
    lms->arg = arg

#define FFDC_ADD_PTR(arg)                                              \\
    lms->arg = (long)arg
    
#define FFDC_END()

struct ffdc_info {
    int version;
    int enabled;
    int min_log_level;
    int count;
    int pid;
    time_t created;
    int mcd_bld_version;
    long buffer_len;
    /* RFC 2181 specfies a maximum DNS name length of 255 characters */
    char hostname[256];
    /* XXX: drew 2009-12-2009 Should encode number of buffers here */
    char *shm[FFDC_MAX_BUFFERS];
};

__BEGIN_DECLS

__inline__ struct ffdc_info *get_ffdc_info();

int ffdc_initialize(int readonly, int bld_version, long buffer_len);

void ffdc_enable();
void ffdc_disable();
void ffdc_detach();
void ffdc_set_min_log_level(int level);
__inline__ int ffdc_get_min_log_level();

#define ffdc_log_func_0(msgid, catid, level, fmt, args...);

EOF
}

sub _sitHeaderAddFooter
{
print FFDC_H << "EOF";
__END_DECLS
#endif /* PLATFORM_FFDC_H */

EOF
}

sub _sitSourceAddHeader
{
print FFDC_C << "EOF";
/*------------------------------------------------------------------------
 * AUTO GENERATED: DO NOT CHECK INTO SVN
 *
 * This file is automatically generated by ffdc_parser.pl.  Please do not
 * modify this file since your changes will be lost once ffdc_parser.pl
 * is run.
 *
 * This file defines one log function for each unique log message in the
 * codebase. This file also contains the API to enable, disable, and
 * initialize FFDC.
 * 
 * Generated On: $TIMESTAMP
 *------------------------------------------------------------------------*/

#define PLATFORM_INTERNAL 1

#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/shmem_global.h"
#include "platform/platform.h"
#include "platform/types.h"
#include "platform/unistd.h"
#include "platform/$FFDC_HEADER_FILE"
#include "fth/fth.h"

struct ffdc_info *_ffdc_info = NULL;

__thread uint64_t _ffdc_thread_offset = -1;
__thread int _ffdc_thread_ndx = -1;

static void 
ffdc_at_fork(void *extra,  pid_t pid) {
    if (_ffdc_info) {
    _ffdc_info->pid = plat_getpid();
    }
}

PLAT_AT_FORK(ffdc, ffdc_at_fork, NULL)

int
ffdc_initialize(int readonly, int bld_version, long buffer_len)
{
    plat_shmem_ptr_base_t shared_ptr;
    // uint64_t existing = 0;
    uint64_t base_addr  = 0;
    struct timeval tv;
    int status;
    
    base_addr = shmem_global_get(SHMEM_GLOBAL_FFDC);
    if (base_addr == 0) {
        if (readonly) {
            /* don't allocate memory if in readonly mode */
            return 1;
        }

        shared_ptr = plat_shmem_steal_from_heap(sizeof(struct ffdc_info));
        if (shared_ptr.int_base == 0) {
            /* memory allocation failed */
            return 1;
        }
        // existing = shmem_global_set(SHMEM_GLOBAL_FFDC, shared_ptr.int_base);
        if (shmem_global_set(SHMEM_GLOBAL_FFDC, shared_ptr.int_base)) {}
        _ffdc_info = (struct ffdc_info *) plat_shmem_ptr_base_to_ptr(shared_ptr);

    memset(_ffdc_info, 0, sizeof (*_ffdc_info));
        _ffdc_info->enabled = 1;
        _ffdc_info->version = FFDC_VERSION;
        _ffdc_info->pid = plat_getpid();
        _ffdc_info->min_log_level = FFDC_DEFAULT_MIN_LOG_LEVEL;
        _ffdc_info->mcd_bld_version = bld_version;
    _ffdc_info->buffer_len = buffer_len;
        fthGetTimeOfDay(&tv);
        _ffdc_info->created = tv.tv_sec;

    status = gethostname(_ffdc_info->hostname, sizeof(_ffdc_info->hostname));
    if (status == -1) {
        fprintf(stderr, "Can't get hostname: %s\\n", sys_strerror(errno));
    }
    } else {
        shared_ptr.int_base = base_addr;
        _ffdc_info = (struct ffdc_info *) plat_shmem_ptr_base_to_ptr(shared_ptr);
    }
    
    return 0;
}

__inline__ struct ffdc_info *
get_ffdc_info()
{
    return _ffdc_info;
}

__inline__ int
ffdc_get_min_log_level()
{
    return _ffdc_info->min_log_level;
}

void
ffdc_set_min_log_level(int level)
{
    _ffdc_info->min_log_level = level;
}

void
ffdc_enable()
{
    _ffdc_info->enabled = 1;
}

void
ffdc_disable()
{
    _ffdc_info->enabled = 0;
}

void
ffdc_detach()
{
    ffdc_disable();
    /*
     * XXX: drew 2010-04-20 Note that this can be called at most once 
     * because of the thread-local buffer pointers not being reset.
     */
    _ffdc_info = NULL;
}

EOF
}

sub _sitSourceAddHeader_short
{
  my ($FF) = @_;

  my $header = << "EOF";
/*------------------------------------------------------------------------
 * AUTO GENERATED: DO NOT CHECK INTO SVN
 *
 * This file is automatically generated by ffdc_parser.pl.  Please do not
 * modify this file since your changes will be lost once ffdc_parser.pl
 * is run.
 *
 * This file defines one log function for each unique log message in the
 * codebase. This file also contains the API to enable, disable, and
 * initialize FFDC.
 * 
 * Generated On: $TIMESTAMP
 *------------------------------------------------------------------------*/

#define PLATFORM_INTERNAL 1

#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/shmem_global.h"
#include "platform/platform.h"
#include "platform/types.h"
#include "platform/unistd.h"
#include "platform/$FFDC_HEADER_FILE"
#include "fth/fth.h"

extern struct ffdc_info *_ffdc_info;

extern __thread uint64_t _ffdc_thread_offset;
extern __thread int _ffdc_thread_ndx;

EOF

print $FF $header;

}

sub _sitReaderAddHeader
{
print READER << "EOF";
/*------------------------------------------------------------------------
 * AUTO GENERATED: DO NOT CHECK INTO SVN
 *
 * This file is automatically generated by ffdc_parser.pl.  Please do not
 * modify this file since your changes will be lost once ffdc_parser.pl
 * is run.
 * 
 * This file implements the utility to parse the binary FFDC buffer and
 * displays human-readable log messages
 *
 * Generated On: $TIMESTAMP
 *------------------------------------------------------------------------*/

#include <limits.h>
#include <inttypes.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "misc/misc.h"

#include "platform/assert.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _ffdc_reader
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/shmem_global.h"
#include "platform/stdlib.h"
#include "platform/string.h"

/* Command line arguments */
#define PLAT_OPTS_ITEMS_ffdc_reader()                                         \\
    item("filter", "filter using --log options", FILTER,                      \\
         ({ config->filter = 1; 0; }), PLAT_OPTS_ARG_NO)                      \\
    item("strings", "use string category names", STRINGS,                     \\
         ({ config->strings = 1; 0; }), PLAT_OPTS_ARG_NO)                     \\
    item("debug", "turn on debugging", DEBUG,                                 \\
         ({ config->debug = 1; 0; }), PLAT_OPTS_ARG_NO)                       \\
    item("hostname", "log hostname (useful for multi-node)", HOSTNAME,        \\
         ({ config->hostname = 1; 0; }), PLAT_OPTS_ARG_NO)                    \\
    item("version", "display ffdc_reader version", READER_VERSION,            \\
         ({ config->version = 1; 0; }), PLAT_OPTS_ARG_NO)                     \\
    item("core", "core file to process", COREFILE,                            \\
         parse_string_alloc(&config->core_file, optarg, PATH_MAX),            \\
         PLAT_OPTS_ARG_REQUIRED)                                              \\
    item("core_offset", "offset into core where to start processing",         \\
         CORE_OFFSET, parse_uint64(&config->core_offset, optarg, NULL),       \\
         PLAT_OPTS_ARG_REQUIRED)                                              \\
    PLAT_OPTS_SHMEM(shmem)

struct plat_opts_config_ffdc_reader { 
    struct plat_shmem_config shmem;
    unsigned filter : 1;
    unsigned strings : 1;
    unsigned debug : 1;
    unsigned hostname : 1;
    unsigned version : 1;
    char* core_file;
    uint64_t core_offset;
};

EOF
}

sub _sitReaderAddFooter
{
    my($ndx);
    
    print READER "char*\ndisplay_log_msg(int msgid, char *shmem, struct plat_opts_config_ffdc_reader *config)\n"; 
    print READER "{\n";
    print READER "    char *p = NULL;\n";
    print READER "    switch(msgid) {\n";
    foreach $ndx (sort { $a <=> $b } keys %FFDC_MSGIDS) {
        print READER "    case $ndx:\n";
        print READER "        p = ffdc_printf_func_${ndx}(shmem, config);\n";
        print READER "        break;\n\n";
    }
    print READER "    default:\n";
    print READER "        fprintf(stderr, \"ERROR: Unhandled msgid [%d]\\n\", msgid);\n";
    print READER "        p = shmem + sizeof(int);\n";
    print READER "        break;\n";
    print READER "    }\n";
    print READER "    return p;\n}\n";
    
print READER << "EOF";

struct ffdc_header {
    int magic;
    int msgid;
};

void process_buffer(struct ffdc_info *infop,
            struct plat_opts_config_ffdc_reader *config) 
{
    struct ffdc_header *fp = NULL;
    int header = 0;
    int ndx = 0;
    char* p = NULL;
    char* data_start;
    long buffer_total;

    fp = (struct ffdc_header*) p;

    for (ndx = 0; ndx < infop->count; ndx++) {
        buffer_total = 0;
        data_start = p = infop->shm[ndx];
        while (p && p < (infop->shm[ndx] + infop->buffer_len)) {
            fp = (struct ffdc_header*) p;
            if (fp->magic == FFDC_MAGIC_NUMBER) {
                if (config->debug) {
                    printf("[%d] ", ndx);
                }
                p = display_log_msg(fp->msgid, p, config);
                header = 0;
            } else {
                if (header == 0) {
                    printf("[FFDC_BREAK] after %ldM  "
                           "============================================\\n",
                           (long)((p - data_start)/(1024 * 1024)));
                    header = 1;
                    buffer_total += (p - data_start);
                }
                p++;
                data_start = p;
            }    
        }
        printf("FFDC buffer %d at %p %ldM used\\n", ndx, infop->shm[ndx],
               buffer_total / (1024 * 1024));
    }
}

void handler(int signal)
{
    fprintf(stderr, 
           "Unable to proceed with FFDC display at this time (%d). "
           "Please try running the command again. Exiting...\\n", signal);
    plat_exit(1);
}   

void display_version()
{
    printf("version: %d\\n", BLD_VERSION);
}

int process_core(struct plat_opts_config_ffdc_reader *config)
{
    int fd = 0;
    char *map = NULL;
    struct ffdc_header *fp = NULL;
    int header = 0;
    int ret = 0;
    char* p = NULL;
    uint64_t file_size = 0;
    struct stat stat_buf;
    struct tm* ltimep = NULL;
    char timebuf[256];
    time_t epoch_time;

    fd = open(config->core_file, O_RDONLY);
    if (fd == -1) {
        perror("Error opening core file for reading");
        return (1);
    }
    printf("Processing core file : %s\\n", config->core_file);

    /* get size of corefile */
    ret = stat(config->core_file, &stat_buf);
    if (ret != 0) {
        perror("Unable to determine size of core file");
        return (1);
    }
    file_size = stat_buf.st_size;
    printf("Core file size       : %lu\\n", file_size);

    if (config->core_offset > 0) {
        printf("Core file offset     : %lu\\n", config->core_offset);
    }

    /* Now the file is ready to be mmapped */
    map = mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        close(fd);
        perror("Error mmapping core file");
        return (1);
    }

    p = &(map[config->core_offset]);
    while (p && p < (map + file_size)) {
        fp = (struct ffdc_header*) p;
        if (fp->magic == FFDC_MAGIC_NUMBER) {
            p = display_log_msg(fp->msgid, p, config);
            header = 0;
        } else {
            if ((unsigned long) p % (1024 * 1024 * 1024) == 0) {
                time(&epoch_time);
                ltimep = localtime(&(epoch_time));
                if (ltimep) {
                    strftime(timebuf, sizeof(timebuf), "%H:%M:%S", ltimep);
                    printf("[%012lu] %s\\n", (p - map), timebuf);
                } else {
                    printf("[%012lu]\\n", (p - map));
                }
                fflush(stdout);
            }
            p++;
        }
    }

    return 0;
}

int process_shmem(struct plat_opts_config_ffdc_reader *config)
{
    int status;
    struct ffdc_info *infop = NULL;
    int ndx = 0;
    char buf[256];
    struct tm* ltimep = NULL;

    status = plat_shmem_attach(plat_shmem_config_get_path(&(config->shmem)));
    if (status) {
        fprintf(stderr, 
                "Unable to attach to shared memory (ret = %d). "
                "Please try running the command again. Exiting...\\n", 
                status);
        return (1);
    }
    
    if (ffdc_initialize(1, 0, FFDC_THREAD_BUFSIZE) != 0) {
        fprintf(stderr,
                "Unable to locate FFDC buffers in shared memory. "
                "Please specify a valid shared memory backing file.\\n");
        return (1);
    }
    
    infop = get_ffdc_info();
    if (infop) {
        ltimep = localtime(&(infop->created));
        if (ltimep) {
            strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", ltimep);
        } else {
            sprintf(buf, "%lu", (unsigned long)infop->created);
        }

        printf("===============================================\\n");
        printf("Memcached PID           : %d\\n", infop->pid);
        printf("Memcached Build Version : %d\\n", infop->mcd_bld_version);
        printf("Hostname                : %s\\n", infop->hostname);
        printf("FFDC created            : %s\\n", buf);
        printf("FFDC ffdc_info address  : %p\\n", infop);
        printf("FFDC version            : %d\\n", infop->version);
        printf("FFDC enabled            : %d\\n", infop->enabled);
        printf("FFDC minimum log level  : %d\\n", infop->min_log_level);
        printf("FFDC buffer len         : %ldM\\n", infop->buffer_len >> 20);
        printf("FFDC buffers allocated  : %d of %d\\n", 
           infop->count, FFDC_MAX_BUFFERS);
        for (ndx = 0; ndx < FFDC_MAX_BUFFERS; ndx++ ) {
            printf("    ffdc[%d] : %p\\n", ndx, infop->shm[ndx]);
        }
        printf("===============================================\\n");
        process_buffer(infop, config);
    }
    
    status = plat_shmem_detach();

    return (0);
}

int main(int argc, char* argv[])
{
    int ret = 0;
    struct plat_opts_config_ffdc_reader config;
 
    /* 
     * If the reader is invoked while memcached is running, the reader may
     * come across a incomplete record (memcached may be in the process of
     * writing the record). This will result in a segv. Let's gracefully 
     * handle these signals and exit the reader.
     */
    signal(SIGINT, handler);
    signal(SIGSEGV, handler);
    signal(SIGABRT, handler);
    signal(SIGTERM, handler);
    
    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);
    config.core_offset = 0;
    config.core_file = NULL;

    if (plat_opts_parse_ffdc_reader(&config, argc, argv)) {
        return (2);
    }

    if (config.version == 1) {
        display_version();
        return(0);
    }

    if (argc < 2) {
        printf("Usage:\\n");
        printf("\\t%s --plat/shmem/file <shmem file>\\n", argv[0]);
        printf("\\t%s --core <core file> [--core_offset <offset>]\\n\\n", argv[0]);
        return (1);
    }

    if (config.core_file == NULL) {
        ret = process_shmem(&config);
    } else {
        ret = process_core(&config);
    }

    plat_shmem_config_destroy(&config.shmem);

    return ret;
}

#include "platform/opts_c.h"

EOF
}

#--------------------------------------------------------------------
# Start of Script
#--------------------------------------------------------------------
my($d);

open (FFDC_DBG, ">$FFDC_DIR/$FFDC_DEBUG_FILE") || 
    die ("Unable to open $FFDC_DIR/$FFDC_DEBUG_FILE\n");

$TIMESTAMP = _sitGetTimestamp();

_sitReadConfig();
_sitReadMC("$FFDC_DIR/$FFDC_MSGCAT_FILE");
_sitInitRangeForUser($WHOAMI);

_sitLogInfo("Top level source directory is:\n    $PATH");
for $d(@DIRS) {
    _sitSearchDir("$PATH/$d"); 
}

print "\n";
_sitLogInfo("Parsed " . (scalar(keys %FFDC_MSGIDS)) . " log messages");
if (_sitNeedToCreateFiles()) {
    _sitCreateFFDCSourceFiles();
    _sitLogInfo("Added $NEW_MSG_CNT new log message(s) to the message catalog\n");
} else {
    _sitLogInfo("No new log messages found. Skipping re-generation of FFDC source files...\n");
}
