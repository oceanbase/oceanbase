#!/usr/bin/env perl
# Copyright 2016 Alibaba Inc. All Rights Reserved.
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# version 2 as published by the Free Software Foundation.
# create date: 06 Nov 2013
# description: script to generate ob_errno.h from ob_errno.def

use strict;
use warnings;
open my $fh, '<', "ob_errno.def";
my %map_share;
my %other_map_share;
my %map_deps;
my %other_map_deps;
my %map;
my %other_map;
my $last_errno = 0;
my $error_count=0;
my $def_ora_errno=600;
my $def_ora_errmsg="\"internal error code, arguments: %d, %s\"";
my $print_def_ora_errmsg="\"%s-%05d: internal error code, arguments: %d, %s\"";
my $print_ora_errmsg="\"%s-%05d: %s\"";
my $print_error_cause="\"Internal Error\"";
my $print_error_solution="\"Contact OceanBase Support\"";

while(<$fh>) {
  my $error_msg;
  my $sqlstate;
  my $error_code;

  if (/^DEFINE_ERROR\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7\n";
    my $tmp_ora_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($5, 1, length($5) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $5, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_errmsg, $6, $7];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_errmsg, $6, $7];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ERROR\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5\n";
    my $tmp_ora_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($5, 1, length($5) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $5, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ERROR_EXT\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8\n";
    my $tmp_ora_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($5, 1, length($5) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($6, 1, length($6) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $6, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $7, $8];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $7, $8];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ERROR_EXT\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6\n";
    my $tmp_ora_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($5, 1, length($5) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($6, 1, length($6) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $6, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9\n";
    #print "\"$1\", $6, $7\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "ORA", $6, substr($7, 1, length($7) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $8, $9];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $8, $9];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7\n";
    #print "\"$1\", $6, $7\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "ORA", $6, substr($7, 1, length($7) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR_EXT\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11\n";
    #print "\"$1\", $7, $8, $9\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "ORA", $7, substr($8, 1, length($8) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_ora_errmsg, "ORA", $7, substr($9, 1, length($9) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $10, $11];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $10, $11];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR_EXT\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9\n";
    #print "\"$1\", $7, $8, $9\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "ORA", $7, substr($8, 1, length($8) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_ora_errmsg, "ORA", $7, substr($9, 1, length($9) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_OTHER_MSG_FMT\(([^,]+),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")/) {
    #print "\"$1\", $1, $2, $3, $4\n";
    $other_map_share{$1} = [$2, $3, $4];
    $other_map{$1} = [$2, $3, $4];
  } elsif (/^DEFINE_PLS_ERROR\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9\n";
    #print "\"$1\", $6, $7\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "PLS", $6, substr($7, 1, length($7) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $8, $9];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $8, $9];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_PLS_ERROR\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7\n";
    #print "\"$1\", $6, $7\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "PLS", $6, substr($7, 1, length($7) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_PLS_ERROR_EXT\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11\n";
    #print "\"$1\", $7, $8, $9\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "PLS", $7, substr($8, 1, length($8) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_ora_errmsg, "PLS", $7, substr($9, 1, length($9) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $10, $11];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $10, $11];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_PLS_ERROR_EXT\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9\n";
    #print "\"$1\", $7, $8, $9\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "PLS", $7, substr($8, 1, length($8) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_ora_errmsg, "PLS", $7, substr($9, 1, length($9) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR_V2_EXT\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11\n";
    #print "\"$1\", $7, $8, $9\n";
    my $tmp_ora_errmsg=sprintf("\"%s\"", substr($8, 1, length($8) - 2));
    my $tmp_ora_user_errmsg=sprintf("\"%s\"", substr($9, 1, length($9) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $10, $11];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $10, $11];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR_V2_EXT\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9\n";
    #print "\"$1\", $7, $8, $9\n";
    my $tmp_ora_errmsg=sprintf("\"%s\"", substr($8, 1, length($8) - 2));
    my $tmp_ora_user_errmsg=sprintf("\"%s\"", substr($9, 1, length($9) - 2));
    $map_share{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ERROR_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7\n";
    my $tmp_ora_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($5, 1, length($5) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $5, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_errmsg, $6, $7];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_errmsg, $6, $7];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ERROR_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5\n";
    my $tmp_ora_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($5, 1, length($5) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $5, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ERROR_EXT_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8\n";
    my $tmp_ora_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($5, 1, length($5) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($6, 1, length($6) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $6, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $7, $8];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $7, $8];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ERROR_EXT_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6\n";
    my $tmp_ora_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($5, 1, length($5) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_def_ora_errmsg, "ORA", $def_ora_errno, $2, substr($6, 1, length($6) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $6, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $def_ora_errno, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9\n";
    #print "\"$1\", $6, $7\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "ORA", $6, substr($7, 1, length($7) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $8, $9];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $8, $9];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7\n";
    #print "\"$1\", $6, $7\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "ORA", $6, substr($7, 1, length($7) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR_EXT_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*"),\s*("[^"]*"),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11\n";
    #print "\"$1\", $7, $8, $9\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "ORA", $7, substr($8, 1, length($8) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_ora_errmsg, "ORA", $7, substr($9, 1, length($9) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $10, $11];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $10, $11];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR_EXT_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9\n";
    #print "\"$1\", $7, $8, $9\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "ORA", $7, substr($8, 1, length($8) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_ora_errmsg, "ORA", $7, substr($9, 1, length($9) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg, $print_error_cause, $print_error_solution];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_OTHER_MSG_FMT_DEP\(([^,]+),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")/) {
    #print "\"$1\", $1, $2, $3, $4\n";
    $other_map_deps{$1} = [$2, $3, $4];
    $other_map{$1} = [$2, $3, $4];
  } elsif (/^DEFINE_PLS_ERROR_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7\n";
    #print "\"$1\", $6, $7\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "PLS", $6, substr($7, 1, length($7) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg];
    $map{$1} = [$2, $3, $4, $5, $5, "$1", $6, $tmp_ora_errmsg, $tmp_ora_errmsg];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_PLS_ERROR_EXT_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9\n";
    #print "\"$1\", $7, $8, $9\n";
    my $tmp_ora_errmsg=sprintf($print_ora_errmsg, "PLS", $7, substr($8, 1, length($8) - 2));
    my $tmp_ora_user_errmsg=sprintf($print_ora_errmsg, "PLS", $7, substr($9, 1, length($9) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  } elsif (/^DEFINE_ORACLE_ERROR_V2_EXT_DEP\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")\s*,\s*([^,]*),\s*("[^"]*")\s*,\s*("[^"]*")/) {
    ++$error_count;
    #print "\"$1\", $1, $2, $3, $4, $5, $6, $7, $8, $9\n";
    #print "\"$1\", $7, $8, $9\n";
    my $tmp_ora_errmsg=sprintf("\"%s\"", substr($8, 1, length($8) - 2));
    my $tmp_ora_user_errmsg=sprintf("\"%s\"", substr($9, 1, length($9) - 2));
    $map_deps{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg];
    $map{$1} = [$2, $3, $4, $5, $6, "$1", $7, $tmp_ora_errmsg, $tmp_ora_user_errmsg];
    $last_errno = $2 if ($2 < $last_errno);
    $error_code = $2;
    $sqlstate = $4;
    $error_msg = $5;
  }
  if (defined $error_code) {
    print "WARN: undefined SQLSTATE for $1\n" if ($sqlstate eq "\"\"");
    print "WARN: undefined error message for $1\n" if ($error_msg eq "\"\"");
    print "WARN: error code out of range: $1\n" if ($error_code <= -1 && $error_code > -3000);
  }
}

print "total error code: $error_count\n";
print "please wait for writing files ...\n";
# check duplicate error number
my %dedup;
for my $oberr (keys % map) {
  my $errno = $map{$oberr}->[0];
  if (defined $dedup{$errno})
  {
    print "Error: error code($errno) is duplicated for $oberr and $dedup{$errno}\n";
    exit 1;
  } else {
    $dedup{$errno} = $oberr;
  }
}

# sort for share
my @pairs_share = map {[$_, $map_share{$_}->[0] ]} keys %map_share;
my @sorted_share = sort {$b->[1] <=> $a->[1]} @pairs_share;
my @errors_share = map {$_->[0]} @sorted_share;

# sort for deps
my @pairs_deps = map {[$_, $map_deps{$_}->[0] ]} keys %map_deps;
my @sorted_deps = sort {$b->[1] <=> $a->[1]} @pairs_deps;
my @errors_deps = map {$_->[0]} @sorted_deps;

# sort for all
my @pairs = map {[$_, $map{$_}->[0] ]} keys %map;
my @sorted = sort {$b->[1] <=> $a->[1]} @pairs;
my @errors = map {$_->[0]} @sorted;
my @errnos = reverse sort { $a <=> $b } map {$map{$_}->[0]} keys %map;

# generate share/ob_errno.h
open my $fh_header, '>', "ob_errno.h";
print $fh_header '
// DO NOT EDIT. This file is automatically generated from `ob_errno.def\'.

// Copyright 2016 Alibaba Inc. All Rights Reserved.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// version 2 as published by the Free Software Foundation.
// ob_errno.h
//   Author:
//   Normalizer:

#ifndef OCEANBASE_LIB_OB_ERRNO_H_
#define OCEANBASE_LIB_OB_ERRNO_H_
#include <stdint.h>
#include "share/mysql_errno.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{';
  print $fh_header "
constexpr int OB_LAST_ERROR_CODE = $last_errno;
constexpr int OB_ERR_SQL_START = -5000;
constexpr int OB_ERR_SQL_END = -5999;
";
  for my $oberr (@errors_share) {
    if (system "grep $oberr ../../deps/oblib/src/lib/ob_errno.h >/dev/null") {
      print $fh_header "constexpr int $oberr = $map_share{$oberr}->[0];\n";
    }
  }
  foreach my $oberr (keys %other_map){
    if (system "grep $oberr ../../deps/oblib/src/lib/ob_errno.h >/dev/null") {
      my $errno;
      if (exists($map{$other_map{$oberr}->[0]})){
        $errno = $map{$other_map{$oberr}->[0]}->[0];
      } else {
        print "Error: error code($other_map{$oberr}->[0]) is not exists\n";
        exit 1;
      }
      print $fh_header "constexpr int $oberr = $errno;\n";
    }
  }
  print $fh_header "\n\n";
  for my $oberr (@errors) {
    print $fh_header "#define ${oberr}__USER_ERROR_MSG $map{$oberr}->[4]\n";
  }
  foreach my $oberr (keys %other_map){
    print $fh_header "#define ${oberr}__USER_ERROR_MSG $other_map{$oberr}->[1]\n";
  }
  print $fh_header "\n\n";
  for my $oberr (@errors) {
    print $fh_header "#define ${oberr}__ORA_USER_ERROR_MSG $map{$oberr}->[8]\n";
  }
  foreach my $oberr (keys %other_map){
    my $ora_errno;
    if (exists($map{$other_map{$oberr}->[0]})){
      $ora_errno = $map{$other_map{$oberr}->[0]}->[6];
    } else {
      print "Error: error code($other_map{$oberr}->[0]) is not exists\n";
      exit 1;
    }
    my $ora_msg=$other_map{$oberr}->[2];
    my $tmp_ora_user_errmsg=sprintf($print_ora_errmsg, "ORA", $ora_errno, substr($ora_msg, 1, length($ora_msg) - 2));
    print $fh_header "#define ${oberr}__ORA_USER_ERROR_MSG $tmp_ora_user_errmsg\n";
  }

  print $fh_header "\nextern int g_all_ob_errnos[${\(scalar @errnos)}];";

  print $fh_header '

  const char *ob_error_name(const int oberr);
  const char* ob_error_cause(const int oberr);
  const char* ob_error_solution(const int oberr);

  int ob_mysql_errno(const int oberr);
  int ob_mysql_errno_with_check(const int oberr);
  const char *ob_sqlstate(const int oberr);
  const char *ob_strerror(const int oberr);
  const char *ob_str_user_error(const int oberr);

  int ob_oracle_errno(const int oberr);
  int ob_oracle_errno_with_check(const int oberr);
  const char *ob_oracle_strerror(const int oberr);
  const char *ob_oracle_str_user_error(const int oberr);

#ifndef __ERROR_CODE_PARSER_
  int get_ob_errno_from_oracle_errno(const int error_no, const char *error_msg, int &ob_errno);
#endif
  int ob_errpkt_errno(const int oberr, const bool is_oracle_mode);
  const char *ob_errpkt_strerror(const int oberr, const bool is_oracle_mode);
  const char *ob_errpkt_str_user_error(const int oberr, const bool is_oracle_mode);


} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_LIB_OB_ERRNO_H_
';

#generate dep/ob_errno.h
open my $fh_header_dep, '>', "../../deps/oblib/src/lib/ob_errno.h";
print $fh_header_dep '/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// DO NOT EDIT. This file is automatically generated from ob_errno.def.
// DO NOT EDIT. This file is automatically generated from ob_errno.def.
// DO NOT EDIT. This file is automatically generated from ob_errno.def.
// To add errno in this header file, you should use DEFINE_***_DEP to define errno in ob_errno.def
// For any question, call fyy280124
#ifndef OB_ERRNO_H
#define OB_ERRNO_H

namespace oceanbase {
namespace common {

constexpr int OB_MAX_ERROR_CODE                      = 65535;
';

for my $oberr (@errors_deps) {
  print $fh_header_dep "\nconstexpr int $oberr = $map_deps{$oberr}->[0];";
}

print $fh_header_dep '
constexpr int OB_MAX_RAISE_APPLICATION_ERROR         = -20000;
constexpr int OB_MIN_RAISE_APPLICATION_ERROR         = -20999;

} // common
using namespace common; // maybe someone can fix
} // oceanbase

#endif /* OB_ERRNO_H */
';


# generate ob_errno.cpp
open my $fh_cpp, '>', "ob_errno.cpp";
print $fh_cpp '
// DO NOT EDIT. This file is automatically generated from `ob_errno.def\'.

// Copyright 2016 Alibaba Inc. All Rights Reserved.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// version 2 as published by the Free Software Foundation.
// ob_errno.h
//   Author:
//   Normalizer:

#define USING_LOG_PREFIX LIB_MYSQLC

// DO NOT DELETE `#include <iostream>` !!!
// fix: ob_error.cpp file requires at least 20g memory for release(-O2) compilation
// and will jam when asan turned on
// it can be solved by introducing <iostream> header file currently
// TODO: it is clang bug and the specific reason to be further located
// issue:
#include <iostream>

#include "ob_errno.h"
#ifndef __ERROR_CODE_PARSER_
#include "ob_define.h"
#include "lib/utility/ob_edit_distance.h"
#else
#define OB_LIKELY
#define OB_UNLIKELY
#include <string.h>
#endif
using namespace oceanbase::common;

struct _error {
  public:
    const char *error_name;
    const char *error_cause;
    const char *error_solution;
    int         mysql_errno;
    const char *sqlstate;
    const char *str_error;
    const char *str_user_error;
    int         oracle_errno;
    const char *oracle_str_error;
    const char *oracle_str_user_error;
};
static _error _error_default;
static _error const *_errors[OB_MAX_ERROR_CODE] = {NULL};
';

for my $oberr (@errors) {
  if (0 > $map{$oberr}->[0]) {
    my $err = "static const _error _error_$oberr = {
      .error_name            = \"$map{$oberr}->[5]\",
      .error_cause           = $map{$oberr}->[9],
      .error_solution        = $map{$oberr}->[10],
      .mysql_errno           = $map{$oberr}->[1],
      .sqlstate              = $map{$oberr}->[2],
      .str_error             = $map{$oberr}->[3],
      .str_user_error        = $map{$oberr}->[4],
      .oracle_errno          = $map{$oberr}->[6],
      .oracle_str_error      = $map{$oberr}->[7],
      .oracle_str_user_error = $map{$oberr}->[8]
};\n";
  print $fh_cpp $err;
  }
}

print $fh_cpp '
struct ObStrErrorInit
{
  ObStrErrorInit()
  {
    memset(&_error_default, 0, sizeof  _error_default);
    for (int i = 0; i < OB_MAX_ERROR_CODE; ++i) {
      _errors[i] = &_error_default;
    }
';
    for my $oberr (@errors) {
      if (0 > $map{$oberr}->[0]) {
        print $fh_cpp "    _errors[-$oberr] = &_error_$oberr;\n";
      }
    }
  print $fh_cpp '
  }
};

inline const _error *get_error(int index)
{
  static ObStrErrorInit error_init;
  return _errors[index];
}

int get_oracle_errno(int index)
{
  return get_error(index)->oracle_errno;
}

int get_mysql_errno(int index)
{
  return get_error(index)->mysql_errno;
}

const char* get_oracle_str_error(int index)
{
  return get_error(index)->oracle_str_error;
}

const char* get_mysql_str_error(int index)
{
  return get_error(index)->str_error;
}

namespace oceanbase
{
namespace common
{
';
print $fh_cpp "int g_all_ob_errnos[${\(scalar @errnos)}] = {" . join(", ", @errnos) . "};";

print $fh_cpp '
  const char *ob_error_name(const int err)
  {
    const char *ret = "Unknown error";
    if (OB_UNLIKELY(0 == err)) {
      ret = "OB_SUCCESS";
    } else if (OB_LIKELY(0 > err && err > -OB_MAX_ERROR_CODE)) {
      ret = get_error(-err)->error_name;
      if (OB_UNLIKELY(NULL == ret || \'\0\' == ret[0]))
      {
        ret = "Unknown Error";
      }
    }
    return ret;
  }
  const char *ob_error_cause(const int err)
  {
    const char *ret = "Internal Error";
    if (OB_UNLIKELY(0 == err)) {
      ret = "Not an Error";
    } else if (OB_LIKELY(0 > err && err > -OB_MAX_ERROR_CODE)) {
      ret = get_error(-err)->error_cause;
      if (OB_UNLIKELY(NULL == ret || \'\0\' == ret[0]))
      {
        ret = "Internal Error";
      }
    }
    return ret;
  }
  const char *ob_error_solution(const int err)
  {
    const char *ret = "Contact OceanBase Support";
    if (OB_UNLIKELY(0 == err)) {
      ret = "Contact OceanBase Support";
    } else if (OB_LIKELY(0 > err && err > -OB_MAX_ERROR_CODE)) {
      ret = get_error(-err)->error_solution;
      if (OB_UNLIKELY(NULL == ret || \'\0\' == ret[0]))
      {
        ret = "Contact OceanBase Support";
      }
    }
    return ret;
  }
  const char *ob_strerror(const int err)
  {
    const char *ret = "Unknown error";
    if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
      ret = get_error(-err)->str_error;
      if (OB_UNLIKELY(NULL == ret || \'\0\' == ret[0]))
      {
        ret = "Unknown Error";
      }
    }
    return ret;
  }
  const char *ob_str_user_error(const int err)
  {
    const char *ret = NULL;
    if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
      ret = get_error(-err)->str_user_error;
      if (OB_UNLIKELY(NULL == ret || \'\0\' == ret[0])) {
        ret = NULL;
      }
    }
    return ret;
  }
  const char *ob_sqlstate(const int err)
  {
    const char *ret = "HY000";
    if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
      ret = get_error(-err)->sqlstate;
      if (OB_UNLIKELY(NULL == ret || \'\0\' == ret[0])) {
        ret = "HY000";
      }
    }
    return ret;
  }
  int ob_mysql_errno(const int err)
  {
    int ret = -1;
    if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
      ret = get_error(-err)->mysql_errno;
    }
    return ret;
  }
  int ob_mysql_errno_with_check(const int err)
  {
    int ret = (err > 0 ? err : ob_mysql_errno(err));
    if (ret < 0) {
      ret = -err;
    }
    return ret;
  }
  const char *ob_oracle_strerror(const int err)
  {
    const char *ret = "Unknown error";
    if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
      ret = get_error(-err)->oracle_str_error;
      if (OB_UNLIKELY(NULL == ret || \'\0\' == ret[0]))
      {
        ret = "Unknown Error";
      }
    }
    return ret;
  }
  const char *ob_oracle_str_user_error(const int err)
  {
    const char *ret = NULL;
    if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
      ret = get_error(-err)->oracle_str_user_error;
      if (OB_UNLIKELY(NULL == ret || \'\0\' == ret[0])) {
        ret = NULL;
      }
    }
    return ret;
  }
  int ob_oracle_errno(const int err)
  {
    int ret = -1;
    if (OB_ERR_PROXY_REROUTE == err) {
      // Oracle Mode and MySQL mode should return same errcode for reroute sql
      // thus we make the specialization here
      ret = -1;
    } else if (err >= OB_MIN_RAISE_APPLICATION_ERROR && err <= OB_MAX_RAISE_APPLICATION_ERROR) {
      ret = err; // PL/SQL Raise Application Error
    } else if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
      ret = get_error(-err)->oracle_errno;
    }
    return ret;
  }
  int ob_oracle_errno_with_check(const int err)
  {
    int ret = ob_oracle_errno(err);
    if (ret < 0) {
      ret = -err;
    }
    return ret;
  }
  int ob_errpkt_errno(const int err, const bool is_oracle_mode)
  {
    return (is_oracle_mode ? ob_oracle_errno_with_check(err) : ob_mysql_errno_with_check(err));
  }
  const char *ob_errpkt_strerror(const int err, const bool is_oracle_mode)
  {
    return (is_oracle_mode ? ob_oracle_strerror(err) : ob_strerror(err));
  }
  const char *ob_errpkt_str_user_error(const int err, const bool is_oracle_mode)
  {
    return (is_oracle_mode ? ob_oracle_str_user_error(err) : ob_str_user_error(err));
  }

} // end namespace common
} // end namespace oceanbase
';
