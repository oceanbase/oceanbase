/**
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

#define USING_LOG_PREFIX SHARE
#include "ob_locality_parser.h"
#include "lib/alloc/alloc_assist.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

// full replica
const char *ObLocalityParser::FULL_REPLICA_STR = "FULL";
const char *ObLocalityParser::F_REPLICA_STR = "F";
// logonly replica
const char *ObLocalityParser::LOGONLY_REPLICA_STR = "LOGONLY";
const char *ObLocalityParser::L_REPLICA_STR = "L";
// backup replica
const char *ObLocalityParser::BACKUP_REPLICA_STR = "BACKUP";
const char *ObLocalityParser::B_REPLICA_STR = "B";
// readonly replica
const char *ObLocalityParser::READONLY_REPLICA_STR = "READONLY";
const char *ObLocalityParser::R_REPLICA_STR = "R";
// memonly replica
const char *ObLocalityParser::MEMONLY_REPLICA_STR = "MEMONLY";
const char *ObLocalityParser::M_REPLICA_STR = "M";
// encryption logonly replica
const char *ObLocalityParser::ENCRYPTION_LOGONLY_REPLICA_STR = "ENCRYPTION_LOGONLY";
const char *ObLocalityParser::E_REPLICA_STR = "E";

int ObLocalityParser::parse_type(const char *str, int64_t len, ObReplicaType &replica_type)
{
  UNUSED(len);
  // TODO: only support F-replica in 4.0 and R-replica in 4.2 for now, will support others in the future
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica type string. null!", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "replica_type, replica_type should not be null");
  } else if (0 == STRCASECMP(FULL_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_FULL;
  } else if (0 == STRCASECMP(F_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_FULL;
  } else if (0 == STRCASECMP(LOGONLY_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_LOGONLY;
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "logonly-replica");
  } else if ( 0 == STRCASECMP(L_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_LOGONLY;
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "logonly-replica");
  } else if (0 == STRCASECMP(ENCRYPTION_LOGONLY_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_ENCRYPTION_LOGONLY;
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "encryption-logonly-replica");
  } else if ( 0 == STRCASECMP(E_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_ENCRYPTION_LOGONLY;
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "encryption-logonly-replica");
  } else if ( 0 == STRCASECMP(BACKUP_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_BACKUP;
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup-replica");
  } else if ( 0 == STRCASECMP(B_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_BACKUP;
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup-replica");
  } else if ( 0 == STRCASECMP(READONLY_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_READONLY;
  } else if ( 0 == STRCASECMP(R_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_READONLY;
  } else if ( 0 == STRCASECMP(MEMONLY_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_MEMONLY;
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "memonly-replica");
  } else if ( 0 == STRCASECMP(M_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_MEMONLY;
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "memonly-replica");
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica type string", K(str), K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "replica_type, unrecognized replica_type");
  }
  return ret;
}

