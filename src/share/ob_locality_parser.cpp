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
const char* ObLocalityParser::FULL_REPLICA_STR = "FULL";
const char* ObLocalityParser::F_REPLICA_STR = "F";
// logonly replica
const char* ObLocalityParser::LOGONLY_REPLICA_STR = "LOGONLY";
const char* ObLocalityParser::L_REPLICA_STR = "L";
// backup replica
const char* ObLocalityParser::BACKUP_REPLICA_STR = "BACKUP";
const char* ObLocalityParser::B_REPLICA_STR = "B";
// readonly replica
const char* ObLocalityParser::READONLY_REPLICA_STR = "READONLY";
const char* ObLocalityParser::R_REPLICA_STR = "R";
// memonly replica
const char* ObLocalityParser::MEMONLY_REPLICA_STR = "MEMONLY";
const char* ObLocalityParser::M_REPLICA_STR = "M";

int ObLocalityParser::parse_type(const char* str, int64_t len, ObReplicaType& replica_type)
{
  UNUSED(len);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica type string. null!", K(ret));
  } else if (0 == STRCASECMP(FULL_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_FULL;
  } else if (0 == STRCASECMP(F_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_FULL;
  } else if (0 == STRCASECMP(LOGONLY_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_LOGONLY;
  } else if (0 == STRCASECMP(L_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_LOGONLY;
  } else if (0 == STRCASECMP(BACKUP_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_BACKUP;
  } else if (0 == STRCASECMP(B_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_BACKUP;
  } else if (0 == STRCASECMP(READONLY_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_READONLY;
  } else if (0 == STRCASECMP(R_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_READONLY;
  } else if (0 == STRCASECMP(MEMONLY_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_MEMONLY;
  } else if (0 == STRCASECMP(M_REPLICA_STR, str)) {
    replica_type = REPLICA_TYPE_MEMONLY;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica type string", K(str), K(ret));
  }
  return ret;
}
