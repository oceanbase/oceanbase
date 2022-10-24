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

#ifndef OCEANBASE_SHARE_OB_LOCALITY_PARSER_H_
#define OCEANBASE_SHARE_OB_LOCALITY_PARSER_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace share
{
class ObLocalityParser
{
public:
  static int parse_type(const char *str, int64_t len, common::ObReplicaType &type);
private:
  // full replica
  static const char *FULL_REPLICA_STR;
  static const char *F_REPLICA_STR;
  // logonly replica
  static const char *LOGONLY_REPLICA_STR;
  static const char *L_REPLICA_STR;
  // backup replica
  static const char *BACKUP_REPLICA_STR;
  static const char *B_REPLICA_STR;
  // readonly replica
  static const char *READONLY_REPLICA_STR;
  static const char *R_REPLICA_STR;
  // memonly replica
  static const char *MEMONLY_REPLICA_STR;
  static const char *M_REPLICA_STR;
  // encryption logonly replica
  static const char *ENCRYPTION_LOGONLY_REPLICA_STR;
  static const char *E_REPLICA_STR;
};

} // end namespace share
} // end namespace oceanbase
#endif
