/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_COLUMN_OB_COLUMN_STORE_REPLICA_DDL_HELPER_H
#define OCEANBASE_STORAGE_COLUMN_OB_COLUMN_STORE_REPLICA_DDL_HELPER_H

#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace storage
{

// For readability and scalability, use a new enum type to replace the ddl_table_type originally.
enum ObCSReplicaDDLReplayStatus : unsigned char
{
  CS_REPLICA_REPLAY_ROW_STORE_FINISH  = ObITable::MAJOR_SSTABLE, // 10
  CS_REPLICA_INVISILE = ObITable::DDL_DUMP_SSTABLE, // 14
  CS_REPLICA_REPLAY_NONE = ObITable::DDL_MEM_SSTABLE, // 16
  CS_REPLICA_REPLAY_COLUMN_FINISH = ObITable::COLUMN_ORIENTED_SSTABLE, // 17
  CS_REPLICA_VISIBLE_AND_REPLAY_COLUMN = ObITable::DDL_MERGE_CO_SSTABLE, // 21
  CS_REPLICA_VISIBLE_AND_REPLAY_ROW = ObITable::DDL_MERGE_CG_SSTABLE, // 22
  CS_REPLICA_COMPAT_OLD_MAX_TABLE_TYPE = 28, //for compat
  CS_REPLICA_REPLAY_MAX
};

inline bool is_valid_cs_replica_ddl_status(const ObCSReplicaDDLReplayStatus &status)
{
  return CS_REPLICA_REPLAY_ROW_STORE_FINISH == status
      || CS_REPLICA_INVISILE == status
      || CS_REPLICA_REPLAY_NONE == status
      || CS_REPLICA_REPLAY_COLUMN_FINISH == status
      || CS_REPLICA_VISIBLE_AND_REPLAY_COLUMN == status
      || CS_REPLICA_VISIBLE_AND_REPLAY_ROW == status
      || CS_REPLICA_COMPAT_OLD_MAX_TABLE_TYPE == status;
}

} // namespace storage
} // namespace oceanbase

#endif