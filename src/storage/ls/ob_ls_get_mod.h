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

#ifndef OCEANBASE_STORAGE_OB_LS_GET_MOD
#define OCEANBASE_STORAGE_OB_LS_GET_MOD

namespace oceanbase
{
namespace storage
{
enum class ObLSGetMod : int
{
  INVALID_MOD = -1,
  TRANS_MOD = 0,
  STORAGE_MOD = 1,
  RS_MOD = 2,
  LOG_MOD = 3,
  OBSERVER_MOD = 4,
  ARCHIVE_MOD = 5,
  DAS_MOD = 6,
  SHARE_MOD = 7,
  APPLY_MOD = 8,
  ADAPTER_MOD = 9,
  DEADLOCK_MOD = 10,
  TABLELOCK_MOD = 11,
  HA_MOD = 12,
  TABLET_MOD = 13,
  DDL_MOD = 14,
  TXSTORAGE_MOD = 15,
  LEADER_COORDINATOR_MOD = 16,
  DATA_DICT_MOD = 17,
  DATA_MEMTABLE_MOD = 18,
  MULTI_VERSION_GARBAGE_COLLOECTOR_MOD = 19,
  MDS_TABLE_MOD = 20,
  TOTAL_MAX_MOD = 21,
};

}
}

#endif // OCEANBASE_STORAGE_OB_LS_GET_MOD
