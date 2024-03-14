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

#ifndef OCEANBASE_STORAGE_OB_COMMON_CHECKPOINT_H_
#define OCEANBASE_STORAGE_OB_COMMON_CHECKPOINT_H_

#include <cstdint>
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
namespace checkpoint
{

enum ObCommonCheckpointType
{
  INVALID_BASE_TYPE = 0,
  TX_CTX_MEMTABLE_TYPE,
  TX_DATA_MEMTABLE_TYPE,
  LOCK_MEMTABLE_TYPE,
  MDS_TABLE_TYPE,
  DATA_CHECKPOINT_TYPE,
  // for unittest
  TEST_COMMON_CHECKPOINT,
  MAX_BASE_TYPE
};

static inline
int common_checkpoint_type_to_string(const ObCommonCheckpointType common_checkpoint_type,
                                     char *str,
                                     const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (common_checkpoint_type == DATA_CHECKPOINT_TYPE) {
    strncpy(str ,"DATA_CHECKPOINT_TYPE", str_len);
  } else if (common_checkpoint_type == TX_CTX_MEMTABLE_TYPE) {
    strncpy(str ,"TX_CTX_MEMTABLE_TYPE", str_len);
  } else if (common_checkpoint_type == TX_DATA_MEMTABLE_TYPE) {
    strncpy(str ,"TX_DATA_MEMTABLE_TYPE", str_len);
  } else if (common_checkpoint_type == LOCK_MEMTABLE_TYPE) {
    strncpy(str ,"LOCK_MEMTABLE_TYPE", str_len);
  } else if (common_checkpoint_type == MDS_TABLE_TYPE) {
    strncpy(str, "MDS_TABLE_TYPE", str_len);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

struct ObCommonCheckpointVTInfo
{
  ObTabletID tablet_id;
  share::SCN rec_scn;
  int checkpoint_type;
  bool is_flushing;

  TO_STRING_KV(
    K(tablet_id),
    K(rec_scn),
    K(checkpoint_type),
    K(is_flushing)
  );
};

inline bool is_valid_log_base_type(const ObCommonCheckpointType &type)
{
  return type > INVALID_BASE_TYPE && type < MAX_BASE_TYPE;
}

// checkpoint unit inherit from CommonCheckpoint
// and register into ls_tx_service's common_list
// the checkpoint units:
// 1. write TRANS_SERVICE_LOG_BASE_TYPE clog
// 2. have no freeze operation and rec_scn can't become smaller
class ObCommonCheckpoint
{
public:
  virtual share::SCN get_rec_scn() = 0;
  virtual int flush(share::SCN recycle_scn, const int64_t trace_id, bool need_freeze = true) = 0;

  virtual ObTabletID get_tablet_id() const = 0;
  virtual share::SCN get_rec_scn(ObTabletID &tablet_id) {
    share::SCN rec_scn = get_rec_scn();
    tablet_id = get_tablet_id();
    return rec_scn;
  }
  virtual bool is_flushing() const = 0;

  VIRTUAL_TO_STRING_KV(KP(this));
};

}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
#endif
