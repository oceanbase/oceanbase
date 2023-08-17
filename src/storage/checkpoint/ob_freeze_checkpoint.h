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

#ifndef OCEANBASE_STORAGE_OB_FREEZE_CHECKPOINT_H_
#define OCEANBASE_STORAGE_OB_FREEZE_CHECKPOINT_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
namespace checkpoint
{
class ObCheckpointDList;
class ObDataCheckpoint;

enum ObFreezeCheckpointLocation
{
  LS_FROZEN = 1,
  NEW_CREATE = 2,
  ACTIVE  = 4,
  PREPARE = 8,
  OUT = 16,
};

static inline
int freeze_checkpoint_location_to_string(const ObFreezeCheckpointLocation location,
                                         char *str,
                                         const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (OUT == location) {
    strncpy(str ,"OUT", str_len);
  } else if (NEW_CREATE == location) {
    strncpy(str ,"NEW_CREATE", str_len);
  } else if (ACTIVE == location) {
    strncpy(str ,"ACTIVE", str_len);
  } else if (PREPARE == location) {
    strncpy(str ,"PREPARE", str_len);
  } else if (LS_FROZEN == location) {
    strncpy(str ,"LS_FROZEN", str_len);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}
struct ObFreezeCheckpointVTInfo
{
  ObTabletID tablet_id;
  share::SCN rec_scn;
  ObFreezeCheckpointLocation location;
  bool rec_scn_is_stable;

  TO_STRING_KV(
    K(tablet_id),
    K(rec_scn),
    K(location),
    K(rec_scn_is_stable)
  );
};

// checkpoint unit like data_memtable and memtable that
// 1. write TRANS_SERVICE_LOG_BASE_TYPE clog
// 2. have freeze operation and rec_scn can become smaller
// inherit from ObFreezeCheckpoint
// register into ObDataCheckpoint
class ObFreezeCheckpoint : public common::ObDLinkBase<ObFreezeCheckpoint>
{
  friend class ObDataCheckpoint;
  friend class ObCheckpointDList;

public:
  ObFreezeCheckpoint() : location_(OUT), data_checkpoint_(nullptr) {}
  virtual ~ObFreezeCheckpoint() {}
  void reset();
  virtual void remove_from_data_checkpoint();
  virtual share::SCN get_rec_scn() = 0;
  virtual int flush(share::ObLSID ls_id) = 0;
  // judge rec_scn of the checkpoint unit won't get smaller
  // by comparing with max_consequent_callbacked_scn
  // a unit will only be moved once by rec_scn_stable_
  virtual bool rec_scn_is_stable() = 0;
  // Whether the dump conditions are met
  virtual bool ready_for_flush() = 0;
  // avoid active checkpoint block minor merge
  virtual bool is_frozen_checkpoint() const = 0;
  // active checkpoint no need to flush
  virtual bool is_active_checkpoint() const = 0;
  // for checkpoint_virtual_table
  virtual ObTabletID get_tablet_id() const = 0;
  // register into ObDataCheckpoint
  int add_to_data_checkpoint(ObDataCheckpoint *data_checkpoint);
  bool is_in_prepare_list_of_data_checkpoint();
  // after checkpoint ready_for_flush
  // move to prepare_list in data_checkpoint
  int finish_freeze();
  // for empty memtable
  virtual int set_frozen() { return OB_SUCCESS; }

  VIRTUAL_TO_STRING_KV(K_(location));

private:
  int unlink_();
  // ensure safe by lock of data_checkpoint
  ObFreezeCheckpointLocation location_;
  ObDataCheckpoint *data_checkpoint_;
};

}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
#endif
