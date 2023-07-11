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

#ifndef OCEABASE_MEMTABLE_OB_MULTI_SOURCE_DATA_
#define OCEABASE_MEMTABLE_OB_MULTI_SOURCE_DATA_

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;
}
namespace memtable
{

enum class MultiSourceDataUnitType
{
  // unit ptr type
  TABLET_TX_DATA = 0,
  TABLET_BINDING_INFO = 1,
  TABLET_SEQ = 2,
  // unit list type
  STORAGE_SCHEMA = 3,
  MEDIUM_COMPACTION_INFO = 4,
  MAX_TYPE
};

class ObIMultiSourceDataUnit: public common::ObDLinkBase<ObIMultiSourceDataUnit>
{
public:
  ObIMultiSourceDataUnit()
    :
      is_tx_end_(false),
      unsynced_cnt_for_multi_data_(0),
      sync_finish_(true)
  {}
  virtual ~ObIMultiSourceDataUnit() = default;

  static bool is_unit_list(const MultiSourceDataUnitType type)
  {
    return type >= MultiSourceDataUnitType::STORAGE_SCHEMA && type < MultiSourceDataUnitType::MAX_TYPE;
  }

  // allocator maybe useless for some data type
  virtual int deep_copy_unit(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator = nullptr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(src)) {
      ret = common::OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), KP(src));
    } else {
      sync_finish_ = src->sync_finish_;
      is_tx_end_ = src->is_tx_end_;
      unsynced_cnt_for_multi_data_ = src->unsynced_cnt_for_multi_data_;
      ret = deep_copy(src, allocator);
    }
    return ret;
  }
  virtual void reset() = 0;

  virtual int dump() { return common::OB_NOT_IMPLEMENT; }
  virtual int load(storage::ObTablet *tablet) { return common::OB_NOT_IMPLEMENT; }

  virtual bool is_valid() const = 0;
  virtual int64_t get_data_size() const = 0;
  virtual MultiSourceDataUnitType type() const = 0;
  virtual int set_scn(const share::SCN &scn)
  {
    UNUSED(scn);
    return common::OB_SUCCESS;
  }
  bool is_sync_finish() const { return sync_finish_; }
  void set_sync_finish(bool sync_finish) { sync_finish_ = sync_finish; }
  bool is_tx_end() const { return is_tx_end_; }
  void set_tx_end(bool is_tx_end) { is_tx_end_ = is_tx_end; }
  void inc_unsync_cnt_for_multi_data() {
    unsynced_cnt_for_multi_data_++;
    TRANS_LOG(INFO, "unsync_cnt_for_multi_data inc", KPC(this));
  }
  void dec_unsync_cnt_for_multi_data() {
    unsynced_cnt_for_multi_data_--;
    TRANS_LOG(INFO, "unsync_cnt_for_multi_data dec", KPC(this));
  }
  int get_unsync_cnt_for_multi_data() const { return unsynced_cnt_for_multi_data_; }
  void set_unsync_cnt_for_multi_data(const int unsynced_cnt_for_multi_data) { unsynced_cnt_for_multi_data_ = unsynced_cnt_for_multi_data; }
  virtual bool is_save_last() const { return true; } // only store one data unit with sync_finish=true
  virtual int64_t get_version() const { return common::OB_INVALID_VERSION; } // have to implement for unit list type
  VIRTUAL_TO_STRING_KV(K_(is_tx_end),
                       K_(unsynced_cnt_for_multi_data),
                       K_(sync_finish));
protected:
  bool is_tx_end_;
  int unsynced_cnt_for_multi_data_;
private:
  virtual int deep_copy(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator = nullptr) = 0;
  bool sync_finish_;
};

} // namespace memtable
} // namespace oceanbase

#endif // OCEABASE_MEMTABLE_OB_MULTI_SOURCE_DATA_
