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

#ifndef OCEANBASE_STORAGE_OB_TABLET_HANDLE
#define OCEANBASE_STORAGE_OB_TABLET_HANDLE

#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "share/leak_checker/obj_leak_checker.h"
#include "storage/tablet/ob_table_store_util.h"

namespace oceanbase
{
namespace storage
{
enum class WashTabletPriority : int8_t
{
  WTP_HIGH = 0,
  WTP_LOW  = 1,
  WTP_MAX
};
class ObTablet;

class ObTabletHandle : public ObMetaObjGuard<ObTablet>
{
private:
  typedef ObMetaObjGuard<ObTablet> Base;
public:
  ObTabletHandle();
  ObTabletHandle(const Base &other);
  ObTabletHandle(const ObTabletHandle &other);
  virtual ~ObTabletHandle();
  ObTabletHandle &operator = (const ObTabletHandle &other);
  virtual void reset() override;
  virtual bool need_hold_time_check() const override { return true; }
  void set_wash_priority(WashTabletPriority priority) { wash_priority_ = priority; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  int64_t calc_wash_score(const WashTabletPriority priority) const;
private:
  WashTabletPriority wash_priority_;
  DEFINE_OBJ_LEAK_DEBUG_NODE(node_);
};

class ObTabletTableIterator final
{
public:
  ObTabletTableIterator() = default;
  ~ObTabletTableIterator() { reset(); }
  void reset()
  {
    tablet_handle_.reset();
    table_iter_.reset();
  }
  // TODO: @yht146439 fix me. return tablet_handle_.is_valid() && table_iter.is_valid();
  bool is_valid() const { return tablet_handle_.is_valid() || table_iter_.is_valid(); }
  TO_STRING_KV(K_(tablet_handle), K_(table_iter));
public:
  ObTabletHandle tablet_handle_;
  ObTableStoreIterator table_iter_;
};

struct ObGetTableParam
{
public:
  ObGetTableParam() : frozen_version_(-1), sample_info_(), tablet_iter_() {}
  ~ObGetTableParam() { reset(); }
  bool is_valid() const { return tablet_iter_.is_valid(); }
  void reset()
  {
    frozen_version_ = -1;
    sample_info_.reset();
    tablet_iter_.reset();
  }
  TO_STRING_KV(K_(frozen_version), K_(sample_info), K_(tablet_iter));
public:
  int64_t frozen_version_;
  common::SampleInfo sample_info_;
  ObTabletTableIterator tablet_iter_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_HANDLE
