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
#include "storage/tablet/ob_tablet_table_store_iterator.h"

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
  ObTabletHandle(
      const Base &other,
      const WashTabletPriority priority);
  ObTabletHandle(const ObTabletHandle &other);
  virtual ~ObTabletHandle();
  ObTabletHandle &operator = (const ObTabletHandle &other);
  virtual void reset() override;
  virtual bool need_hold_time_check() const override { return true; }
  void set_wash_priority(const WashTabletPriority priority) { wash_priority_ = priority; }
  common::ObArenaAllocator *get_allocator() { return static_cast<common::ObArenaAllocator *>(allocator_); }
  void disallow_copy_and_assign() { allow_copy_and_assign_ = false; }
  bool is_tmp_tablet() const { return !allow_copy_and_assign_; }
  char *get_buf() { return reinterpret_cast<char *>(obj_); }
  int64_t get_buf_len() const { return get_buf_header().buf_len_; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  int64_t calc_wash_score(const WashTabletPriority priority) const;
  ObMetaObjBufferHeader &get_buf_header() const
  {
    return ObMetaObjBufferHelper::get_buffer_header(reinterpret_cast<char *>(obj_));
  }
private:
  WashTabletPriority wash_priority_;
  bool allow_copy_and_assign_;
  DEFINE_OBJ_LEAK_DEBUG_NODE(node_);
};

class ObTabletTableIterator final
{
  friend class ObTablet;
  friend class ObLSTabletService;
public:
  ObTabletTableIterator() : tablet_handle_(), table_store_iter_(), transfer_src_handle_(nullptr) {}
  explicit ObTabletTableIterator(const bool is_reverse) : tablet_handle_(), table_store_iter_(is_reverse), transfer_src_handle_(nullptr) {}

  ObTabletTableIterator(const ObTabletTableIterator& other) { *this = other; } ;
  void operator=(const ObTabletTableIterator& other);
  ~ObTabletTableIterator() { reset(); }
  void reset()
  {
    table_store_iter_.reset();
    tablet_handle_.reset();
    if (nullptr != transfer_src_handle_) {
      transfer_src_handle_->~ObTabletHandle();
      ob_free(transfer_src_handle_);
      transfer_src_handle_ = nullptr;
    }
  }
  bool is_valid() const { return tablet_handle_.is_valid() || table_store_iter_.is_valid(); }
  ObTableStoreIterator *table_iter();
  const ObTableStoreIterator *table_iter() const;
  const ObTablet *get_tablet() const { return tablet_handle_.get_obj(); }
  ObTablet *get_tablet() { return tablet_handle_.get_obj(); }
  const ObTabletHandle &get_tablet_handle() { return tablet_handle_; }
  int set_tablet_handle(const ObTabletHandle &tablet_handle);
  int set_transfer_src_tablet_handle(const ObTabletHandle &tablet_handle);
  int refresh_read_tables_from_tablet(
      const int64_t snapshot_version,
      const bool allow_no_ready_read,
      const bool major_sstable_only = false);
  TO_STRING_KV(K_(tablet_handle), K_(transfer_src_handle), K_(table_store_iter));
private:
  ObTabletHandle tablet_handle_;
  ObTableStoreIterator table_store_iter_;
  ObTabletHandle *transfer_src_handle_;
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
