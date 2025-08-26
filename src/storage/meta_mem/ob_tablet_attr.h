/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_TABLET_ATTR_H_
#define OCEANBASE_STORAGE_TABLET_ATTR_H_

#include "common/log/ob_log_constants.h"
#include "share/ob_define.h"
#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"

namespace oceanbase
{
namespace storage
{

class ObTabletFastIterAttr final
{
public:
  ObTabletFastIterAttr();
  explicit ObTabletFastIterAttr(const uint64_t v) : v_(v) {}
  ~ObTabletFastIterAttr() { reset(); }
  void reset() { v_ = 0; }
  bool is_valid() const { return valid_; }
  TO_STRING_KV(K_(valid),
               K_(is_empty_shell),
               K_(has_transfer_table),
               K_(has_nested_table),
               K_(initial_state));

  OB_UNIS_VERSION_V(1);
public:
  union {
    uint64_t v_;
    struct {
      uint64_t valid_              : 1; // valid_ = true means attr is filled
      uint64_t is_empty_shell_     : 1;
      uint64_t has_transfer_table_ : 1;
      uint64_t has_nested_table_   : 1;
      uint64_t initial_state_      : 1;
      uint64_t reserved_           : 59;
    };
  };
};

class ObTabletAttr final
{
public:
  ObTabletAttr();
  ~ObTabletAttr() = default;
  void reset()
  {
    iter_attr_.reset();
    ha_status_ = 0;
    all_sstable_data_occupy_size_ = 0;
    all_sstable_data_required_size_ = 0;
    tablet_meta_size_ = 0;
    ss_public_sstable_occupy_size_ = 0;
    backup_bytes_ = 0;
    auto_part_size_ = OB_INVALID_SIZE;
    tablet_max_checkpoint_scn_ = share::SCN::invalid_scn();
    ss_change_version_ = share::SCN::min_scn();
    last_match_tablet_meta_version_ = 0;
    notify_ss_change_version_ = share::SCN::min_scn();
  }
  bool is_valid() const { return iter_attr_.valid_; }
  bool is_empty_shell() const { return iter_attr_.is_empty_shell_; }
  bool has_transfer_table() const { return iter_attr_.has_transfer_table_; }
  bool has_nested_table() const { return iter_attr_.has_nested_table_; }
  bool initial_state() const { return iter_attr_.initial_state_; }
  void refresh_cache(
      const share::SCN &clog_ckpt_scn,
      const share::SCN &ddl_ckpt_scn,
      const share::SCN &mds_ckpt_scn)
  {
    notify_ss_change_version_ = ss_change_version_;
    tablet_max_checkpoint_scn_ = MAX(clog_ckpt_scn, MAX(ddl_ckpt_scn, mds_ckpt_scn));
  }
  TO_STRING_KV(K_(iter_attr),
               K_(ha_status),
               K_(all_sstable_data_occupy_size),
               K_(all_sstable_data_required_size),
               K_(tablet_meta_size),
               K_(ss_public_sstable_occupy_size),
               K_(backup_bytes),
               K_(ss_change_version),
               K_(last_match_tablet_meta_version),
               K_(auto_part_size),
               K_(notify_ss_change_version),
               K_(tablet_max_checkpoint_scn));

  OB_UNIS_VERSION(1);
public:
  ObTabletFastIterAttr iter_attr_;
  int64_t ha_status_;
  // all sstable data occupy_size, include major sstable
  // <data_block real_size> + <small_sstable_nest_size (in share_nothing)>
  int64_t all_sstable_data_occupy_size_;
  // all sstable data requred_size, data_block_count * 2MB, include major sstable
  int64_t all_sstable_data_required_size_;
  // meta_size in shared_nothing, meta_block_count * 2MB
  int64_t tablet_meta_size_;
  // major sstable data occupy_size
  // which is same as major_sstable_required_size_;
  // because the alignment size is 1B in object_storage.
  int64_t ss_public_sstable_occupy_size_;
  int64_t backup_bytes_;
  share::SCN ss_change_version_; // 8B
  int64_t last_match_tablet_meta_version_; // 8B

  // =================== ATTENTION !!! ======================
  // The following fields do not need to be persisted
  // --------------------------------------------------------
  int64_t auto_part_size_; // 8B
  share::SCN notify_ss_change_version_; // 8B
  // max(tablet.clog_ckpt_scn, tablet.ddl_ckpt_scn, tablet.mds_ckpt_scn)
  // if tablet_max_checkpoint_scn_ < ls_tablet_ss_checkpoint_scn, there maybe (ddl/data/mds)sstables need upload to shared_storage;
  // for shared_storage, these scns (clog/mds/ddl) should be record in slog.
  share::SCN tablet_max_checkpoint_scn_; // 8B
  // --------------------------------------------------------
};

class ObTabletResidentInfo final
{
public:
  ObTabletResidentInfo()
    : attr_(),
      addr_(),
      key_()
  {}
  ObTabletResidentInfo(
    const ObTabletMapKey &tablet_key,
    const ObTabletAttr &tablet_attr,
    const ObMetaDiskAddr &tablet_addr)
    : attr_(tablet_attr),
      addr_(tablet_addr),
      key_(tablet_key)
  {}
  ~ObTabletResidentInfo() { reset(); }
  bool is_valid() const { return attr_.is_valid() && key_.is_valid() && addr_.is_valid(); }
  bool has_transfer_table() const { return attr_.has_transfer_table(); }
  bool is_empty_shell() const { return attr_.is_empty_shell(); }
  bool has_nested_table() const { return attr_.has_nested_table(); }
  int64_t get_required_size() const { return attr_.all_sstable_data_required_size_; }
  int64_t get_occupy_size() const { return attr_.all_sstable_data_occupy_size_; }
  uint64_t get_tablet_meta_size() const { return attr_.tablet_meta_size_; }
  int64_t get_ss_public_sstable_occupy_size() const { return attr_.ss_public_sstable_occupy_size_; }
  int64_t get_backup_size() const { return attr_.backup_bytes_; }
  void reset()
  {
    attr_.reset();
    addr_.set_none_addr();
    key_.reset();
  }
  TO_STRING_KV(K_(key), K_(addr), K_(attr));
public:
  ObTabletAttr attr_;
  ObMetaDiskAddr addr_;
  ObTabletMapKey key_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TABLET_ATTR_H_
