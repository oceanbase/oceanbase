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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_OBJECT_RW_INFO_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_OBJECT_RW_INFO_H_

#include "common/storage/ob_io_device.h"
#include "lib/oblog/ob_log_module.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/basic/ob_lake_table_reader_profile.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObLocalCacheWriteInfo final
{
public:
  ObLocalCacheWriteInfo()
    : ls_epoch_id_(0), tmp_file_valid_length_(0), effective_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      is_write_cache_(true)
  {}
  ~ObLocalCacheWriteInfo() = default;
  TO_STRING_KV(K_(ls_epoch_id), K_(tmp_file_valid_length), K_(effective_tablet_id), K_(is_write_cache));
public:
  int64_t ls_epoch_id_; // for share storage file path
  int64_t tmp_file_valid_length_; // for shared storage tmp file path
  uint64_t effective_tablet_id_; // indicate effective tablet id about partition split
  bool is_write_cache_; // indicate write or read macro cache
};

struct ObStorageObjectWriteInfo final
{
public:
  ObStorageObjectWriteInfo()
    : buffer_(NULL), offset_(0), size_(0), io_timeout_ms_(GCONF._data_storage_io_timeout / 1000L), io_desc_(),
      io_callback_(NULL), device_handle_(NULL), has_backup_device_handle_(false),
      mtl_tenant_id_(OB_INVALID_TENANT_ID), local_cache_write_info_()
  {}
  ~ObStorageObjectWriteInfo() = default;
  OB_INLINE bool is_valid() const
  {
    bool bret = false;
    bret = io_desc_.is_valid() && (NULL != buffer_) && (offset_ >= 0) && (size_ > 0)
           && (io_timeout_ms_ > 0) && (get_ls_epoch_id() >= 0) && is_valid_tenant_id(mtl_tenant_id_)
           && (get_tmp_file_valid_length() >= 0);
    if (has_backup_device_handle_) {
      bret = bret && OB_NOT_NULL(device_handle_);
    } else {
      bret = bret && OB_ISNULL(device_handle_);
    }
    return bret;
  }
  int fill_io_info_for_backup(const blocksstable::MacroBlockId &macro_id, ObIOInfo &io_info) const;
  OB_INLINE void set_ls_epoch_id(const int64_t ls_epoch_id)
  {
    local_cache_write_info_.ls_epoch_id_ = ls_epoch_id;
  }
  OB_INLINE int64_t get_ls_epoch_id() const
  {
    return local_cache_write_info_.ls_epoch_id_;
  }
  OB_INLINE void set_tmp_file_valid_length(const int64_t tmp_file_valid_length)
  {
    local_cache_write_info_.tmp_file_valid_length_ = tmp_file_valid_length;
  }
  OB_INLINE int64_t get_tmp_file_valid_length() const
  {
    return local_cache_write_info_.tmp_file_valid_length_;
  }
  OB_INLINE void set_effective_tablet_id(const uint64_t effective_tablet_id)
  {
    local_cache_write_info_.effective_tablet_id_ = effective_tablet_id;
  }
  OB_INLINE int64_t get_effective_tablet_id() const
  {
    return local_cache_write_info_.effective_tablet_id_;
  }
  OB_INLINE void set_is_write_cache(const bool is_write_cache)
  {
    local_cache_write_info_.is_write_cache_ = is_write_cache;
  }
  OB_INLINE bool get_is_write_cache() const
  {
    return local_cache_write_info_.is_write_cache_;
  }
  TO_STRING_KV(KP_(buffer), K_(offset), K_(size), K_(io_timeout_ms), K_(io_desc), KP_(io_callback),
               KP_(device_handle), K_(has_backup_device_handle), K_(mtl_tenant_id),
               K_(local_cache_write_info));
public:
  const char *buffer_;
  int64_t offset_;
  int64_t size_;
  int64_t io_timeout_ms_;
  common::ObIOFlag io_desc_;
  common::ObIOCallback *io_callback_;
  ObIODevice *device_handle_;
  bool has_backup_device_handle_;
  uint64_t mtl_tenant_id_;
  ObLocalCacheWriteInfo local_cache_write_info_;
};


struct ObLocalCacheReadInfo final
{
public:
  ObLocalCacheReadInfo()
    : logic_micro_id_(), micro_crc_(0), ls_epoch_id_(0), effective_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      bypass_micro_cache_(false)
  {}
  ~ObLocalCacheReadInfo() = default;
  TO_STRING_KV(K_(logic_micro_id), K_(micro_crc), K_(ls_epoch_id), K_(effective_tablet_id),
               K_(bypass_micro_cache));

public:
  // @logic_micro_id_ and @micro_crc_ are components of ObSSMicroBlockCacheKey, which are used
  // for interacting with disk cache of shared storage
  ObLogicMicroBlockId logic_micro_id_;
  int64_t micro_crc_;
  int64_t ls_epoch_id_; // for shared storage file path
  common::ObTabletID effective_tablet_id_; // indicate effective tablet id about partition split
  bool bypass_micro_cache_;
};


struct ObStorageObjectReadInfo final
{
public:
  ObStorageObjectReadInfo()
    : macro_block_id_(), offset_(), size_(), io_timeout_ms_(GCONF._data_storage_io_timeout / 1000L), io_desc_(),
      io_callback_(NULL), buf_(NULL), mtl_tenant_id_(OB_INVALID_TENANT_ID), local_cache_read_info_(),
      path_(), access_info_(nullptr), io_metrics_(nullptr)
  {}
  ~ObStorageObjectReadInfo() = default;
  OB_INLINE bool is_valid() const
  {
    return macro_block_id_.is_valid() && (offset_ >= 0) && (size_ > 0)
           && io_desc_.is_valid() && (nullptr != io_callback_ || nullptr != buf_)
           && (get_ls_epoch_id() >= 0)
           && (macro_block_id_.is_id_mode_local() || is_valid_tenant_id(mtl_tenant_id_));
  }
  OB_INLINE void set_logic_micro_id(const ObLogicMicroBlockId &logic_micro_id)
  {
    local_cache_read_info_.logic_micro_id_ = logic_micro_id;
  }
  OB_INLINE const ObLogicMicroBlockId &get_logic_micro_id() const
  {
    return local_cache_read_info_.logic_micro_id_;
  }
  OB_INLINE void set_micro_crc(const int64_t micro_crc)
  {
    local_cache_read_info_.micro_crc_ = micro_crc;
  }
  OB_INLINE int64_t get_micro_crc() const
  {
    return local_cache_read_info_.micro_crc_;
  }
  OB_INLINE void set_ls_epoch_id(const int64_t ls_epoch_id)
  {
    local_cache_read_info_.ls_epoch_id_ = ls_epoch_id;
  }
  OB_INLINE int64_t get_ls_epoch_id() const
  {
    return local_cache_read_info_.ls_epoch_id_;
  }
  OB_INLINE void set_effective_tablet_id(const common::ObTabletID &effective_tablet_id)
  {
    local_cache_read_info_.effective_tablet_id_ = effective_tablet_id;
  }
  OB_INLINE const common::ObTabletID &get_effective_tablet_id() const
  {
    return local_cache_read_info_.effective_tablet_id_;
  }
  OB_INLINE void set_bypass_micro_cache(const int64_t bypass_micro_cache)
  {
    local_cache_read_info_.bypass_micro_cache_ = bypass_micro_cache;
  }
  OB_INLINE bool get_bypass_micro_cache() const
  {
    return local_cache_read_info_.bypass_micro_cache_;
  }
  TO_STRING_KV(K_(macro_block_id), K_(offset), K_(size), K_(io_timeout_ms), K_(io_desc),
               KP_(io_callback), KP_(buf), K_(mtl_tenant_id), K_(local_cache_read_info),
               K(path_), KPC(access_info_), KPC(io_metrics_));
public:
  blocksstable::MacroBlockId macro_block_id_;
  int64_t offset_;
  int64_t size_;
  int64_t io_timeout_ms_;
  common::ObIOFlag io_desc_;
  common::ObIOCallback *io_callback_;
  char *buf_;
  uint64_t mtl_tenant_id_;
  ObLocalCacheReadInfo local_cache_read_info_;
  ObString path_;
  const ObObjectStorageInfo *access_info_;
  sql::ObLakeTableIOMetrics *io_metrics_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_OBJECT_RW_INFO_H_
