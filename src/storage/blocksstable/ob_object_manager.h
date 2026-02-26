/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_BLOCKSSTABLE_OB_OBJECT_MANAGER_H_
#define OCEANBASE_BLOCKSSTABLE_OB_OBJECT_MANAGER_H_

#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_super_block_buffer_holder.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/ob_super_block_struct.h"
#include "storage/blocksstable/ob_storage_object_type.h"


namespace oceanbase
{
namespace blocksstable
{
// ================================= ObStorageObjectOpt ====================================//

class ObStorageObjectOpt final
{
public:
  ObStorageObjectOpt()
    : object_type_(ObStorageObjectType::PRIVATE_DATA_MACRO) {}

  ~ObStorageObjectOpt() {}
  void set_private_object_opt(const int64_t tablet_id = -1, const int64_t tablet_transfer_seq = -1)
  {
    object_type_ = ObStorageObjectType::PRIVATE_DATA_MACRO;
    private_opt_.tablet_id_ = tablet_id;
    private_opt_.tablet_trasfer_seq_ = tablet_transfer_seq;
  }
  void set_private_meta_macro_object_opt(const int64_t tablet_id = -1, const int64_t tablet_transfer_seq = -1)
  {
    object_type_ = ObStorageObjectType::PRIVATE_META_MACRO;
    private_opt_.tablet_id_ = tablet_id;
    private_opt_.tablet_trasfer_seq_ = tablet_transfer_seq;
  }
  void set_private_ckpt_opt(const uint64_t tenant_id, const int64_t tenant_epoch_id, const int64_t file_id)
  {
    object_type_ = ObStorageObjectType::PRIVATE_CKPT_FILE;
    ss_slog_ckpt_obj_opt_.tenant_id_ = tenant_id;
    ss_slog_ckpt_obj_opt_.tenant_epoch_id_ = tenant_epoch_id;
    ss_slog_ckpt_obj_opt_.file_id_ = file_id;
  }
  void set_private_slog_opt(const uint64_t tenant_id, const int64_t tenant_epoch_id, const int64_t file_id)
  {
    object_type_ = ObStorageObjectType::PRIVATE_SLOG_FILE;
    ss_slog_ckpt_obj_opt_.tenant_id_ = tenant_id;
    ss_slog_ckpt_obj_opt_.tenant_epoch_id_ = tenant_epoch_id;
    ss_slog_ckpt_obj_opt_.file_id_ = file_id;
  }
  void set_ss_share_object_opt(
      const ObStorageObjectType obj_type,
      const bool is_ls_inner_tablet,
      const int64_t ls_id,
      const int64_t tablet_id,
      const int64_t data_seq,
      const int64_t column_group_id,
      const int64_t reorganization_scn)
  {
    object_type_ = obj_type;
    ss_share_opt_.is_ls_inner_tablet_ = is_ls_inner_tablet;
    ss_share_opt_.ls_id_ = ls_id;
    ss_share_opt_.tablet_id_ = tablet_id;
    ss_share_opt_.data_seq_ = data_seq;
    ss_share_opt_.column_group_id_ = column_group_id;
    ss_share_opt_.reorganization_scn_ = reorganization_scn;
  }
  void set_ss_share_data_macro_object_opt(
     const int64_t tablet_id,
     const int64_t data_seq,
     const int64_t column_group_id)
  {
    set_ss_share_object_opt(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, false, 0, tablet_id, data_seq, column_group_id, 0);
  }
  void set_ss_share_meta_macro_object_opt(
      const int64_t tablet_id,
      const int64_t data_seq,
      const int64_t column_group_id)
  {
    set_ss_share_object_opt(ObStorageObjectType::SHARED_MAJOR_META_MACRO, false, 0, tablet_id, data_seq, column_group_id, 0);
  }
  void set_ss_tmp_file_object_opt()
  {
    object_type_ = ObStorageObjectType::TMP_FILE;
  };
  void set_ss_sever_level_meta_object_opt(const ObStorageObjectType object_type)
  {
    object_type_ = object_type;
  };
  void set_ss_tenant_level_meta_object_opt(
      const ObStorageObjectType object_type,
      const int64_t tenant_id,
      const int64_t tenant_epoch_id)
  {
    object_type_ = object_type;
    ss_tenant_level_opt_.tenant_id_ = tenant_id;
    ss_tenant_level_opt_.tenant_epoch_id_ = tenant_epoch_id;
  };
  void set_ss_ls_level_meta_object_opt(
      const ObStorageObjectType object_type, const int64_t ls_id)
  {
    object_type_ = object_type;
    ss_ls_level_opt_.ls_id_ = ls_id;
  };

  void set_ss_private_tablet_meta_object_opt(
      const int64_t ls_id, const uint64_t tablet_id, const uint64_t version, const int64_t tablet_private_trasfer_epoch)
  {
    object_type_ = ObStorageObjectType::PRIVATE_TABLET_META;
    ss_private_tablet_opt_.ls_id_ = ls_id;
    ss_private_tablet_opt_.tablet_id_ = tablet_id;
    ss_private_tablet_opt_.version_ = version;
    ss_private_tablet_opt_.tablet_private_transfer_epoch_ = tablet_private_trasfer_epoch;
  }

  void set_ss_is_shared_tenant_deleted_object_opt(
    const uint64_t tenant_id)
  {
    object_type_ = ObStorageObjectType::IS_SHARED_TENANT_DELETED;
    ss_shared_tenant_id_opt_.tenant_id_ = tenant_id;
  }

  void set_ss_major_prewarm_opt(
    const ObStorageObjectType object_type, const uint64_t tablet_id, const uint64_t compaction_scn,
    const uint64_t reorganization_scn)
  {
    object_type_ = object_type;
    ss_major_prewarm_opt_.tablet_id_ = tablet_id;
    ss_major_prewarm_opt_.compaction_scn_ = compaction_scn;
    ss_major_prewarm_opt_.reorganization_scn_ = reorganization_scn;
  }

  void set_ss_tablet_sub_meta_opt(
    const uint64_t ls_id,
    const uint64_t tablet_id,
    const uint32_t op_id,
    const uint32_t data_seq,
    const bool is_inner_tablet,
    const int64_t reorganization_scn)
  {
    object_type_ = ObStorageObjectType::SHARED_TABLET_SUB_META;
    ss_tablet_sub_meta_opt_.ls_id_ = ls_id;
    ss_tablet_sub_meta_opt_.tablet_id_ = tablet_id;
    ss_tablet_sub_meta_opt_.is_inner_tablet_ = is_inner_tablet;
    ss_tablet_sub_meta_opt_.op_id_ = op_id;
    ss_tablet_sub_meta_opt_.data_seq_ = data_seq;
    ss_tablet_sub_meta_opt_.reorganization_scn_ = reorganization_scn;
  }

  void set_ss_root_key_object_opt()
  {
    object_type_ = ObStorageObjectType::TENANT_ROOT_KEY;
  }

  void set_ss_external_table_file_opt(const uint64_t server_seq_id, const int64_t offset_idx)
  {
    object_type_ = ObStorageObjectType::EXTERNAL_TABLE_FILE;
    ss_external_table_file_opt_.server_seq_id_ = server_seq_id;
    ss_external_table_file_opt_.offset_idx_ = offset_idx;
  }

  void set_ss_macro_cache_ckpt_opt(
      const bool is_meta, const uint64_t version_id, const uint64_t seq_id)
  {
    object_type_ = (is_meta
        ? ObStorageObjectType::MACRO_CACHE_CKPT_META
        : ObStorageObjectType::MACRO_CACHE_CKPT_DATA);
    ss_macro_cache_ckpt_opt_.version_id_ = version_id;
    ss_macro_cache_ckpt_opt_.seq_id_ = seq_id;
  }

  void set_ss_tablet_meta_opt(
      const ObStorageObjectType object_type,
      const uint64_t ls_id,
      const uint64_t tablet_id,
      const uint64_t op_id,
      const bool is_inner_tablet,
      const int64_t reorganization_scn)
  {
    object_type_ = object_type;
    ss_tablet_meta_opt_.tablet_id_ = tablet_id;
    ss_tablet_meta_opt_.op_id_ = op_id;
    ss_tablet_meta_opt_.ls_id_ = ls_id;
    ss_tablet_meta_opt_.is_inner_tablet_ = is_inner_tablet;
    ss_tablet_meta_opt_.reorganization_scn_ = reorganization_scn;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  static constexpr uint64_t INVALID_TABLET_VERSION = (1llu << ((MacroBlockId::SF_BIT_META_VERSION_ID) + 1)) - 1;
  static const int64_t INVALID_TABLET_TRANSFER_SEQ = -1;

private:
  struct PrivateObjectOpt
  {
    uint64_t tablet_id_;
    uint64_t tablet_trasfer_seq_;
  };
  struct SSShareObjectOpt
  {
    bool is_ls_inner_tablet_;
    uint64_t ls_id_;          // only valid for inner tablet
    uint64_t tablet_id_;
    int64_t data_seq_;
    int64_t column_group_id_;
    int64_t reorganization_scn_;
  };
  struct SSTmpFileObjectOpt
  {
  };

  // server level meta include: server_meta
  struct SSServerLevelMetaObjectOpt
  {
  };
  // tenant level meta include: tenant_meta/unit_meta
  struct SSTenantLevelMetaObjectOpt
  {
    uint64_t tenant_id_;
    int64_t tenant_epoch_id_;
  };

  // tenant slog/ckpt object
  struct SSTenantSlogCkptObjectOpt
  {
    uint64_t tenant_id_;
    int64_t tenant_epoch_id_;
    uint64_t file_id_;
  };
  // ls level meta include:
  // ls_meta/dup_table_meta/active_tablet_array/pending_free_tablet_array/tansfer_id_array
  struct SSLSLevelMetaObjectOpt
  {
    uint64_t ls_id_;
  };

  struct SSPrivateTabletMetaObjectOpt
  {
    uint64_t ls_id_;
    uint64_t tablet_id_;
    int64_t version_;
    int64_t tablet_private_transfer_epoch_;
  };
  struct SSPrivateTabletCurrentVersionObjectOpt
  {
    uint64_t ls_id_;
    uint64_t tablet_id_;
  };

  struct SSMajorPrewarmObjectOpt
  {
    uint64_t tablet_id_;
    uint64_t compaction_scn_;
    uint64_t reorganization_scn_;
  };

  struct SSSharedTenantIdOpt
  {
    uint64_t tenant_id_;
  };

  struct SSTabletSubMetaObjectOpt
  {
    uint64_t ls_id_;
    uint64_t tablet_id_;
    uint32_t op_id_;
    uint32_t data_seq_;
    bool is_inner_tablet_;
    int64_t reorganization_scn_;
  };
  struct SSExternalTableFileObjectOpt
  {
    uint64_t server_seq_id_;
    int64_t offset_idx_;
  };
  struct SSMacroCacheCkptObjectOpt
  {
    uint64_t version_id_;
    uint64_t seq_id_;
  };
  struct SSTabletMetaObjectOpt
  {
    uint64_t tablet_id_;
    uint64_t op_id_;
    uint64_t ls_id_;
    bool is_inner_tablet_;
    int64_t reorganization_scn_;
  };

public:
  ObStorageObjectType object_type_;
  union
  {
    PrivateObjectOpt private_opt_;
    SSShareObjectOpt ss_share_opt_;
    SSTmpFileObjectOpt ss_tmp_file_opt_;
    SSServerLevelMetaObjectOpt ss_server_level_opt_;
    SSTenantLevelMetaObjectOpt ss_tenant_level_opt_;
    SSLSLevelMetaObjectOpt ss_ls_level_opt_;
    SSPrivateTabletMetaObjectOpt ss_private_tablet_opt_;
    SSPrivateTabletCurrentVersionObjectOpt ss_private_tablet_current_version_opt_;
    SSMajorPrewarmObjectOpt ss_major_prewarm_opt_;
    SSSharedTenantIdOpt ss_shared_tenant_id_opt_;
    SSTabletSubMetaObjectOpt ss_tablet_sub_meta_opt_;
    SSTenantSlogCkptObjectOpt ss_slog_ckpt_obj_opt_;
    SSExternalTableFileObjectOpt ss_external_table_file_opt_;
    SSMacroCacheCkptObjectOpt ss_macro_cache_ckpt_opt_;
    SSTabletMetaObjectOpt ss_tablet_meta_opt_;
  };

};


// ================================== ObObjectManager =====================================//
class ObObjectManager final
{
public:
  int init(const bool is_shared_storage, const int64_t macro_object_size);
  int start(const int64_t reserved_size);
  void stop();
  void wait();
  void destroy();

  int alloc_object(
      const ObStorageObjectOpt &opt,
      ObStorageObjectHandle &object_handle);
  static int async_read_object(
      const ObStorageObjectReadInfo &read_info,
      ObStorageObjectHandle &object_handle);
  static int async_write_object(
      const ObStorageObjectOpt &opt,
      const ObStorageObjectWriteInfo &write_info,
      ObStorageObjectHandle &object_handle);
  static int read_object(
      const ObStorageObjectReadInfo &read_info,
      ObStorageObjectHandle &object_handle);
  static int write_object(
      const ObStorageObjectOpt &opt,
      const ObStorageObjectWriteInfo &write_info,
      ObStorageObjectHandle &object_handle);
  int get_object_size(
      const MacroBlockId &object_id,
      const int64_t ls_epoch,
      int64_t &object_size) const;
  int64_t get_macro_object_size() const { return macro_object_size_; }
  int inc_ref(const MacroBlockId &object_id) const;
  int dec_ref(const MacroBlockId &object_id) const;
  int resize_local_device(
      const int64_t new_device_size,
      const int64_t new_device_disk_percentage,
      const int64_t reserved_size);
  int check_disk_space_available();
  int update_super_block(
      const common::ObLogCursor &replay_start_point,
      const blocksstable::MacroBlockId &tenant_meta_entry,
      const storage::ObSlogCheckpointFdDispenser &fd_dispenser);

  static ObObjectManager &get_instance();

  const storage::ObServerSuperBlock &get_server_super_block() const
  {
    return super_block_;
  }

  void set_min_max_file_id_in_server_super_block(const int64_t min_file_id, const int64_t max_file_id)
  {
    SpinWLockGuard guard(lock_);
    super_block_.min_file_id_ = min_file_id;
    super_block_.max_file_id_ = max_file_id;
  }

  void get_server_super_block_by_copy(storage::ObServerSuperBlock &other) const
  {
    SpinRLockGuard guard(lock_);
    other = super_block_;
  }

  int64_t get_macro_block_size() const
  {
    return super_block_.get_macro_block_size();
  }
  int64_t get_max_macro_block_count(int64_t reserved_size) const;
  int64_t get_used_macro_block_count() const;
  int64_t get_free_macro_block_count() const;
  int64_t get_total_macro_block_count() const
  {
    return super_block_.get_total_macro_block_count();
  }
  int read_or_format_super_block_(const bool need_format);

#ifdef OB_BUILD_SHARED_STORAGE
  int alloc_tenant_epoch(const uint64_t tenant_id, int64_t &tenant_epoch);
  int create_super_block_tenant_item(
      const uint64_t tenant_id,
      const int64_t tenant_epoch,
      const storage::ObTenantCreateStatus status);
  int delete_super_block_tenant_item(
      const uint64_t tenant_id, const int64_t tenant_epoch);
  static int ss_get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id);
  static int ss_is_exist_object(const MacroBlockId &object_id, const int64_t ls_epoch, bool &is_exist);
  static int seal_object(const MacroBlockId &object_id, const int64_t ls_epoch_id);
  static int delete_object(const MacroBlockId &object_id, const int64_t ls_epoch_id);
  static int async_write_object(
    const blocksstable::MacroBlockId &macro_block_id,
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle);
#endif

private:
  ObObjectManager();
  ~ObObjectManager();

#ifdef OB_BUILD_SHARED_STORAGE
  static void set_ss_object_first_id_(
      const uint64_t object_type, const uint64_t incarnation_id,
      const uint64_t column_group_id, MacroBlockId &object_id);
  static MacroBlockId ss_get_super_block_object_id_();
  int ss_read_or_format_super_block_();
  int ss_read_super_block_(const MacroBlockId &macro_id, storage::ObServerSuperBlock &super_block);
  int ss_write_super_block_(const storage::ObServerSuperBlock &super_block);

#endif

private:
  bool is_inited_;
  bool is_shared_storage_;
  int64_t macro_object_size_;

  common::SpinRWLock lock_;
  storage::ObServerSuperBlock super_block_; // read only memory cache
  ObSuperBlockBufferHolder super_block_buf_holder_;
  lib::ObMutex resize_file_lock_;

  DISALLOW_COPY_AND_ASSIGN(ObObjectManager);
};

#define OB_STORAGE_OBJECT_MGR (oceanbase::blocksstable::ObObjectManager::get_instance())

}  // namespace blocksstable
}  // namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_OB_OBJECT_MANAGER_H_
