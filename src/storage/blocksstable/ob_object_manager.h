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
#include "storage/ob_super_block_struct.h"


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
  static int get_tablet_version(const MacroBlockId &tablet_object_id, int64_t &version);
  void set_private_object_opt(const int64_t tablet_id = -1)
  {
    object_type_ = ObStorageObjectType::PRIVATE_DATA_MACRO;
    private_opt_.tablet_id_ = tablet_id;
  }
  void set_private_meta_macro_object_opt(const int64_t tablet_id = -1)
  {
    object_type_ = ObStorageObjectType::PRIVATE_META_MACRO;
    private_opt_.tablet_id_ = tablet_id;
  }
  void set_ss_share_data_macro_object_opt(
      const int64_t tablet_id,
      const int64_t data_seq,
      const int64_t column_group_id)
  {
    object_type_ = ObStorageObjectType::SHARED_MAJOR_DATA_MACRO;
    ss_share_opt_.tablet_id_ = tablet_id;
    ss_share_opt_.data_seq_ = data_seq;
    ss_share_opt_.column_group_id_ = column_group_id;
  }
  void set_ss_share_meta_macro_object_opt(
      const int64_t tablet_id,
      const int64_t data_seq,
      const int64_t column_group_id)
  {
    object_type_ = ObStorageObjectType::SHARED_MAJOR_META_MACRO;
    ss_share_opt_.tablet_id_ = tablet_id;
    ss_share_opt_.data_seq_ = data_seq;
    ss_share_opt_.column_group_id_ = column_group_id;
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
      const int64_t ls_id, const uint64_t tablet_id, const int64_t version)
  {
    object_type_ = ObStorageObjectType::PRIVATE_TABLET_META;
    ss_private_tablet_opt_.ls_id_ = ls_id;
    ss_private_tablet_opt_.tablet_id_ = tablet_id;
    ss_private_tablet_opt_.version_ = version;
  }
  void set_ss_private_tablet_meta_current_verison_object_opt(
      const int64_t ls_id, const uint64_t tablet_id)
  {
    object_type_ = ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION;
    ss_private_tablet_current_version_opt_.ls_id_ = ls_id;
    ss_private_tablet_current_version_opt_.tablet_id_ = tablet_id;
  }

  void set_ss_share_tablet_meta_object_opt(
      const uint64_t tablet_id,
      const int64_t version)
  {
    object_type_ = ObStorageObjectType::SHARED_MAJOR_TABLET_META;
    ss_share_tablet_opt_.tablet_id_ = tablet_id;
    ss_share_tablet_opt_.version_ = version;
  }
  void set_ss_shared_tablet_id_object_opt(
    const uint64_t tablet_id)
  {
    object_type_ = ObStorageObjectType::SHARED_TABLET_ID;
    ss_shared_tablet_id_opt_.tablet_id_ = tablet_id;
  }
  void set_ss_is_deleted_object_opt(
    const uint64_t tablet_id)
  {
    object_type_ = ObStorageObjectType::IS_SHARED_TABLET_DELETED;
    ss_shared_tablet_id_opt_.tablet_id_ = tablet_id;
  }
  void set_ss_is_shared_tenant_deleted_object_opt(
    const uint64_t tenant_id)
  {
    object_type_ = ObStorageObjectType::IS_SHARED_TENANT_DELETED;
    ss_shared_tenant_id_opt_.tenant_id_ = tenant_id;
  }
  void set_ss_compaction_scheduler_object_opt(
      const ObStorageObjectType object_type, const int64_t ls_id)
  {
    object_type_ = object_type;
    ss_compaction_scheduler_opt_.ls_id_ = ls_id;
  };
  void set_ss_compactor_svr_object_opt(
      const ObStorageObjectType object_type, const int64_t server_id)
  {
    object_type_ = object_type;
    ss_svr_compactor_opt_.server_id_ = server_id;
  };
  void set_ss_compactor_ls_svr_object_opt(
    const ObStorageObjectType object_type,  const int64_t ls_id, const int64_t server_id)
  {
    object_type_ = object_type;
    ss_ls_svr_compactor_opt_.ls_id_ = ls_id;
    ss_ls_svr_compactor_opt_.server_id_ = server_id;
  };
  void set_ss_gc_info_object_opt(
    const uint64_t tablet_id)
  {
    object_type_ = ObStorageObjectType::SHARED_MAJOR_GC_INFO;
    ss_gc_info_opt_.tablet_id_ = tablet_id;
  }
  void set_ss_meta_list_object_opt(
    const uint64_t tablet_id)
  {
    object_type_ = ObStorageObjectType::SHARED_MAJOR_META_LIST;
    ss_meta_list_opt_.tablet_id_ = tablet_id;
  }
  void set_ss_tablet_compaction_status_object_opt(
    const uint64_t tablet_id,
    const int64_t scn_id)
  {
    object_type_ = ObStorageObjectType::TABLET_COMPACTION_STATUS;
    ss_tablet_compaction_status_opt_.tablet_id_ = tablet_id;
    ss_tablet_compaction_status_opt_.scn_id_ = scn_id;
  }

  void set_ss_major_prewarm_opt(
    const ObStorageObjectType object_type, const uint64_t tablet_id, const uint64_t compaction_scn)
  {
    object_type_ = object_type;
    ss_major_prewarm_opt_.tablet_id_ = tablet_id;
    ss_major_prewarm_opt_.compaction_scn_ = compaction_scn;
  }

  void set_ss_checksum_error_dump_macro_opt(
    const ObStorageObjectType object_type,
    const uint64_t tablet_id,
    const uint64_t cg_id,
    const uint64_t compaction_scn,
    const uint64_t block_seq)
  {
    object_type_ = object_type;
    ss_ckm_error_dump_macro_id_opt_.tablet_id_ = tablet_id;
    ss_ckm_error_dump_macro_id_opt_.cg_id_ = cg_id;
    ss_ckm_error_dump_macro_id_opt_.compaction_scn_ = compaction_scn;
    ss_ckm_error_dump_macro_id_opt_.block_seq_ = block_seq;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  static const int64_t INVALID_TABLET_VERSION = -1;
  static bool is_inaccurate_tablet_addr(const storage::ObMetaDiskAddr &tablet_meta_addr)
  {
    return tablet_meta_addr.is_block() && tablet_meta_addr.fifth_id() == INVALID_TABLET_VERSION;
  }

private:
  struct PrivateObjectOpt
  {
    uint64_t tablet_id_;
  };
  struct SSShareObjectOpt
  {
    uint64_t tablet_id_;
    int64_t data_seq_;
    int64_t column_group_id_;
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
  };
  struct SSPrivateTabletCurrentVersionObjectOpt
  {
    uint64_t ls_id_;
    uint64_t tablet_id_;
  };

  struct SSShareTabletMetaObjectOpt
  {
    uint64_t tablet_id_;
    int64_t version_;
  };

  struct SSCompactionSchedulerObjectOpt
  {
    uint64_t ls_id_;
  };
  struct SSCompactorSvrObjectOpt
  {
    int64_t server_id_;
  };
  struct SSCompactorLSSvrObjectOpt
  {
    uint64_t ls_id_;
    int64_t server_id_;
  };
  struct SSGCInfoObjectOpt
  {
    uint64_t tablet_id_;
  };
  struct SSGCMetaListObjectOpt
  {
    uint64_t tablet_id_;
  };
  struct SSTabletCompactionStatusObjectOpt
  {
    uint64_t tablet_id_;
    int64_t scn_id_;
  };
  struct SSMajorPrewarmObjectOpt
  {
    uint64_t tablet_id_;
    uint64_t compaction_scn_;
  };
  struct SSSharedTabletIdOpt
  {
    uint64_t tablet_id_;
  };

  struct SSSharedTenantIdOpt
  {
    uint64_t tenant_id_;
  };
  struct SSCkmErrorDumpMacroObjectOpt
  {
    uint64_t tablet_id_;
    uint64_t cg_id_;
    uint64_t compaction_scn_;
    uint64_t block_seq_;
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
    SSShareTabletMetaObjectOpt ss_share_tablet_opt_;
    SSCompactionSchedulerObjectOpt ss_compaction_scheduler_opt_;
    SSCompactorSvrObjectOpt ss_svr_compactor_opt_;
    SSCompactorLSSvrObjectOpt ss_ls_svr_compactor_opt_;
    SSGCInfoObjectOpt ss_gc_info_opt_;
    SSGCMetaListObjectOpt ss_meta_list_opt_;
    SSTabletCompactionStatusObjectOpt ss_tablet_compaction_status_opt_;
    SSMajorPrewarmObjectOpt ss_major_prewarm_opt_;
    SSSharedTabletIdOpt ss_shared_tablet_id_opt_;
    SSSharedTenantIdOpt ss_shared_tenant_id_opt_;
    SSCkmErrorDumpMacroObjectOpt ss_ckm_error_dump_macro_id_opt_;
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
  int update_super_block(const common::ObLogCursor &replay_start_point,
                         const blocksstable::MacroBlockId &tenant_meta_entry);

  static ObObjectManager &get_instance();

  const storage::ObServerSuperBlock &get_server_super_block() const
  {
    return super_block_;
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
  int create_super_block_tenant_item(const uint64_t tenant_id, int64_t &tenant_epoch);
  int update_super_block_tenant_item(
      const uint64_t tenant_id, const int64_t tenant_epoch,
      const storage::ObTenantCreateStatus status);
  int delete_super_block_tenant_item(
      const uint64_t tenant_id, const int64_t tenant_epoch);
  static int ss_get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id);
  static int ss_is_exist_object(const MacroBlockId &object_id, const int64_t ls_epoch, bool &is_exist);
  static int seal_object(const MacroBlockId &object_id, const int64_t ls_epoch_id);
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
