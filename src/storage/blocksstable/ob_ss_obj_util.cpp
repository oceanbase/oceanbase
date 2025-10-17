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
#define USING_LOG_PREFIX STORAGE
#include "ob_ss_obj_util.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/shared_storage/ob_file_helper.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_dir_manager.h"
#include "close_modules/shared_storage/storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#endif
namespace oceanbase
{
namespace blocksstable
{

bool SSObjUtil::is_private(const ObStorageObjectType type)
{
  return STI(type).is_private();
}
bool SSObjUtil::is_shared(const ObStorageObjectType type)
{
  return STI(type).is_shared();
}
bool SSObjUtil::is_macro_data(const ObStorageObjectType type)
{
  return STI(type).is_macro_data();
}
bool SSObjUtil::is_macro_meta(const ObStorageObjectType type)
{
  return STI(type).is_macro_meta();
}
bool SSObjUtil::is_macro(const ObStorageObjectType type)
{
  return STI(type).is_macro();
}
bool SSObjUtil::is_tenant_data(const ObStorageObjectType type)
{
  return STI(type).is_tenant_data();
}
bool SSObjUtil::is_tenant_meta(const ObStorageObjectType type)
{
  return STI(type).is_tenant_meta();
}
bool SSObjUtil::is_tablet_meta(const ObStorageObjectType type)
{
  return STI(type).is_tablet_meta();
}
bool SSObjUtil::is_major(const ObStorageObjectType type)
{
  return STI(type).is_major();
}
bool SSObjUtil::is_mds(const ObStorageObjectType type)
{
  return STI(type).is_mds();
}
bool SSObjUtil::is_direct_read(const ObStorageObjectType type)
{
  return STI(type).is_direct_read();
}
bool SSObjUtil::is_direct_write(const ObStorageObjectType type)
{
  return STI(type).is_direct_write();
}
bool SSObjUtil::use_reserved_disk_space(const ObStorageObjectType type)
{
  return STI(type).use_reserved_disk_space();
}
// judge whether object type need alloc disk space
// 1. is_direct_write = true: means write directly to object storage and do not write local cache file,
//    thus no need to alloc file size
// 2. is_overwrite = true: do not alloc/free file size for this kind of object, ignore these disk
//    space usage.
bool SSObjUtil::is_need_alloc_file_size(const ObStorageObjectType type)
{
  return !STI(type).is_direct_write() &&
         !STI(type).is_overwrite();
}

/* files with the following object types are pin:
 * SERVER_META and TENANT_DISK_SPACE_META
 */
bool SSObjUtil::is_pin_local(const ObStorageObjectType type)
{
  return STI(type).is_pin_local();
}

/* files with the following object types are overwrite:
 * SERVER_META, TENANT_DISK_SPACE_META, SHARED_TABLET_SUB_META, TENANT_ROOT_KEY
 */
bool SSObjUtil::is_overwrite(const ObStorageObjectType type)
{
  return STI(type).is_overwrite();
}

bool SSObjUtil::need_check_inner_tablet_type(const ObStorageObjectType type)
{
  return STI(type).is_path_include_inner_tablet();
}

bool SSObjUtil::is_tmp_file(const ObStorageObjectType type)
{
  return STI(type).is_tmp_file();
}

bool SSObjUtil::is_support_sn(const ObStorageObjectType type)
{
  return STI(type).is_support_sn();
}

int SSObjUtil::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id)
{
  return STI(opt.object_type_).get_object_id(opt, object_id);
}

int SSObjUtil::opt_to_string(const ObStorageObjectType type, char *buf, const int64_t buf_len,
                             int64_t &pos, const ObStorageObjectOpt &opt)
{
  return STI(type).opt_to_string(buf, buf_len, pos, opt);
}
// 1. is_read_through = true: means there exists no local cache file, thus no need to stat.
// 2. overwrite = true: do not alloc/free file size for this kind of object, thus no need to stat.
// ObDirManager::create_dir use ObStorageObjectType::MAX object_type in default. e.g., create
// ls and tenant dir, it does not alloc file size. because these dirs occupies very small disk
// space, which can be ignored.
bool SSObjUtil::is_need_stat(const ObStorageObjectType type)
{
  bool is_need_stat = !STI(type).is_direct_read() &&
                      !STI(type).is_overwrite();
  if (ObStorageObjectType::MAX == type) {
    is_need_stat = false;
  }
  return is_need_stat;
}
bool SSObjUtil::has_effective_tablet_id(const ObStorageObjectType type)
{
  return STI(type).has_effective_tablet_id();
}

bool SSObjUtil::is_support_fd_cache(const ObStorageObjectType type)
{
  return STI(type).is_support_fd_cache();
}

bool SSObjUtil::is_private(const MacroBlockId &macro_id)
{
  return is_private(macro_id.storage_object_type());
}

bool SSObjUtil::is_shared(const MacroBlockId &macro_id)
{
  return is_shared(macro_id.storage_object_type());
}

bool SSObjUtil::is_macro(const MacroBlockId &macro_id)
{
  return is_macro(macro_id.storage_object_type());
}

bool SSObjUtil::is_macro_meta(const MacroBlockId &macro_id)
{
  return is_macro_meta(macro_id.storage_object_type());
}

bool SSObjUtil::is_macro_data(const MacroBlockId &macro_id)
{
  return is_macro_data(macro_id.storage_object_type());
}

bool SSObjUtil::is_major(const MacroBlockId &macro_id)
{
  return is_major(macro_id.storage_object_type());
}

bool SSObjUtil::is_direct_write(const MacroBlockId &macro_id)
{
  return is_direct_write(macro_id.storage_object_type());
}

bool SSObjUtil::use_reserved_disk_space(const MacroBlockId &macro_id)
{
  return use_reserved_disk_space(macro_id.storage_object_type());
}

bool SSObjUtil::has_effective_tablet_id(const MacroBlockId &macro_id)
{
  return has_effective_tablet_id(macro_id.storage_object_type());
}

bool SSObjUtil::is_tmp_file(const MacroBlockId &macro_id)
{
  return is_tmp_file(macro_id.storage_object_type());
}

bool SSObjUtil::is_support_sn(const MacroBlockId &macro_id)
{
  return is_support_sn(macro_id.storage_object_type());
}

bool SSObjUtil::is_valid(const MacroBlockId &macro_id)
{
  return STI(macro_id.storage_object_type()).is_valid(macro_id);
}

bool SSObjUtil::need_fsync(const MacroBlockId &macro_id)
{
  return STI(macro_id.storage_object_type()).need_fsync();
}

bool SSObjUtil::server_tenant_can_have(const MacroBlockId &macro_id)
{
  return STI(macro_id.storage_object_type()).server_tenant_can_have();
}

bool SSObjUtil::is_read_out_of_bounds(const MacroBlockId &macro_id)
{
  return STI(macro_id.storage_object_type()).is_read_out_of_bounds();
}

bool SSObjUtil::can_append_write(const MacroBlockId &macro_id)
{
  return STI(macro_id.storage_object_type()).can_append_write();
}

#ifdef OB_BUILD_SHARED_STORAGE
int SSObjUtil::get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id)
{
  return STI(macro_id.storage_object_type()).get_effective_tablet_id(macro_id, effective_tablet_id);
}

int SSObjUtil::get_macro_cache_type(const ObStorageObjectType type, const uint64_t effective_tablet_id,
                                    const bool use_effective_tablet_id, storage::ObSSMacroCacheType &macro_cache_type)
{
  return STI(type).get_macro_cache_type(effective_tablet_id, use_effective_tablet_id, macro_cache_type);
}
int SSObjUtil::get_open_flag_for_write(const ObStorageObjectType type)
{
  return STI(type).get_open_flag_for_write();
}

int SSObjUtil::get_open_flag_for_read(const ObStorageObjectType type)
{
  return STI(type).get_open_flag_for_read();
}
int SSObjUtil::local_path_to_macro_id(const ObStorageObjectType type,const char *path, MacroBlockId &macro_id)
{
  return STI(type).local_path_to_macro_id(path, macro_id);
}
int SSObjUtil::aio_write(const ObStorageObjectWriteInfo &write_info, ObStorageObjectHandle &object_handle)
{
  return STI(object_handle.get_macro_id().storage_object_type()).aio_write(write_info, object_handle);
}

int SSObjUtil::aio_read(const ObStorageObjectReadInfo &read_info, ObStorageObjectHandle &object_handle)
{
  return STI(read_info.macro_block_id_.storage_object_type()).aio_read(read_info, object_handle);
}
int SSObjUtil::get_open_flag_for_read(const MacroBlockId &macro_id)
{
  return get_open_flag_for_read(macro_id.storage_object_type());
}
void SSObjUtil::get_ss_macro_block_type(const MacroBlockId &macro_id, ObSSMacroBlockType &block_type)
{
  STI(macro_id.storage_object_type()).get_ss_macro_block_type(macro_id, block_type);
}
int SSObjUtil::to_local_path_format(const MacroBlockId &macro_id, char *path, const int64_t length, int64_t &pos,
                                    const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id)
{
  return STI(macro_id.storage_object_type()).to_local_path_format(path, length, pos, macro_id, tenant_id, tenant_epoch_id, ls_epoch_id);
}

int SSObjUtil::to_remote_path_format(const MacroBlockId &macro_id, char *path, const int64_t length, int64_t &pos,
                                     const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
                                     const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id)
{
  return STI(macro_id.storage_object_type()).to_remote_path_format(path, length, pos, macro_id, object_storage_root_dir,
                                                                   cluster_id, tenant_id, tenant_epoch_id, server_id, ls_epoch_id);
}

int SSObjUtil::get_parent_dir(const MacroBlockId &macro_id, char *path, const int64_t length, int64_t &pos,
                              const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id)
{
  return STI(macro_id.storage_object_type()).get_parent_dir(path, length, pos, macro_id, tenant_id,
                                                            tenant_epoch_id, ls_epoch_id);
}

int SSObjUtil::create_parent_dir(const MacroBlockId &macro_id, const uint64_t tenant_id,
                                 const uint64_t tenant_epoch_id, const int64_t ls_epoch_id)
{
  return STI(macro_id.storage_object_type()).create_parent_dir(macro_id, tenant_id, tenant_epoch_id, ls_epoch_id);
}
#endif

}
}
