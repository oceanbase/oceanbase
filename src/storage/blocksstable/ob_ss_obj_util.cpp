/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_ss_obj_util.h"
#include "storage/blocksstable/ob_object_manager.h"
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

bool SSObjUtil::use_reserved_disk_space(const ObStorageObjectType type)
{
  return STI(type).use_reserved_disk_space();
}
// judge whether object type need alloc disk space
// 1. has_write_back_strategy = true: means may write local cache file, and thus need to alloc
//    file size.
// 2. is_overwrite = true: do not alloc/free file size for this kind of object, ignore these disk
//    space usage.
bool SSObjUtil::is_need_alloc_file_size(const ObStorageObjectType type)
{
  return STI(type).has_write_back_strategy() && !STI(type).is_overwrite();
}

/* files with the following object types are pin:
 * SERVER_META and TENANT_DISK_SPACE_META
 */
bool SSObjUtil::is_pin_local(const ObStorageObjectType type)
{
  return STI(type).is_pin_local();
}

/* files with the following object types are overwrite:
 * SERVER_META, TENANT_DISK_SPACE_META, TENANT_ROOT_KEY
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

bool SSObjUtil::is_store_in_table(const ObStorageObjectType type)
{
  return STI(type).is_store_in_table();
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

bool SSObjUtil::is_store_in_table(const MacroBlockId &macro_id)
{
  return STI(macro_id.storage_object_type()).is_store_in_table();
}

bool SSObjUtil::is_tablet_meta(const MacroBlockId &macro_id)
{
  return STI(macro_id.storage_object_type()).is_tablet_meta();
}

bool SSObjUtil::is_shared_tablet_sub_meta(const ObStorageObjectType type)
{
  return STI(type).is_shared_tablet_sub_meta();
}

bool SSObjUtil::is_shared_tablet_sub_meta(const MacroBlockId &macro_id)
{
  return is_shared_tablet_sub_meta(macro_id.storage_object_type());
}

bool SSObjUtil::is_shared_tablet_sub_meta_in_table(const ObStorageObjectType type)
{
  return STI(type).is_shared_tablet_sub_meta_in_table();
}

bool SSObjUtil::is_shared_tablet_sub_meta_in_table(const MacroBlockId &macro_id)
{
  return is_shared_tablet_sub_meta_in_table(macro_id.storage_object_type());
}


}
}
