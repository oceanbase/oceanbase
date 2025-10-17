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
#ifndef OCEANBASE_BLOCKSSTABLE_OB_SS_OBJ_UTIL_H_
#define OCEANBASE_BLOCKSSTABLE_OB_SS_OBJ_UTIL_H_

#include "storage/blocksstable/ob_storage_object_type.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
class ObStorageObjectOpt;
class SSObjUtil
{
public:
  SSObjUtil();
  ~SSObjUtil();
  static bool is_private(const ObStorageObjectType type);
  static bool is_shared(const ObStorageObjectType type);
  static bool is_macro_data(const ObStorageObjectType type);
  static bool is_macro_meta(const ObStorageObjectType type);
  static bool is_macro(const ObStorageObjectType type);
  static bool is_tenant_data(const ObStorageObjectType type);
  static bool is_tenant_meta(const ObStorageObjectType type);
  static bool is_tablet_meta(const ObStorageObjectType type);
  static bool is_major(const ObStorageObjectType type);
  static bool is_mds(const ObStorageObjectType type);
  static bool is_direct_read(const ObStorageObjectType type);
  static bool is_direct_write(const ObStorageObjectType type);
  static bool is_need_alloc_file_size(const ObStorageObjectType type);
  static bool is_pin_local(const ObStorageObjectType type);
  static bool is_overwrite(const ObStorageObjectType type);
  static bool need_check_inner_tablet_type(const ObStorageObjectType type);
  static bool is_tmp_file(const ObStorageObjectType type);
  static bool is_support_sn(const ObStorageObjectType type);
  static bool is_support_fd_cache(const ObStorageObjectType type);
  static bool use_reserved_disk_space(const ObStorageObjectType type);
  static bool is_need_stat(const ObStorageObjectType type);
  static bool has_effective_tablet_id(const ObStorageObjectType type);

  static int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id);
  static int opt_to_string(const ObStorageObjectType type, char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt);
  static bool is_valid(const MacroBlockId &macro_id);
  static bool is_private(const MacroBlockId &macro_id);
  static bool is_shared(const MacroBlockId &macro_id);
  static bool is_macro_meta(const MacroBlockId &macro_id);
  static bool is_macro_data(const MacroBlockId &macro_id);
  static bool is_macro(const MacroBlockId &macro_id);
  static bool is_major(const MacroBlockId &macro_id);
  static bool need_fsync(const MacroBlockId &macro_id);
  static bool is_direct_write(const MacroBlockId &macro_id);
  static bool server_tenant_can_have(const MacroBlockId &macro_id);
  static bool use_reserved_disk_space(const MacroBlockId &macro_id);
  static bool can_append_write(const MacroBlockId &macro_id);
  static bool is_read_out_of_bounds(const MacroBlockId &macro_id);
  static bool has_effective_tablet_id(const MacroBlockId &macro_id);
  static bool is_tmp_file(const MacroBlockId &macro_id);
  static bool is_support_sn(const MacroBlockId &macro_id);
#ifdef OB_BUILD_SHARED_STORAGE
  static int get_open_flag_for_write(const ObStorageObjectType type);
  static int get_open_flag_for_read(const ObStorageObjectType type);
  static int aio_write(const ObStorageObjectWriteInfo &write_info, ObStorageObjectHandle &object_handle);
  static int aio_read(const ObStorageObjectReadInfo &read_info, ObStorageObjectHandle &object_handle);
  static int get_macro_cache_type(const ObStorageObjectType type, const uint64_t effective_tablet_id,
                          const bool use_effective_tablet_id, storage::ObSSMacroCacheType &macro_cache_type);
  static int local_path_to_macro_id(const ObStorageObjectType type,const char *path, MacroBlockId &macro_id);
  static int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id);
  static int get_open_flag_for_read(const MacroBlockId &macro_id);
  static void get_ss_macro_block_type(const MacroBlockId &macro_id, storage::ObSSMacroBlockType &block_type);
  static int to_local_path_format(const MacroBlockId &macro_id, char *path, const int64_t length, int64_t &pos,
                                  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id);
  static int to_remote_path_format(const MacroBlockId &macro_id, char *path, const int64_t length, int64_t &pos,
                                   const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id);
  static int local_path_to_macro_id(const MacroBlockId &macro_id, const char *path);
  static int get_parent_dir(const MacroBlockId &macro_id, char *path, const int64_t length, int64_t &pos,
                            const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id);
  static int create_parent_dir(const MacroBlockId &macro_id, const uint64_t tenant_id,
                               const uint64_t tenant_epoch_id, const int64_t ls_epoch_id);
#endif
};
}
}

#endif