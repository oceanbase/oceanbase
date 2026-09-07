/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  static bool is_store_in_table(const ObStorageObjectType type);
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
  static bool server_tenant_can_have(const MacroBlockId &macro_id);
  static bool use_reserved_disk_space(const MacroBlockId &macro_id);
  static bool can_append_write(const MacroBlockId &macro_id);
  static bool is_read_out_of_bounds(const MacroBlockId &macro_id);
  static bool has_effective_tablet_id(const MacroBlockId &macro_id);
  static bool is_tmp_file(const MacroBlockId &macro_id);
  static bool is_support_sn(const MacroBlockId &macro_id);
  static bool is_store_in_table(const MacroBlockId &macro_id);
  static bool is_tablet_meta(const MacroBlockId &macro_id);
  static bool is_shared_tablet_sub_meta(const ObStorageObjectType type);
  static bool is_shared_tablet_sub_meta(const MacroBlockId &macro_id);
  static bool is_shared_tablet_sub_meta_in_table(const ObStorageObjectType type);
  static bool is_shared_tablet_sub_meta_in_table(const MacroBlockId &macro_id);
};
}
}

#endif
