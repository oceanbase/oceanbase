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

#ifndef SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_
#define SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_

#include "share/ob_define.h"
#include "common/storage/ob_io_device.h"

namespace oceanbase
{
namespace blocksstable
{
/*
  *******************************************************************************
  * WARNING!!! Forbid insert object type in the middle! Only allow at the tail! *
  *******************************************************************************
  Note for add new ObjectType
  obj_id: ObStorageObjectType ID name
  obj_str: ObStorageObjectType str name
  is_pin_local: the ObjetType only store in local cache, true or false
  is_read_through: the ObjetType only store in remote object storage, true or false
  is_valid: the MacroBlockId is valid check logic
  to_local_path_format: the MacroBlockId convert to local cache path logic, if does not exist return OB_NOT_SUPPORTED
  to_remote_path_format: the MacroBlockId convert to object storage path logic, if does not exist return OB_NOT_SUPPORTED
  get_parent_dir: the MacroBlockId get parent local dir logic, if does not exist return OB_NOT_SUPPORTED
  create_parent_dir: the MacroBlockId get parent local dir logic, if does not exist return OB_NOT_SUPPORTED
  Finally when adding new type, we need to ensure if the new type have to add into MacroBlockId::is_shared_data_or_meta() or MacroBlockId::is_private_data_or_meta() or MacroBlockId::is_data() or MacroBlockId::is_meta()
*/
// STORAGE_OBJECT_TYPE_INFO(obj_id, obj_str, is_pin_local, is_read_through, is_valid, to_local_path_format, to_remote_path_format, get_parent_dir, create_parent_dir)
#define OB_STORAGE_OBJECT_TYPE_LIST \
  STORAGE_OBJECT_TYPE_INFO(PRIVATE_DATA_MACRO, "PRIVATE_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:server_id, fourth_id:macro_transfer_seq+tenant_seq */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() > 0) && (file_id_.macro_transfer_seq() >= 0) && (file_id_.tenant_seq() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data/macro_server_id_seq_id */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld/%ld/%s/%ld_%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     TABLET_DATA_DIR_STR, file_id_.second_id(), file_id_.macro_transfer_seq(), \
                     DATA_MACRO_DIR_STR, file_id_.third_id(), file_id_.tenant_seq())), \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data/macro_server_id_seq_id */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%ld/%lu_%ld/%s/%ld/%ld/%s/%ld_%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, \
                     file_id_.third_id(), tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR, file_id_.second_id(), \
                     file_id_.macro_transfer_seq(), DATA_MACRO_DIR_STR, file_id_.third_id(), file_id_.tenant_seq())), \
    /*get_parent_dir: tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data/ */ \
    (OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), file_id.macro_transfer_seq(), ObMacroType::DATA_MACRO)), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(tenant_id, tenant_epoch_id, file_id.second_id(), file_id.macro_transfer_seq()))) \
  STORAGE_OBJECT_TYPE_INFO(PRIVATE_META_MACRO, "PRIVATE_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:server_id, fourth_id:macro_transfer_seq+tenant_seq */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() > 0) && (file_id_.macro_transfer_seq() >= 0) && (file_id_.tenant_seq() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/meta/macro_server_id_seq_id */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld/%ld/%s/%ld_%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     TABLET_DATA_DIR_STR, file_id_.second_id(), file_id_.macro_transfer_seq(), \
                     META_MACRO_DIR_STR, file_id_.third_id(), file_id_.tenant_seq())), \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/tansfer_seq/meta/macro_server_id_seq_id */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%ld/%lu_%ld/%s/%ld/%ld/%s/%ld_%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, \
                     file_id_.third_id(), tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR, file_id_.second_id(), \
                     file_id_.macro_transfer_seq(), META_MACRO_DIR_STR, file_id_.third_id(), file_id_.tenant_seq())), \
    /*get_parent_dir: tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/meta/ */ \
    (OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), file_id.macro_transfer_seq(), ObMacroType::META_MACRO)), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(tenant_id, tenant_epoch_id, file_id.second_id(), file_id.macro_transfer_seq()))) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MINI_DATA_MACRO, "SHARED_MINI_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MINI_META_MACRO, "SHARED_MINI_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid*/false, \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MINOR_DATA_MACRO, "SHARED_MINOR_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid*/false, \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MINOR_META_MACRO, "SHARED_MINOR_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid*/false, \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MAJOR_DATA_MACRO, "SHARED_MAJOR_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/shared_major_macro_cache/tablet_id_cg_id_macro_seq_id_data */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld_%ld_%ld_%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     MAJOR_DATA_DIR_STR, file_id_.second_id(), file_id_.column_group_id(), \
                     file_id_.third_id(), DATA_MACRO_DIR_STR)), \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/sstable/cg_id/data/macro_seq_id */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s_%ld/%s/%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, \
                     file_id_.column_group_id(), DATA_MACRO_DIR_STR, file_id_.third_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MAJOR_META_MACRO, "SHARED_MAJOR_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/shared_major_macro_cache/tablet_id_cg_id_macro_seq_id_meta */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld_%ld_%ld_%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     MAJOR_DATA_DIR_STR, file_id_.second_id(), file_id_.column_group_id(), \
                     file_id_.third_id(), META_MACRO_DIR_STR)), \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/sstable/cg_id/meta/macro_seq_id */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s_%ld/%s/%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, \
                     file_id_.column_group_id(), META_MACRO_DIR_STR, file_id_.third_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(TMP_FILE, "TMP_FILE", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:tmp_file_id, third_id:segment_id, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id or segment_id.deleted */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld/%ld%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, TMP_DATA_DIR_STR, \
                     file_id_.second_id(), file_id_.third_id(), (is_logical_delete_ ? DEFAULT_DELETED_STR : ""))), \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id, \
                     TMP_DATA_DIR_STR, file_id_.second_id(), file_id_.third_id())), \
    /*get_parent_dir: tenant_id_epoch_id/tmp_data/tmp_file_id/ */ \
    (OB_DIR_MGR.get_local_tmp_file_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id())), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tmp_file_dir(tenant_id, tenant_epoch_id, file_id.second_id()))) \
  STORAGE_OBJECT_TYPE_INFO(SERVER_META, "SERVER_META", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:N/A, third_id:N/A, fourth_id:N/A */ true, \
    /*to_local_path_format: super_block*/ \
    (databuff_printf(path_, length, pos, "%s/%s", OB_DIR_MGR.get_local_cache_root_dir(), get_storage_objet_type_str(object_type))), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: local_cache_root_dir */ \
    (databuff_printf(path, length, pos, "%s", OB_DIR_MGR.get_local_cache_root_dir())), \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(TENANT_SUPER_BLOCK, "TENANT_SUPER_BLOCK", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:N/A */ \
    ((is_valid_tenant_id(file_id_.second_id())) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/tenant_super_block */ \
    (databuff_printf(path_, length, pos, "%s/%ld_%ld/%s", \
                    OB_DIR_MGR.get_local_cache_root_dir(), file_id_.second_id(), file_id_.third_id(), \
                     get_storage_objet_type_str(object_type))), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id/ */ \
    (OB_DIR_MGR.get_local_tenant_dir(path, length, file_id.second_id(), file_id.third_id())), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tenant_dir(file_id.second_id(), file_id.third_id()))) \
  STORAGE_OBJECT_TYPE_INFO(TENANT_UNIT_META, "TENANT_UNIT_META", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:N/A */ \
    ((is_valid_tenant_id(file_id_.second_id())) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/tenant_unit_meta */ \
    (databuff_printf(path_, length, pos, "%s/%ld_%ld/%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), file_id_.second_id(), file_id_.third_id(), \
                     get_storage_objet_type_str(object_type))), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id */ \
    (OB_DIR_MGR.get_local_tenant_dir(path, length, file_id.second_id(), file_id.third_id())), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tenant_dir(file_id.second_id(), file_id.third_id()))) \
  STORAGE_OBJECT_TYPE_INFO(LS_META, "LS_META", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format: tenant_id_epoch_id/ls/ls_id_epoch_id/ls_meta */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld_%ld/%s", \
                    OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, LS_DIR_STR, \
                    file_id_.second_id(), ls_epoch_id_, get_storage_objet_type_str(object_type))), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id/ls/ls_id_epoch_id */ \
    (OB_DIR_MGR.get_ls_id_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id)), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_ls_id_dir(tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id))) \
  STORAGE_OBJECT_TYPE_INFO(LS_DUP_TABLE_META, "LS_DUP_TABLE_META", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format: tenant_id_epoch_id/ls/ls_id_epoch_id/ls_dup_table_meta */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld_%ld/%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, LS_DIR_STR, \
                     file_id_.second_id(), ls_epoch_id_, get_storage_objet_type_str(object_type))), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id/ls/ls_id_epoch_id */ \
    (OB_DIR_MGR.get_ls_id_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id)), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_ls_id_dir(tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id))) \
  STORAGE_OBJECT_TYPE_INFO(LS_ACTIVE_TABLET_ARRAY, "LS_ACTIVE_TABLET_ARRAY", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format: tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_id_array */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld_%ld/%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, LS_DIR_STR, \
                     file_id_.second_id(), ls_epoch_id_, get_storage_objet_type_str(object_type))), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id/ls/ls_id_epoch_id */ \
    (OB_DIR_MGR.get_ls_id_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id)), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_ls_id_dir(tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id))) \
  STORAGE_OBJECT_TYPE_INFO(LS_PENDING_FREE_TABLET_ARRAY, "LS_PENDING_FREE_TABLET_ARRAY", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format: tenant_id_epoch_id/ls/ls_id_epoch_id/pending_free_tablet_array */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld_%ld/%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, LS_DIR_STR, \
                     file_id_.second_id(), ls_epoch_id_, get_storage_objet_type_str(object_type))), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id/ls/ls_id_epoch_id */ \
    (OB_DIR_MGR.get_ls_id_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id)), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_ls_id_dir(tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id))) \
  STORAGE_OBJECT_TYPE_INFO(LS_TRANSFER_TABLET_ID_ARRAY, "LS_TRANSFER_TABLET_ID_ARRAY", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format: tenant_id_epoch_id/ls/ls_id_epoch_id/transfer_tablet_id_array */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld_%ld/%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, LS_DIR_STR, \
                     file_id_.second_id(), ls_epoch_id_, get_storage_objet_type_str(object_type))), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id/ls/ls_id_epoch_id */ \
    (OB_DIR_MGR.get_ls_id_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id)), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_ls_id_dir(tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id))) \
  STORAGE_OBJECT_TYPE_INFO(PRIVATE_TABLET_META, "PRIVATE_TABLET_META", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:tablet_id, fourth_id:meta_transfer_seq+meta_version_id */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() > 0) && (file_id_.meta_transfer_seq() >= 0) && (file_id_.meta_version_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/tablet_meta_version_transfer_seq */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld_%ld/%s/%ld/%ld_%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     LS_DIR_STR, file_id_.second_id(), ls_epoch_id_, TABLET_META_DIR_STR, \
                     file_id_.third_id(), file_id_.meta_version_id(), file_id_.meta_transfer_seq())), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/ */ \
    (OB_DIR_MGR.get_tablet_meta_tablet_id_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id, file_id.third_id())), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tablet_meta_tablet_id_dir(tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id, file_id.third_id()))) \
  STORAGE_OBJECT_TYPE_INFO(PRIVATE_TABLET_CURRENT_VERSION, "PRIVATE_TABLET_CURRENT_VERSION", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:tablet_id, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() > 0)), \
    /*to_local_path_format: tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/current_version */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld_%ld/%s/%ld/%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, LS_DIR_STR, \
                     file_id_.second_id(), ls_epoch_id_, TABLET_META_DIR_STR, file_id_.third_id(), \
                     get_storage_objet_type_str(object_type))), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/ */ \
    (OB_DIR_MGR.get_tablet_meta_tablet_id_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id, file_id.third_id())), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tablet_meta_tablet_id_dir(tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id, file_id.third_id()))) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MAJOR_TABLET_META, "SHARED_MAJOR_TABLET_META", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:meta_version_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/meta/tablet_meta_version */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     tenant_id, TABLET_DIR_STR, file_id_.second_id(), MAJOR_DIR_STR, \
                     SHARED_TABLET_META_DIR_STR, file_id_.third_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(COMPACTION_SERVER, "COMPACTION_SERVER", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/compaction/scheduler/ls_id_compaction_servers */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%s/%ld_%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     tenant_id, COMPACTION_DIR_STR, SCHEDULER_DIR_STR, file_id_.second_id(), \
                     get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(LS_SVR_COMPACTION_STATUS, "LS_SVR_COMPACTION_STATUS", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:server_id, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() > 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/compaction/compactor/ls_id_server_id_ls_svr_compaction_status */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%s/%ld_%ld_%s", \
                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, \
                    COMPACTION_DIR_STR, COMPACTOR_DIR_STR, file_id_.second_id(), file_id_.third_id(), \
                    get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(COMPACTION_REPORT, "COMPACTION_REPORT", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:server_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/compaction/compactor/server_id_compaction_report */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%s/%ld_%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     tenant_id, COMPACTION_DIR_STR, COMPACTOR_DIR_STR, file_id_.second_id(), \
                     get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MAJOR_GC_INFO, "SHARED_MAJOR_GC_INFO", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/meta/gc_info */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     tenant_id, TABLET_DIR_STR, file_id_.second_id(), MAJOR_DIR_STR, \
                     SHARED_TABLET_META_DIR_STR, get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MAJOR_META_LIST, "SHARED_MAJOR_META_LIST", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/meta/meta_list */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     tenant_id, TABLET_DIR_STR, file_id_.second_id(), MAJOR_DIR_STR, \
                     SHARED_TABLET_META_DIR_STR, get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(LS_COMPACTION_STATUS, "LS_COMPACTION_STATUS", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/compaction/scheduler/ls_id_ls_compaction_status */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%s/%ld_%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     tenant_id, COMPACTION_DIR_STR, SCHEDULER_DIR_STR, file_id_.second_id(), \
                     get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(TABLET_COMPACTION_STATUS, "TABLET_COMPACTION_STATUS", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:compaction_scn, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/scn_id_tablet_compaction_status */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%s/%ld_%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     MAJOR_DIR_STR, file_id_.third_id(), get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(MAJOR_PREWARM_DATA, "MAJOR_PREWARM_DATA", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:compaction_scn, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/compaction_scn_prewarm_data */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%s/%ld_%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     MAJOR_DIR_STR, file_id_.third_id(), get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(MAJOR_PREWARM_DATA_INDEX, "MAJOR_PREWARM_DATA_INDEX", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:compaction_scn, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/compaction_scn_prewarm_data_index */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%s/%ld_%s", \
                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                    TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                    MAJOR_DIR_STR, file_id_.third_id(), get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(MAJOR_PREWARM_META, "MAJOR_PREWARM_META", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:compaction_scn, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/compaction_scn_prewarm_meta */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%s/%ld_%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     MAJOR_DIR_STR, file_id_.third_id(), get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(MAJOR_PREWARM_META_INDEX, "MAJOR_PREWARM_META_INDEX", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:compaction_scn, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/compaction_scn_prewarm_meta_index */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%s/%ld_%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     MAJOR_DIR_STR, file_id_.third_id(), get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(TENANT_DISK_SPACE_META, "TENANT_DISK_SPACE_META", true/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:N/A */ \
    ((is_valid_tenant_id(file_id_.second_id())) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/tenant_disk_space_meta*/ \
    (databuff_printf(path_, length, pos, "%s/%ld_%ld/%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), file_id_.second_id(), file_id_.third_id(), \
                     get_storage_objet_type_str(object_type))), \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id */ \
    (OB_DIR_MGR.get_local_tenant_dir(path, length, file_id.second_id(), file_id.third_id())), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tenant_dir(file_id.second_id(), file_id.third_id()))) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_TABLET_ID, "SHARED_TABLET_ID", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet_ids/tablet_id */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     tenant_id, TABLET_IDS_DIR_STR, file_id_.second_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(LS_COMPACTION_LIST, "LS_COMPACTION_LIST", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:ls_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/compaction/scheduler/ls_id_ls_compaction_list */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%s/%ld_%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     tenant_id, COMPACTION_DIR_STR, SCHEDULER_DIR_STR, file_id_.second_id(), \
                     get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(IS_SHARED_TABLET_DELETED, "IS_SHARED_TABLET_DELETED", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:N/A, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/is_shared_tablet_deleted */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(IS_SHARED_TENANT_DELETED, "IS_SHARED_TENANT_DELETED", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tenant_id, third_id:N/A, fourth_id:N/A */ is_valid_tenant_id(file_id_.second_id()), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/is_shared_tenant_deleted */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     file_id_.second_id(), get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(CHECKSUM_ERROR_DUMP_MACRO, "CHECKSUM_ERROR_DUMP_MACRO", false/*is_pin_local*/, true/*is_read_through*/, \
    /*is_valid second_id:tablet_id, third_id:compaction_scn, fourth_id:block_seq */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0) && (file_id_.third_id() < INT64_MAX) && (file_id_.fourth_id() >= 0) && (file_id_.fourth_id() < INT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/major/sstable/cg_id/checksum_error_macro/svr_id_compaction_scn_block_id */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s_%ld/%s/%ld_%ld_%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id()/*tablet_id*/, \
                     MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, \
                     COLUMN_GROUP_STR, file_id_.column_group_id(), CKM_ERROR_DIR_STR, \
                     server_id, file_id_.third_id()/*compaction_scn*/, file_id_.fourth_id()/*block_seq*/)), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MICRO_DATA_MACRO, "SHARED_MICRO_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid*/false, \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(SHARED_MICRO_META_MACRO, "SHARED_MICRO_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid*/false, \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(UNSEALED_REMOTE_SEG_FILE, "UNSEALED_REMOTE_SEG_FILE", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid second_id:tmp_file_id, third_id:segment_id, fourth_id:valid_length */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0) && (file_id_.fourth_id() > 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id_valid_length */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%ld_%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id, \
                     TMP_DATA_DIR_STR, file_id_.second_id(), file_id_.third_id(), file_id_.fourth_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED) \
  STORAGE_OBJECT_TYPE_INFO(MAX, "MAX", false/*is_pin_local*/, false/*is_read_through*/, \
    /*is_valid*/false, \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)
/*
  *******************************************************************************
  * WARNING!!! Forbid insert object type in the middle! Only allow at the tail! *
  *******************************************************************************
 */
enum class ObMacroBlockIdMode : uint8_t
{
  ID_MODE_LOCAL = 0,
  ID_MODE_BACKUP = 1,
  ID_MODE_SHARE = 2,
  ID_MODE_MAX,
};

enum class ObStorageObjectType : uint8_t // FARM COMPAT WHITELIST
{
#define STORAGE_OBJECT_TYPE_INFO(obj_id, obj_str, is_pin_local, is_read_through, is_valid, to_local_path_format, to_remote_path_format, get_parent_dir, create_parent_dir) obj_id,
  OB_STORAGE_OBJECT_TYPE_LIST
#undef STORAGE_OBJECT_TYPE_INFO
};

bool is_read_through_storage_object_type(const ObStorageObjectType type);
bool is_object_type_only_store_remote(const ObStorageObjectType type);
bool is_pin_storage_object_type(const ObStorageObjectType type);
bool is_ls_replica_prewarm_filter_object_type(const ObStorageObjectType type);

static const char *get_storage_objet_type_str(const ObStorageObjectType type)
{
  static const char *type_str_map_[static_cast<int32_t>(ObStorageObjectType::MAX) + 1] = {
#define STORAGE_OBJECT_TYPE_INFO(obj_id, obj_str, is_pin_local, is_read_through, is_valid, to_local_path_format, to_remote_path_format, get_parent_dir, create_parent_dir) obj_str,
    OB_STORAGE_OBJECT_TYPE_LIST
#undef STORAGE_OBJECT_TYPE_INFO
  };
  const char *type_str = type_str_map_[static_cast<int32_t>(ObStorageObjectType::MAX)];
  if (OB_LIKELY((type >= ObStorageObjectType::PRIVATE_DATA_MACRO)) ||
                (type <= ObStorageObjectType::MAX)) {
    type_str = type_str_map_[static_cast<int32_t>(type)];
  }
  return type_str;
}

class MacroBlockId final
{
public:
  MacroBlockId();
  // only for ID_MODE_LOCAL
  MacroBlockId(const uint64_t write_seq, const int64_t block_index, const int64_t third_id);
  MacroBlockId(const int64_t first_id, const int64_t second_id, const int64_t third_id, const int64_t fourth_id);
  MacroBlockId(const MacroBlockId &id);
  ~MacroBlockId() = default;

  OB_INLINE void reset()
  {
    first_id_ = 0;
    second_id_ = INT64_MAX;
    third_id_ = 0;
    fourth_id_ = 0;
    version_ = MACRO_BLOCK_ID_VERSION_V2;
  }
  bool is_valid() const;

  OB_INLINE bool is_local_id() const
  {
    return static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_LOCAL) == id_mode_;
  }
  OB_INLINE bool is_backup_id() const
  {
    return static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_BACKUP) == id_mode_;
  }
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  // reuse by ObMetaDiskAddr::to_string
  void first_id_to_string(char *buf, const int64_t buf_len, int64_t &pos) const;

  // General GETTER/SETTER
  int64_t first_id() const { return first_id_; }
  int64_t second_id() const { return second_id_; }
  int64_t third_id() const { return third_id_; }
  int64_t fourth_id() const { return fourth_id_; }

  void set_first_id(const int64_t first_id) { first_id_ = first_id; }
  void set_second_id(const int64_t second_id) { second_id_ = second_id; }
  void set_third_id(const int64_t third_id) { third_id_ = third_id; }
  void set_fourth_id(const int64_t fourth_id) { fourth_id_ = fourth_id; }

  void set_version_v2() { version_ = MACRO_BLOCK_ID_VERSION_V2; }
  uint64_t id_mode() const { return id_mode_; }
  bool is_id_mode_local() const; // sn deploy mode, but local macro id.
  bool is_id_mode_backup() const; // sn deploy mode, but backup macro id.
  bool is_id_mode_share() const; // ss deploy mode
  bool is_shared_data_or_meta() const; // shared tablet macro block in ss mode
  bool is_private_data_or_meta() const; // private tablet macro block in ss mode
  bool is_data() const; // shared data or private data
  bool is_meta() const; // shared meta or private meta
  void set_id_mode(const uint64_t id_mode) { id_mode_ = id_mode; }
  // Local mode
  int64_t block_index() const { return block_index_; }
  void set_block_index(const int64_t block_index) { block_index_ = block_index; }
  int64_t write_seq() const { return write_seq_; }
  void set_write_seq(const uint64_t write_seq) { write_seq_ = write_seq; }

  // Share mode
  void set_ss_version(const uint64_t ss_version) { ss_version_ = ss_version; }
  void set_ss_id_mode(const uint64_t ss_mode_id) { ss_id_mode_ = ss_mode_id; }
  ObStorageObjectType storage_object_type() const { return static_cast<ObStorageObjectType>(storage_object_type_); }
  void set_storage_object_type(const uint64_t storage_object_type) { storage_object_type_ = storage_object_type; }
  int64_t incarnation_id() const { return incarnation_id_; }
  void set_incarnation_id(const uint64_t incarnation_id) { incarnation_id_ = incarnation_id; }
  int64_t column_group_id() const { return column_group_id_; }
  void set_column_group_id(const uint64_t column_group_id) { column_group_id_ = column_group_id; }
  int64_t macro_transfer_seq() const { return macro_transfer_seq_; }  
  void set_macro_transfer_seq(const int64_t macro_transfer_seq) { macro_transfer_seq_ = macro_transfer_seq; }
  uint64_t tenant_seq() const { return tenant_seq_; }  
  void set_tenant_seq(const uint64_t tenant_seq) { tenant_seq_ = tenant_seq; }
  int64_t meta_transfer_seq() const { return meta_transfer_seq_; }  
  void set_meta_transfer_seq(const int64_t meta_transfer_seq) { meta_transfer_seq_ = meta_transfer_seq; }
  uint64_t meta_version_id() const { return meta_version_id_; } 
  void set_meta_version_id(const uint64_t meta_version_id) { meta_version_id_ = meta_version_id; }

  // Deivce mode
  void set_from_io_fd(const common::ObIOFd &block_id)
  {
    first_id_ = block_id.first_id_;
    second_id_ = block_id.second_id_;
    third_id_ = block_id.third_id_;
  }

  MacroBlockId& operator =(const MacroBlockId &other)
  {
    first_id_ = other.first_id_;
    second_id_ = other.second_id_;
    third_id_ = other.third_id_;
    fourth_id_ = other.fourth_id_;
    return *this;
  }
  bool operator ==(const MacroBlockId &other) const;
  bool operator !=(const MacroBlockId &other) const;
  bool operator <(const MacroBlockId &other) const;

  NEED_SERIALIZE_AND_DESERIALIZE;

  // just for compatibility, the macro block id of V1 is serialized directly by memcpy in some scenarios
  int memcpy_deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  static MacroBlockId mock_valid_macro_id() { return MacroBlockId(0, AUTONOMIC_BLOCK_INDEX, 0); }

public:
  static const int64_t MACRO_BLOCK_ID_VERSION_V1 = 0;
  static const int64_t MACRO_BLOCK_ID_VERSION_V2 = 1; // addding fourth_id_ for V1

  static const int64_t EMPTY_ENTRY_BLOCK_INDEX = -1;
  static const int64_t AUTONOMIC_BLOCK_INDEX = -1;

  static const uint64_t SF_BIT_WRITE_SEQ = 52;
  static const uint64_t SF_BIT_STORAGE_OBJECT_TYPE = 8;
  static const uint64_t SF_BIT_INCARNATION_ID = 24;
  static const uint64_t SF_BIT_COLUMN_GROUP_ID = 16;
  static const uint64_t SF_BIT_RESERVED = 4;
  static const uint64_t SF_BIT_ID_MODE = 8;
  static const uint64_t SF_BIT_VERSION = 4;
  static const uint64_t SF_BIT_TRANSFER_SEQ = 20;
  static const uint64_t SF_BIT_TENANT_SEQ = 44;
  static constexpr uint64_t SF_BIT_META_VERSION_ID = 44;
  static const uint64_t MAX_TRANSFER_SEQ = (0x1UL << MacroBlockId::SF_BIT_TRANSFER_SEQ) - 1;
  static const uint64_t MAX_WRITE_SEQ = (0x1UL << MacroBlockId::SF_BIT_WRITE_SEQ) - 1;

private:
  static const uint64_t HASH_MAGIC_NUM = 2654435761;

private:
  union {
    int64_t first_id_;
    // for share nothing mode
    struct {
      uint64_t write_seq_ : SF_BIT_WRITE_SEQ;
      uint64_t id_mode_   : SF_BIT_ID_MODE;
      uint64_t version_   : SF_BIT_VERSION;
    };
    // for share storage mode
    struct {
      uint64_t storage_object_type_ : SF_BIT_STORAGE_OBJECT_TYPE;
      uint64_t incarnation_id_  : SF_BIT_INCARNATION_ID;
      uint64_t column_group_id_ : SF_BIT_COLUMN_GROUP_ID;
      uint64_t ss_reserved_ : SF_BIT_RESERVED;
      uint64_t ss_id_mode_   : SF_BIT_ID_MODE;
      uint64_t ss_version_   : SF_BIT_VERSION;
    };
  };
  union {
    int64_t second_id_;
    int64_t block_index_;    // the block index in the block_file when Local mode
  };
  union {
    int64_t third_id_;
    struct {
      uint64_t device_id_ : 8;
      uint64_t reserved_ : 56;
    };
  };
  union {
    int64_t fourth_id_;
    // for PRIVATE_DATA_MACRO and PRIVATE_META_MACRO
    struct {
      int64_t macro_transfer_seq_  : SF_BIT_TRANSFER_SEQ;
      uint64_t tenant_seq_          : SF_BIT_TENANT_SEQ;
    };
    // for PRIVATE_TABLET_META and PRIVATE_TABLET_CURRENT_VERSION
    struct {
      int64_t meta_transfer_seq_   : SF_BIT_TRANSFER_SEQ;
      uint64_t meta_version_id_     : SF_BIT_META_VERSION_ID;
    };
  };
};

OB_INLINE bool MacroBlockId::operator ==(const MacroBlockId &other) const
{
  return other.first_id_ == first_id_ && other.second_id_ == second_id_
      && other.third_id_ == third_id_ && other.fourth_id_ == fourth_id_;
}

OB_INLINE bool MacroBlockId::operator !=(const MacroBlockId &other) const
{
  return !(other == *this);
}

#define SERIALIZE_MEMBER_WITH_MEMCPY(member)                                 \
  if (OB_SUCC(ret)) {                                                        \
    if (OB_UNLIKELY(buf_len - pos < sizeof(member))) {                       \
      ret = OB_BUF_NOT_ENOUGH;                                               \
      LOG_WARN("buffer not enough", K(ret), KP(buf), K(buf_len), K(pos));    \
    } else {                                                                 \
      MEMCPY(buf + pos, &member, sizeof(member));                            \
      pos += sizeof(member);                                                 \
    }                                                                        \
  }

#define DESERIALIZE_MEMBER_WITH_MEMCPY(member)                               \
  if (OB_SUCC(ret)) {                                                        \
    if (OB_UNLIKELY(data_len - pos < sizeof(member))) {                      \
      ret = OB_DESERIALIZE_ERROR;                                            \
      LOG_WARN("buffer not enough", K(ret), KP(buf), K(data_len), K(pos));   \
    } else {                                                                 \
      MEMCPY(&member, buf + pos, sizeof(member));                            \
      pos += sizeof(member);                                                 \
    }                                                                        \
  }

} // namespace blocksstable
} // namespace oceanbase

#endif /* SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_ */
