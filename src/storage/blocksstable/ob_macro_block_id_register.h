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

#ifdef REGISTER_MACRO_BLOCK_ID
/*
  *******************************************************************************
  * WARNING!!! Forbid insert object type in the middle! Only allow at the tail! *
  *******************************************************************************
  Note for add new ObjectType
  obj_id: ObStorageObjectType ID name
  obj_str: ObStorageObjectType str name
  is_pin_local: the ObjetType only store in local cache, true or false
  is_read_through: the ObjetType only store in remote object storage, true or false
  is_write_through: whether this type of object write through object storage, true or false
  is_overwrite: whether this type of object exists overwrite with 'different content', true or false
  is_support_fd_cache: whether use fd cache when reading local cache file of this type, true or false.
                       note: only MACRO BLOCK (e.g., PRIVATE_DATA_MACRO, SHARED_MAJOR_DATA_MACRO,
                       SHARED_MDS_MINI_DATA_MACRO...) need fd cache to improve read performance.
  is_valid: the MacroBlockId is valid check logic
  to_local_path_format: the MacroBlockId convert to local cache path logic, if does not exist return OB_NOT_SUPPORTED
  local_path_to_macro_id: logic of convert from local cache path to MacroBlockId, if does not exist, fill ret = OB_NOT_SUPPORTED;
  to_remote_path_format: the MacroBlockId convert to object storage path logic, if does not exist return OB_NOT_SUPPORTED
  get_parent_dir: the MacroBlockId get parent local dir logic, if does not exist return OB_NOT_SUPPORTED
  create_parent_dir: the MacroBlockId get parent local dir logic, if does not exist return OB_NOT_SUPPORTED
  Finally when adding new type, we need to ensure if the new type have to add into MacroBlockId::is_shared_data_or_meta() or MacroBlockId::is_private_data_or_meta() or MacroBlockId::is_data() or MacroBlockId::is_meta()
*/
// STORAGE_OBJECT_TYPE_INFO(obj_id, obj_str, is_pin_local, is_read_through, is_write_through, is_overwrite, is_support_fd_cache, is_valid, to_local_path_format, local_path_to_macro_id, to_remote_path_format, get_parent_dir, create_parent_dir)

  STORAGE_OBJECT_TYPE_INFO(PRIVATE_DATA_MACRO, "PRIVATE_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    false/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:server_id, fourth_id:macro_transfer_epoch+tenant_seq */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() > 0) && (file_id_.macro_transfer_epoch() >= 0) && (file_id_.tenant_seq() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/data/svr%ldseq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02ld/%ld/%ld/%s/%s%ld%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     TABLET_DATA_DIR_STR, (file_id_.second_id() % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM), \
                     file_id_.second_id(), file_id_.macro_transfer_epoch(), \
                     DATA_MACRO_DIR_STR, SVR_KEY_STR, file_id_.third_id(), SEQ_KEY_STR, file_id_.tenant_seq())), \
    /*local_path_to_macro_id*/ \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 4))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      char format[512] = {0}; \
      int num = 0; \
      int64_t tablet_id = 0; \
      int64_t transfer_seq = 0; \
      int64_t server_id = 0; \
      int64_t seq_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s%%ld%s%%ld.T%hhu", \
                  DATA_MACRO_DIR_STR, SVR_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::PRIVATE_DATA_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &transfer_seq, &server_id, &seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_macro_transfer_epoch(transfer_seq); \
        macro_id.set_third_id(server_id); \
        macro_id.set_tenant_seq(seq_id); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data/svr%ldseq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%ld/%lu_%ld/%s/%ld/%ld/%s/%s%ld%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, \
                     file_id_.third_id(), tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR, file_id_.second_id(), \
                     file_id_.macro_transfer_epoch(), DATA_MACRO_DIR_STR, SVR_KEY_STR, file_id_.third_id(), SEQ_KEY_STR, file_id_.tenant_seq())), \
    /*get_parent_dir: tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/data/ */ \
    (OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), file_id.macro_transfer_epoch(), ObMacroType::DATA_MACRO)), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(tenant_id, tenant_epoch_id, file_id.second_id(), file_id.macro_transfer_epoch())))

  STORAGE_OBJECT_TYPE_INFO(PRIVATE_META_MACRO, "PRIVATE_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    false/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:server_id, fourth_id:macro_transfer_epoch+tenant_seq */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() > 0) && (file_id_.macro_transfer_epoch() >= 0) && (file_id_.tenant_seq() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/meta/svr%ldseq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02ld/%ld/%ld/%s/%s%ld%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     TABLET_DATA_DIR_STR, (file_id_.second_id() % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM), \
                     file_id_.second_id(), file_id_.macro_transfer_epoch(), \
                     META_MACRO_DIR_STR, SVR_KEY_STR, file_id_.third_id(), SEQ_KEY_STR, file_id_.tenant_seq())), \
    /*local_path_to_macro_id*/ \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 4))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      char format[512] = {0}; \
      int num = 0; \
      int64_t tablet_id = 0; \
      int64_t transfer_seq = 0; \
      int64_t server_id = 0; \
      int64_t seq_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s%%ld%s%%ld.T%hhu", \
                  META_MACRO_DIR_STR, SVR_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::PRIVATE_META_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &transfer_seq, &server_id, &seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_META_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_macro_transfer_epoch(transfer_seq); \
        macro_id.set_third_id(server_id); \
        macro_id.set_tenant_seq(seq_id); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/tansfer_seq/meta/svr%ldseq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%ld/%lu_%ld/%s/%ld/%ld/%s/%s%ld%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, \
                     file_id_.third_id(), tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR, file_id_.second_id(), \
                     file_id_.macro_transfer_epoch(), META_MACRO_DIR_STR, SVR_KEY_STR, file_id_.third_id(), SEQ_KEY_STR, file_id_.tenant_seq())), \
    /*get_parent_dir: tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/meta/ */ \
    (OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), file_id.macro_transfer_epoch(), ObMacroType::META_MACRO)), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(tenant_id, tenant_epoch_id, file_id.second_id(), file_id.macro_transfer_epoch())))

  STORAGE_OBJECT_TYPE_INFO(SHARED_MINI_DATA_MACRO, "SHARED_MINI_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:op_id + seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: inner_tablet: tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld user_tablet: tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld*/ \
    (file_id_.meta_is_inner_tablet() ? (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld", \
                                        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                                        SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id_.meta_ls_id(), \
                                        get_ls_inner_tablet_name_(file_id_.second_id()), OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/)) \
                                     : (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld", \
                                        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                                        SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id_.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM), \
                                        TABLET_KEY_STR, file_id_.second_id(), REORG_KEY_STR, file_id_.reorganization_scn(), \
                                        OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    bool is_inner_tablet = false; \
    int64_t tablet_id = 0; \
    int64_t reorganization_scn = 0; \
    int64_t ls_id = 0; \
    char tablet_name_part1[256] = {0}; \
    char tablet_name_part2[256] = {0}; \
    char tablet_name[512] = {0}; \
    int64_t op_id = 0; \
    int64_t macro_seq_id = 0; \
    const char *judge_path = nullptr; \
    if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) { \
      const char *sub_path = nullptr; \
      if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
      } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu", \
                         OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MINI_DATA_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(5 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) { \
        LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2)); \
      } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) { \
        LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name)); \
      } else { \
        is_inner_tablet = true; \
      } \
    } else { \
      const char *sub_path = nullptr; \
      if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
      } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", \
                 TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MINI_DATA_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        is_inner_tablet = false; \
      } \
    } \
    if (OB_SUCC(ret)) { \
      if (is_inner_tablet) { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MINI_DATA_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_meta_ls_id(ls_id); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MINI_DATA_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_reorganization_scn(reorganization_scn); \
      } \
    }, \
    /*to_remote_path_format: inner_tablet: cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/data/seq%ld user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/seq%ld*/ \
    (file_id_.meta_is_inner_tablet() ? (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld", \
                                        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                                        TENANT_DIR_STR, tenant_id, LS_DIR_STR, file_id_.meta_ls_id(), \
                                        get_ls_inner_tablet_name_(file_id_.second_id()), \
                                        MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/)) \
                                     : (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld", \
                                        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                                        TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), file_id_.reorganization_scn(),\
                                        MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_shared_mini_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id())))

  STORAGE_OBJECT_TYPE_INFO(SHARED_MINI_META_MACRO, "SHARED_MINI_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid: second_id:tablet_id, third_id:op_id + seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: inner_tablet:tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld user_tablet:tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld */ \
    (file_id_.meta_is_inner_tablet() ? (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld", \
                                        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                                        SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id_.meta_ls_id(), \
                                        get_ls_inner_tablet_name_(file_id_.second_id()), OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/)) \
                                     : (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld", \
                                        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                                        SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id_.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM), \
                                        TABLET_KEY_STR, file_id_.second_id(), REORG_KEY_STR, file_id_.reorganization_scn(), \
                                        OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    bool is_inner_tablet = false; \
    int64_t tablet_id = 0; \
    int64_t reorganization_scn = 0; \
    int64_t ls_id = 0; \
    char tablet_name_part1[256] = {0}; \
    char tablet_name_part2[256] = {0}; \
    char tablet_name[512] = {0}; \
    int64_t op_id = 0; \
    int64_t macro_seq_id = 0; \
    const char *judge_path = nullptr; \
    if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) { \
      const char *sub_path = nullptr; \
      if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
      } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu", \
                         OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MINI_META_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(5 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) { \
        LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2)); \
      } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) { \
        LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name)); \
      } else { \
        is_inner_tablet = true; \
      } \
    } else { \
      const char *sub_path = nullptr; \
      if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
      } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", \
                         TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MINI_META_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        is_inner_tablet = false; \
      } \
    } \
    if (OB_SUCC(ret)) { \
      if (is_inner_tablet) { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MINI_META_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_meta_ls_id(ls_id); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MINI_META_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_reorganization_scn(reorganization_scn); \
      } \
    }, \
    /*to_remote_path_format: inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/meta/seq%ld user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/seq%ld*/ \
    (file_id_.meta_is_inner_tablet() ? (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld", \
                                        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                                        TENANT_DIR_STR, tenant_id, LS_DIR_STR, file_id_.meta_ls_id(), \
                                        get_ls_inner_tablet_name_(file_id_.second_id()), \
                                        MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        META_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/)) \
                                     : (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld", \
                                        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                                        TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), file_id_.reorganization_scn(),\
                                        MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32) /*op_id*/, \
                                        META_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_shared_mini_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id())))

  STORAGE_OBJECT_TYPE_INFO(SHARED_MINOR_DATA_MACRO, "SHARED_MINOR_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:op_id + seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: inner_tablet: tenant_id_epoch_id/shared_minor_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld user_tablet: tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld*/ \
    (file_id_.meta_is_inner_tablet() ? (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld", \
                                        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                                        SHARED_MINOR_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id_.meta_ls_id(), \
                                        get_ls_inner_tablet_name_(file_id_.second_id()), OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/)) \
                                     : (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld", \
                                        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                                        SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id_.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM), \
                                        TABLET_KEY_STR, file_id_.second_id(), REORG_KEY_STR, file_id_.reorganization_scn(), \
                                        OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    bool is_inner_tablet = false; \
    int64_t tablet_id = 0; \
    int64_t reorganization_scn = 0; \
    int64_t ls_id = 0; \
    char tablet_name_part1[256] = {0}; \
    char tablet_name_part2[256] = {0}; \
    char tablet_name[512] = {0}; \
    int64_t op_id = 0; \
    int64_t macro_seq_id = 0; \
    const char *judge_path = nullptr; \
    if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) { \
      const char *sub_path = nullptr; \
      if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
      } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu", \
                         OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MINOR_DATA_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(5 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) { \
        LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2)); \
      } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) { \
        LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name)); \
      } else { \
        is_inner_tablet = true; \
      } \
    } else { \
      const char *sub_path = nullptr; \
      if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
      } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", \
                         TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MINOR_DATA_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        is_inner_tablet = false; \
      } \
    } \
    if (OB_SUCC(ret)) { \
      if (is_inner_tablet) { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MINOR_DATA_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_meta_ls_id(ls_id); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MINOR_DATA_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_reorganization_scn(reorganization_scn); \
      } \
    }, \
    /*to_remote_path_format: inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/data/seq%ld user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/seq%ld*/ \
    (file_id_.meta_is_inner_tablet() ? (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld", \
                                        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                                        TENANT_DIR_STR, tenant_id, LS_DIR_STR, file_id_.meta_ls_id(), \
                                        get_ls_inner_tablet_name_(file_id_.second_id()), \
                                        MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/)) \
                                     : (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld", \
                                        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                                        TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), file_id_.reorganization_scn(),\
                                        MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_shared_minor_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id())))

  STORAGE_OBJECT_TYPE_INFO(SHARED_MINOR_META_MACRO, "SHARED_MINOR_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:op_id + seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: inner_tablet:tenant_id_epoch_id/shared_minor_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld user_tablet:tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld */ \
    (file_id_.meta_is_inner_tablet() ? (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld", \
                                        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                                        SHARED_MINOR_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id_.meta_ls_id(), \
                                        get_ls_inner_tablet_name_(file_id_.second_id()), OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/)) \
                                     : (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld", \
                                        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                                        SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id_.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM), \
                                        TABLET_KEY_STR, file_id_.second_id(), REORG_KEY_STR, file_id_.reorganization_scn(), \
                                        OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    bool is_inner_tablet = false; \
    int64_t tablet_id = 0; \
    int64_t reorganization_scn = 0; \
    int64_t ls_id = 0; \
    char tablet_name_part1[256] = {0}; \
    char tablet_name_part2[256] = {0}; \
    char tablet_name[512] = {0}; \
    int64_t op_id = 0; \
    int64_t macro_seq_id = 0; \
    const char *judge_path = nullptr; \
    if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) { \
      const char *sub_path = nullptr; \
      if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
      } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu", \
                         OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MINOR_META_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(5 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) { \
        LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2)); \
      } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) { \
        LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name)); \
      } else { \
        is_inner_tablet = true; \
      } \
    } else { \
      const char *sub_path = nullptr; \
      if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
      } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", \
                         TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MINOR_META_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        is_inner_tablet = false; \
      } \
    } \
    if (OB_SUCC(ret)) { \
      if (is_inner_tablet) { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MINOR_META_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_meta_ls_id(ls_id); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MINOR_META_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_reorganization_scn(reorganization_scn); \
      } \
    }, \
    /*to_remote_path_format: inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/meta/seq%ld user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/seq%ld */ \
    (file_id_.meta_is_inner_tablet() ? (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld", \
                                        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                                        TENANT_DIR_STR, tenant_id, LS_DIR_STR, file_id_.meta_ls_id(), \
                                        get_ls_inner_tablet_name_(file_id_.second_id()), \
                                        MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        META_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/)) \
                                     : (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld", \
                                        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                                        TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), file_id_.reorganization_scn(),\
                                        MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32) /*op_id*/, \
                                        META_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_shared_minor_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id())))

  STORAGE_OBJECT_TYPE_INFO(SHARED_MAJOR_DATA_MACRO, "SHARED_MAJOR_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/shared_major_macro_cache/scatter_id/tablet%ldreorg%ldcg%ldseq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     MAJOR_DATA_DIR_STR, (file_id_.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM), \
                     TABLET_KEY_STR, file_id_.second_id(), REORG_KEY_STR, file_id_.reorganization_scn(), \
                     CG_KEY_STR, file_id_.column_group_id(), SEQ_KEY_STR, file_id_.third_id())), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      int64_t tablet_id = 0; \
      int64_t reorganization_scn = 0; \
      int64_t cg_id = 0; \
      int64_t macro_seq_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", \
                  TABLET_KEY_STR, REORG_KEY_STR, CG_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &cg_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO); \
        macro_id.set_column_group_id(cg_id); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id(macro_seq_id); \
        macro_id.set_reorganization_scn(reorganization_scn); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/sstable/cg_id/data/seq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/%s_%ld/%s/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     file_id_.reorganization_scn(), MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, \
                     file_id_.column_group_id(), DATA_MACRO_DIR_STR, SEQ_KEY_STR, file_id_.third_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(SHARED_MAJOR_META_MACRO, "SHARED_MAJOR_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/shared_major_macro_cache/scatter_id/tablet%ldreorg%ldcg%ldseq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     MAJOR_DATA_DIR_STR, (file_id_.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM), \
                     TABLET_KEY_STR, file_id_.second_id(), REORG_KEY_STR, file_id_.reorganization_scn(), \
                     CG_KEY_STR, file_id_.column_group_id(), SEQ_KEY_STR, file_id_.third_id())), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      int64_t tablet_id = 0; \
      int64_t reorganization_scn = 0; \
      int64_t cg_id = 0; \
      int64_t macro_seq_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", \
                  TABLET_KEY_STR, REORG_KEY_STR, CG_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MAJOR_META_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &cg_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_META_MACRO); \
        macro_id.set_column_group_id(cg_id); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id(macro_seq_id); \
        macro_id.set_reorganization_scn(reorganization_scn); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/sstable/cg_id/meta/seq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/%s_%ld/%s/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     file_id_.reorganization_scn(), MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, \
                     file_id_.column_group_id(), META_MACRO_DIR_STR, SEQ_KEY_STR, file_id_.third_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(TMP_FILE, "TMP_FILE", false/*is_pin_local*/, false/*is_read_through*/, \
    false/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:tmp_file_id, third_id:segment_id, fourth_id:N/A */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ld*/ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld/%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, TMP_DATA_DIR_STR, \
                     file_id_.second_id(), SEG_KEY_STR, file_id_.third_id())), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      int64_t tmp_file_id = 0; \
      int64_t segment_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%s%%ld.T%hhu", SEG_KEY_STR, (uint8_t)ObStorageObjectType::TMP_FILE))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tmp_file_id, &segment_id))) { \
      } else if (OB_UNLIKELY(2 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE); \
        macro_id.set_second_id(tmp_file_id); \
        macro_id.set_third_id(segment_id); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id, \
                     TMP_DATA_DIR_STR, file_id_.second_id(), SEG_KEY_STR, file_id_.third_id())), \
    /*get_parent_dir: tenant_id_epoch_id/tmp_data/tmp_file_id/ */ \
    (OB_DIR_MGR.get_local_tmp_file_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id())), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tmp_file_dir(tenant_id, tenant_epoch_id, file_id.second_id())))

  STORAGE_OBJECT_TYPE_INFO(SERVER_META, "SERVER_META", true/*is_pin_local*/, false/*is_read_through*/, \
    false/*is_write_through*/, true/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:N/A, third_id:N/A, fourth_id:N/A */ true, \
    /*to_local_path_format: super_block*/ \
    (databuff_printf(path_, length, pos, "%s/%s", OB_DIR_MGR.get_local_cache_root_dir(), get_storage_objet_type_str(object_type))), \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: local_cache_root_dir */ \
    (databuff_printf(path, length, pos, "%s", OB_DIR_MGR.get_local_cache_root_dir())), \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(PRIVATE_TABLET_META, "PRIVATE_TABLET_META", false/*is_pin_local*/, false/*is_read_through*/, \
    false/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:ls_id, third_id:tablet_id, fourth_id:meta_transfer_epoch+meta_version_id */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() > 0) && (file_id_.meta_transfer_epoch() >= 0) && (file_id_.meta_version_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/transfer_seq/ver%ld */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%ld_%ld/%s/%02ld/%ld/%ld/%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     LS_DIR_STR, file_id_.second_id(), ls_epoch_id_, TABLET_META_DIR_STR, \
                     (file_id_.third_id() % ObDirManager::PRIVATE_TABLET_META_SCATTER_DIR_NUM), \
                     file_id_.third_id(), file_id_.meta_transfer_epoch(), VER_KEY_STR, file_id_.meta_version_id())), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 6))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      int64_t ls_id = 0; \
      int64_t epoch_id = 0; \
      int64_t scatter_id = 0; \
      int64_t tablet_id = 0; \
      int64_t meta_transfer_epoch = 0; \
      int64_t meta_version_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld_%%ld/%s/%%ld/%%ld/%%ld/%s%%ld.T%hhu", \
                  TABLET_META_DIR_STR, VER_KEY_STR, (uint8_t)ObStorageObjectType::PRIVATE_TABLET_META))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, &epoch_id, &scatter_id, &tablet_id, &meta_transfer_epoch, &meta_version_id))) { \
      } else if (OB_UNLIKELY(6 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_TABLET_META); \
        macro_id.set_second_id(ls_id); \
        macro_id.set_third_id(tablet_id); \
        macro_id.set_meta_transfer_epoch(meta_transfer_epoch); \
        macro_id.set_meta_version_id(meta_version_id); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/ls/ls_id/tablet_meta/tablet_id/transfer_seq/ver%ld*/ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s/%ld/%ld/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, \
                     tenant_id, tenant_epoch_id, LS_DIR_STR, file_id_.second_id(), TABLET_META_DIR_STR, \
                     file_id_.third_id(), file_id_.meta_transfer_epoch(), VER_KEY_STR, file_id_.meta_version_id())), \
    /*get_parent_dir: tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/transfer_seq */ \
    (OB_DIR_MGR.get_tablet_meta_tablet_id_transfer_seq_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id, file_id.third_id(), file_id.meta_transfer_epoch())), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tablet_meta_tablet_id_transfer_seq_dir(tenant_id, tenant_epoch_id, file_id.second_id(), ls_epoch_id, file_id.third_id(), file_id.meta_transfer_epoch())))

  STORAGE_OBJECT_TYPE_INFO(PRIVATE_SLOG_FILE, "PRIVATE_SLOG_FILE", false/*is_pin_local*/, false/*is_read_through*/, \
    false/*is_write_through*/, true/*is_overwrite slog is overwrite local cache do not need alloc and stat*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:file_id */ \
    ((file_id_.fourth_id() >= 0) && (file_id_.fourth_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: server_slog/seq%ld or tenant_id_epoch_id/slog/seq%ld */ \
    (OB_SERVER_TENANT_ID == file_id_.second_id() ? (databuff_printf(path_, length, pos, "%s/%s_%s/%s%ld", \
                                                    OB_DIR_MGR.get_local_cache_root_dir(), SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, file_id_.fourth_id())) \
                                                 : (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%s%ld", \
                                                    OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, file_id_.third_id(), SLOG_STR, SEQ_KEY_STR, file_id_.fourth_id()))), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 3))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      int64_t tenant_id = 0; \
      int64_t epoch_id = 0; \
      int64_t object_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%s%%ld.T%hhu", \
                  SLOG_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::PRIVATE_SLOG_FILE))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tenant_id, &epoch_id, &object_id))) { \
      } else if (OB_UNLIKELY(3 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_SLOG_FILE); \
        macro_id.set_third_id(epoch_id); \
        macro_id.set_fourth_id(object_id); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/slog/seq%ld or cluster_id/server_id/server_slog/seq%ld */ \
    (OB_SERVER_TENANT_ID == file_id_.second_id() ? (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s_%s/%s%ld", \
                                                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, \
                                                    server_id, SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, file_id_.fourth_id())) \
                                                 : (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s%ld", \
                                                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, \
                                                    server_id, tenant_id, file_id_.third_id(), SLOG_STR, SEQ_KEY_STR, file_id_.fourth_id()))), \
    /*get_parent_dir: tenant_id_epoch_id/slog/ */ \
    (OB_DIR_MGR.get_tenant_slog_dir(path, length, file_id.second_id(), file_id.third_id())), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_slog_dir(file_id.second_id(), file_id.third_id())))

  STORAGE_OBJECT_TYPE_INFO(PRIVATE_CKPT_FILE, "PRIVATE_CKPT_FILE", false/*is_pin_local*/, true/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:file_id */ \
    ((file_id_.fourth_id() >= 0) && (file_id_.fourth_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/ckpt/object_id or cluster_id/server_id/server_ckpt/object_id */ \
    (OB_SERVER_TENANT_ID == file_id_.second_id() ? (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s_%s/%ld", \
                                                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, SERVER_DIR_STR, CKPT_STR, file_id_.fourth_id())) \
                                                 : (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld", \
                                                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, tenant_id, file_id_.third_id(), CKPT_STR, file_id_.fourth_id()))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(MAJOR_PREWARM_DATA, "MAJOR_PREWARM_DATA", false/*is_pin_local*/, true/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0) && (file_id_.fourth_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%lu/%s/%s/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     file_id_.fourth_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id_.third_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(MAJOR_PREWARM_DATA_INDEX, "MAJOR_PREWARM_DATA_INDEX", false/*is_pin_local*/, true/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0) && (file_id_.fourth_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%lu/%s/%s/%s%ld", \
                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                    TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                    file_id_.fourth_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id_.third_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(MAJOR_PREWARM_META, "MAJOR_PREWARM_META", false/*is_pin_local*/, true/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0) && (file_id_.fourth_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%lu/%s/%s/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     file_id_.fourth_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id_.third_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(MAJOR_PREWARM_META_INDEX, "MAJOR_PREWARM_META_INDEX", false/*is_pin_local*/, true/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0) && (file_id_.fourth_id() >= 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%lu/%s/%s/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), \
                     file_id_.fourth_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id_.third_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(TENANT_DISK_SPACE_META, "TENANT_DISK_SPACE_META", true/*is_pin_local*/, false/*is_read_through*/, \
    false/*is_write_through*/, true/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:N/A */ \
    ((is_valid_tenant_id(file_id_.second_id())) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/tenant_disk_space_meta*/ \
    (databuff_printf(path_, length, pos, "%s/%ld_%ld/%s", \
                     OB_DIR_MGR.get_local_cache_root_dir(), file_id_.second_id(), file_id_.third_id(), \
                     get_storage_objet_type_str(object_type))), \
    /*local_path_to_macro_id: no need to calibrate by list*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir: tenant_id_epoch_id */ \
    (OB_DIR_MGR.get_local_tenant_dir(path, length, file_id.second_id(), file_id.third_id())), \
    /*create_parent_dir*/ \
    (OB_DIR_MGR.create_tenant_dir(file_id.second_id(), file_id.third_id())))

  STORAGE_OBJECT_TYPE_INFO(IS_SHARED_TENANT_DELETED, "IS_SHARED_TENANT_DELETED", false/*is_pin_local*/, true/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:tenant_id, third_id:N/A, fourth_id:N/A */ is_valid_tenant_id(file_id_.second_id()), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: cluster_id/tenant_id/is_shared_tenant_deleted */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     file_id_.second_id(), get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(SHARED_MICRO_DATA_MACRO, "SHARED_MICRO_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid*/false, \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(SHARED_MICRO_META_MACRO, "SHARED_MICRO_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid*/false, \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(UNSEALED_REMOTE_SEG_FILE, "UNSEALED_REMOTE_SEG_FILE", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:tmp_file_id, third_id:segment_id, fourth_id:valid_length */ \
    ((file_id_.second_id() >= 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0) && (file_id_.fourth_id() > 0)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ldlen%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s%ld%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id, \
                     TMP_DATA_DIR_STR, file_id_.second_id(), SEG_KEY_STR, file_id_.third_id(), LEN_KEY_STR, file_id_.fourth_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

    STORAGE_OBJECT_TYPE_INFO(SHARED_MDS_MINI_DATA_MACRO, "SHARED_MDS_MINI_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid: second_id:tablet_id, third_id:op_id + seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id_.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM), \
                     TABLET_KEY_STR, file_id_.second_id(), REORG_KEY_STR, file_id_.reorganization_scn(), \
                     OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                     SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/)), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      int64_t tablet_id = 0; \
      int64_t reorganization_scn = 0; \
      int64_t op_id = 0; \
      int64_t macro_seq_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", \
                  TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MDS_MINI_DATA_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MDS_MINI_DATA_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_reorganization_scn(reorganization_scn); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/mini/sstable/op_id/data/seq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), file_id_.reorganization_scn(),\
                     MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                     DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/)), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(SHARED_MDS_MINI_META_MACRO, "SHARED_MDS_MINI_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid: second_id:tablet_id, third_id:op_id + seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id_.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM), \
                     TABLET_KEY_STR, file_id_.second_id(), REORG_KEY_STR, file_id_.reorganization_scn(), \
                     OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                     SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/)), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      int64_t tablet_id = 0; \
      int64_t reorganization_scn = 0; \
      int64_t op_id = 0; \
      int64_t macro_seq_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", \
                  TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MDS_MINI_META_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MDS_MINI_META_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_reorganization_scn(reorganization_scn); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/mini/sstable/op_id/meta/seq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), file_id_.reorganization_scn(),\
                     MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32) /*op_id*/, \
                     META_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/)), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(SHARED_MDS_MINOR_DATA_MACRO, "SHARED_MDS_MINOR_DATA_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid: second_id:tablet_id, third_id:op_id + seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id_.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM), \
                     TABLET_KEY_STR, file_id_.second_id(), REORG_KEY_STR, file_id_.reorganization_scn(), \
                     OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                     SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/)), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      int64_t tablet_id = 0; \
      int64_t reorganization_scn = 0; \
      int64_t op_id = 0; \
      int64_t macro_seq_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", \
                  TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MDS_MINOR_DATA_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MDS_MINOR_DATA_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_reorganization_scn(reorganization_scn); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/minor/sstable/op_id/data/seq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), file_id_.reorganization_scn(),\
                     MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                     DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/)), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(SHARED_MDS_MINOR_META_MACRO, "SHARED_MDS_MINOR_META_MACRO", false/*is_pin_local*/, false/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, true/*is_support_fd_cache*/, \
    /*is_valid: second_id:tablet_id, third_id:op_id + seq_id, fourth_id:N/A */ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0)), \
    /*to_local_path_format: tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id_.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM), \
                     TABLET_KEY_STR, file_id_.second_id(), REORG_KEY_STR, file_id_.reorganization_scn(), \
                     OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                     SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF)/*macro_seq_id*/)), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      int64_t tablet_id = 0; \
      int64_t reorganization_scn = 0; \
      int64_t op_id = 0; \
      int64_t macro_seq_id = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", \
                  TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)ObStorageObjectType::SHARED_MDS_MINOR_META_MACRO))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) { \
      } else if (OB_UNLIKELY(4 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MDS_MINOR_META_MACRO); \
        macro_id.set_second_id(tablet_id); \
        macro_id.set_third_id((op_id << 32) + macro_seq_id); \
        macro_id.set_reorganization_scn(reorganization_scn); \
      } \
    }, \
    /*to_remote_path_format: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/minor/sstable/op_id/meta/seq%ld */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, \
                     TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id_.second_id(), file_id_.reorganization_scn(),\
                     MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, (file_id_.third_id() >> 32) /*op_id*/, \
                     META_MACRO_DIR_STR, SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/)), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  /* atomic file protocol type register macro */
  #ifdef OB_BUILD_CLOSE_MODULES
  #define ATOMIC_PROTOCOL_REGISTER_OBJECT_TYPE
    #include "storage/incremental/atomic_protocol/compile_utility/ob_atomic_file_register_helper.h"
  #undef ATOMIC_PROTOCOL_REGISTER_OBJECT_TYPE
  #endif

  STORAGE_OBJECT_TYPE_INFO(SHARED_TABLET_SUB_META, "SHARED_TABLET_SUB_META", false/*is_pin_local*/, true/*is_read_through*/, \
    true/*is_write_through*/, true/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid: user_tablet: second_id:tablet_id, third_id:op_id, fourth_id:N/A inner_tablet: second_id:tablet_id, third_id:op_id, fourth_id:ls_id*/ \
    ((file_id_.second_id() > 0) && (file_id_.second_id() < INT64_MAX) && (file_id_.third_id() >= 0) && (file_id_.third_id() < INT64_MAX) && ((file_id_.meta_is_inner_tablet() == true && (file_id_.meta_ls_id() > 0 && file_id_.meta_ls_id() < INT64_MAX)) || (file_id_.meta_is_inner_tablet() == false))), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: inner_tablet: cluster_id/tenant_id/ls/ls_id/tablet_name/meta/op%ldseq%lu user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/meta/op%ldseq%lu*/ \
    (file_id_.meta_is_inner_tablet() ? (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s%ld%s%lu", \
                                        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                                        tenant_id, LS_DIR_STR, file_id_.meta_ls_id()/*ls_id*/, \
                                        get_ls_inner_tablet_name_(file_id_.second_id())/*tablet_name*/, \
                                        META_MACRO_DIR_STR, \
                                        OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/)) \
                                     : (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s%ld%s%lu", \
                                        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                                        tenant_id, TABLET_DIR_STR, file_id_.second_id()/*tablet_id*/, file_id_.reorganization_scn(),\
                                        META_MACRO_DIR_STR, \
                                        OP_KEY_STR, (file_id_.third_id() >> 32)/*op_id*/, \
                                        SEQ_KEY_STR, (file_id_.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(TENANT_ROOT_KEY, "TENANT_ROOT_KEY", false/*is_pin_local*/, true/*is_read_through*/, \
    true/*is_write_through*/, true/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid*/true, \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: cluster_id/tenant_id/TENANT_ROOT_KEY */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%s", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, \
                     tenant_id, get_storage_objet_type_str(object_type))), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(EXTERNAL_TABLE_FILE, "EXTERNAL_TABLE_FILE", false/*is_pin_local*/, false/*is_read_through*/, \
    false/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id:server-level seq id, third_id:offset / 2MB, fourth_id:N/A */ \
    ((file_id_.second_id() < UINT64_MAX) && (file_id_.third_id() >= 0) && (file_id_.third_id() < INT64_MAX)), \
    /*to_local_path_format: tenant_id_epoch_id/external_table_file/scatter_id/seq%ldidx%ld*/ \
    (databuff_printf(path_, length, pos, "%s/%lu_%ld/%s/%02lX/%s%lu%s%ld", \
                     OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, \
                     EXTERNAL_TABLE_FILE_DIR_STR, (file_id_.hash() % ObDirManager::EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM), \
                     SEQ_KEY_STR, file_id_.second_id(), IDX_KEY_STR, file_id_.third_id())), \
    /*local_path_to_macro_id*/ \
    char format[512] = {0}; \
    int num = 0; \
    const char *sub_path = nullptr; \
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) { \
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path)); \
    } else { \
      uint64_t server_seq_id = 0; \
      int64_t offset_idx = 0; \
      if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld.T%hhu", \
                  SEQ_KEY_STR, IDX_KEY_STR, (uint8_t)ObStorageObjectType::EXTERNAL_TABLE_FILE))) { \
        LOG_WARN("fail to databuff printf", KR(ret)); \
      } else if (FALSE_IT(num = sscanf(sub_path, format, &server_seq_id, &offset_idx))) { \
      } else if (OB_UNLIKELY(2 != num)) { \
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE; \
        LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path)); \
      } else { \
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE); \
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::EXTERNAL_TABLE_FILE); \
        macro_id.set_second_id(server_seq_id); \
        macro_id.set_third_id(offset_idx); \
      } \
    }, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/ \
    (OB_DIR_MGR.get_external_table_file_dir(path, length, tenant_id, tenant_epoch_id)), \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(MACRO_CACHE_CKPT_DATA, "MACRO_CACHE_CKPT_DATA", false/*is_pin_local*/, true/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id: version id, third_id: tenant-level seq id, fourth_id: N/A */ \
    ((file_id_.second_id() < UINT64_MAX) && (file_id_.third_id() < UINT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/data/version_id/seq_id */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s/%lu/%s%lu", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
                     tenant_id, tenant_epoch_id, MACRO_CACHE_CKPT_DIR_STR, DATA_MACRO_DIR_STR,
                     file_id_.second_id(), SEQ_KEY_STR, file_id_.third_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(MACRO_CACHE_CKPT_META, "MACRO_CACHE_CKPT_META", false/*is_pin_local*/, true/*is_read_through*/, \
    true/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid second_id: version id, third_id: N/A, fourth_id: N/A */ \
    ((file_id_.second_id() < UINT64_MAX)), \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format: cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/meta/version_id */ \
    (databuff_printf(path_, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s/%s%lu", \
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
                     tenant_id, tenant_epoch_id, MACRO_CACHE_CKPT_DIR_STR, META_MACRO_DIR_STR,
                     VER_KEY_STR, file_id_.second_id())), \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

  STORAGE_OBJECT_TYPE_INFO(MAX, "MAX", false/*is_pin_local*/, false/*is_read_through*/, \
    false/*is_write_through*/, false/*is_overwrite*/, false/*is_support_fd_cache*/, \
    /*is_valid*/false, \
    /*to_local_path_format*/OB_NOT_SUPPORTED, \
    /*local_path_to_macro_id*/ \
    ret = OB_NOT_SUPPORTED;, \
    /*to_remote_path_format*/OB_NOT_SUPPORTED, \
    /*get_parent_dir*/OB_NOT_SUPPORTED, \
    /*create_parent_dir*/OB_NOT_SUPPORTED)

/*
  *******************************************************************************
  * WARNING!!! Forbid insert object type in the middle! Only allow at the tail! *
  *******************************************************************************
 */

#endif
