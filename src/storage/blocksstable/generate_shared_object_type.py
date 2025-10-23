#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2025 - 2025 Alibaba Inc. All Rights Reserved.
# Author:
#  shell> python3 generate_shared_object_type.py
# author: zhaomiao
import os
import sys
import re
import glob

# global variables
copyright = '''/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */'''

# store all configed object types
all_storage_object_types = []
storage_object_type_strs = []

# Base class function signatures for validation
BASE_FUNCTION_SIGNATURES = {
    'is_valid': 'bool is_valid(const MacroBlockId &file_id) const',
    'to_local_path_format': 'int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const',
    'to_remote_path_format': 'int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const',
    'local_path_to_macro_id': 'int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const',
    'get_parent_dir': 'int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const',
    'create_parent_dir': 'int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const',
    'remote_path_to_macro_id': 'int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const',
    'get_effective_tablet_id': 'int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const',
    'opt_to_string': 'int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const',
    'get_object_id': 'int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const'
}

def extract_function_signature(code_str):
    """Extract function signature from complete function definition"""
    if not code_str or code_str == 'OB_NOT_SUPPORTED':
        return None

    # Remove leading/trailing whitespace
    code_str = code_str.strip()

    # Find the first '{' to identify the end of function signature
    brace_pos = code_str.find('{')
    if brace_pos == -1:
        return None

    # Extract the signature part (everything before the first '{')
    signature = code_str[:brace_pos].strip()

    # Remove any leading 'virtual' keyword if present
    if signature.startswith('virtual '):
        signature = signature[8:].strip()

    return signature

def validate_function_signature(config_signature, base_signature, function_name, obj_type):
    """Validate that config function signature matches base class signature"""
    if not config_signature:
        return True  # Skip validation if no signature found

    # Normalize signatures by removing extra spaces
    config_norm = re.sub(r'\s+', ' ', config_signature.strip())
    base_norm = re.sub(r'\s+', ' ', base_signature.strip())

    if config_norm != base_norm:
        print(f"ERROR: Function signature mismatch for {function_name} in {obj_type}")
        print(f"  Expected: {base_signature}")
        print(f"  Found:    {config_signature}")
        return False

    return True

def validate_config_functions():
    """Validate all function signatures in config against base class"""
    global all_storage_object_types
    print("Validating function signatures...")
    all_valid = True

    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        for func_name, base_signature in BASE_FUNCTION_SIGNATURES.items():
            if func_name in cfg and cfg[func_name] != 'OB_NOT_SUPPORTED':
                config_signature = extract_function_signature(cfg[func_name])
                if not validate_function_signature(config_signature, base_signature, func_name, obj_type):
                    all_valid = False
    if all_valid:
        print("✅ All function signatures are valid!")
    else:
        print("❌ Function signature validation failed!")
        sys.exit(1)

    return all_valid

def extract_function_body(code_str):
    """Extract function body from complete function definition"""
    if not code_str or code_str == 'OB_NOT_SUPPORTED':
        return code_str

    # Remove leading/trailing whitespace
    code_str = code_str.strip()

    # If the code starts with '{', it's already just the function body
    if code_str.startswith('{'):
        return code_str

    # If it contains a function signature, extract the body
    # Look for the first '{' and return everything from there
    brace_pos = code_str.find('{')
    if brace_pos != -1:
        return code_str[brace_pos:]

    # If no '{' found, return as is (might be just a return statement)
    return code_str

def def_storage_object_type_cfg(**keywords):
    """process storage object type config"""
    global all_storage_object_types, storage_object_type_strs

    obj_type = keywords.get('obj_type', 'UNKNOWN')
    if obj_type != 'none':
        all_storage_object_types[obj_type] = keywords
        storage_object_type_strs.append(obj_type)

def start_generate_h(h_file_name):
    """start to generate header file"""
    global h_f
    h_f = open(h_file_name, 'w')
    head = copyright + '''

#ifndef OCEANBASE_BLOCKSSTABLE_OB_STORAGE_OBJECT_TYPE_H_
#define OCEANBASE_BLOCKSSTABLE_OB_STORAGE_OBJECT_TYPE_H_

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_ss_reader_writer.h"
#endif
#include "common/storage/ob_device_common.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
class ObStorageObjectOpt;

#define STI(object_type) (ObStorageObjectTypeInstance::get_instance(object_type))

// Generated enum class
enum class ObStorageObjectType : uint8_t
{
'''
    h_f.write(head)

def end_generate_h():
    """end to generate header file"""
    global h_f
    end = '''
};
static constexpr uint8_t SS_OBJECT_MAX_TYPE_VAL = static_cast<uint8_t>(ObStorageObjectType::MAX);
const char *get_storage_objet_type_str(const ObStorageObjectType type);

class ObStorageObjectTypeBase
{
public:
  ObStorageObjectTypeBase() : type_(ObStorageObjectType::MAX) {}
  ObStorageObjectTypeBase(ObStorageObjectType type) : type_(type) {}
  virtual ~ObStorageObjectTypeBase() {}

  ObStorageObjectType get_type() const { return type_; }
  const char *get_type_str() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  //the ObjectType is macro type, true or false
  bool is_macro() const { return is_macro_data() || is_macro_meta(); }
  int get_open_flag_for_write() const;
  int get_open_flag_for_read() const;
  int aio_read(const ObStorageObjectReadInfo &read_info, ObStorageObjectHandle &object_handle) const;
  int aio_write(const ObStorageObjectWriteInfo &write_info, ObStorageObjectHandle &object_handle) const;
  // the ObjectType is macro data type, true or false
  virtual bool is_macro_data() const { return false; }
  // the ObjectType is tenant data type, true or false
  virtual bool is_tenant_data() const { return false; }
  // the ObjectType is macro meta type, true or false
  virtual bool is_macro_meta() const { return false; }
  //the ObjectType is tablet meta type, true or false
  virtual bool is_tablet_meta() const { return false; }
  //the ObjectType is tenant meta type, true or false
  virtual bool is_tenant_meta() const { return false; }
  //the ObjectType is private type, true or false
  virtual bool is_private() const { return false; }
  //the ObjectType is shared type, true or false
  virtual bool is_shared() const { return false; }
  //the ObjectType only store in local cache, true or false
  virtual bool is_pin_local() const { return false; }
  //whether need fsync when write
  virtual bool need_fsync() const { return true; }
  //the ObjectType whether use reserved disk space
  virtual bool use_reserved_disk_space() const { return false; }
  //whether can append write
  virtual bool can_append_write() const { return false; }
  //whether use fd cache when reading local cache file of this type, true or false
  virtual bool is_support_fd_cache() const { return false; }
  //whether path include inner tablet
  virtual bool is_path_include_inner_tablet() const { return false; }
  //the ObjectType only store in remote object storage, true or false
  virtual bool is_direct_read() const { return false; }
  //whether this type of object write through object storage, true or false
  virtual bool is_direct_write() const { return false; }
  //whether this type of object exists overwrite with 'different content', true or false
  virtual bool is_overwrite() const { return false; }
  // whether this type of object can read out of bounds
  virtual bool is_read_out_of_bounds() const { return true; }
  //the ObjectType is major type, true or false
  virtual bool is_major() const { return false; }
  //the ObjectType is mds type, true or false
  virtual bool is_mds() const { return false; }
  //the ObjectType is tmp type, true or false
  virtual bool is_tmp_file() const { return false; }
  //whether this type of object support sn mode, true or false
  virtual bool is_support_sn() const { return false; }
  //the ObjectType which 500 tenant can write
  virtual bool server_tenant_can_have() const { return false; }
  // check macro block id valid
  virtual bool is_valid(const MacroBlockId &file_id) const { return false; }
  virtual bool has_effective_tablet_id() const { return false; }
#ifdef OB_BUILD_SHARED_STORAGE
  // path format reverse, macro id to local path
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const { return OB_NOT_SUPPORTED; }
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const { return OB_NOT_SUPPORTED; }
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const { return OB_NOT_SUPPORTED; }
  virtual int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const { return OB_NOT_SUPPORTED; }
  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,
                             const MacroBlockId &file_id, const uint64_t tenant_id,
                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const { return OB_NOT_SUPPORTED; }
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const { return OB_NOT_SUPPORTED; }
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const { return OB_NOT_SUPPORTED; }
  //whethe the objecttype has effective tablet id, true or false
  void get_ss_macro_block_type(const MacroBlockId &macro_id, storage::ObSSMacroBlockType &ss_macro_block_type) const;
  int get_macro_cache_type(const uint64_t effective_tablet_id, const bool use_effective_tablet_id,
                           storage::ObSSMacroCacheType &macro_cache_type) const;
#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const { return OB_SUCCESS; }
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const { return OB_SUCCESS; }
  void set_ss_object_first_id_(const uint64_t incarnation_id, const uint64_t column_group_id, MacroBlockId &object_id) const;

protected:
  ObStorageObjectType type_;
};

class ObStorageObjectTypeInstance
{
public:
  static const ObStorageObjectTypeBase &get_instance(ObStorageObjectType type);
};

'''
    h_f.write(end)

def start_generate_cpp(cpp_file_name):
    """start to generate source file"""
    global cpp_f
    cpp_f = open(cpp_file_name, 'w')
    head = copyright + '''
#define USING_LOG_PREFIX STORAGE
#include "ob_storage_object_type.h"
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

// Generated string array
static const char *ob_storage_object_type_strs[] = {
'''
    cpp_f.write(head)

def end_generate_cpp():
    """end to generate source file"""
    global cpp_f
    end = '''
};
static inline const char *get_ls_inner_tablet_name_(const int64_t tablet_id)
{
  ObTabletID id(tablet_id);
  if (id.is_ls_tx_ctx_tablet()) {
    return "TX_CTX";
  } else if (id.is_ls_tx_data_tablet()) {
    return "TX_DATA";
  } else if (id.is_ls_lock_tablet()) {
    return "TX_LOCK";
  } else if (id.is_ls_reorg_info_tablet()) {
    return "REORG_INFO";
  } else {
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "unexpected tablet_id", K(tablet_id));
    return "UNEXPECTED";
  }
}

static int get_ls_inner_tablet_id_(const char *str, int64_t &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(str))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid str", KR(ret));
  } else if (0 == STRNCMP("TX_CTX", str, STRLEN("TX_CTX"))) {
    tablet_id = ObTabletID::LS_TX_CTX_TABLET_ID;
  } else if (0 == STRNCMP("TX_DATA", str, STRLEN("TX_DATA"))) {
    tablet_id = ObTabletID::LS_TX_DATA_TABLET_ID;
  } else if (0 == STRNCMP("TX_LOCK", str, STRLEN("TX_LOCK"))) {
    tablet_id = ObTabletID::LS_LOCK_TABLET_ID;
  } else if (0 == STRNCMP("REORG_INFO", str, STRLEN("REORG_INFO"))) {
    tablet_id = ObTabletID::LS_REORG_INFO_TABLET_ID;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected str", K(str));
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
static int prewarm_file_to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%lu/%s_%lu/%s/%ld/%ld/%s/%s/%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id.second_id(),
              file_id.fourth_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

static int prewarm_file_remote_path_to_macro_id(const char *path, const ObStorageObjectType type, MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  // /tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 5))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t scn_id = 0;
    int64_t fourth_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s/%s%%ld.T%hhu",
                MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, (uint8_t)type))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &fourth_id, &scn_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(scn_id);
      macro_id.set_fourth_id(fourth_id);
    }
  }
  return ret;
}
#endif

const char *get_storage_objet_type_str(const ObStorageObjectType type)
{
  return STI(type).get_type_str();
}

/**
 * ---------------------------------------ObStorageObjectTypeBase----------------------------------------
 */
const char *ObStorageObjectTypeBase::get_type_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(ob_storage_object_type_strs) == (int64_t)ObStorageObjectType::MAX + 1,
                "ob_storage_object_type string array size mismatch enum ObStorageObjectType count");
  const char *str = NULL;
    if (type_ < ObStorageObjectType::MAX) {
    str = ob_storage_object_type_strs[static_cast<int64_t>(type_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid ObStorageObjectType", K_(type));
  }
  return str;
}

int64_t ObStorageObjectTypeBase::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type), "type_str", get_type_str());
  J_OBJ_END();
  return pos;
}

#ifdef OB_BUILD_SHARED_STORAGE
void ObStorageObjectTypeBase::get_ss_macro_block_type(
  const MacroBlockId &macro_id, storage::ObSSMacroBlockType &ss_macro_block_type) const
{
  ss_macro_block_type = ObSSMacroBlockType::MAX_TYPE;
  if (ObStorageObjectType::EXTERNAL_TABLE_FILE == type_) {
    ss_macro_block_type = ObSSMacroBlockType::EXTERNAL_TABLE;
  } else if (macro_id.is_shared_data_or_meta()) {
    ss_macro_block_type = ObSSMacroBlockType::SHARED_MACRO;
  } else if (macro_id.is_private_data_or_meta()) {
    ss_macro_block_type = ObSSMacroBlockType::PRIVATE_MACRO;
  }
}

int ObStorageObjectTypeBase::get_macro_cache_type(const uint64_t effective_tablet_id,
    const bool use_effective_tablet_id, storage::ObSSMacroCacheType &macro_cache_type) const
{
  int ret = OB_SUCCESS;
  macro_cache_type = ObSSMacroCacheType::MAX_TYPE;
  if (OB_UNLIKELY(ObStorageObjectType::MAX == type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object type", KR(ret), K(type_));
  } else {
    switch (type_) {
      case ObStorageObjectType::PRIVATE_TABLET_META: {
        macro_cache_type = ObSSMacroCacheType::META_FILE;
        break;
      }
      case ObStorageObjectType::TMP_FILE: {
        macro_cache_type = ObSSMacroCacheType::TMP_FILE;
        break;
      }
      default: {
        macro_cache_type = ObSSMacroCacheType::MACRO_BLOCK;
        break;
      }
    }
  }
  if (macro_cache_type == ObSSMacroCacheType::MACRO_BLOCK) {
      // treat macro_cache_type as ObSSMacroCacheType::MACRO_BLOCK in default.
      // e.g., PRIVATE_DATA_MACRO, SHARED_MINI_DATA_MACRO, SHARED_MDS_MINI_DATA_MACRO, SHARED_MAJOR_DATA_MACRO...
    if (OB_UNLIKELY(use_effective_tablet_id && ObTabletID::INVALID_TABLET_ID == effective_tablet_id)) {
      // ObIndexBlockScanEstimator and ObSSTableSecMetaIterator do not fill effective_tablet_id.
      // preread io triggered by these routes has no effective_tablet_id.
      // treat these macros as ObSSMacroCacheType::MACRO_BLOCK.
    } else if (use_effective_tablet_id && is_user_tenant(MTL_ID())) {
      // 1. is_user_tenant, only user tenant has ObStorageCachePolicyService.
      // both oracle mode and mysql mode user tenants have ObStorageCachePolicyService,
      // although oracle mode user tenant's ObStorageCachePolicyService is empty.
      bool is_hot = false;
      ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
      if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage cache policy service is null", KR(ret));
      } else if (OB_FAIL(storage_cache_policy_service->is_hot_tablet(effective_tablet_id, is_hot))) {
        LOG_WARN("fail to check is hot tablet", KR(ret), K(effective_tablet_id));
      } else if (is_hot) {
        macro_cache_type = ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK;
      }
    }
  }
  return ret;
}

int ObStorageObjectTypeBase::get_open_flag_for_write() const
{
  int open_flag = ObSSIOCommonOp::SS_DEFAULT_WRITE_FLAG;
  switch (type_) {
    case ObStorageObjectType::TMP_FILE: {
      open_flag = ObSSIOCommonOp::SS_TMP_FILE_WRITE_FLAG;
      break;
    }
    case ObStorageObjectType::PRIVATE_SLOG_FILE: {
      open_flag = ObSSIOCommonOp::SS_SLOG_WRITE_FLAG;
      break;
    }
    default: {
      open_flag = ObSSIOCommonOp::SS_DEFAULT_WRITE_FLAG;
      break;
    }
  }
  return open_flag;
}

int ObStorageObjectTypeBase::get_open_flag_for_read() const
{
  int open_flag = ObSSIOCommonOp::SS_DEFAULT_READ_FLAG;
  switch (type_) {
    case ObStorageObjectType::TMP_FILE: {
      open_flag = ObSSIOCommonOp::SS_TMP_FILE_READ_FLAG;
      break;
    }
    default: {
      open_flag = ObSSIOCommonOp::SS_DEFAULT_READ_FLAG;
      break;
    }
  }
  return open_flag;
}

int ObStorageObjectTypeBase::aio_read(
  const ObStorageObjectReadInfo &read_info, ObStorageObjectHandle &object_handle) const
{
  int ret = OB_SUCCESS;
  if (is_direct_read()) {
    ObSSObjectStorageReader object_storage_reader;
    if (OB_FAIL(object_storage_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else if (is_pin_local()) {
    ObSSLocalCacheReader local_cache_reader;
    if (OB_FAIL(local_cache_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else {
    switch (type_) {
      case ObStorageObjectType::PRIVATE_DATA_MACRO:
      case ObStorageObjectType::PRIVATE_META_MACRO: {
        ObSSPrivateMacroReader private_macro_reader;
        if (OB_FAIL(private_macro_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::PRIVATE_TABLET_META: {
        ObSSPrivateTabletMetaReader private_tablet_meta_reader;
        if (OB_FAIL(private_tablet_meta_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::PRIVATE_SLOG_FILE: {
        ObSSPrivateSlogReader private_slog_reader;
        if (OB_FAIL(private_slog_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::SHARED_MINI_DATA_MACRO:
      case ObStorageObjectType::SHARED_MINI_META_MACRO:
      case ObStorageObjectType::SHARED_MINOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MINOR_META_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINI_DATA_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINI_META_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINOR_META_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
        ObSSShareMacroReader share_macro_reader;
        if (OB_FAIL(share_macro_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::TMP_FILE: {
        ObSSTmpFileReader tmp_file_reader;
        if (OB_FAIL(tmp_file_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::EXTERNAL_TABLE_FILE: {
        ObSSExternalFileReader external_file_reader;
        if (OB_FAIL(external_file_reader.init(&read_info))) {
          LOG_WARN("fail to init external data reader", KR(ret), K(read_info));
        } else if (OB_FAIL(external_file_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected storage object type", KR(ret), K(type_), "object_type_str",
                 get_type_str(), K(read_info), K(object_handle));
        break;
      }
    }
  }
  return ret;
}

int ObStorageObjectTypeBase::aio_write(
  const ObStorageObjectWriteInfo &write_info, ObStorageObjectHandle &object_handle) const
{
  int ret = OB_SUCCESS;
  if (is_direct_read()) {
    ObSSObjectStorageWriter object_storage_writer;
    if (OB_FAIL(object_storage_writer.aio_write(write_info, object_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    }
  } else if (is_pin_local()) {
    ObSSLocalCacheWriter local_cache_writer;
    if (OB_FAIL(local_cache_writer.aio_write(write_info, object_handle))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
      }
    }
  } else {
    switch (type_) {
      case ObStorageObjectType::PRIVATE_DATA_MACRO:
      case ObStorageObjectType::PRIVATE_META_MACRO: {
        ObSSPrivateMacroWriter private_macro_writer;
        if (OB_FAIL(private_macro_writer.aio_write(write_info, object_handle))) {
          if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
        }
        break;
      }
      case ObStorageObjectType::PRIVATE_TABLET_META: {
        ObSSPrivateTabletMetaWriter private_tablet_meta_writer;
        if (OB_FAIL(private_tablet_meta_writer.aio_write(write_info, object_handle))) {
          LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::PRIVATE_SLOG_FILE: {
        ObSSPrivateSlogWriter private_slog_writer;
        if (OB_FAIL(private_slog_writer.aio_write(write_info, object_handle))) {
          if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
        }
        break;
      }
      case ObStorageObjectType::SHARED_MINI_DATA_MACRO:
      case ObStorageObjectType::SHARED_MINI_META_MACRO:
      case ObStorageObjectType::SHARED_MINOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MINOR_META_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINI_DATA_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINI_META_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINOR_META_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
        ObSSShareMacroWriter share_macro_writer;
        if (OB_FAIL(share_macro_writer.aio_write(write_info, object_handle))) {
          LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::TMP_FILE: {
        ObSSTmpFileWriter tmp_file_writer;
        if (OB_FAIL(tmp_file_writer.aio_write(write_info, object_handle))) {
          if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
        }
        break;
      }
      case ObStorageObjectType::EXTERNAL_TABLE_FILE: {
        ObSSLocalCacheWriter local_cache_writer;
        if (OB_FAIL(local_cache_writer.aio_write(write_info, object_handle))) {
          if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected storage object type", KR(ret), K(type_), "object_type_str",
                 get_type_str(), K(write_info), K(object_handle));
        break;
      }
    }
  }
  return ret;
}
#endif

void ObStorageObjectTypeBase::set_ss_object_first_id_(
  const uint64_t incarnation_id, const uint64_t column_group_id, MacroBlockId &object_id) const
{
  object_id.set_version_v2();
  object_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  object_id.set_storage_object_type((uint64_t)type_);
  object_id.set_incarnation_id(incarnation_id);
  object_id.set_column_group_id(column_group_id);
}
'''
    cpp_f.write(end)

def print_enum_values():
    """print enum values"""
    global h_f, all_storage_object_types
    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        h_f.write(f'    {obj_type},\n')
    h_f.write('    MAX')

def print_string_array():
    """print string array"""
    global cpp_f, all_storage_object_types
    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        cpp_f.write(f'  "{obj_type}",\n')
    cpp_f.write('  "MAX"')

def generate_class_name(obj_type):
    parts = obj_type.split("_")
    capitalized_parts = [part.capitalize() for part in parts]
    combined = "".join(capitalized_parts)
    class_name = f"Ob{combined}Type"
    return class_name


def generate_class_declarations():
    """generate class declaration"""
    global h_f, all_storage_object_types

    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        class_name = generate_class_name(obj_type)
        h_f.write(f"""
/**
 * ---------------------------------------{class_name}----------------------------------------
 */
class {class_name} : public ObStorageObjectTypeBase
{{
public:
  {class_name}() : ObStorageObjectTypeBase(ObStorageObjectType::{obj_type}) {{}}
  virtual ~{class_name}() {{}}
""")

        # generate virtual function declaration according to config
        if cfg.get('data_type') == 'macro_data':
            h_f.write('  virtual bool is_macro_data() const { return true; }\n')
        elif cfg.get('data_type') == 'tenant_data':
            h_f.write('  virtual bool is_tenant_data() const{ return true; }\n')
        elif cfg.get('data_type') == 'macro_meta':
            h_f.write('  virtual bool is_macro_meta() const { return true; }\n')
        elif cfg.get('data_type') == 'tablet_meta':
            h_f.write('  virtual bool is_tablet_meta() const { return true; }\n')
        elif cfg.get('data_type') == 'tenant_meta':
            h_f.write('  virtual bool is_tenant_meta() const { return true; }\n')

        if cfg.get('access_mode') == 'private':
            h_f.write('  virtual bool is_private() const { return true; }\n')
        elif cfg.get('access_mode') == 'shared':
            h_f.write('  virtual bool is_shared() const { return true; }\n')

        if cfg.get('read_odirect'):
            h_f.write('  virtual bool is_direct_read() const { return true; }\n')
        if cfg.get('write_odirect'):
            h_f.write('  virtual bool is_direct_write() const { return true; }\n')
        if cfg.get('is_pin_local'):
            h_f.write('  virtual bool is_pin_local() const { return true; }\n')

        if not cfg.get('need_fsync', True):
            h_f.write('  virtual bool need_fsync() const { return false; }\n')

        if cfg.get('use_reserved_disk_space'):
            h_f.write('  virtual bool use_reserved_disk_space() const { return true; }\n')

        if cfg.get('can_append_write'):
            h_f.write('  virtual bool can_append_write() const { return true; }\n')

        if cfg.get('is_support_fd_cache'):
            h_f.write('  virtual bool is_support_fd_cache() const { return true; }\n')

        if cfg.get('is_overwrite'):
            h_f.write('  virtual bool is_overwrite() const { return true; }\n')

        if not cfg.get('is_read_out_of_bounds', True):
            h_f.write('  virtual bool is_read_out_of_bounds() const { return false; }\n')

        if cfg.get('is_mds'):
            h_f.write('  virtual bool is_mds() const { return true; }\n')

        if cfg.get('is_major'):
            h_f.write('  virtual bool is_major() const { return true; }\n')

        if cfg.get('is_tmp'):
            h_f.write('  virtual bool is_tmp_file() const { return true; }\n')

        if cfg.get('is_support_sn'):
            h_f.write('  virtual bool is_support_sn() const { return true; }\n')

        if cfg.get('server_tenant_can_have'):
            h_f.write('  virtual bool server_tenant_can_have() const { return true; }\n')

        if cfg.get('is_path_include_inner_tablet'):
            h_f.write('  virtual bool is_path_include_inner_tablet() const { return true; }\n')

        # generate virtual function declaration
        if cfg.get('is_valid') and cfg['is_valid'] != 'OB_NOT_SUPPORTED':
            h_f.write('  virtual bool is_valid(const MacroBlockId &file_id) const;\n')

        # Collect shared storage functions to group them under one macro
        shared_storage_functions = []

        if cfg.get('to_local_path_format') and cfg['to_local_path_format'] != 'OB_NOT_SUPPORTED':
            shared_storage_functions.append('  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,\n'
                                         '                                   const MacroBlockId &file_id, const uint64_t tenant_id,\n'
                                         '                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;')

        if cfg.get('to_remote_path_format') and cfg['to_remote_path_format'] != 'OB_NOT_SUPPORTED':
            shared_storage_functions.append('  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,\n'
                                         '                                    const MacroBlockId &file_id, const char *object_storage_root_dir,\n'
                                         '                                    const uint64_t cluster_id, const uint64_t tenant_id,\n'
                                         '                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;')
        if cfg.get('remote_path_to_macro_id') and cfg['remote_path_to_macro_id'] != 'OB_NOT_SUPPORTED':
            shared_storage_functions.append('  virtual int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;')

        if cfg.get('local_path_to_macro_id') and cfg['local_path_to_macro_id'] != 'OB_NOT_SUPPORTED':
            shared_storage_functions.append('  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;')


        if cfg.get('get_parent_dir') and cfg['get_parent_dir'] != 'OB_NOT_SUPPORTED':
            shared_storage_functions.append('  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,\n'
                                         '                             const MacroBlockId &file_id, const uint64_t tenant_id,\n'
                                         '                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;')

        if cfg.get('create_parent_dir') and cfg['create_parent_dir'] != 'OB_NOT_SUPPORTED':
            shared_storage_functions.append('  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,\n'
                                         '                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;')

        if cfg.get('get_effective_tablet_id') and cfg['get_effective_tablet_id'] != 'OB_NOT_SUPPORTED':
            shared_storage_functions.append('  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;')
            h_f.write('  virtual bool has_effective_tablet_id() const { return true; }\n')

        # Write shared storage functions under one macro
        if shared_storage_functions:
            h_f.write('\n#ifdef OB_BUILD_SHARED_STORAGE\n')
            for func_decl in shared_storage_functions:
                h_f.write(func_decl + '\n')
            h_f.write('\n#endif\n')

        if cfg.get('opt_to_string') and cfg['opt_to_string'] != 'OB_NOT_SUPPORTED':
            h_f.write('  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;\n')

        if cfg.get('get_object_id') and cfg['get_object_id'] != 'OB_NOT_SUPPORTED':
            h_f.write('  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;\n')

        h_f.write('};\n')

def generate_class_implementations():
    """generate class implementation"""
    global cpp_f, all_storage_object_types

    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        class_name = generate_class_name(obj_type)
        cpp_f.write(f"""
/**
 * ---------------------------------------{class_name}----------------------------------------
 */
""")
        # generate virtual function implementation
        if cfg.get('is_valid') and cfg['is_valid'] != 'OB_NOT_SUPPORTED':
            cpp_f.write(f'\nbool {class_name}::is_valid(const MacroBlockId &file_id) const\n')
            cpp_f.write(extract_function_body(cfg['is_valid']))

        # Collect shared storage function implementations to group them under one macro
        shared_storage_impls = []

        if cfg.get('to_local_path_format') and cfg['to_local_path_format'] != 'OB_NOT_SUPPORTED':
            impl = f'int {class_name}::to_local_path_format(char *path, const int64_t length, int64_t &pos,\n'
            impl += '  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const\n'
            impl += extract_function_body(cfg['to_local_path_format'])
            shared_storage_impls.append(impl)

        if cfg.get('to_remote_path_format') and cfg['to_remote_path_format'] != 'OB_NOT_SUPPORTED':
            impl = f'int {class_name}::to_remote_path_format(char *path, const int64_t length, int64_t &pos,\n'
            impl += '  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,\n'
            impl += '  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const\n'
            impl += extract_function_body(cfg['to_remote_path_format'])
            shared_storage_impls.append(impl)

        if cfg.get('remote_path_to_macro_id') and cfg['remote_path_to_macro_id'] != 'OB_NOT_SUPPORTED':
            impl = f'\nint {class_name}::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const\n'
            impl += extract_function_body(cfg['remote_path_to_macro_id'])
            shared_storage_impls.append(impl)

        if cfg.get('local_path_to_macro_id') and cfg['local_path_to_macro_id'] != 'OB_NOT_SUPPORTED':
            impl = f'int {class_name}::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const\n'
            impl += extract_function_body(cfg['local_path_to_macro_id'])
            shared_storage_impls.append(impl)

        if cfg.get('get_parent_dir') and cfg['get_parent_dir'] != 'OB_NOT_SUPPORTED':
            impl = f'int {class_name}::get_parent_dir(char *path, const int64_t length, int64_t &pos,\n'
            impl += '  const MacroBlockId &file_id, const uint64_t tenant_id,\n'
            impl += '  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const\n'
            impl += extract_function_body(cfg['get_parent_dir'])
            shared_storage_impls.append(impl)

        if cfg.get('create_parent_dir') and cfg['create_parent_dir'] != 'OB_NOT_SUPPORTED':
            impl = f'int {class_name}::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,\n'
            impl += '  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const\n'
            impl += extract_function_body(cfg['create_parent_dir'])
            shared_storage_impls.append(impl)

        if cfg.get('get_effective_tablet_id') and cfg['get_effective_tablet_id'] != 'OB_NOT_SUPPORTED':
            impl = f'int {class_name}::get_effective_tablet_id(const MacroBlockId &macro_id,\n'
            impl += '  uint64_t &effective_tablet_id) const\n'
            impl += extract_function_body(cfg['get_effective_tablet_id'])
            shared_storage_impls.append(impl)

        # Write shared storage implementations under one macro
        if shared_storage_impls:
            cpp_f.write('\n#ifdef OB_BUILD_SHARED_STORAGE')
            for impl in shared_storage_impls:
                cpp_f.write('\n' + impl)
            cpp_f.write('\n#endif\n')

        if cfg.get('opt_to_string') and cfg['opt_to_string'] != 'OB_NOT_SUPPORTED':
            cpp_f.write(f'\nint {class_name}::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,\n')
            cpp_f.write('  const ObStorageObjectOpt &opt) const\n')
            cpp_f.write(extract_function_body(cfg['opt_to_string']))

        if cfg.get('get_object_id') and cfg['get_object_id'] != 'OB_NOT_SUPPORTED':
            cpp_f.write(f'\nint {class_name}::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const\n')
            cpp_f.write(extract_function_body(cfg['get_object_id']))
    cpp_f.write('\n')

def generate_get_instance_method():
    """generate get_instance method"""
    global cpp_f, all_storage_object_types

    cpp_f.write('\nconst ObStorageObjectTypeBase &ObStorageObjectTypeInstance::get_instance(ObStorageObjectType type)\n{\n')
    cpp_f.write('  switch (type) {\n')

    # generate static instance
    for cfg in all_storage_object_types:
        obj_type = cfg.get('obj_type')
        class_name = generate_class_name(obj_type)
        cpp_f.write(f'    case ObStorageObjectType::{obj_type}: {{\n')
        cpp_f.write(f'      static const {class_name} instance;\n')
        cpp_f.write(f'      return instance;\n    }}\n')
    cpp_f.write(f'    default: {{\n')
    cpp_f.write(f'      static const ObStorageObjectTypeBase instance;\n')
    cpp_f.write(f'      return instance;\n    }}\n')
    cpp_f.write('  }\n')
    cpp_f.write('}\n')

def clean_files(globstr):
    """clean files"""
    print(f"clean files by glob [{globstr}]")
    for f in glob.glob(os.path.join('.', globstr)):
        print(f"remove {f} ...")
        os.remove(f)

def main():
    # clean old files
    clean_files("ob_storage_object_type.h")
    clean_files("ob_storage_object_type.cpp")

    # execute config file, collect types
    exec(open("ob_storage_object_type_def.py", encoding="utf-8").read())
    # now all_storage_object_types is all types defined

    print(f"Found {len(all_storage_object_types)} storage object types:")
    for obj in all_storage_object_types:
        print(f"  - {obj.get('obj_type')}")

    if len(all_storage_object_types) == 0:
        print("Warning: No storage object types found. Please check ob_storage_object_type_def.py")
        return

    # Validate function signatures before generating code
    validate_config_functions()

    # generate code using all_storage_object_types
    print("Generating ob_storage_object_type.h...")
    start_generate_h("ob_storage_object_type.h")
    print_enum_values()
    end_generate_h()
    generate_class_declarations()

    # add namespace end
    h_f.write('''
} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_OB_STORAGE_OBJECT_TYPE_H_
''')
    h_f.close()

    print("Generating ob_storage_object_type.cpp...")
    start_generate_cpp("ob_storage_object_type.cpp")
    print_string_array()
    end_generate_cpp()
    generate_class_implementations()
    generate_get_instance_method()

    # add namespace end
    cpp_f.write('''
} // end namespace blocksstable
} // end namespace oceanbase
''')
    cpp_f.close()

    print("Generation completed successfully!")

if __name__ == "__main__":
    main()
