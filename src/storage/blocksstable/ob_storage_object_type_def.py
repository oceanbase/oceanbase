# -*- coding: utf-8 -*-
# Copyright 2025 - 2025 Alibaba Inc. All Rights Reserved.
# author: zhaomiao
# this file is used to define the storage object type
# you can add new storage object type here
# and then run `python3 generate_shared_object_type.py` to generate the code
# the generated code will be in the `ob_storage_object_type.h` and `ob_storage_object_type.cpp` files
# if you need to modify the code in these files, please modify this file or generate_shared_object_type.py.
# the following is the parameter description:
#
# required parameters:
# obj_type: the name of the storage object type
# id: explicitly set the enum value
# owner: the owner of the storage object type
# data_type: the type of the storage object data, include macro_data, tenant_data, macro_meta, tablet_meta, tenant_meta, others
# access_mode: the access mode of the storage object, include private, shared
# read_odirect: whether to read the storage object directly, True or False
# write_odirect: whether to write the storage object directly, True or False
#
# optional parameters: All parameters are optional, if you don't need to modify, just not add it.If you want modify, please contact zhaomiao.
# is_pin_local: the ObjetType only store in local cache, True or False. If is_pin_local is True, is_overwrite must be true.
# need_fsync: whether to need fsync, True or False, default is True
# use_reserved_disk_space: whether to use reserved disk space, True or False
# can_append_write: whether to can append write, True or False
# is_support_fd_cache: whether use fd cache when reading local cache file of this type, True or False.
#                       note: only MACRO BLOCK (e.g., PRIVATE_DATA_MACRO, SHARED_MAJOR_DATA_MACRO,
#                       SHARED_MDS_MINI_DATA_MACRO...) need fd cache to improve read performance.
# is_overwrite: whether this type of object exists overwrite with 'different content', True or False
# is_read_out_of_bounds: whether to read out of bounds, True or False, default is True
# is_mds: whether it is mds, True or False
# is_major: whether it is major, True or False
# is_tmp: whether it is tmp file, True or False
# is_support_sn: whether it is support sn mode, True or False
# server_tenant_can_have: whether it is 500 tenant can have, True or False
# is_path_include_inner_tablet: whether to is path include inner tablet, True or False.
#   a normal tablet's id is unique in tenant, but the id of a log stream internal tablet is the same in each log stream.
#   so if the path include inner tablet, you need to set this parameter to True.
# is_valid: the MacroBlockId is valid check logic
# to_local_path_format: the MacroBlockId convert to local cache path logic, if does not exist return OB_NOT_SUPPORTED
# local_path_to_macro_id: logic of convert from local cache path to MacroBlockId, if does not exist, fill ret = OB_NOT_SUPPORTED;
# to_remote_path_format: the MacroBlockId convert to object storage path logic, if does not exist return OB_NOT_SUPPORTED
# get_parent_dir: get parent local dir logic according to MacroBlockId, if does not exist return OB_NOT_SUPPORTED
# create_parent_dir: create parent local dir logic according to MacroBlockId, if does not exist return OB_NOT_SUPPORTED
# opt_to_string: the MacroBlockId convert to string logic, if does not exist return OB_NOT_SUPPORTED
# get_object_id: the MacroBlockId get object id logic, if does not exist return OB_NOT_SUPPORTED
# get_effective_tablet_id: the MacroBlockId get effective tablet id logic, if does not exist return OB_NOT_SUPPORTED

all_storage_object_types = []
def def_storage_object_type_cfg(**kwargs):
    # check obj_type is required parameter
    if 'obj_type' not in kwargs:
        raise ValueError("obj_type is required parameter but not provided")

    obj_type = kwargs['obj_type']
    if obj_type is None or obj_type == 'none':
        raise ValueError("obj_type cannot be None or 'none'")

    # check data_type is required parameter
    if 'data_type' not in kwargs:
        raise ValueError("data_type is required parameter but not provided")

    data_type = kwargs['data_type']
    if data_type is None:
        raise ValueError("data_type cannot be None")

    # check access_mode is required parameter
    if 'access_mode' not in kwargs:
        raise ValueError("access_mode is required parameter but not provided")

    owner = 'zhaomiao',
    access_mode = kwargs['access_mode']
    if access_mode is None:
        raise ValueError("access_mode cannot be None")

    # check read_odirect is required parameter
    if 'read_odirect' not in kwargs:
        raise ValueError("read_odirect is required parameter but not provided")

    read_odirect = kwargs['read_odirect']
    if read_odirect is None:
        raise ValueError("read_odirect cannot be None")

    # check write_odirect is required parameter
    if 'write_odirect' not in kwargs:
        raise ValueError("write_odirect is required parameter but not provided")

    write_odirect = kwargs['write_odirect']
    if write_odirect is None:
        raise ValueError("write_odirect cannot be None")

    # check owner is required parameter
    if 'owner' not in kwargs:
        raise ValueError("owner is required parameter but not provided")

    all_storage_object_types.append(kwargs)

def_storage_object_type_default_cfg = {
    'obj_type': None,
    'owner': None,
    'data_type': 'others',# {macro_data, tenant_data, macro_meta, tablet_meta, tenant_meta, others}
    'access_mode':'private', #{private, shared}
    'read_odirect': False,
    'write_odirect': False,
    # low frequency modify parameters, if you need to modify, please contact zhaomiao
    'is_pin_local': False,
    'need_fsync': True,
    'use_reserved_disk_space': False,
    'can_append_write': False,
    'is_support_fd_cache': False,
    'is_overwrite': False,
    'is_read_out_of_bounds': True,
    'is_mds': False,
    'is_major': False,
    'is_tmp': False,
    'server_tenant_can_have': False,
    'is_path_include_inner_tablet': False,
    # complex function implementation
    'is_valid': False,
    'to_local_path_format': 'OB_NOT_SUPPORTED',
    'local_path_to_macro_id': 'OB_NOT_SUPPORTED',
    'to_remote_path_format': 'OB_NOT_SUPPORTED',
    'get_parent_dir': 'OB_NOT_SUPPORTED',
    'create_parent_dir': 'OB_NOT_SUPPORTED',
    'opt_to_string':'OB_NOT_SUPPORTED',
    'get_object_id':'OB_NOT_SUPPORTED',
    'get_effective_tablet_id':'OB_NOT_SUPPORTED',
}

# PRIVATE_DATA_MACRO
def_storage_object_type_cfg(
    obj_type = 'PRIVATE_DATA_MACRO',  #ObPrivateDataMacroType
    id = 0,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'macro_data',
    read_odirect = False,
    write_odirect = False,
    is_support_fd_cache = True,
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:server_id, fourth_id:macro_transfer_seq+tenant_seq
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.macro_transfer_seq() >= 0) && (file_id.tenant_seq() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  //tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/data/svr%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02ld/%ld/%ld/%s/%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR,
              (file_id.second_id() % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM), file_id.second_id(),
              file_id.macro_transfer_seq(), DATA_MACRO_DIR_STR, SVR_KEY_STR, file_id.third_id(), SEQ_KEY_STR,
              file_id.tenant_seq()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  const char *sub_path = nullptr;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 4))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    char format[512] = {0};
    int num = 0;
    int64_t tablet_id = 0;
    int64_t transfer_seq = 0;
    int64_t server_id = 0;
    int64_t seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s%%ld%s%%ld.T%hhu",
                DATA_MACRO_DIR_STR, SVR_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &transfer_seq, &server_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_macro_transfer_seq(transfer_seq);
      macro_id.set_third_id(server_id);
      macro_id.set_tenant_seq(seq_id);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data/svr%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%ld/%lu_%ld/%s/%ld/%ld/%s/%s%ld%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, file_id.third_id(), tenant_id,
              tenant_epoch_id, TABLET_DATA_DIR_STR, file_id.second_id(), file_id.macro_transfer_seq(),
              DATA_MACRO_DIR_STR, SVR_KEY_STR, file_id.third_id(), SEQ_KEY_STR, file_id.tenant_seq()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/data
  if (OB_FAIL(OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(),
              file_id.macro_transfer_seq(), ObMacroType::DATA_MACRO))) {
    LOG_WARN("fail to get local tablet id macro dir", KR(ret));
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(tenant_id, tenant_epoch_id, file_id.second_id(),
              file_id.macro_transfer_seq()))) {
    LOG_WARN("fail to create tablet data tablet id transfer seq dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type:%s (tablet_id=%lu, transfer_seq=%lu)",
             get_type_str(), opt.private_opt_.tablet_id_, opt.private_opt_.tablet_trasfer_seq_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
      K(opt.private_opt_.tablet_id_), K(opt.private_opt_.tablet_trasfer_seq_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t seq = 0;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.private_opt_.tablet_id_);
  object_id.set_third_id(GCONF.observer_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(seq))) {
    LOG_WARN("fail to get private object seq", K(ret), K(opt));
  } else {
    object_id.set_tenant_seq(seq);
    object_id.set_macro_transfer_seq(opt.private_opt_.tablet_trasfer_seq_);
  }
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'PRIVATE_META_MACRO',  #ObPrivateMetaMacroType
    id = 1,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'macro_meta',
    read_odirect = False,
    write_odirect = False,
    is_support_fd_cache = True,
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:server_id, fourth_id:macro_transfer_seq+tenant_seq
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.macro_transfer_seq() >= 0) && (file_id.tenant_seq() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/meta/svr%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02ld/%ld/%ld/%s/%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              TABLET_DATA_DIR_STR, (file_id.second_id() % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM),
              file_id.second_id(), file_id.macro_transfer_seq(),
              META_MACRO_DIR_STR, SVR_KEY_STR, file_id.third_id(), SEQ_KEY_STR, file_id.tenant_seq()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 4))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    char format[512] = {0};
    int num = 0;
    int64_t tablet_id = 0;
    int64_t transfer_seq = 0;
    int64_t server_id = 0;
    int64_t seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s%%ld%s%%ld.T%hhu", META_MACRO_DIR_STR,
                SVR_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &transfer_seq, &server_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_macro_transfer_seq(transfer_seq);
      macro_id.set_third_id(server_id);
      macro_id.set_tenant_seq(seq_id);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/meta/svr%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%ld/%lu_%ld/%s/%ld/%ld/%s/%s%ld%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, file_id.third_id(),
              tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR, file_id.second_id(), file_id.macro_transfer_seq(),
              META_MACRO_DIR_STR, SVR_KEY_STR, file_id.third_id(), SEQ_KEY_STR, file_id.tenant_seq()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/meta/
  if (OB_FAIL(OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(),
              file_id.macro_transfer_seq(), ObMacroType::META_MACRO))) {
    LOG_WARN("fail to get local tablet id macro dir", KR(ret));
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(
              tenant_id, tenant_epoch_id, file_id.second_id(), file_id.macro_transfer_seq()))) {
    LOG_WARN("fail to create tablet data tablet id transfer seq dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type:%s (tablet_id=%lu, transfer_seq=%lu)", get_type_str(),
            opt.private_opt_.tablet_id_, opt.private_opt_.tablet_trasfer_seq_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
      K(opt.private_opt_.tablet_id_), K(opt.private_opt_.tablet_trasfer_seq_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t seq = 0;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.private_opt_.tablet_id_);
  object_id.set_third_id(GCONF.observer_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(seq))) {
    LOG_WARN("fail to get private object seq", K(ret), K(opt));
  } else {
    object_id.set_tenant_seq(seq);
    object_id.set_macro_transfer_seq(opt.private_opt_.tablet_trasfer_seq_);
  }
  return ret;
}
'''
)
def_storage_object_type_cfg(
    obj_type = 'SHARED_MINI_DATA_MACRO',  #ObSharedMiniDataMacroType
    id = 2,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_odirect = True,
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id+macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet: tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld
  // user_tablet: tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.meta_ls_id(),
              get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
              TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
              OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet: cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/data/seq%ld
  // user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/seq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINI_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_mini_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared mini ls id dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
'''
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MINI_META_MACRO',  #ObSharedMiniDataMacroType
    id = 3,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_odirect = True,
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet:tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld
  // user_tablet:tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.meta_ls_id(),
              get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
              TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
              OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/meta/seq%ld
  // user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/seq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINI_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_mini_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared mini ls id dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
'''
)
def_storage_object_type_cfg(
    obj_type = 'SHARED_MINOR_DATA_MACRO',  #ObSharedMinorDataMacroType
    id = 4,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_odirect = True,
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet: tenant_id_epoch_id/shared_minor_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld
  // user_tablet: tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
                LS_DIR_STR, file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                        TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/data/seq%ld
  // user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/seq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINOR_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_minor_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared minor ls id dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MINOR_META_MACRO',  #ObSharedMinorMetaMacroType
    id = 5,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_odirect = True,
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet:tenant_id_epoch_id/shared_minor_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld
  // user_tablet:tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
                LS_DIR_STR, file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                        OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                        TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/meta/seq%ld
  // user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/seq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINOR_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_minor_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared minor ls id dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MAJOR_DATA_MACRO',  #ObSharedMajorDataMacroType
    id = 6,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_odirect = True,
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_major = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_major_macro_cache/scatter_id/tablet%ldreorg%ldcg%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
                    OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                    MAJOR_DATA_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                    TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
                    CG_KEY_STR, file_id.column_group_id(), SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t cg_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, CG_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &cg_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_column_group_id(cg_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/sstable/cg_id/data/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/%s_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                COLUMN_GROUP_STR, file_id.column_group_id(), DATA_MACRO_DIR_STR, SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu,data_seq=%lu,cg_id=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.data_seq_,
            opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MAJOR_META_MACRO',  #ObSharedMajorMetaMacroType
    id = 7,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_odirect = True,
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_major = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_major_macro_cache/scatter_id/tablet%ldreorg%ldcg%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
                    OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                    MAJOR_DATA_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                    TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
                    CG_KEY_STR, file_id.column_group_id(), SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t cg_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, CG_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &cg_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_column_group_id(cg_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/sstable/cg_id/meta/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/%s_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                COLUMN_GROUP_STR, file_id.column_group_id(), META_MACRO_DIR_STR, SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu,data_seq=%lu,cg_id=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.data_seq_,
            opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'TMP_FILE',  #ObTmpFileType
    id = 8,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'tenant_data',
    read_odirect = False,
    write_odirect = False,
    need_fsync = False,
    can_append_write = True,
    is_read_out_of_bounds = False,
    is_tmp = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tmp_file_id, third_id:segment_id
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%ld/%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, TMP_DATA_DIR_STR,
              file_id.second_id(), SEG_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tmp_file_id = 0;
    int64_t segment_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%s%%ld.T%hhu", SEG_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tmp_file_id, &segment_id))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tmp_file_id);
      macro_id.set_third_id(segment_id);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s%ld",
                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
                    SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
                    TMP_DATA_DIR_STR, file_id.second_id(), SEG_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tmp_data/tmp_file_id/
  if (OB_FAIL(OB_DIR_MGR.get_local_tmp_file_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id()))) {
    LOG_WARN("fail to get local tmp file dir", KR(ret));
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;

  // ERRSIM for testing: simulate disk space exhaustion during parent dir creation
  // This allows testing the scenario where file allocation succeeds but parent dir creation fails
  ret = OB_E(EventTable::EN_SHARED_STORAGE_DIR_DISK_OUTOF_SPACE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    LOG_INFO("ERRSIM: simulating disk space exhaustion during parent dir creation", KR(ret), K(file_id));
  } else if (OB_FAIL(OB_DIR_MGR.create_tmp_file_dir(tenant_id, tenant_epoch_id, file_id.second_id()))) {
    LOG_WARN("fail to create tmp file dir", KR(ret));
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t file_id = 0;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_tmp_file_seq(file_id))) {
    LOG_WARN("fail to get private tmp file seq", K(ret), K(opt));
  } else {
    object_id.set_second_id(file_id);
  }
  return ret;
}
''',
)

# SERVER_META
def_storage_object_type_cfg(
    obj_type = 'SERVER_META',  #ObServerMetaType
    id = 9,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = False,
    write_odirect = False,
    is_pin_local = True,
    is_overwrite = True,
    server_tenant_can_have = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return true;
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // super_block
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s", OB_DIR_MGR.get_local_cache_root_dir(),
              get_type_str()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // local_cache_root_dir
  if (OB_FAIL(databuff_printf(path, length, pos, "%s", OB_DIR_MGR.get_local_cache_root_dir()))) {
    LOG_WARN("fail to get local cache root dir", KR(ret));
  }
  return ret;
}''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  return OB_SUCCESS;
}''',
)

def_storage_object_type_cfg(
    obj_type = 'PRIVATE_TABLET_META',  #ObPrivateTabletMetaType
    id = 10,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'tablet_meta',
    read_odirect = False,
    write_odirect = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:ls_id, third_id:tablet_id, fourth_id:meta_transfer_seq+meta_version_id
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.meta_transfer_seq() >= 0) && (file_id.meta_version_id() >= 0);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/transfer_seq/ver%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%ld_%ld/%s/%02ld/%ld/%ld/%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, LS_DIR_STR, file_id.second_id(),
              ls_epoch_id, TABLET_META_DIR_STR, (file_id.third_id() % ObDirManager::PRIVATE_TABLET_META_SCATTER_DIR_NUM),
              file_id.third_id(), file_id.meta_transfer_seq(), VER_KEY_STR, file_id.meta_version_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 6))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t ls_id = 0;
    int64_t epoch_id = 0;
    int64_t scatter_id = 0;
    int64_t tablet_id = 0;
    int64_t meta_transfer_seq = 0;
    int64_t meta_version_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld_%%ld/%s/%%ld/%%ld/%%ld/%s%%ld.T%hhu",
                TABLET_META_DIR_STR, VER_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, &epoch_id, &scatter_id, &tablet_id, &meta_transfer_seq, &meta_version_id))) {
    } else if (OB_UNLIKELY(6 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(ls_id);
      macro_id.set_third_id(tablet_id);
      macro_id.set_meta_transfer_seq(meta_transfer_seq);
      macro_id.set_meta_version_id(meta_version_id);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/transfer_seq/ver%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld_%ld/%s/%ld/%ld/%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
              tenant_id, tenant_epoch_id, LS_DIR_STR, file_id.second_id(), ls_epoch_id, TABLET_META_DIR_STR,
              file_id.third_id(), file_id.meta_transfer_seq(), VER_KEY_STR, file_id.meta_version_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/transfer_seq
  if (OB_FAIL(OB_DIR_MGR.get_tablet_meta_tablet_id_transfer_seq_dir(path, length, tenant_id, tenant_epoch_id,
              file_id.second_id(), ls_epoch_id, file_id.third_id(), file_id.meta_transfer_seq()))) {
    LOG_WARN("fail to get tablet meta tablet id transfer seq dir", KR(ret));
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_tablet_meta_tablet_id_transfer_seq_dir(tenant_id, tenant_epoch_id, file_id.second_id(),
              ls_epoch_id, file_id.third_id(), file_id.meta_transfer_seq()))) {
    LOG_WARN("fail to create tablet meta tablet id transfer seq dir", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (ls_id=%lu,tablet_id=%lu,version=%lu,transfer_seq=%lu)",
            get_type_str(), opt.ss_private_tablet_opt_.ls_id_, opt.ss_private_tablet_opt_.tablet_id_,
            opt.ss_private_tablet_opt_.version_, opt.ss_private_tablet_opt_.tablet_transfer_seq_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
            K(opt.ss_private_tablet_opt_.ls_id_), K(opt.ss_private_tablet_opt_.tablet_id_),
            K(opt.ss_private_tablet_opt_.version_), K(opt.ss_private_tablet_opt_.tablet_transfer_seq_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_private_tablet_opt_.ls_id_);
  object_id.set_third_id(opt.ss_private_tablet_opt_.tablet_id_);
  object_id.set_meta_version_id(opt.ss_private_tablet_opt_.version_);
  object_id.set_meta_transfer_seq(opt.ss_private_tablet_opt_.tablet_transfer_seq_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'PRIVATE_SLOG_FILE',  #ObPrivateSlogFileType
    id = 11,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'tenant_meta',
    read_odirect = False,
    write_odirect = False,
    is_overwrite = True,
    use_reserved_disk_space = True,
    can_append_write = True,
    server_tenant_can_have = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:file_id
  return (file_id.fourth_id() >= 0) && (file_id.fourth_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // server_slog/seq%ld or tenant_id_epoch_id/slog/seq%ld
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%s/%s%ld",
                OB_DIR_MGR.get_local_cache_root_dir(), SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s%ld", OB_DIR_MGR.get_local_cache_root_dir(),
                tenant_id, file_id.third_id(), SLOG_STR, SEQ_KEY_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tenant_id = 0;
    int64_t epoch_id = 0;
    int64_t object_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%s%%ld.T%hhu",
                SLOG_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tenant_id, &epoch_id, &object_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_third_id(epoch_id);
      macro_id.set_fourth_id(object_id);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/slog/seq%ld or cluster_id/server_id/server_slog/seq%ld
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s_%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR,
                server_id, SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR,
                server_id, tenant_id, file_id.third_id(), SLOG_STR, SEQ_KEY_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/slog/
  if (OB_FAIL(OB_DIR_MGR.get_tenant_slog_dir(path, length, file_id.second_id(), file_id.third_id()))) {
    LOG_WARN("fail to get tenant slog dir", KR(ret));
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_slog_dir(file_id.second_id(), file_id.third_id()))) {
    LOG_WARN("fail to create slog dir", KR(ret));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_slog_ckpt_obj_opt_.tenant_id_);
  object_id.set_third_id(opt.ss_slog_ckpt_obj_opt_.tenant_epoch_id_);
  object_id.set_fourth_id(opt.ss_slog_ckpt_obj_opt_.file_id_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'PRIVATE_CKPT_FILE',  #ObPrivateCkptFileType
    id = 12,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'tenant_meta',
    read_odirect = True,
    write_odirect = True,
    server_tenant_can_have = True,
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:file_id
  return (file_id.fourth_id() >= 0) && (file_id.fourth_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/ckpt/object_id or cluster_id/server_id/server_ckpt/object_id
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s_%s/%ld", object_storage_root_dir,
                CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, SERVER_DIR_STR, CKPT_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, tenant_id,
                file_id.third_id(), CKPT_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_slog_ckpt_obj_opt_.tenant_id_);
  object_id.set_third_id(opt.ss_slog_ckpt_obj_opt_.tenant_epoch_id_);
  object_id.set_fourth_id(opt.ss_slog_ckpt_obj_opt_.file_id_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'MAJOR_PREWARM_DATA',  #ObMajorPrewarmDataType
    id = 13,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_odirect = True,
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%lu/%s/%s/%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id.second_id(),
              file_id.fourth_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'MAJOR_PREWARM_DATA_INDEX',  #ObMajorPrewarmDataIndexType
    id = 14,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_odirect = True,
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%lu/%s/%s/%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id.second_id(),
              file_id.fourth_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'MAJOR_PREWARM_META',  #ObMajorPrewarmMetaType
    id = 15,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_odirect = True,
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%lu/%s/%s/%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id.second_id(),
              file_id.fourth_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'MAJOR_PREWARM_META_INDEX',  #ObMajorPrewarmMetaIndexType
    id = 16,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_odirect = True,
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%lu/%lu/%s/%s/%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id.second_id(),
              file_id.fourth_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'TENANT_DISK_SPACE_META',  #ObTenantDiskSpaceMetaType
    id = 17,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = False,
    write_odirect = False,
    is_pin_local = True,
    is_overwrite = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:tenant_epoch_id
  return (is_valid_tenant_id(file_id.second_id())) && (file_id.third_id() >= 0);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tenant_disk_space_meta
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld_%ld/%s",
              OB_DIR_MGR.get_local_cache_root_dir(), file_id.second_id(), file_id.third_id(), get_type_str()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id
  if (OB_FAIL(OB_DIR_MGR.get_local_tenant_dir(path, length, file_id.second_id(), file_id.third_id()))) {
    LOG_WARN("fail to get local tenant dir", KR(ret));
  }
  return ret;
}
''',
    create_parent_dir = '''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_tenant_dir(file_id.second_id(), file_id.third_id()))) {
    LOG_WARN("fail to create tenant dir", KR(ret));
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tenant_id=%lu,tenant_epoch_id=%lu)",
               get_type_str(), opt.ss_tenant_level_opt_.tenant_id_, opt.ss_tenant_level_opt_.tenant_epoch_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
                K(opt.ss_tenant_level_opt_.tenant_id_), K(opt.ss_tenant_level_opt_.tenant_epoch_id_));
    }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_tenant_level_opt_.tenant_id_);
  object_id.set_third_id(opt.ss_tenant_level_opt_.tenant_epoch_id_);
  return ret;
}
''',
)
def_storage_object_type_cfg(
    obj_type = 'IS_SHARED_TENANT_DELETED',  #ObIsSharedTenantDeletedType
    id = 18,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_odirect = True,
    server_tenant_can_have = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id
  return is_valid_tenant_id(file_id.second_id());
}''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/is_shared_tenant_deleted
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR,
              file_id.second_id(), get_type_str()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_shared_tenant_id_opt_.tenant_id_);
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tenant_id=%lu)",
             get_type_str(), opt.ss_shared_tenant_id_opt_.tenant_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_shared_tenant_id_opt_.tenant_id_));
  }
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MICRO_DATA_MACRO',  #ObSharedMicroDataMacroType
    id = 19,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_odirect = True,
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MICRO_META_MACRO',  #ObSharedMicroMetaMacroType
    id = 20,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_odirect = True,
)

def_storage_object_type_cfg(
    obj_type = 'UNSEALED_REMOTE_SEG_FILE',  #ObUnsealedRemoteSegFileType
    id = 21,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = False,
    write_odirect = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tmp_file_id, third_id:segment_id, fourth_id:valid_length
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() > 0);
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ldlen%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s%ld%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
              TMP_DATA_DIR_STR, file_id.second_id(), SEG_KEY_STR, file_id.third_id(), LEN_KEY_STR, file_id.fourth_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MDS_MINI_DATA_MACRO',  #ObSharedMdsMiniDataMacroType
    id = 22,
    owner = 'yanyuan.cxf',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_odirect = True,
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_mds = True,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
               OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
               SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
               TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
               (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t op_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/mini/sstable/op_id/data/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld",
             object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
             file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
             (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR, SEQ_KEY_STR,
             (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
'''
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MDS_MINI_META_MACRO',  #ObSharedMdsMiniMetaMacroType
    id = 23,
    owner = 'yanyuan.cxf',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_odirect = True,
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_mds = True,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
               OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
               SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
               TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
               (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t op_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/mini/sstable/op_id/meta/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld",
             object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
             file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
             (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR, SEQ_KEY_STR,
             (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
'''
)
def_storage_object_type_cfg(
    obj_type = 'SHARED_MDS_MINOR_DATA_MACRO',  #ObSharedMdsMinorDataMacroType
    id = 24,
    owner = 'yanyuan.cxf',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_odirect = True,
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_mds = True,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
               OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
               SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
               TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
               (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t op_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/minor/sstable/op_id/data/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld",
             object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
             file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
             (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR, SEQ_KEY_STR,
             (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
'''
)
def_storage_object_type_cfg(
    obj_type = 'SHARED_MDS_MINOR_META_MACRO',  #ObSharedMdsMinorMetaMacroType
    id = 25,
    owner = 'yanyuan.cxf',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_odirect = True,
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_mds = True,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
               OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
               SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
               TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
               (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t op_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/minor/sstable/op_id/meta/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld",
             object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
             file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
             (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR, SEQ_KEY_STR,
             (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
'''
)
def_storage_object_type_cfg(
    obj_type = 'SHARED_TABLET_META',  #ObSharedTabletMetaType
    id = 41,
    owner = 'jianyue',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_odirect = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return true;
}''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_tablet_meta_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_tablet_meta_opt_.op_id_);
  object_id.set_ss_fourth_id(opt.ss_tablet_meta_opt_.is_inner_tablet_,
  opt.ss_tablet_meta_opt_.ls_id_, opt.ss_tablet_meta_opt_.reorganization_scn_);
  return ret;
}
'''
)
def_storage_object_type_cfg(
    obj_type = 'SHARED_TABLET_SUB_META',  #ObSharedTabletSubMetaType
    id = 76,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'tablet_meta',
    read_odirect = True,
    write_odirect = True,
    is_overwrite = True,
    is_path_include_inner_tablet = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // user_tablet: second_id:tablet_id, third_id:op_id, fourth_id:N/A inner_tablet: second_id:tablet_id, third_id:op_id, fourth_id:ls_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.third_id() < INT64_MAX) && ((file_id.meta_is_inner_tablet() == true && (file_id.meta_ls_id() > 0 &&
         file_id.meta_ls_id() < INT64_MAX)) || (file_id.meta_is_inner_tablet() == false));
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet: cluster_id/tenant_id/ls/ls_id/tablet_name/meta/op%ldseq%lu
  // user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/meta/op%ldseq%lu
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s%ld%s%lu",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id()/*ls_id*/, get_ls_inner_tablet_name_(file_id.second_id())/*tablet_name*/,
                META_MACRO_DIR_STR, OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s%ld%s%lu",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id()/*tablet_id*/, file_id.reorganization_scn(), META_MACRO_DIR_STR, OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  }
  return ret;
}
''',
    get_effective_tablet_id = '''
int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu, ls_id=%lu, op_id=%u, data_seq=%u,"
             "is_inner_tablet=%d, reorganization_scn=%lu)",
             get_type_str(), opt.ss_tablet_sub_meta_opt_.tablet_id_,
             opt.ss_tablet_sub_meta_opt_.ls_id_, opt.ss_tablet_sub_meta_opt_.op_id_,
             opt.ss_tablet_sub_meta_opt_.data_seq_ , opt.ss_tablet_sub_meta_opt_.is_inner_tablet_,
             opt.ss_tablet_sub_meta_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_tablet_sub_meta_opt_.tablet_id_),
              K(opt.ss_tablet_sub_meta_opt_.ls_id_),
              K(opt.ss_tablet_sub_meta_opt_.is_inner_tablet_),
              K(opt.ss_tablet_sub_meta_opt_.op_id_),
              K(opt.ss_tablet_sub_meta_opt_.data_seq_),
              K(opt.ss_tablet_sub_meta_opt_.reorganization_scn_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_tablet_sub_meta_opt_.tablet_id_);
  object_id.set_third_id((uint64_t(opt.ss_tablet_sub_meta_opt_.op_id_) << 32) | (opt.ss_tablet_sub_meta_opt_.data_seq_ & 0xFFFFFFFF));
  object_id.set_ss_fourth_id(opt.ss_tablet_sub_meta_opt_.is_inner_tablet_,
  opt.ss_tablet_sub_meta_opt_.ls_id_, opt.ss_tablet_sub_meta_opt_.reorganization_scn_);
  return OB_SUCCESS;
}
''',
)
def_storage_object_type_cfg(
    obj_type = 'TENANT_ROOT_KEY',
    id = 77,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'others',
    read_odirect = True,
    write_odirect = True,
    is_overwrite = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return true;
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/TENANT_ROOT_KEY
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s", object_storage_root_dir, CLUSTER_DIR_STR,
              cluster_id, TENANT_DIR_STR, tenant_id, get_type_str()))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  return OB_SUCCESS;
}''',
)

def_storage_object_type_cfg(
    obj_type = 'EXTERNAL_TABLE_FILE',
    id = 78,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'tenant_data',
    read_odirect = False,
    write_odirect = False,
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:server-level seq id, third_id:offset / 2MB
  return (file_id.second_id() < UINT64_MAX) && (file_id.third_id() >= 0) && (file_id.third_id() < INT64_MAX);
}
''',
    to_local_path_format = '''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/external_table_file/scatter_id/seq%ldidx%ld*
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%lu%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              EXTERNAL_TABLE_FILE_DIR_STR, (file_id.hash() % ObDirManager::EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM),
              SEQ_KEY_STR, file_id.second_id(), IDX_KEY_STR, file_id.third_id()))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    local_path_to_macro_id = '''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    uint64_t server_seq_id = 0;
    int64_t offset_idx = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld.T%hhu",
                SEQ_KEY_STR, IDX_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &server_seq_id, &offset_idx))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(server_seq_id);
      macro_id.set_third_id(offset_idx);
    }
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (server_seq_id=%lu, offset_idx=%ld)",
              get_type_str(), opt.ss_external_table_file_opt_.server_seq_id_,
              opt.ss_external_table_file_opt_.offset_idx_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos),
              K(get_type_str()),
              K(opt.ss_external_table_file_opt_.server_seq_id_),
              K(opt.ss_external_table_file_opt_.offset_idx_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_external_table_file_opt_.server_seq_id_);
  object_id.set_third_id(opt.ss_external_table_file_opt_.offset_idx_);
  return OB_SUCCESS;
}
''',
    get_parent_dir = '''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, EXTERNAL_TABLE_FILE_DIR_STR))) {
    LOG_WARN("failed to get external table file dir", KR(ret),
        K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}
''',
)
def_storage_object_type_cfg(
    obj_type = 'MACRO_CACHE_CKPT_DATA',
    id = 79,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = True,
    write_odirect = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id: version id, third_id: tenant-level seq id
  return (file_id.second_id() < UINT64_MAX) && (file_id.third_id() < UINT64_MAX);
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/data/version_id/seq_id
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s/%lu/%s%lu", object_storage_root_dir,
              CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
              MACRO_CACHE_CKPT_DIR_STR, DATA_MACRO_DIR_STR, file_id.second_id(), SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (version_id_=%lu, seq_id=%lu)", get_type_str(),
              opt.ss_macro_cache_ckpt_opt_.version_id_, opt.ss_macro_cache_ckpt_opt_.seq_id_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_macro_cache_ckpt_opt_.version_id_), K(opt.ss_macro_cache_ckpt_opt_.seq_id_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_macro_cache_ckpt_opt_.version_id_);
  object_id.set_third_id(opt.ss_macro_cache_ckpt_opt_.seq_id_);
  return OB_SUCCESS;
}
''',
)
def_storage_object_type_cfg(
    obj_type = 'MACRO_CACHE_CKPT_META',
    id = 80,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = True,
    write_odirect = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id: version id
  return (file_id.second_id() < UINT64_MAX);
}
''',
    to_remote_path_format = '''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/meta/version_id
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s/%s%lu",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
              tenant_id, tenant_epoch_id, MACRO_CACHE_CKPT_DIR_STR, META_MACRO_DIR_STR,
              VER_KEY_STR, file_id.second_id()))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
''',
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (version_id_=%lu)", get_type_str(),
              opt.ss_macro_cache_ckpt_opt_.version_id_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_macro_cache_ckpt_opt_.version_id_));
  }
  return ret;
}
''',
    get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_macro_cache_ckpt_opt_.version_id_);
  return OB_SUCCESS;
}
''',
)