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
# write_strategy: write strategy of this type object, including WRITE_THROUGH, WRITE_BACK and WRITE_THROUGH_AND_TRY_WRITE_LCACHE
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
#
# complex function implementation:
# is_store_in_table: whether it is store in table, True or False, default is False
# is_valid: the MacroBlockId is valid check logic
# has_effective_tablet_id: whether the MacroBlockId contains an effective tablet id
# opt_to_string: the MacroBlockId convert to string logic, if does not exist return OB_NOT_SUPPORTED
# get_object_id: the MacroBlockId get object id logic, if does not exist return OB_NOT_SUPPORTED

all_storage_object_types = []
def def_storage_object_type_cfg(**kwargs):
    # check obj_type is required parameter
    if 'obj_type' not in kwargs:
        raise ValueError("obj_type is required parameter but not provided")

    obj_type = kwargs['obj_type']
    if obj_type is None or obj_type == 'none':
        raise ValueError("obj_type cannot be None or 'none'")

     # check id is required parameter
    if 'id' not in kwargs:
        raise ValueError("id is required parameter but not provided")

    id = kwargs['id']
    if id is None:
        raise ValueError("id cannot be None")

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

    if 'write_strategy' not in kwargs:
        raise ValueError("write_strategy is required parameter but not provided")

    write_strategy = kwargs['write_strategy']
    if write_strategy is None:
        raise ValueError("write_strategy cannot be None")

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
    'write_strategy': ["WRITE_THROUGH"],
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
    'is_store_in_table': False,
    # complex function implementation
    'is_valid': False,
    'has_effective_tablet_id': False,
    'opt_to_string':'OB_NOT_SUPPORTED',
    'get_object_id':'OB_NOT_SUPPORTED',
}

# PRIVATE_DATA_MACRO
def_storage_object_type_cfg(
    obj_type = 'PRIVATE_DATA_MACRO',  #ObPrivateDataMacroType
    id = 0,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'macro_data',
    read_odirect = False,
    write_strategy = ["WRITE_BACK"],
    is_support_fd_cache = True,
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:server_id, fourth_id:macro_private_transfer_epoch+tenant_seq
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.macro_private_transfer_epoch() >= 0) && (file_id.tenant_seq() >= 0);
}
''',
    has_effective_tablet_id = True,
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type:%s (tablet_id=%lu, private_transfer_epoch=%lu)",
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
    object_id.set_macro_private_transfer_epoch(opt.private_opt_.tablet_trasfer_seq_);
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
    write_strategy = ["WRITE_BACK"],
    is_support_fd_cache = True,
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:server_id, fourth_id:macro_private_transfer_epoch+tenant_seq
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.macro_private_transfer_epoch() >= 0) && (file_id.tenant_seq() >= 0);
}
''',
    has_effective_tablet_id = True,
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type:%s (tablet_id=%lu, private_transfer_epoch=%lu)", get_type_str(),
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
    object_id.set_macro_private_transfer_epoch(opt.private_opt_.tablet_trasfer_seq_);
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
    write_strategy = ["WRITE_THROUGH"],
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
    has_effective_tablet_id = True,
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
    obj_type = 'SHARED_MINI_META_MACRO',  #ObSharedMiniMetaMacroType
    id = 3,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
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
    has_effective_tablet_id = True,
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
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"],
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
    has_effective_tablet_id = True,
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
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"],
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
    has_effective_tablet_id = True,
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
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"],
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_major = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    has_effective_tablet_id = True,
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
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"],
    is_support_fd_cache = True,
    is_read_out_of_bounds = False,
    is_major = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    has_effective_tablet_id = True,
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
    write_strategy = ["WRITE_BACK"],
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
    write_strategy = ["WRITE_BACK"],
    is_pin_local = True,
    is_overwrite = True,
    server_tenant_can_have = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return true;
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
    write_strategy = ["WRITE_BACK"],
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:ls_id, third_id:tablet_id, fourth_id:meta_private_transfer_epoch+meta_version_id
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.meta_private_transfer_epoch() >= 0) && (file_id.meta_version_id() >= 0);
}''',
    has_effective_tablet_id = True,
    opt_to_string = '''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (ls_id=%lu,tablet_id=%lu,version=%lu,private_transfer_epoch=%lu)",
            get_type_str(), opt.ss_private_tablet_opt_.ls_id_, opt.ss_private_tablet_opt_.tablet_id_,
            opt.ss_private_tablet_opt_.version_, opt.ss_private_tablet_opt_.tablet_private_transfer_epoch_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
            K(opt.ss_private_tablet_opt_.ls_id_), K(opt.ss_private_tablet_opt_.tablet_id_),
            K(opt.ss_private_tablet_opt_.version_), K(opt.ss_private_tablet_opt_.tablet_private_transfer_epoch_));
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
  object_id.set_meta_private_transfer_epoch(opt.ss_private_tablet_opt_.tablet_private_transfer_epoch_);
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
    write_strategy = ["WRITE_BACK"],
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
    write_strategy = ["WRITE_THROUGH"],
    server_tenant_can_have = True,
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:file_id
  return (file_id.fourth_id() >= 0) && (file_id.fourth_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
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
    write_strategy = ["WRITE_THROUGH"],
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
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
    write_strategy = ["WRITE_THROUGH"],
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
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
    write_strategy = ["WRITE_THROUGH"],
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
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
    write_strategy = ["WRITE_THROUGH"],
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}''',
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
    write_strategy = ["WRITE_BACK"],
    is_pin_local = True,
    is_overwrite = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:tenant_epoch_id
  return (is_valid_tenant_id(file_id.second_id())) && (file_id.third_id() >= 0);
}''',
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
    write_strategy = ["WRITE_THROUGH"],
    server_tenant_can_have = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id
  return is_valid_tenant_id(file_id.second_id());
}''',
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
    write_strategy = ["WRITE_THROUGH"],
)

def_storage_object_type_cfg(
    obj_type = 'SHARED_MICRO_META_MACRO',  #ObSharedMicroMetaMacroType
    id = 20,
    owner = 'zhaomiao',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
)

def_storage_object_type_cfg(
    obj_type = 'UNSEALED_REMOTE_SEG_FILE',  #ObUnsealedRemoteSegFileType
    id = 21,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tmp_file_id, third_id:segment_id, fourth_id:valid_length
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() > 0);
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
    write_strategy = ["WRITE_THROUGH"],
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
    has_effective_tablet_id = True,
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
    write_strategy = ["WRITE_THROUGH"],
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
    has_effective_tablet_id = True,
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
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"],
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
    has_effective_tablet_id = True,
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
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"],
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
    has_effective_tablet_id = True,
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
    write_strategy = ["WRITE_THROUGH"],
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
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
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
    has_effective_tablet_id = True,
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
    write_strategy = ["WRITE_THROUGH"],
    is_overwrite = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return true;
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
    write_strategy = ["WRITE_THROUGH"],
    is_support_sn = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:server-level seq id, third_id:offset / 2MB
  return (file_id.second_id() < UINT64_MAX) && (file_id.third_id() >= 0) && (file_id.third_id() < INT64_MAX);
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
)
def_storage_object_type_cfg(
    obj_type = 'MACRO_CACHE_CKPT_DATA',
    id = 79,
    owner = 'zhaomiao',
    access_mode = 'private',
    data_type = 'others',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id: version id, third_id: tenant-level seq id
  return (file_id.second_id() < UINT64_MAX) && (file_id.third_id() < UINT64_MAX);
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
    write_strategy = ["WRITE_THROUGH"],
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id: version id
  return (file_id.second_id() < UINT64_MAX);
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

def_storage_object_type_cfg(
    obj_type = 'SHARED_INC_MAJOR_DATA_MACRO',  #ObSharedIncMajorDataMacroType
    id = 81,
    owner = 'cyh438514',
    access_mode = 'shared',
    data_type = 'macro_data',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_support_fd_cache = True,
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    has_effective_tablet_id = True,
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
    obj_type = 'SHARED_INC_MAJOR_META_MACRO',  #ObSharedIncMajorMetaMacroType
    id = 82,
    owner = 'cyh438514',
    access_mode = 'shared',
    data_type = 'macro_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_support_fd_cache = True,
    is_major = True,
    is_read_out_of_bounds = False,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}''',
    has_effective_tablet_id = True,
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
    obj_type = 'SHARED_TABLET_SUB_META_IN_TABLE',  #ObSharedTabletSubMetaInTableType
    id = 83,
    owner = 'wangxiaohui.wxh',
    access_mode = 'shared',
    data_type = 'tablet_meta',
    read_odirect = False,
    write_strategy = ["WRITE_THROUGH"],
    is_overwrite = True,
    is_path_include_inner_tablet = True,
    is_store_in_table = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // user_tablet: second_id:tablet_id, third_id:op_id, fourth_id:N/A inner_tablet: second_id:tablet_id, third_id:op_id, fourth_id:ls_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.third_id() < INT64_MAX) && ((file_id.meta_is_inner_tablet() == true && (file_id.meta_ls_id() > 0 &&
         file_id.meta_ls_id() < INT64_MAX)) || (file_id.meta_is_inner_tablet() == false));
}
''',
    has_effective_tablet_id = True,
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

# Keep the legacy V2 type identities so persisted MacroBlockId values 84-87
# remain recognizable after their shared-storage path implementations retire.
legacy_v2_is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id+macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
'''

legacy_v2_get_object_id = '''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(!false && opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}
'''

legacy_v2_macro_types = (
    ('SHARED_MINI_V2_DATA_MACRO', 'macro_data', 84, True),
    ('SHARED_MINI_V2_META_MACRO', 'macro_meta', 85, True),
    ('SHARED_MINOR_V2_DATA_MACRO', 'macro_data', 86, False),
    ('SHARED_MINOR_V2_META_MACRO', 'macro_meta', 87, False),
)

for obj_type, data_type, type_id, include_write_back in legacy_v2_macro_types:
    write_strategy = ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"]
    if include_write_back:
        write_strategy.insert(1, "WRITE_BACK")
    def_storage_object_type_cfg(
        obj_type=obj_type,
        owner='yunxing.cyx',
        id=type_id,
        access_mode='shared',
        data_type=data_type,
        read_odirect=False,
        write_strategy=write_strategy,
        is_support_fd_cache=True,
        is_read_out_of_bounds=False,
        is_path_include_inner_tablet=True,
        is_valid=legacy_v2_is_valid,
        get_object_id=legacy_v2_get_object_id,
    )

def_storage_object_type_cfg(
    obj_type = 'SHARED_TABLET_MACRO_DIFF',  #ObSharedTabletMacroDiffType
    id = 88,
    owner = 'cxf262476',
    access_mode = 'shared',
    data_type = 'tablet_meta',
    read_odirect = True,
    write_strategy = ["WRITE_THROUGH"],
    is_path_include_inner_tablet = True,
    is_shared_tablet_macro_diff = True,
    is_valid = '''
bool is_valid(const MacroBlockId &file_id) const
{
  return true;
}
''',
)
