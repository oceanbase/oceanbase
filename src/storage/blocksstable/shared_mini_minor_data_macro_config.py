# -*- coding: utf-8 -*-
# SHARED_MINI_*_MACRO configurations

def create_shared_macro_config(obj_type, data_type, type_id, include_write_back=True):
    """Create shared macro configuration with common parameters"""
    write_strategy = ["WRITE_THROUGH", "WRITE_BACK", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"] if include_write_back else ["WRITE_THROUGH", "WRITE_THROUGH_AND_TRY_WRITE_LCACHE"]

    base_config = {
        'obj_type': obj_type,
        'owner': 'yunxing.cyx',
        'id': type_id,
        'access_mode': 'shared',
        'data_type': data_type,
        'read_odirect': False,
        'write_strategy': write_strategy,
        'is_support_fd_cache': True,
        'is_read_out_of_bounds': False,
        'is_path_include_inner_tablet': True,
    }

    return base_config

# Common function templates
def create_common_functions(is_mds= 'false'):
    """Create common functions used by all shared macro configurations"""
    return {
        'is_valid': '''
bool is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id+macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
''',
        'get_effective_tablet_id': '''
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
        'get_object_id': f'''
int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(!{is_mds} && opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}}
''',
    }

def create_path_functions(cache_dir_str,
                          remote_dir_str=None,
                          data_or_meta_macro_dir_str=None,
                          source_str_func=None,
                          parse_func=None,
                          source_type_func=None,
                          build_data_seq_func=None,
                          create_parent_dir_func_str=None,
                          is_mds= 'false'):
    """Create path-related functions for specific macro types"""
    to_local_path_format = f'''
int to_local_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{{
  int ret = OB_SUCCESS;
  int64_t op_id = 0;
  int64_t seq = 0;
  int64_t source_type = 0;
  if (OB_FAIL({parse_func}(file_id.third_id(), source_type, op_id, seq))) {{
    LOG_WARN("fail to parse shared mini op id and seq", KR(ret));
  }}
  // inner_tablet: tenant_id_epoch_id/{cache_dir_str}/ls/ls_id/tablet_name_op%s%ldseq%ld
  // user_tablet: tenant_id_epoch_id/{cache_dir_str}/scatter_id/tablet%ldreorg%ldop%s%ldseq%ld
  else if (file_id.meta_is_inner_tablet() && !{is_mds}) {{
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              {cache_dir_str}, LS_DIR_STR, file_id.meta_ls_id(),
              get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR, {source_str_func}(source_type), op_id,
              SEQ_KEY_STR, seq))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }}
  }} else {{
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              {cache_dir_str}, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
              TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
              OP_KEY_STR, {source_str_func}(source_type), op_id,
              SEQ_KEY_STR, seq))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }}
  }}
  return ret;
}}
'''

    local_path_to_macro_id = f'''
int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{{
  int ret = OB_SUCCESS;
  char format[512] = {{0}};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {{0}};
  char tablet_name_part2[256] = {{0}};
  char tablet_name[512] = {{0}};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  char source_type_str[32] = {{0}};
  int64_t source_type = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {{
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  }} else if (!{is_mds} && NULL != STRSTR(judge_path, LS_DIR_STR)) {{
    // inner_tablet: tenant_id_epoch_id/{cache_dir_str}/ls/ls_id/tablet_name_op%s%ldseq%ld
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    }} else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%[^0-9]%%ld%s%%ld.T%hhu",
                          OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }} else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, source_type_str, &op_id, &macro_seq_id))) {{
    }} else if (OB_UNLIKELY(6 != num)) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    }} else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {{
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    }} else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {{
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    }} else if (OB_FAIL({source_type_func}(source_type_str, source_type))) {{
      LOG_WARN("fail to get shared mini source type", KR(ret), K(source_type_str));
    }} else {{
      is_inner_tablet = true;
    }}
  }} else {{
    // user_tablet: tenant_id_epoch_id/{cache_dir_str}/scatter_id/tablet%ldreorg%ldop%s%ldseq%ld
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    }} else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%[^0-9]%%ld%s%%ld.T%hhu",
                  TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }} else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, source_type_str, &op_id, &macro_seq_id))) {{
    }} else if (OB_UNLIKELY(5 != num)) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    }} else if (OB_FAIL({source_type_func}(source_type_str, source_type))) {{
      LOG_WARN("fail to get shared mini source type", KR(ret), K(source_type_str));
    }} else {{
      is_inner_tablet = false;
    }}
  }}
  uint64_t data_seq = 0;
  if (FAILEDx({build_data_seq_func}(source_type, op_id, macro_seq_id, data_seq))) {{
    LOG_WARN("fail to build op id and seq", KR(ret));
  }}
  if (OB_SUCC(ret)) {{
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id(data_seq);
    if (is_inner_tablet) {{
      macro_id.set_meta_ls_id(ls_id);
    }} else {{
      macro_id.set_reorganization_scn(reorganization_scn);
    }}
  }}
  return ret;
}}
'''

    to_remote_path_format = f'''
int to_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{{
  int ret = OB_SUCCESS;
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  int64_t source_type = 0;
  if (OB_FAIL({parse_func}(file_id.third_id(), source_type, op_id, macro_seq_id))) {{
    LOG_WARN("fail to parse op id and seq", KR(ret));
  }} else if (file_id.meta_is_inner_tablet() && !{is_mds}) {{
    // cluster_id/tenant_id/ls/ls_id/TABLET_NAME/{remote_dir_str}/sstable/op_SOURCE_TYPE_id/{data_or_meta_macro_dir_str}/seq%ld
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%s_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), {remote_dir_str},
                SHARED_TABLET_SSTABLE_DIR_STR, {source_str_func}(source_type), op_id,
                {data_or_meta_macro_dir_str}, SEQ_KEY_STR, macro_seq_id))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }}
  }} else {{
    // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/{remote_dir_str}/sstable/op_SOURCE_TYPE_id/{data_or_meta_macro_dir_str}/seq%ld
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%s_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), {remote_dir_str}, SHARED_TABLET_SSTABLE_DIR_STR,
                {source_str_func}(source_type), op_id,
                {data_or_meta_macro_dir_str}, SEQ_KEY_STR, macro_seq_id))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }}
  }}
  return ret;
}}
'''

    remote_path_to_macro_id = f'''
int remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{{
  int ret = OB_SUCCESS;
  char format[512] = {{0}};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {{0}};
  char source_type_str[32] = {{0}};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  int64_t source_type = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {{
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  }} else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', {is_mds} ? 9 : 8))) {{
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  }} else if (!{is_mds} && NULL != STRSTR(judge_path, LS_DIR_STR)) {{
    // cluster_id/tenant_id/ls/ls_id/TABLET_NAME/{remote_dir_str}/sstable/op_{{source_type}}_id/{data_or_meta_macro_dir_str}/seq%ld.T%d
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%[^/]/%s/%s/%s_%%[^_]_%%ld/%s/%s%%ld.T%hhu",
                       LS_DIR_STR, {remote_dir_str}, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, {data_or_meta_macro_dir_str}, SEQ_KEY_STR, (uint8_t)type_))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }} else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name, source_type_str, &op_id, &macro_seq_id))) {{
    }} else if (OB_UNLIKELY(5 != num)) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    }} else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {{
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    }} else if (OB_FAIL({source_type_func}(source_type_str, source_type))) {{
      LOG_WARN("fail to get shared mini source type", KR(ret), K(source_type_str));
    }} else {{
      is_inner_tablet = true;
    }}
  }} else {{
    // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/{remote_dir_str}/sstable/op_{{source_type}}_id/{data_or_meta_macro_dir_str}/seq%ld.T%d
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%%ld/%s/%s/%s_%%[^_]_%%ld/%s/%s%%ld.T%hhu",
                TABLET_DIR_STR, {remote_dir_str}, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, {data_or_meta_macro_dir_str}, SEQ_KEY_STR, (uint8_t)type_))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }} else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn, source_type_str, &op_id, &macro_seq_id))) {{
    }} else if (OB_UNLIKELY(5 != num)) {{
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    }} else if (OB_FAIL({source_type_func}(source_type_str, source_type))) {{
      LOG_WARN("fail to get shared mini source type", KR(ret), K(source_type_str));
    }} else {{
      is_inner_tablet = false;
    }}
  }}
  uint64_t data_seq = 0;
  if (FAILEDx({build_data_seq_func}(source_type, op_id, macro_seq_id, data_seq))) {{
    LOG_WARN("fail to build op id and seq", KR(ret));
  }}
  if (OB_SUCC(ret)) {{
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id(data_seq);
    if (is_inner_tablet) {{
      macro_id.set_meta_ls_id(ls_id);
    }} else {{
      macro_id.set_reorganization_scn(reorganization_scn);
    }}
  }}
  return ret;
}}
'''

    opt_to_string = f'''
int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const
{{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  int64_t op_id = 0;
  int64_t data_seq = 0;
  int64_t source_type = 0;
  const char *fourth_name = (opt.ss_share_opt_.is_ls_inner_tablet_ && !{is_mds}) ? "ls_id" : "reorganization_scn";
  int64_t fourth_value = (opt.ss_share_opt_.is_ls_inner_tablet_ && !{is_mds}) ? opt.ss_share_opt_.ls_id_ : opt.ss_share_opt_.reorganization_scn_;
  if (OB_FAIL({parse_func}(opt.ss_share_opt_.data_seq_, source_type, op_id, data_seq))) {{
    LOG_WARN("fail to parse op id and seq", KR(ret));
  }}
  else if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu,source_type=%s,op_id=%lu,data_seq=%lu,%s=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, {source_str_func}(source_type), op_id, data_seq,
            fourth_name, fourth_value))) {{
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K({source_str_func}(source_type)), K(op_id), K(data_seq),
              K(fourth_name), K(fourth_value));
  }}
#endif
  return ret;
}}
'''

    get_parent_dir = f'''
int get_parent_dir(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{{
  int ret = OB_SUCCESS;
  // inner_tablet: tenant_id_epoch_id/{cache_dir_str}/ls/ls_id
  // user_tablet: tenant_id_epoch_id/{cache_dir_str}/scatter_id
  if (file_id.meta_is_inner_tablet()) {{
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              {cache_dir_str}, LS_DIR_STR, file_id.meta_ls_id()))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }}
  }} else {{
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              {cache_dir_str}, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM)))) {{
      LOG_WARN("fail to databuff printf", KR(ret));
    }}
  }}
  return ret;
}}
'''

    create_parent_dir = f'''
int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_{create_parent_dir_func_str}_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {{
    LOG_WARN("fail to create shared mini ls id dir", KR(ret));
  }}
  return ret;
}}
'''

    return {
        'to_local_path_format': to_local_path_format,
        'local_path_to_macro_id': local_path_to_macro_id,
        'to_remote_path_format': to_remote_path_format,
        'remote_path_to_macro_id': remote_path_to_macro_id,
        'opt_to_string': opt_to_string,
        'get_parent_dir': get_parent_dir,
        'create_parent_dir': create_parent_dir
    }
# SHARED_MINI_DATA_MACRO configuration
def_storage_object_type_cfg(
    **create_shared_macro_config('SHARED_MINI_V2_DATA_MACRO', 'macro_data', 84, include_write_back=True),
    **create_common_functions(),
    **create_path_functions(
      'SHARED_MINI_MACRO_CACHE_DIR_STR',
      'MINI_DIR_STR',
      'DATA_MACRO_DIR_STR',
      'ObIncSSMacroSeqHelper::get_shared_mini_source_str',
      'ObIncSSMacroSeqHelper::parse_shared_mini_data_seq',
      'ObIncSSMacroSeqHelper::get_shared_mini_source_type',
      'ObIncSSMacroSeqHelper::build_shared_mini_data_seq',
      "shared_mini")
)

# SHARED_MINI_META_MACRO configuration
def_storage_object_type_cfg(
    **create_shared_macro_config('SHARED_MINI_V2_META_MACRO', 'macro_meta', 85, include_write_back=True),
    **create_common_functions(),
    **create_path_functions(
      'SHARED_MINI_MACRO_CACHE_DIR_STR',
      'MINI_DIR_STR',
      'META_MACRO_DIR_STR',
      'ObIncSSMacroSeqHelper::get_shared_mini_source_str',
      'ObIncSSMacroSeqHelper::parse_shared_mini_data_seq',
      'ObIncSSMacroSeqHelper::get_shared_mini_source_type',
      'ObIncSSMacroSeqHelper::build_shared_mini_data_seq',
      "shared_mini")
)

# SHARED_MINOR_DATA_MACRO configuration
def_storage_object_type_cfg(
    **create_shared_macro_config('SHARED_MINOR_V2_DATA_MACRO', 'macro_data', 86, include_write_back=False),
    **create_common_functions(),
    **create_path_functions(
      'SHARED_MINOR_MACRO_CACHE_DIR_STR',
      'MINOR_DIR_STR',
      'DATA_MACRO_DIR_STR',
      'ObIncSSMacroSeqHelper::get_shared_minor_source_str',
      'ObIncSSMacroSeqHelper::parse_shared_minor_data_seq',
      'ObIncSSMacroSeqHelper::get_shared_minor_source_type',
      'ObIncSSMacroSeqHelper::build_shared_minor_data_seq',
      "shared_minor")
)

# SHARED_MINOR_META_MACRO configuration
def_storage_object_type_cfg(
    **create_shared_macro_config('SHARED_MINOR_V2_META_MACRO', 'macro_meta', 87, include_write_back=False),
    **create_common_functions(),
    **create_path_functions(
      'SHARED_MINOR_MACRO_CACHE_DIR_STR',
      'MINOR_DIR_STR',
      'META_MACRO_DIR_STR',
      'ObIncSSMacroSeqHelper::get_shared_minor_source_str',
      'ObIncSSMacroSeqHelper::parse_shared_minor_data_seq',
      'ObIncSSMacroSeqHelper::get_shared_minor_source_type',
      'ObIncSSMacroSeqHelper::build_shared_minor_data_seq',
      "shared_minor")
)
