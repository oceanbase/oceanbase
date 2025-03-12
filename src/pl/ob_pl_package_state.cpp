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

#define USING_LOG_PREFIX PL

#include "ob_pl_package_state.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_persistent.h"
namespace oceanbase
{
using namespace common;
using namespace obmysql;
using namespace observer;
using namespace sql;
namespace pl
{

static const char* PL_PACKAGE_OVERSIZE_VALUE = "###oversize_package_val###";
static const char* PL_PACKAGE_INVALID_VALUE  = "###invalid_package_val###";  // for user var encoded as old rule

OB_SERIALIZE_MEMBER(ObPackageStateVersion, package_version_, package_body_version_);

ObPackageStateVersion::ObPackageStateVersion(const ObPackageStateVersion &other)
{
  *this = other;
}

int64_t ObPackageStateVersion::get_version_serialize_size()
{
  return serialization::encoded_length(package_version_) +
         serialization::encoded_length(package_body_version_) +
         serialization::encoded_length(header_merge_version_) +
         serialization::encoded_length(body_merge_version_) +
         serialization::encoded_length(header_public_syn_count_) +
         serialization::encoded_length(body_public_syn_count_);
}

int ObPackageStateVersion::encode(char *dst, const int64_t dst_len, int64_t &dst_pos)
{
  int ret = OB_SUCCESS;

  OZ (serialization::encode(dst, dst_len, dst_pos, package_version_));
  OZ (serialization::encode(dst, dst_len, dst_pos, package_body_version_));
  OZ (serialization::encode(dst, dst_len, dst_pos, header_merge_version_));
  OZ (serialization::encode(dst, dst_len, dst_pos, body_merge_version_));
  OZ (serialization::encode(dst, dst_len, dst_pos, header_public_syn_count_));
  OZ (serialization::encode(dst, dst_len, dst_pos, body_public_syn_count_));

  return ret;
}

int ObPackageStateVersion::decode(const char *src, const int64_t src_len, int64_t &src_pos)
{
  int ret = OB_SUCCESS;

  OZ (serialization::decode(src, src_len, src_pos, package_version_));
  OZ (serialization::decode(src, src_len, src_pos, package_body_version_));
  OZ (serialization::decode(src, src_len, src_pos, header_merge_version_));
  OZ (serialization::decode(src, src_len, src_pos, body_merge_version_));
  OZ (serialization::decode(src, src_len, src_pos, header_public_syn_count_));
  OZ (serialization::decode(src, src_len, src_pos, body_public_syn_count_));

  return ret;
}


ObPackageStateVersion &ObPackageStateVersion::operator =(const ObPackageStateVersion &other)
{
  if (this != &other) {
    package_version_ = other.package_version_;
    package_body_version_ = other.package_body_version_;
    header_merge_version_ = other.header_merge_version_;
    body_merge_version_ = other.body_merge_version_;
    header_public_syn_count_ = other.header_public_syn_count_;
    body_public_syn_count_ = other.body_public_syn_count_;
  }
  return *this;
}

bool ObPackageStateVersion::equal(const ObPackageStateVersion &other)
{
  bool b_ret = true;
  if (package_version_ != other.package_version_
      || package_body_version_ != other.package_body_version_) {
    b_ret = false;
  }
  return b_ret;
}

bool ObPackageStateVersion::operator ==(const ObPackageStateVersion &other) const
{
  bool b_ret = true;
  if (package_version_ != other.package_version_
      || package_body_version_ != other.package_body_version_
      || header_merge_version_ != other.header_merge_version_
      || body_merge_version_ != other.body_merge_version_
      || header_public_syn_count_ != other.header_public_syn_count_
      || body_public_syn_count_ != other.body_public_syn_count_) {
    b_ret = false;
  }
  return b_ret;
}

void ObPackageStateVersion::set_merge_version_and_public_syn_cnt(const ObPLPackage &head, const ObPLPackage *body)
{
  header_merge_version_ = head.get_tenant_schema_version();
  header_public_syn_count_ = head.get_public_syn_count();
  if (OB_NOT_NULL(body)) {
    body_merge_version_ = body->get_tenant_schema_version();
    body_public_syn_count_ = body->get_public_syn_count();
  }
}



OB_SERIALIZE_MEMBER(ObPackageVarSetName, package_id_,
                                         state_version_,
                                         var_type_,
                                         var_idx_);

int ObPackageVarSetName::encode_key(common::ObIAllocator &alloc,
                                    common::ObString &key_str)
{
  int ret = OB_SUCCESS;

  const char *key_prefix = package_key_prefix_v2;
  uint64_t key_prefix_len = strlen(key_prefix);
  int64_t ser_buf_pos = 0;
  uint64_t ser_buf_len = serialization::encoded_length(package_id_);
  ser_buf_len += serialization::encoded_length(state_version_.package_version_);
  ser_buf_len += serialization::encoded_length(state_version_.package_body_version_);

  char *ser_buf = static_cast<char *>(alloc.alloc(ser_buf_len));
  if (OB_ISNULL(ser_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    if (OB_FAIL(serialization::encode(ser_buf, ser_buf_len, ser_buf_pos, package_id_))) {
      LOG_WARN("fail to encode value", K(ret));
    } else if(OB_FAIL(serialization::encode(ser_buf, ser_buf_len, ser_buf_pos, state_version_.package_version_))) {
      LOG_WARN("fail to encode value", K(ret));
    } else if (OB_FAIL(serialization::encode(ser_buf, ser_buf_len, ser_buf_pos, state_version_.package_body_version_))) {
      LOG_WARN("fail to encode value", K(ret));
    } else {
      uint64_t key_buf_len = 2*ser_buf_len + key_prefix_len + 1;
      char *key_buf = static_cast<char *>(alloc.alloc(key_buf_len));
      if (OB_ISNULL(key_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(key_buf, key_prefix, static_cast<ObString::obstr_size_t>(key_prefix_len));
        if (OB_FAIL(to_hex_cstr(ser_buf, ser_buf_len, key_buf+key_prefix_len, key_buf_len-key_prefix_len))) {
          LOG_WARN("hex encode failed", K(ret));
        } else if (key_buf_len != ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, key_buf, key_buf_len, key_buf, key_buf_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("package var to down case failed", K(ret));
        } else {
          key_str.assign_ptr(key_buf, static_cast<ObString::obstr_size_t>(key_buf_len-1));
        }
      }
    }
  }

  return ret;
}

int ObPackageVarSetName::decode_key(common::ObIAllocator &alloc,
                                    const common::ObString &key_str)
{
  int ret = OB_SUCCESS;
  const char *key_prefix = package_key_prefix_v2;
  uint64_t key_prefix_len = strlen(key_prefix);
  int32_t var_name_len = key_str.length();
  int32_t hex_decoed_buf_len = var_name_len / 2;
  int64_t deser_buf_pos = 0;
  ObString var_name_str_upcase;
  char* serialize_buf = NULL;
  char *hex_decoed_buf = static_cast<char *>(alloc.alloc(hex_decoed_buf_len));
  if (OB_ISNULL(hex_decoed_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, key_str, var_name_str_upcase))) {
    LOG_WARN("package var name string copy failed", K(ret));
  } else if (var_name_len != ObCharset::caseup(CS_TYPE_UTF8MB4_GENERAL_CI,
                                               var_name_str_upcase.ptr(),
                                               var_name_str_upcase.length(),
                                               var_name_str_upcase.ptr(),
                                               var_name_str_upcase.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package var to upper case failed", K(ret));
  } else if (FALSE_IT(serialize_buf = (var_name_str_upcase.ptr() + key_prefix_len))) {
  } else if (OB_FAIL(hex_to_cstr(serialize_buf,
                                 var_name_len - key_prefix_len,
                                 hex_decoed_buf,
                                 hex_decoed_buf_len))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hex decode failed", K(ret));
  } else if (OB_FAIL(serialization::decode(hex_decoed_buf, hex_decoed_buf_len, deser_buf_pos, package_id_))) {
    LOG_WARN("fail to encode value", K(ret));
  } else if(OB_FAIL(serialization::decode(hex_decoed_buf, hex_decoed_buf_len, deser_buf_pos, state_version_.package_version_))) {
    LOG_WARN("fail to encode value", K(ret));
  } else if (OB_FAIL(serialization::decode(hex_decoed_buf, hex_decoed_buf_len, deser_buf_pos, state_version_.package_body_version_))) {
    LOG_WARN("fail to encode value", K(ret));
  } else {
    LOG_DEBUG("decode package var set name",
              K(package_id_), K(var_idx_), K(var_type_), K(state_version_));
  }

  return ret;
}

int ObPackageVarSetName::encode(common::ObIAllocator &alloc, common::ObString &var_name_str)
{
  // @pkg.$package_id$package_version$var_idx
  int ret = OB_SUCCESS;
  const char *key_prefix = package_key_prefix_v1;
  uint64_t key_prefix_len = strlen(key_prefix);
  int64_t ser_buf_pos = 0;
  uint64_t ser_buf_len = get_serialize_size();
  char *ser_buf = static_cast<char *>(alloc.alloc(ser_buf_len));
  if (OB_ISNULL(ser_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    if (OB_FAIL(serialize(ser_buf, ser_buf_len, ser_buf_pos))) {
      LOG_WARN("package var name serialize failed", K(ret));
    } else {
      uint64_t key_buf_len = 2*ser_buf_len + key_prefix_len+1;
      char *key_buf = static_cast<char *>(alloc.alloc(key_buf_len));
      if (OB_ISNULL(key_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(key_buf, key_prefix, static_cast<ObString::obstr_size_t>(key_prefix_len));
        if (OB_FAIL(to_hex_cstr(ser_buf, ser_buf_len, key_buf+key_prefix_len, key_buf_len-key_prefix_len))) {
          LOG_WARN("hex encode failed", K(ret));
        } else if (key_buf_len != ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, key_buf, key_buf_len, key_buf, key_buf_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("package var to down case failed", K(ret));
        } else {
          var_name_str.assign_ptr(key_buf, static_cast<ObString::obstr_size_t>(key_buf_len-1));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !OB_ISNULL(ser_buf)) {
    alloc.free(ser_buf);
  }
  return ret;
}

int ObPackageVarSetName::decode(common::ObIAllocator &alloc, const common::ObString &var_name_str)
{
  // $package_id$package_version$var_idx
  int ret = OB_SUCCESS;
  const char *key_prefix = package_key_prefix_v1;
  uint64_t key_prefix_len = strlen(key_prefix);
  int32_t var_name_len = var_name_str.length();
  int32_t hex_decoed_buf_len = var_name_len / 2;
  int64_t deser_buf_pos = 0;
  ObString var_name_str_upcase;
  char* serialize_buf = NULL;
  char *hex_decoed_buf = static_cast<char *>(alloc.alloc(hex_decoed_buf_len));
  if (OB_ISNULL(hex_decoed_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, var_name_str, var_name_str_upcase))) {
    LOG_WARN("package var name string copy failed", K(ret));
  } else if (var_name_len != ObCharset::caseup(CS_TYPE_UTF8MB4_GENERAL_CI,
                                               var_name_str_upcase.ptr(),
                                               var_name_str_upcase.length(),
                                               var_name_str_upcase.ptr(),
                                               var_name_str_upcase.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package var to upper case failed", K(ret));
  } else if (FALSE_IT(serialize_buf = (var_name_str_upcase.ptr() + key_prefix_len))) {
  } else if (OB_FAIL(hex_to_cstr(serialize_buf,
                                 var_name_len - key_prefix_len,
                                 hex_decoed_buf,
                                 hex_decoed_buf_len))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hex decode failed", K(ret));
  } else if (OB_FAIL(deserialize(hex_decoed_buf, hex_decoed_buf_len, deser_buf_pos))) {
    LOG_WARN("package var name serialize failed", K(ret));
  } else {
    LOG_DEBUG("decode package var set name",
              K(package_id_), K(var_idx_), K(var_type_),
              K(state_version_.package_version_),
              K(state_version_.package_body_version_));
  }
  return ret;
}

int ObPLPackageState::init()
{
  return inner_allocator_.init(nullptr);
}

int ObPLPackageState::add_package_var_val(const common::ObObj &value, ObPLType type)
{
  int ret = OB_SUCCESS;
  OZ (types_.push_back(type));
  if (OB_SUCC(ret) && OB_FAIL(vars_.push_back(value))) {
    types_.pop_back();
    LOG_WARN("failed to push back", K(ret), K(value), K(type));
  }
  return ret;
}

void ObPLPackageState::reset(ObSQLSessionInfo *session_info)
{
  changed_vars_.reset();
  for (int64_t i = 0; i < types_.count(); ++i) {
    if (!vars_.at(i).is_ext()) {
      void * ptr = vars_.at(i).get_deep_copy_obj_ptr();
      if (nullptr != ptr) {
        inner_allocator_.free(ptr);
      }
    } else if (PL_RECORD_TYPE == types_.at(i)
               || PL_NESTED_TABLE_TYPE == types_.at(i)
               || PL_ASSOCIATIVE_ARRAY_TYPE == types_.at(i)
               || PL_VARRAY_TYPE == types_.at(i)
               || PL_OPAQUE_TYPE == types_.at(i)) {
      int ret = OB_SUCCESS;
      if (OB_FAIL(ObUserDefinedType::destruct_objparam(inner_allocator_, vars_.at(i), session_info))) {
        LOG_WARN("failed to destruct composte obj", K(ret));
      }
    } else if (PL_CURSOR_TYPE == types_.at(i)) {
      ObPLCursorInfo *cursor = reinterpret_cast<ObPLCursorInfo *>(vars_.at(i).get_ext());
      if (OB_NOT_NULL(cursor)) {
        cursor->close(*session_info);
        cursor->~ObPLCursorInfo();
      }
    }
  }
  types_.reset();
  vars_.reset();
  inner_allocator_.reset();
  cursor_allocator_.reset();
  package_id_ = common::OB_INVALID_ID;
}

int ObPLPackageState::set_package_var_val(const int64_t var_idx,
                                          const ObObj &value,
                                          const ObPLResolveCtx &resolve_ctx,
                                          bool deep_copy_complex)
{
  int ret = OB_SUCCESS;
  if (var_idx < 0 || var_idx >= vars_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("invalid var index", K(var_idx), K(vars_.count()), K(ret));
  } else {
    // VAR的生命周期是SESSION级, 因此这里需要深拷贝下
    if (value.need_deep_copy() && deep_copy_complex) {
      int64_t pos = 0;
      char *buf = static_cast<char*>(inner_allocator_.alloc(value.get_deep_copy_size()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for pacakge var", K(ret), K(buf));
      }
      OZ (vars_.at(var_idx).deep_copy(value, buf, value.get_deep_copy_size(), pos));
    } else if (value.is_pl_extend()
               && value.get_meta().get_extend_type() != PL_CURSOR_TYPE
               && value.get_meta().get_extend_type() != PL_REF_CURSOR_TYPE
               && deep_copy_complex) {
      ObObj copy;
      OZ (ObUserDefinedType::deep_copy_obj(inner_allocator_, value, copy));
      OX (vars_.at(var_idx) = copy);
    } else if (value.is_null()
               && vars_.at(var_idx).is_pl_extend()
               && types_.at(var_idx) != PL_CURSOR_TYPE
               && types_.at(var_idx) != PL_REF_CURSOR_TYPE) {
      CK (vars_.at(var_idx).get_ext() != 0);
      OZ (ObUserDefinedType::reset_composite(vars_.at(var_idx), NULL));
    } else {
      vars_.at(var_idx) = value;
    }
  }
  return ret;
}

int ObPLPackageState::update_changed_vars(const int64_t var_idx)
{
  int ret = OB_SUCCESS;
  if (var_idx < 0 || var_idx >= vars_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("invalid var index", K(ret), K(var_idx), K(vars_.count()));
  }
  // NOTE: trigger package variables do not need to sync!
  if (!share::schema::ObTriggerInfo::is_trigger_package_id(package_id_)) {
    OZ (changed_vars_.add_member(var_idx));
  }
  return ret;
}

int ObPLPackageState::get_package_var_val(const int64_t var_idx, ObObj &value)
{
  int ret = OB_SUCCESS;
  if (var_idx < 0 || var_idx >= vars_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("invalid var index", K(var_idx), K(vars_.count()), K(ret));
  } else {
    OX (value = vars_.at(var_idx));
  }
  return ret;
}

int ObPLPackageState::make_pkg_var_kv_key(ObIAllocator &alloc, int64_t var_idx, PackageVarType var_type, ObString &key)
{
  int ret = OB_SUCCESS;
  if (var_idx < 0 || var_idx > vars_.count() || INVALID == var_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(var_idx), K(var_type), K(ret));
  } else {
    ObPackageVarSetName key_name;
    key_name.package_id_ = package_id_;
    key_name.state_version_ = state_version_;
    key_name.var_type_ = var_type;
    key_name.var_idx_ = var_idx;
    if (OB_FAIL(key_name.encode(alloc, key))) {
      LOG_WARN("package var name encode failed", K(ret));
    }
  }
  return ret;
}

int ObPLPackageState::get_invalid_value(ObObj &val)
{
  int ret = OB_SUCCESS;

  ObString str(PL_PACKAGE_INVALID_VALUE);
  val.set_string(ObVarcharType, str);

  return ret;
}

int ObPLPackageState::is_invalid_value(const ObObj &val, bool &is_invalid)
{
  int ret = OB_SUCCESS;
  is_invalid = false;

  if (val.is_varchar_or_char()) {
    CK (0 == val.get_string().case_compare(PL_PACKAGE_INVALID_VALUE));
    OX (is_invalid = true);
  }
  return ret;
}

int ObPLPackageState::get_oversize_value(ObObj &val)
{
  int ret = OB_SUCCESS;

  ObString str(PL_PACKAGE_OVERSIZE_VALUE);
  val.set_string(ObVarcharType, str);

  return ret;
}

int ObPLPackageState::is_oversize_value(const ObObj &val, bool &is_invalid)
{
  int ret = OB_SUCCESS;
  is_invalid = false;

  if (val.is_varchar_or_char()) {
    CK (0 == val.get_string().case_compare(PL_PACKAGE_OVERSIZE_VALUE));
    OX (is_invalid = true);
  }
  return ret;
}

int ObPLPackageState::encode_pkg_var_value(ObPLExecCtx &pl_ctx,
                                            ObPLResolveCtx &resolve_ctx,
                                            common::ObString &key,
                                            common::ObObj &value,
                                            ObIArray<ObString> &old_keys)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPackageVarEncodeInfo, 4> serialize_values;
  const ObObj *cur_ser_val = nullptr;
  bool is_oversize_value = false;
  hash::ObHashMap<int64_t, ObPackageVarEncodeInfo> value_map;
  ObPackageStateVersion state_version(OB_INVALID_VERSION, OB_INVALID_VERSION);
  if (OB_ISNULL(cur_ser_val = pl_ctx.exec_ctx_->get_my_session()->get_user_variable_value(key))) {
    // do nothing
  } else if (cur_ser_val->is_null()) {
    // expired data, do nothing
  } else if (OB_FAIL(ObPLPackageState::is_oversize_value(*cur_ser_val, is_oversize_value))) {
    LOG_WARN("fail to check value oversize", K(ret));
  } else if (is_oversize_value) {
    value = *cur_ser_val;
  } else if (OB_FAIL(value_map.create(4, ObModIds::OB_PL_TEMP, ObModIds::OB_HASH_NODE, MTL_ID()))) {
    LOG_WARN("fail to create hash map", K(ret));
  } else if (OB_FAIL(decode_pkg_var_value(*cur_ser_val, state_version, value_map))) {
    LOG_WARN("fail to decode pkg var value", K(ret));
  } else {
    // add_changed_package_info intf has check package state valid, do not check again
  }
  if (!is_oversize_value) {
    // collect new modify package var and remove exists package var info from user var value
    for (int64_t i = 0; OB_SUCC(ret) && i < vars_.count(); ++i) {
      ObPackageVarEncodeInfo pkg_enc_info;
      ObString old_key;
      const ObObj *old_value = nullptr;
      if (changed_vars_.has_member(i)
          && vars_.at(i).get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
        pkg_enc_info.var_idx_ = i;
        if (OB_FAIL(make_pkg_var_kv_value(pl_ctx, resolve_ctx, vars_.at(i), i, pkg_enc_info.encode_value_))) {
          LOG_WARN("fail to make pkg var value", K(ret));
        } else if (OB_FAIL(pkg_enc_info.construct())) {
          LOG_WARN("fail to construct pkg enc info", K(ret));
        }

        if (FAILEDx(serialize_values.push_back(pkg_enc_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (value_map.created() && OB_FAIL(value_map.erase_refactored(i))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to erase value from hashmap", K(ret));
          }
        }

        // for mark old user var invalid which encode with old rule
        if (FAILEDx(make_pkg_var_kv_key(*pl_ctx.allocator_, i, VARIABLE, old_key))) {
          LOG_WARN("make package var name failed", K(ret), K(package_id_), K(i));
        } else if (OB_ISNULL(old_value = pl_ctx.exec_ctx_->get_my_session()->get_user_variable_value(old_key))) {
          // do nothing
        } else if (OB_FAIL(old_keys.push_back(old_key))) {
          LOG_WARN("fail to push back old key", K(ret));
        }
      }
    }

    // obtain total serialize size
    int64_t total_serialize_len = state_version_.get_version_serialize_size(); // header size
    for (int64_t i = 0; OB_SUCC(ret) && i < serialize_values.count(); ++i) {
      int64_t size = 0;
      OZ (serialize_values.at(i).get_serialize_size(size));
      OX (total_serialize_len += size);
    }
    for (hash::ObHashMap<int64_t, ObPackageVarEncodeInfo>::iterator it = value_map.begin();
          OB_SUCC(ret) && it != value_map.end(); ++it) {
      int64_t size = 0;
      OZ (it->second.get_serialize_size(size));
      OX (total_serialize_len += size);
    }

    if (OB_SUCC(ret)) {
      int64_t max_serialize_size = 0;
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (OB_LIKELY(tenant_config.is_valid())) {
        max_serialize_size = tenant_config->package_state_sync_max_size;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid tenant_config", K(ret));
      }
      if (total_serialize_len > max_serialize_size) {
        OZ (get_oversize_value(value));
      } else {
        char *serialize_buff = nullptr;
        int64_t serialize_pos = 0;
        if (OB_ISNULL(serialize_buff = static_cast<char*>(pl_ctx.allocator_->alloc(total_serialize_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocator memory for serialize pkg var buffer!",
                  K(ret), K(total_serialize_len));
        } else {
          // encode header
          OZ (state_version_.encode(serialize_buff, total_serialize_len, serialize_pos));
          //encode package var stage1
          for (int64_t i = 0; OB_SUCC(ret) && i < serialize_values.count(); ++i) {
            OZ (serialize_values.at(i).encode(serialize_buff, total_serialize_len, serialize_pos));
          }
          // encode package var stage2
          if (!value_map.empty()) {
            for (hash::ObHashMap<int64_t, ObPackageVarEncodeInfo>::iterator it = value_map.begin();
                  OB_SUCC(ret) && it != value_map.end(); ++it) {
              OZ (it->second.encode(serialize_buff, total_serialize_len, serialize_pos));
            }
          }
          CK (serialize_pos <= total_serialize_len);
          OX (value.set_hex_string(ObString(serialize_pos, serialize_buff)));
        }
      }
    }
  }
  if (value_map.created()) {
    int tmp_ret = value_map.destroy();
    ret = OB_SUCCESS != ret ? ret : tmp_ret;
  }

  return ret;
}

int ObPLPackageState::decode_pkg_var_value(const common::ObObj &serialize_value,
                                            ObPackageStateVersion &state_version,
                                            hash::ObHashMap<int64_t, ObPackageVarEncodeInfo> &value_map)
{
  int ret = OB_SUCCESS;
  if (serialize_value.is_hex_string()) {
    int64_t pos = 0;
    const char *src = serialize_value.get_hex_string().ptr();
    int64_t src_len = serialize_value.get_hex_string().length();

    OZ (state_version.decode(src, src_len, pos));
    while (OB_SUCC(ret) && pos < src_len) {
      ObPackageVarEncodeInfo pkg_enc_info;
      OZ (pkg_enc_info.decode(src, src_len, pos));
      OZ (value_map.set_refactored(pkg_enc_info.var_idx_, pkg_enc_info));
    }
    CK (pos == src_len);
    CK (state_version.is_valid());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value type", K(ret), K(serialize_value));
  }

  return ret;
}


int ObPLPackageState::make_pkg_var_kv_value(ObPLExecCtx &ctx, ObPLResolveCtx &resolve_ctx, ObObj &var_val, int64_t var_idx, ObObj &value)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *sql_session = ctx.exec_ctx_->get_my_session();
  if (OB_ISNULL(sql_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql session is null.", K(ret));
  } else {
    const ObPLVar *var = NULL;
    OZ (sql_session->get_pl_engine()
        ->get_package_manager().get_package_var(resolve_ctx, package_id_, var_idx, var));
    CK (OB_NOT_NULL(var));
    if (OB_FAIL(ret)) {
    } else if (var->get_type().is_cursor_type()) {
      ObPLCursorInfo *cursor = reinterpret_cast<ObPLCursorInfo *>(var_val.get_ext());
      // package cursor sync, we only sync open status and close status.
      // when remote server got open status, set cursor is sync status,
      // and user can not use cursor when cursor is synced.
      // when remote server got close status, set cursor is normal status,
      // and user can use cursor normally.
      if (OB_ISNULL(cursor) || !cursor->isopen()) {
        OX (value.set_bool(false));
      } else {
        OX (value.set_bool(true));
      }
    } else if (var->get_type().is_opaque_type()) {
      value.set_null();
    } else {
      OZ (var->get_type().serialize(resolve_ctx, var_val, value));
    }
  }
  return ret;
}

int ObPLPackageState::convert_info_to_string_kv(
  ObPLExecCtx &pl_ctx, ObPLResolveCtx &resolve_ctx, int64_t var_idx, PackageVarType var_type, ObString &key, ObObj &value)
{
  int ret = OB_SUCCESS;
  if (var_idx < 0 || var_idx >= vars_.count() || INVALID == var_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(var_idx), K(var_type), K(ret));
  } else if (OB_FAIL(make_pkg_var_kv_key(*pl_ctx.allocator_, var_idx, var_type, key))) {
    LOG_WARN("make package var kv key failed", K(var_idx), K(var_type), K(ret));
  } else if (OB_FAIL(make_pkg_var_kv_value(pl_ctx, resolve_ctx, vars_.at(var_idx), var_idx, value))) {
    LOG_WARN("make package var kv value failed", K(var_idx), K(var_type), K(ret));
  } else {
    LOG_DEBUG("convert pacakge var info to string kv",
              K(package_id_), K(var_idx), K(key), K(value), K(var_type));
  }
  return ret;
}

int ObPLPackageState::encode_pkg_var_key(ObIAllocator &alloc, ObString &key)
{
  int ret = OB_SUCCESS;

  ObPackageVarSetName key_name;
  key_name.package_id_ = package_id_;
  key_name.state_version_ = state_version_;
  if (OB_FAIL(key_name.encode_key(alloc, key))) {
    LOG_WARN("package var name encode failed", K(ret));
  }
  return ret;
}

int ObPLPackageState::encode_info_to_string_kvs(ObPLExecCtx &pl_ctx,
                                                ObPLResolveCtx &resolve_ctx,
                                                common::ObString &key,
                                                common::ObObj &value,
                                                ObIArray<ObString> &old_keys)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(encode_pkg_var_key(*pl_ctx.allocator_, key))) {
    LOG_WARN("package var key encode failed", K(ret));
  } else if (OB_FAIL(encode_pkg_var_value(pl_ctx, resolve_ctx, key, value, old_keys))) {
    LOG_WARN("package var value encode failed", K(ret));
  } else {
    LOG_TRACE("encode_info_to_string_kvs", K(key), K(value), K(old_keys));
  }

  return ret;
}

int ObPLPackageState::need_use_new_sync_policy(int64_t tenant_id, bool &use_new)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  use_new = false;
  int64_t use_old_sync_policy = -EVENT_CALL(EventTable::EN_PL_PACKAGE_ENCODE_SWITCH);
  if (use_old_sync_policy > 0) {
    use_new = false;
  } else {
    OZ (GET_MIN_DATA_VERSION(tenant_id, data_version));
    if (OB_SUCC(ret) && data_version >= DATA_VERSION_4_3_5_1 &&
        GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_5_1) {
      use_new = true;
    }
  }
  return ret;
}

int ObPLPackageState::convert_changed_info_to_string_kvs(ObPLExecCtx &pl_ctx, ObIArray<ObString> &key, ObIArray<ObObj> &value)
{
  int ret = OB_SUCCESS;
  ObString key_str;
  ObObj value_obj;
  bool use_new = true;
  ObSEArray<ObString, 4> old_keys;
  const share::schema::ObPackageInfo *package_info = NULL;
  bool is_valid = false;
  const uint64_t tenant_id = get_tenant_id_by_object_id(package_id_);
  share::schema::ObSchemaGetterGuard schema_guard;
  ObPLPackageGuard package_guard(MTL_ID());

  if (OB_ISNULL(pl_ctx.exec_ctx_) || OB_ISNULL(pl_ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pointer", K(ret));
  } else if (OB_FAIL(package_guard.init())) {
    LOG_WARN("fail to init package guard", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_package_info(tenant_id, package_id_, package_info))) {
    LOG_WARN("fail to get package info", K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(package_info)) {
    ObPLResolveCtx resolve_ctx(pl_ctx.exec_ctx_->get_allocator(),
                              *pl_ctx.exec_ctx_->get_my_session(),
                              schema_guard,
                              package_guard,
                              *pl_ctx.exec_ctx_->get_sql_proxy(),
                              false);
    if (OB_FAIL(check_package_state_valid(*pl_ctx.exec_ctx_, resolve_ctx, is_valid))) {
      LOG_WARN("check package state failed", K(ret), KPC(this));
    } else if (!is_valid) {
      LOG_INFO("package state is invalid, ignore this package.", KPC(this));
      ObString key;
      if (OB_FAIL(encode_pkg_var_key(*pl_ctx.allocator_, key))) {
        LOG_WARN("fail to encode pkg var key", K(ret));
      } else if (OB_FAIL(disable_expired_user_variables(*pl_ctx.exec_ctx_->get_my_session(), key))) {
        LOG_WARN("fail to disable expired user var", K(ret));
      }
    } else if (OB_FAIL(need_use_new_sync_policy(MTL_ID(), use_new))) {
      LOG_WARN("fail to get sync policy", K(ret));
    } else if (use_new) {
      if (is_package_info_changed()) {
        if (OB_FAIL(encode_info_to_string_kvs(pl_ctx, resolve_ctx, key_str, value_obj, old_keys))) {
          LOG_WARN("fail to encode info", K(ret));
        } else if (OB_FAIL(key.push_back(key_str))) {
          LOG_WARN("fail to push key ", K(ret));
        } else if (OB_FAIL(value.push_back(value_obj))) {
          LOG_WARN("fail to push value ", K(ret));
        } else if (old_keys.count() > 0) {
          ObObj invalid_val;
          if (OB_FAIL(get_invalid_value(invalid_val))) {
            LOG_WARN("fail to get invalid value", K(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < old_keys.count(); ++i) {
            if (OB_FAIL(key.push_back(old_keys.at(i)))) {
              LOG_WARN("fail to push key", K(ret));
            } else if (OB_FAIL(value.push_back(invalid_val))) {
              LOG_WARN("fail to push value", K(ret));
            }
          }
        }
      }
    } else {
      for (int64_t i = 0; i < vars_.count() && OB_SUCCESS == ret; ++i) {
        if (changed_vars_.has_member(i)
            && vars_.at(i).get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
          key_str.reset();
          value_obj.reset();
          if (OB_FAIL(convert_info_to_string_kv(pl_ctx, resolve_ctx, i, VARIABLE, key_str, value_obj))) {
            LOG_WARN("fail to convert package variable to string kv", K(i), K(ret));
          } else if (OB_FAIL(key.push_back(key_str))) {
            LOG_WARN("fail to push key ", K(ret));
          } else if (OB_FAIL(value.push_back(value_obj))) {
            LOG_WARN("fail to push value ", K(ret));
          } else {
            LOG_DEBUG("convert changed info to strings kvs success!",
                      K(package_id_), K(i), K(key_str), K(value_obj));
          }
        }
      }
    }
  }
  return ret;
}

int ObPLPackageState::remove_user_variables_for_package_state(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  int64_t var_count = vars_.count();
  ObArenaAllocator allocator(ObModIds::OB_PL_TEMP);
  ObString key;
  for (int64_t var_idx = 0; OB_SUCC(ret) && var_idx < var_count; var_idx++) {
    // ignore error code, reset all variables
    key.reset();
    if (OB_FAIL(make_pkg_var_kv_key(allocator, var_idx, VARIABLE, key))) {
      LOG_WARN("make package var name failed", K(ret), K(package_id_), K(var_idx));
    } else if (session.user_variable_exists(key)) {
      if (OB_FAIL(session.remove_user_variable(key))) {
        LOG_WARN("fail to remove user var", K(ret), K(key), K(package_id_), K(var_idx));
      } else if (OB_FAIL(session.remove_changed_user_var(key))) {
        LOG_WARN("fail to remove change user var", K(ret), K(key), K(package_id_), K(var_idx));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObString new_key;
    if (OB_FAIL(encode_pkg_var_key(allocator, new_key))) {
      LOG_WARN("fail to encode pkg var key", K(ret));
    } else if (session.user_variable_exists(new_key)) {
      if (OB_FAIL(session.remove_user_variable(new_key))) {
        LOG_WARN("fail to remove user var", K(ret), K(new_key), K(package_id_));
      } else if (OB_FAIL(session.remove_changed_user_var(new_key))) {
        LOG_WARN("fail to remove change user var", K(ret), K(new_key), K(package_id_));
      }
    }
  }
  return ret;
}

int ObPLPackageState::check_version(const ObPackageStateVersion &state_version,
                                    const ObPackageStateVersion &cur_state_version,
                                    ObSchemaGetterGuard &schema_guard,
                                    const ObPLPackage &spec,
                                    const ObPLPackage *body,
                                    bool &match)
{
  int ret = OB_SUCCESS;
  match = true;

  if (cur_state_version == state_version) {
    match = true;
  } else if (cur_state_version.header_public_syn_count_ != state_version.header_public_syn_count_ ||
             cur_state_version.body_public_syn_count_ != state_version.body_public_syn_count_) {
    match = false;
  } else {
    if (cur_state_version.header_merge_version_ != state_version.header_merge_version_) {
      if(OB_FAIL(ObRoutinePersistentInfo::check_dep_schema(schema_guard,
                                                           spec.get_dependency_table(),
                                                           cur_state_version.header_merge_version_,
                                                           match))) {
        LOG_WARN("fail to check dep schema", K(ret), K(cur_state_version), K(state_version));
      }
    }
    if (OB_SUCC(ret) && match &&
        cur_state_version.body_merge_version_ != state_version.body_merge_version_ &&
        OB_NOT_NULL(body)) {
      if(OB_FAIL(ObRoutinePersistentInfo::check_dep_schema(schema_guard,
                                                           body->get_dependency_table(),
                                                           cur_state_version.body_merge_version_,
                                                           match))) {
        LOG_WARN("fail to check dep schema", K(ret), K(cur_state_version), K(state_version));
      }
    }
  }

  return ret;
}


int ObPLPackageState::check_package_state_valid(ObExecContext &exec_ctx, ObPLResolveCtx &resolve_ctx, bool &valid)
{
  int ret = OB_SUCCESS;
  valid = false;
  ObSQLSessionInfo *sql_session = exec_ctx.get_my_session();
  if (OB_ISNULL(sql_session) || OB_ISNULL(sql_session->get_pl_engine())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql session is null.", K(ret));
  } else if (OB_ISNULL(exec_ctx.get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql ctx or schema guard is null.", K(ret));
  } else {
    ObPLPackage* package_spec = nullptr;
    ObPLPackage* package_body = nullptr;
    if (OB_FAIL(sql_session->get_pl_engine()->get_package_manager().get_cached_package(resolve_ctx,
                                                                                        package_id_,
                                                                                        package_spec,
                                                                                        package_body))) {
      LOG_WARN("package not exist", K(ret), K(package_id_));
      ret = OB_SUCCESS;
    } else if (OB_ISNULL(package_spec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("package info is null", K(ret), K(package_id_));
    } else {
      ObPackageStateVersion package_version(common::OB_INVALID_VERSION, common::OB_INVALID_VERSION);
      package_version.package_version_ = package_spec->get_version();
      if (OB_NOT_NULL(package_body)) {
        package_version.package_body_version_ = package_body->get_version();
      }
      package_version.set_merge_version_and_public_syn_cnt(*package_spec, package_body);
      if (OB_FAIL(check_version(package_version,
                                state_version_,
                                resolve_ctx.schema_guard_,
                                *package_spec,
                                package_body,
                                valid))) {
        LOG_WARN("fail to check version", K(ret));
      }
    }
  }
  return ret;
}

int ObPLPackageState::disable_expired_user_variables(sql::ObSQLSessionInfo &session, const ObString &key)
{
  int ret = OB_SUCCESS;

  if (session.user_variable_exists(key)) {
    ObSessionVariable sess_var;
    sess_var.value_.set_null();
    sess_var.meta_ = sess_var.value_.get_meta();
    if (OB_FAIL(session.replace_user_variable(key, sess_var))) {
      LOG_WARN("fail to remove user var", K(ret), K(key));
    } else {
      LOG_TRACE("disable user var", K(key));
    }
  }

  return ret;
}

} // end namespace pl
} // end namespace oceanbase
