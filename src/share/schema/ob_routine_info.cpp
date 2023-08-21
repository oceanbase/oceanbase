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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_se_array_iterator.h"
#include "ob_routine_info.h"
#include "ob_udt_info.h"
#include "ob_schema_getter_guard.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
ObRoutineParam::ObRoutineParam()
{
  reset();
}

ObRoutineParam::ObRoutineParam(ObIAllocator *allocator) : ObSchema(allocator)
{
  reset();
}

ObRoutineParam::ObRoutineParam(const ObRoutineParam &src_schema) : ObSchema()
{
  reset();
  *this = src_schema;
}

ObRoutineParam::~ObRoutineParam()
{
}

ObRoutineParam &ObRoutineParam::operator =(const ObRoutineParam &src_schema)
{
  if (this != &src_schema) {
    reset();
    int &ret = error_ret_;
    OX (tenant_id_ = src_schema.get_tenant_id());
    OX (routine_id_ = src_schema.get_routine_id());
    OX (sequence_ = src_schema.get_sequence());
    OX (subprogram_id_ = src_schema.get_subprogram_id());
    OX (param_position_ = src_schema.get_param_position());
    OX (param_level_ = src_schema.get_param_level());
    OZ (deep_copy_str(src_schema.get_param_name(), param_name_));
    OX (schema_version_ = src_schema.get_schema_version());
    OX (param_type_ = src_schema.get_param_type());
    OX (flag_ = src_schema.flag_);
    OZ (deep_copy_str(src_schema.get_default_value(), default_value_));
    OX (type_owner_ = src_schema.type_owner_);
    OZ (deep_copy_str(src_schema.get_type_name(), type_name_));
    OZ (deep_copy_str(src_schema.get_type_subname(), type_subname_));
    OZ (set_extended_type_info(src_schema.get_extended_type_info()));
    error_ret_ = ret;
  }
  return *this;
}

int ObRoutineParam::set_extended_type_info(const ObIArray<common::ObString> &extended_type_info)
{
  return deep_copy_string_array(extended_type_info, extended_type_info_);
}

int ObRoutineParam::serialize_extended_type_info(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialize_string_array(buf, buf_len, pos, extended_type_info_))) {
    LOG_WARN("fail to serialize extended type info", K(ret));
  }
  return ret;
}

int ObRoutineParam::deserialize_extended_type_info(const char *buf,
                                                      const int64_t data_len,
                                                      int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deserialize_string_array(buf, data_len, pos, extended_type_info_))) {
    LOG_WARN("fail to deserialize extended type info", K(ret));
  }
  return ret;
}

bool ObRoutineParam::is_user_field_valid() const
{
  bool bret = false;
  if (ObSchema::is_valid()) {
    bret = (OB_INVALID_INDEX != param_position_)
        && (OB_INVALID_INDEX != param_level_)
        && ((param_position_ == RET_PARAM_POSITION && param_name_.empty())
            || (param_position_ != RET_PARAM_POSITION && !param_name_.empty()))
        && (ObMaxType != param_type_.get_obj_type());
  } else {}
  return bret;
}

bool ObRoutineParam::is_valid() const
{
  bool bret = false;
  if (ObSchema::is_valid()) {
    if (is_user_field_valid()) {
      bret = (OB_INVALID_ID != tenant_id_)
          && (OB_INVALID_ID != routine_id_)
          && (OB_INVALID_INDEX != sequence_)
          && (OB_INVALID_INDEX != subprogram_id_)
          && (OB_INVALID_VERSION != schema_version_);
    } else {}
  } else {}
  return bret;
}

bool ObRoutineParam::is_same(const ObRoutineParam &other) const
{
  bool bret = true;
  // 这个比较不正确，因为param的routine id可能还不存在，这个id是schema操作更新的，只有resolve的话这个id不正确。
  // udt对param进行verify的时候进行is_same比较，这个时候body的routine param还没有schema操作，is_valid无效
  // if (!is_valid() || !other.is_valid()) {
  //   LOG_WARN("param is no valid", K(is_valid()), K(*this), K(other.is_valid()));
  //   bret = false;
  // }
  if (bret && 0 != get_name().case_compare(other.get_name())) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "param name is not same", K(get_name()), K(other.get_name()));
    bret = false;
  }
  if (bret && get_flag() != other.get_flag()) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "param flag is not same", K(get_flag()), K(other.get_flag()));
    bret = false;
  }
  if (bret && get_param_type().get_obj_type() != other.get_param_type().get_obj_type()) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "param type is not same", K(get_param_type().get_obj_type()),
                                       K(other.get_param_type().get_obj_type()));
    bret = false;
  }
  if (bret && is_complex_type() && get_type_owner() != other.get_type_owner()) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "param type is complex and not same", K(is_complex_type()),
                                                   K(get_type_owner()),
                                                   K(other.get_type_owner()));
    bret = false;
  }
  return bret;
}

void ObRoutineParam::reset()
{
  tenant_id_ = OB_INVALID_ID;
  routine_id_ = OB_INVALID_ID;
  sequence_ = OB_INVALID_INDEX;
  subprogram_id_ = OB_INVALID_INDEX;
  param_position_ = OB_INVALID_INDEX;
  param_level_ = OB_INVALID_INDEX;
  reset_string(param_name_);
  schema_version_ = OB_INVALID_VERSION;
  param_type_.reset();
  flag_ = SP_PARAM_NO_FLAG;
  default_value_.reset();
  type_owner_ = OB_INVALID_ID;
  reset_string(type_name_);
  reset_string(type_subname_);
  reset_string_array(extended_type_info_);
  ObSchema::reset();
}

int64_t ObRoutineParam::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObRoutineParam));
  len += param_name_.length() + 1;
  len += default_value_.length() + 1;
  len += type_name_.length() + 1;
  len += type_subname_.length() + 1;
  len += extended_type_info_.count() * static_cast<int64_t>(sizeof(ObString));
  for (int64_t i = 0; i < extended_type_info_.count(); ++i) {
    len += extended_type_info_.at(i).length() + 1;
  }
  return len;
}

OB_DEF_SERIALIZE(ObRoutineParam)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              routine_id_,
              sequence_,
              subprogram_id_,
              param_position_,
              param_level_,
              param_name_,
              param_type_,
              flag_,
              default_value_,
              type_owner_,
              type_name_,
              type_subname_);
  if (OB_SUCC(ret) && OB_FAIL(serialize_string_array(buf, buf_len, pos, extended_type_info_))) {
    LOG_WARN("serialize_string_array failed", K(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObRoutineParam)
{
  int ret = OB_SUCCESS;
  reset();
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              routine_id_,
              sequence_,
              subprogram_id_,
              param_position_,
              param_level_,
              param_name_,
              param_type_,
              flag_,
              default_value_,
              type_owner_,
              type_name_,
              type_subname_);
  if (OB_SUCC(ret) && OB_FAIL(deserialize_string_array(buf, data_len, pos, extended_type_info_))) {
    LOG_WARN("deserialize_string_array failed", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRoutineParam)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              routine_id_,
              sequence_,
              subprogram_id_,
              param_position_,
              param_level_,
              param_name_,
              param_type_,
              flag_,
              default_value_,
              type_owner_,
              type_name_,
              type_subname_);
  len += get_string_array_serialize_size(extended_type_info_);
  return len;
}

ObRoutineInfo::ObRoutineInfo()
{
  reset();
}

ObRoutineInfo::ObRoutineInfo(ObIAllocator *allocator)
  : ObSchema(allocator),
    routine_params_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator))
{
  reset();
}

ObRoutineInfo::ObRoutineInfo(const ObRoutineInfo &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObRoutineInfo::~ObRoutineInfo()
{
}

ObRoutineInfo &ObRoutineInfo::operator =(const ObRoutineInfo &src_schema)
{
  if (this != &src_schema) {
    reset();
    int &ret = error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    database_id_ = src_schema.database_id_;
    package_id_ =  src_schema.package_id_;
    owner_id_ = src_schema.owner_id_;
    routine_id_ = src_schema.routine_id_;
    overload_ = src_schema.overload_;
    subprogram_id_ = src_schema.subprogram_id_;
    schema_version_ = src_schema.schema_version_;
    routine_type_ = src_schema.routine_type_;
    flag_ = src_schema.flag_;
    comp_flag_ = src_schema.comp_flag_;
    type_id_ = src_schema.type_id_;
    tg_timing_event_ = src_schema.tg_timing_event_;
    dblink_id_ = src_schema.dblink_id_;
    if (OB_FAIL(deep_copy_str(src_schema.routine_name_, routine_name_))) {
      LOG_WARN("deep copy name failed", K(ret), K_(src_schema.routine_name));
    } else if (OB_FAIL(deep_copy_str(src_schema.priv_user_, priv_user_))) {
      LOG_WARN("deep copy priv user failed", K(ret), K_(src_schema.priv_user));
    } else if (OB_FAIL(deep_copy_str(src_schema.exec_env_, exec_env_))) {
      LOG_WARN("deep copy exec env failed", K(ret), K_(src_schema.exec_env));
    } else if (OB_FAIL(deep_copy_str(src_schema.routine_body_, routine_body_))) {
      LOG_WARN("deep copy routine body failed", K(ret), K_(src_schema.routine_body));
    } else if (OB_FAIL(deep_copy_str(src_schema.comment_, comment_))) {
      LOG_WARN("deep copy comment failed", K(ret), K_(src_schema.comment));
    } else if (OB_FAIL(deep_copy_str(src_schema.route_sql_, route_sql_))) {
      LOG_WARN("deep copy route sql failed", K(ret), K_(src_schema.route_sql));
    } else if (OB_FAIL(routine_params_.reserve(src_schema.routine_params_.count()))) {
      LOG_WARN("failed to reserve routine params size", K(ret), K(src_schema));
    } else if (OB_FAIL(deep_copy_str(src_schema.dblink_db_name_, dblink_db_name_))) {
      LOG_WARN("deep copy dblink database name failed", K(ret), K(src_schema.dblink_db_name_));
    } else if (OB_FAIL(deep_copy_str(src_schema.dblink_pkg_name_, dblink_pkg_name_))) {
      LOG_WARN("deep copy dblink pkg name failed", K(ret), K(src_schema.dblink_pkg_name_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src_schema.routine_params_.count(); ++i) {
      if (OB_ISNULL(src_schema.routine_params_.at(i))) {
        LOG_WARN("routine param is null", K(i));
      } else if (OB_FAIL(add_routine_param(*src_schema.routine_params_.at(i)))) {
        LOG_WARN("add routine param to routine info failed", K(ret), K(i));
      }
    }
    error_ret_ = ret;
  }
  return *this;
}

int ObRoutineInfo::assign(const ObRoutineInfo &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

bool ObRoutineInfo::is_user_field_valid() const
{
  bool bret = false;
  if (ObSchema::is_valid()) {
    bret = (OB_INVALID_ID != tenant_id_)
   //   && (OB_INVALID_ID != owner_id_)
        && (!routine_name_.empty())
        && (OB_INVALID_INDEX != overload_)
        && (OB_INVALID_INDEX != subprogram_id_)
        && (INVALID_ROUTINE_TYPE != routine_type_);
  }
  return bret;
}

bool ObRoutineInfo::is_valid() const
{
  bool bret = false;
  if (ObSchema::is_valid()) {
    if (is_user_field_valid()) {
      bret = (OB_INVALID_ID != database_id_)
          && (OB_INVALID_ID != routine_id_)
          && (OB_INVALID_VERSION != schema_version_);
    } else {}
  } else {}
  return bret;
}

void ObRoutineInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  package_id_ = OB_INVALID_ID;
  owner_id_ = OB_INVALID_ID;
  routine_id_ = OB_INVALID_ID;
  type_id_ = OB_INVALID_ID;
  reset_string(routine_name_);
  overload_ = OB_INVALID_INDEX;
  subprogram_id_ = OB_INVALID_INDEX;
  schema_version_ = OB_INVALID_VERSION;
  routine_type_ = INVALID_ROUTINE_TYPE;
  flag_ = 0;
  reset_string(priv_user_);
  comp_flag_ = 0;
  reset_string(exec_env_);
  reset_string(routine_body_);
  reset_string(comment_);
  reset_string(route_sql_);
  routine_params_.reset();
  ObSchema::reset();
  tg_timing_event_ = TgTimingEvent::TG_TIMING_EVENT_INVALID;
  dblink_id_ = OB_INVALID_ID;
  reset_string(dblink_db_name_);
  reset_string(dblink_pkg_name_);
  // routine_params_.set_allocator(get_allocator());
  // routine_params_.set_capacity(OB_MAX_PROC_PARAM_COUNT+1); //one more ret type param for function
}

int64_t ObRoutineInfo::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObRoutineInfo));
  len += routine_name_.length() + 1;
  len += priv_user_.length() + 1;
  len += exec_env_.length() + 1;
  len += routine_body_.length() + 1;
  len += comment_.length() + 1;
  len += route_sql_.length() + 1;
  len += (routine_params_.count()+1) * sizeof(ObRoutineParam *);
  len += routine_params_.get_data_size();
  len += dblink_db_name_.length() + 1;
  len += dblink_pkg_name_.length() + 1;
  ARRAY_FOREACH_NORET(routine_params_, i) {
    if (routine_params_.at(i) != NULL) {
      len += routine_params_.at(i)->get_convert_size();
    }
  }
  return len;
}

int ObRoutineInfo::add_routine_param(const ObRoutineParam &routine_param)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (!is_user_field_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("routine basic info invalid", K(ret));
  } else if (OB_ISNULL(ptr = alloc(sizeof(ObRoutineParam)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocated routine param memory failed", K(ret));
  } else {
    ObRoutineParam *local_param = NULL;
    local_param = new (ptr) ObRoutineParam(get_allocator());
    *local_param = routine_param;
    if (OB_FAIL(routine_params_.push_back(local_param))) {
      LOG_WARN("push local param failed", K(ret));
    } else if (local_param->is_self_param()) {
      ObRoutineParam *first_param = is_procedure() ? routine_params_.at(0) : 1 < routine_params_.count() ? routine_params_.at(1) : NULL;
      bool more_than_one = is_procedure() ? routine_params_.count() > 1 : routine_params_.count() > 2;
      if (OB_NOT_NULL(first_param) && more_than_one && first_param->is_self_param()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected self param, duplicate", K(ret));
      } else if (routine_params_.at(0)->is_ret_param() && 2 < routine_params_.count()) {
        std::rotate(routine_params_.begin() + 1,
                  routine_params_.begin() + routine_params_.count() - 1,
                  routine_params_.end());
      } else if (!routine_params_.at(0)->is_ret_param() && 1 < routine_params_.count()) {
        std::rotate(routine_params_.begin(),
                  routine_params_.begin() + routine_params_.count() - 1,
                  routine_params_.end());
      }
    } else {
    }
  }
  return ret;
}

int ObRoutineInfo::get_routine_param(int64_t idx, ObRoutineParam*& param) const
{
  int ret = OB_SUCCESS;
  ObIRoutineParam *iparam = NULL;
  OZ (get_routine_param(idx, iparam));
  CK (OB_NOT_NULL(iparam));
  CK (OB_NOT_NULL(param = static_cast<ObRoutineParam*>(iparam)));
  return ret;
}

int ObRoutineInfo::get_routine_param(int64_t idx, ObIRoutineParam*& param) const
{
  int ret = OB_SUCCESS;
  idx = idx + 1; // idx start with 0, but position start with 1
  for (int64_t i = 0; i < routine_params_.count(); ++i) {
    if (idx == routine_params_.at(i)->get_param_position()) {
      param = routine_params_.at(i);
    }
  }
  if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx is invalid", K(ret), K(idx), KPC(this));
  }
  return ret;
}

const ObIRoutineParam *ObRoutineInfo::get_ret_info() const
{
  const ObIRoutineParam *ret_info = NULL;
  for (int64_t i = 0; i < routine_params_.count(); ++i) {
    if (RET_PARAM_POSITION == routine_params_.at(i)->get_param_position()) {
      ret_info = routine_params_.at(i);
    }
  }
  return ret_info;
}

const ObDataType *ObRoutineInfo::get_ret_type() const
{
  const ObDataType *ret_type = NULL;
  const ObRoutineParam *ret_info = static_cast<const ObRoutineParam*>(get_ret_info());
  if (OB_NOT_NULL(ret_info)) {
    ret_type = &(ret_info->get_param_type());
  }
  return ret_type;
}

const ObIArray<ObString>* ObRoutineInfo::get_ret_type_info() const
{
  const ObIArray<ObString> *type_info = NULL;
  const ObRoutineParam *ret_info = static_cast<const ObRoutineParam*>(get_ret_info());
  if (OB_NOT_NULL(ret_info)) {
    type_info = &(ret_info->get_extended_type_info());
  }
  return type_info;
}

int ObRoutineInfo::find_param_by_name(const ObString &name, int64_t &position) const
{
  int ret = OB_SUCCESS;
  position = -1;
  if (name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalue param name", K(ret), K(name));
  } else {
    for (int64_t i = 0; i < get_routine_params().count(); ++i) {
      if (get_routine_params().at(i)->is_ret_param()) {
        continue;
      } else if (0 == get_routine_params().at(i)->get_param_name().case_compare(name)) {
        position = get_routine_params().at(i)->get_param_position() - 1;
      }
    }
    if (-1 == position) {
      ret = OB_ERR_SP_UNDECLARED_VAR;
      LOG_WARN("param name is not found in param list", K(ret), K(name), KPC(this), K(position));
    }
  }
  return ret;
}

int64_t ObRoutineInfo::get_out_param_count() const
{
  int64_t count = 0;
  for (uint64_t i = 0; i < get_routine_params().count(); i++) {
    if (get_routine_params().at(i)->is_out_sp_param()
        || get_routine_params().at(i)->is_inout_sp_param()) {
      count++;
    }
  }
  return count;
}

OB_DEF_SERIALIZE(ObRoutineInfo)
{
  int ret = OB_SUCCESS;
  int64_t param_cnt = routine_params_.count();
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              database_id_,
              package_id_,
              owner_id_,
              routine_id_,
              routine_name_,
              overload_,
              subprogram_id_,
              routine_type_,
              flag_,
              priv_user_,
              comp_flag_,
              exec_env_,
              routine_body_,
              comment_,
              route_sql_,
              type_id_,
              param_cnt,
              tg_timing_event_,
              dblink_db_name_,
              dblink_pkg_name_);
  for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
    if (OB_ISNULL(routine_params_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("routine_param is null", K(i));
    } else if (OB_FAIL(routine_params_.at(i)->serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize routine param failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObRoutineInfo)
{
  int ret = OB_SUCCESS;
  int64_t param_cnt = 0;
  ObRoutineParam routine_param;
  reset();
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              database_id_,
              package_id_,
              owner_id_,
              routine_id_,
              routine_name_,
              overload_,
              subprogram_id_,
              routine_type_,
              flag_,
              priv_user_,
              comp_flag_,
              exec_env_,
              routine_body_,
              comment_,
              route_sql_,
              type_id_,
              param_cnt,
              tg_timing_event_,
              dblink_db_name_,
              dblink_pkg_name_);
  for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
    routine_param.reset();
    if (OB_FAIL(routine_param.deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize routine param failed", K(ret));
    } else if (OB_FAIL(add_routine_param(routine_param))) {
      LOG_WARN("add routine param failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRoutineInfo)
{
  int64_t len = 0;
  int64_t param_cnt = routine_params_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              database_id_,
              package_id_,
              owner_id_,
              routine_id_,
              routine_name_,
              overload_,
              subprogram_id_,
              routine_type_,
              flag_,
              priv_user_,
              comp_flag_,
              exec_env_,
              routine_body_,
              comment_,
              route_sql_,
              type_id_,
              param_cnt,
              tg_timing_event_,
              dblink_db_name_,
              dblink_pkg_name_);
  for (int64_t i = 0; i < param_cnt; ++i) {
    if (routine_params_.at(i) != NULL) {
      len += routine_params_.at(i)->get_serialize_size();
    }
  }
  return len;
}
}  // namespace schema
}  // namespace share
}  // namespace oceanbase
