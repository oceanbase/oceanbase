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

#define USING_LOG_PREFIX SQL_PC
#include "ob_prepare_stmt_struct.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/plan_cache/ob_ps_sql_utils.h"
#include "sql/plan_cache/ob_ps_cache.h"

namespace oceanbase {
using namespace common;
namespace sql {
int ObPsSqlKey::deep_copy(const ObPsSqlKey& other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is invalid", K(ret));
  } else {
    db_id_ = other.db_id_;
    if (OB_FAIL(ObPsSqlUtils::deep_copy_str(*allocator_, other.get_ps_sql(), ps_sql_))) {
      LOG_WARN("deep copy str failed", K(other), K(ret));
    }
  }
  return ret;
}

ObPsSqlKey& ObPsSqlKey::operator=(const ObPsSqlKey& other)
{
  if (this != &other) {
    db_id_ = other.db_id_;
    ps_sql_ = other.ps_sql_;
  }
  return *this;
}

bool ObPsSqlKey::operator==(const ObPsSqlKey& other) const
{
  return db_id_ == other.db_id_ && ps_sql_.compare(other.get_ps_sql()) == 0;
}

int64_t ObPsSqlKey::hash() const
{
  int64_t hash_val = 0;
  hash_val = murmurhash(&db_id_, sizeof(uint64_t), hash_val);
  hash_val = ps_sql_.hash(hash_val);
  return hash_val;
}

ObPsStmtItem::ObPsStmtItem()
    : ref_count_(1),
      db_id_(OB_INVALID_ID),
      ps_sql_(),
      stmt_id_(OB_INVALID_STMT_ID),
      allocator_(NULL),
      external_allocator_(NULL)
{}

ObPsStmtItem::ObPsStmtItem(const ObPsStmtId ps_stmt_id)
    : ref_count_(1), db_id_(OB_INVALID_ID), ps_sql_(), stmt_id_(ps_stmt_id), allocator_(NULL), external_allocator_(NULL)
{}

ObPsStmtItem::ObPsStmtItem(ObIAllocator* inner_allocator, ObIAllocator* external_allocator)
    : ref_count_(1),
      db_id_(OB_INVALID_ID),
      ps_sql_(),
      stmt_id_(OB_INVALID_STMT_ID),
      allocator_(inner_allocator),
      external_allocator_(external_allocator)
{}

int ObPsStmtItem::deep_copy(const ObPsStmtItem& other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is invalid", K(ret));
  } else {
    stmt_id_ = other.stmt_id_;
    db_id_ = other.db_id_;
    // not copy refcount
    if (OB_FAIL(ObPsSqlUtils::deep_copy_str(*allocator_, other.get_ps_sql(), ps_sql_))) {
      LOG_WARN("deep copy str failed", K(other), K(ret));
    }
  }
  return ret;
}

ObPsStmtItem& ObPsStmtItem::operator=(const ObPsStmtItem& other)
{
  if (this != &other) {
    ref_count_ = other.ref_count_;
    db_id_ = other.get_db_id();
    ps_sql_ = other.get_ps_sql();
    stmt_id_ = other.get_ps_stmt_id();
  }
  return *this;
}

bool ObPsStmtItem::is_valid() const
{
  bool bret = false;
  if (!ps_sql_.empty() && OB_INVALID_STMT_ID != stmt_id_) {
    bret = true;
  }
  return bret;
}

int ObPsStmtItem::get_convert_size(int64_t& cv_size) const
{
  cv_size = sizeof(ObPsStmtItem);
  cv_size += ps_sql_.length() + 1;
  return OB_SUCCESS;
}

bool ObPsStmtItem::check_erase_inc_ref_count()
{
  bool need_erase = false;
  bool cas_ret = false;
  do {
    need_erase = false;
    int64_t cur_ref_cnt = ATOMIC_LOAD(&ref_count_);
    int64_t next_ref_cnt = cur_ref_cnt + 1;
    if (0 == cur_ref_cnt) {
      need_erase = true;
      break;
    }
    cas_ret = ATOMIC_BCAS(&ref_count_, cur_ref_cnt, next_ref_cnt);
    LOG_TRACE("ps item check erase inc ref count after cas", K(cur_ref_cnt), K(next_ref_cnt), K(cas_ret));
  } while (!cas_ret);

  LOG_TRACE("ps item inc ref count", K(*this), K(need_erase));

  return need_erase;
}

void ObPsStmtItem::dec_ref_count_check_erase()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("ps item dec ref count", K(*this));
  int64_t ref_count = ATOMIC_SAF(&ref_count_, 1);
  if (ref_count > 0) {
    LOG_TRACE("ps item dec ref count", K(ref_count));
  } else if (0 == ref_count) {
    LOG_INFO("free ps item", K(ref_count), K(*this));
    ObPsStmtItem* ps_item = this;
    ObIAllocator* allocator = NULL;
    if (OB_ISNULL(allocator = get_external_allocator())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(allocator), K(ret));
    } else {
      ps_item->~ObPsStmtItem();
      allocator->free(ps_item);
    }
  } else if (ref_count < 0) {
    BACKTRACE(ERROR, true, "ObPsStmtItem %p ref count < 0, ref_count = %ld", this, ref_count);
  }
}

int ObPsSqlMeta::add_param_field(const ObField& field)
{
  int ret = OB_SUCCESS;

  ObField tmp_field;
  if (OB_FAIL(tmp_field.full_deep_copy(field, allocator_))) {
    LOG_WARN("deep copy field failed", K(ret));
  } else if (OB_FAIL(param_fields_.push_back(tmp_field))) {
    LOG_WARN("push back field param failed", K(ret));
  } else {
    LOG_DEBUG("succ to push back param field", K(field));
  }

  return ret;
}

int ObPsSqlMeta::add_column_field(const ObField& field)
{
  int ret = OB_SUCCESS;

  ObField tmp_field;
  if (OB_FAIL(tmp_field.full_deep_copy(field, allocator_))) {
    LOG_WARN("deep copy field failed", K(ret));
  } else if (OB_FAIL(column_fields_.push_back(tmp_field))) {
    LOG_WARN("push back field param failed", K(ret));
  } else {
    LOG_DEBUG("succ to push back column field", K(field));
  }

  return ret;
}

int ObPsSqlMeta::reverse_fileds(int64_t param_size, int64_t column_size)
{
  int64_t ret = OB_SUCCESS;
  if (OB_FAIL(param_fields_.init(param_size))) {
    LOG_WARN("fail to init param fields", K(ret), K(param_size));
  } else if (OB_FAIL(column_fields_.init(column_size))) {
    LOG_WARN("fail to init column fields", K(ret), K(column_size));
  }

  return ret;
}

int ObPsSqlMeta::deep_copy(const ObPsSqlMeta& sql_meta)
{
  int ret = OB_SUCCESS;
  const int64_t column_size = sql_meta.get_column_size();
  const int64_t param_size = sql_meta.get_param_size();
  if (OB_FAIL(reverse_fileds(param_size, column_size))) {
    LOG_WARN("reserve_fix_size failed", K(column_size), K(param_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_size; ++i) {
      if (OB_FAIL(add_param_field(sql_meta.get_param_fields().at(i)))) {
        LOG_WARN("add column field failed", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_size; ++i) {
      if (OB_FAIL(add_column_field(sql_meta.get_column_fields().at(i)))) {
        LOG_WARN("add column field failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPsSqlMeta::get_convert_size(int64_t& cv_size) const
{
  int ret = OB_SUCCESS;
  cv_size = 0;
  int64_t convert_size = sizeof(ObPsSqlMeta);
  for (int i = 0; OB_SUCC(ret) && i < column_fields_.count(); ++i) {
    convert_size += column_fields_.at(i).get_convert_size();
  }
  for (int i = 0; OB_SUCC(ret) && i < param_fields_.count(); ++i) {
    convert_size += param_fields_.at(i).get_convert_size();
  }
  if (OB_SUCC(ret)) {
    cv_size = convert_size;
  }
  return ret;
}

ObPsStmtInfo::ObPsStmtInfo(ObIAllocator* inner_allocator)
    : stmt_type_(stmt::T_NONE),
      ps_stmt_checksum_(0),
      db_id_(OB_INVALID_ID),
      ps_sql_(),
      ps_sql_meta_(inner_allocator),
      ref_count_(1),
      question_mark_count_(0),
      can_direct_use_param_(false),
      has_complex_argument_(false),
      item_and_info_size_(0),
      last_closed_timestamp_(0),
      dep_objs_(NULL),
      dep_objs_cnt_(0),
      ps_item_(NULL),
      allocator_(inner_allocator),
      external_allocator_(NULL)

{}

ObPsStmtInfo::ObPsStmtInfo(ObIAllocator* inner_allocator, ObIAllocator* external_allocator)
    : stmt_type_(stmt::T_NONE),
      ps_stmt_checksum_(0),
      db_id_(OB_INVALID_ID),
      ps_sql_(),
      ps_sql_meta_(inner_allocator),
      ref_count_(1),
      question_mark_count_(0),
      can_direct_use_param_(false),
      has_complex_argument_(false),
      item_and_info_size_(0),
      last_closed_timestamp_(0),
      dep_objs_(NULL),
      dep_objs_cnt_(0),
      ps_item_(NULL),
      tenant_version_(OB_INVALID_VERSION),
      is_expired_(false),
      allocator_(inner_allocator),
      external_allocator_(external_allocator)
{}

bool ObPsStmtInfo::is_valid() const
{
  return !ps_sql_.empty();
}

int ObPsStmtInfo::deep_copy(const ObPsStmtInfo& other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is invalid", K(ret));
  } else {
    stmt_type_ = other.stmt_type_;
    ps_stmt_checksum_ = other.ps_stmt_checksum_;
    db_id_ = other.db_id_;
    question_mark_count_ = other.question_mark_count_;
    can_direct_use_param_ = other.can_direct_use_param();
    has_complex_argument_ = other.has_complex_argument();
    item_and_info_size_ = other.item_and_info_size_;
    ps_item_ = other.ps_item_;
    tenant_version_ = other.tenant_version_;
    is_expired_ = other.is_expired_;
    if (other.get_dep_objs_cnt() > 0) {
      dep_objs_cnt_ = other.get_dep_objs_cnt();
      if (NULL == (dep_objs_ = reinterpret_cast<ObSchemaObjVersion*>(
                       allocator_->alloc(dep_objs_cnt_ * sizeof(ObSchemaObjVersion))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(dep_objs_cnt_));
      } else {
        MEMCPY(dep_objs_, other.get_dep_objs(), dep_objs_cnt_ * sizeof(ObSchemaObjVersion));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObPsSqlUtils::deep_copy_str(*allocator_, other.get_ps_sql(), ps_sql_))) {
      LOG_WARN("deep copy str failed", K(other), K(ret));
    } else if (OB_FAIL(ps_sql_meta_.deep_copy(other.get_ps_sql_meta()))) {
      LOG_WARN("deep copy ps sql meta faield", K(ret));
    }
  }
  return ret;
}

int ObPsStmtInfo::add_column_field(const ObField& field)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ps_sql_meta_.add_column_field(field))) {
    LOG_WARN("add column field failed", K(ret));
  }
  return ret;
}

int ObPsStmtInfo::add_param_field(const ObField& param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ps_sql_meta_.add_param_field(param))) {
    LOG_WARN("add param field failed", K(ret));
  }
  return ret;
}

int ObPsStmtInfo::get_convert_size(int64_t& cv_size) const
{
  int ret = OB_SUCCESS;
  cv_size = 0;
  int64_t convert_size = sizeof(ObPsStmtInfo);
  convert_size += ps_sql_.length() + 1;
  int64_t meta_convert_size = 0;
  if (OB_FAIL(ps_sql_meta_.get_convert_size(meta_convert_size))) {
    LOG_WARN("get sql meta convert size failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    convert_size += meta_convert_size;
    convert_size += dep_objs_cnt_ * sizeof(share::schema::ObSchemaObjVersion);
    cv_size = convert_size;
  }

  return ret;
}

bool ObPsStmtInfo::check_erase_inc_ref_count()
{
  bool need_erase = false;
  bool cas_ret = false;
  do {
    need_erase = false;
    int64_t cur_ref_cnt = ATOMIC_LOAD(&ref_count_);
    int64_t next_ref_cnt = cur_ref_cnt + 1;
    if (0 == cur_ref_cnt) {
      need_erase = true;
      break;
    }
    cas_ret = ATOMIC_BCAS(&ref_count_, cur_ref_cnt, next_ref_cnt);
    LOG_TRACE("ps info check erase inc ref count after cas", K(cur_ref_cnt), K(next_ref_cnt), K(cas_ret));
  } while (!cas_ret);

  LOG_TRACE("ps info inc ref count", K(*this), K(need_erase));

  return need_erase;
}

bool ObPsStmtInfo::dec_ref_count_check_erase()
{
  bool need_erase = false;
  LOG_TRACE("ps info dec ref count", K(*this));
  int64_t ref_count = ATOMIC_SAF(&ref_count_, 1);
  if (ref_count > 0) {
    if (ref_count == 1) {
      last_closed_timestamp_ = common::ObTimeUtility::current_time();
    }
    LOG_TRACE("ps info dec ref count", K(ref_count), K(*this));
  } else if (0 == ref_count) {
    need_erase = true;
    LOG_INFO("free ps info", K(ref_count), K(*this), K(need_erase));
  } else if (ref_count < 0) {
    BACKTRACE(ERROR, true, "ObPsStmtInfo %p ref count < 0, ref_count = %ld", this, ref_count);
  }
  return need_erase;
}

int64_t ObPsStmtInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  J_OBJ_START();
  J_KV(K_(db_id),
      K_(ps_sql),
      K_(ref_count),
      K_(can_direct_use_param),
      K_(question_mark_count),
      K_(last_closed_timestamp),
      K_(tenant_version));
  J_COMMA();
  J_NAME("columns");
  J_COLON();
  J_ARRAY_START();
  for (int64_t i = 0; OB_SUCC(ret) && i < ps_sql_meta_.get_column_fields().count(); ++i) {
    J_KV("column", ps_sql_meta_.get_column_fields().at(i));
  }
  J_ARRAY_END();
  J_COMMA();
  J_NAME("params");
  J_COLON();
  J_ARRAY_START();
  for (int64_t i = 0; OB_SUCC(ret) && i < ps_sql_meta_.get_param_fields().count(); ++i) {
    J_KV("params", ps_sql_meta_.get_param_fields().at(i));
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

ObPsStmtInfoGuard::~ObPsStmtInfoGuard()
{
  int ret = OB_SUCCESS;
  if (NULL != ps_cache_) {
    if (NULL != stmt_info_) {
      if (OB_FAIL(ps_cache_->deref_stmt_info(stmt_id_))) {
        LOG_WARN("deref stmt item faield", K(ret), K(*stmt_info_), K_(stmt_id));
      }
      LOG_TRACE("destroy PsStmtInfo Guard", K_(stmt_id));
    } else {
      LOG_WARN("stmt info is null", K(stmt_id_));
    }
  } else {
    LOG_WARN("ps_cache is null", K(stmt_id_), K(stmt_info_));
  }
}

int ObPsStmtInfoGuard::get_ps_sql(ObString& ps_sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt info is null", K(ret));
  } else {
    ps_sql = stmt_info_->get_ps_sql();
  }
  return ret;
}

int TypeInfo::deep_copy(common::ObIAllocator* allocator, const TypeInfo* other)
{
  int ret = OB_SUCCESS;
  if (this != other) {
    CK(OB_NOT_NULL(allocator));
    CK(OB_NOT_NULL(other));
    OZ(ob_write_string(*allocator, other->relation_name_, relation_name_));
    OZ(ob_write_string(*allocator, other->package_name_, package_name_));
    OZ(ob_write_string(*allocator, other->type_name_, type_name_));
    OX(elem_type_ = other->elem_type_);
    OX(is_elem_type_ = other->is_elem_type_);
    OX(is_basic_type_ = other->is_basic_type_);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
