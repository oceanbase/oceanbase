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
 * This file is for implement of func multimode expr helper
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "ob_expr_multi_mode_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

uint64_t ObMultiModeExprHelper::get_tenant_id(ObSQLSessionInfo *session)
{
  uint64_t tenant_id = 0;
  if (OB_ISNULL(session)) {
    // If the session is not obtained, 500 tenant memory will be temporarily used, 
    // but it will not affect subsequent execution.
    tenant_id = OB_SERVER_TENANT_ID;
    int ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to get session, set tenant_id 500.", K(ret), K(lbt()));
  } else if (session->get_ddl_info().is_ddl_check_default_value()) {
    tenant_id = OB_SERVER_TENANT_ID;
  } else {
    tenant_id = session->get_effective_tenant_id();
  }
  return tenant_id;
}

MultimodeAlloctor::MultimodeAlloctor(ObArenaAllocator &arena, uint64_t type, int64_t tenant_id, int &ret, const char *func_name/* = ""*/)
    : arena_(arena),
      baseline_size_(0),
      type_(type),
      mem_threshold_flag_(0),
      check_level_(0),
      func_name_(func_name),
      children_used_(used()),
      expect_threshold_(0),
      ret_(ret),
      ext_used_(0)
{
  uint64_t alloc_tenant = arena.get_arena().get_tenant_id();
  if (alloc_tenant != tenant_id) {
    INIT_SUCC(ret);
    LOG_WARN("[Multi-mode ALARM] different tenants", K(ret), K(alloc_tenant), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(alloc_tenant));
    if (tenant_config.is_valid()) {
      check_level_ = tenant_config->_multimodel_memory_trace_level;
      if (check_level_ > 2) {
        check_level_ = 0;
      }
    }
  }
}

MultimodeAlloctor::MultimodeAlloctor(ObArenaAllocator &arena, uint64_t type, int &ret)
    : arena_(arena),
      baseline_size_(0),
      type_(type),
      mem_threshold_flag_(0),
      check_level_(0),
      func_name_(""),
      children_used_(used()),
      expect_threshold_(0),
      ret_(ret),
      ext_used_(0)
{
  uint64_t alloc_tenant = arena.get_arena().get_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(alloc_tenant));
  if (tenant_config.is_valid()) {
    check_level_ = tenant_config->_multimodel_memory_trace_level;
    if (check_level_ > 2) {
      check_level_ = 0;
    }
  }
}

MultimodeAlloctor::~MultimodeAlloctor() 
{
  if (ret_ == OB_SUCCESS && check_level_ > 0 && has_reached_threshold()) {
    INIT_SUCC(ret);
    LOG_ERROR("[Multi-mode ALARM ERROR] Allocator has reached threshold.", K(*this), K(ext_used_), K(lbt()));
  }
}

void MultimodeAlloctor::set_baseline_size(uint64_t baseline_size)
{
  baseline_size_ = baseline_size;
  expect_threshold_ = baseline_size_ * get_expected_multiple(type_);
}

void MultimodeAlloctor::set_baseline_size_and_flag(uint64_t baseline_size)
{
  baseline_size_ = baseline_size;
  expect_threshold_ = baseline_size_ * get_expected_multiple(type_);
  if (has_reached_specified_threshold(1.0)) {
    set_reached_threshold();
  }
}

void MultimodeAlloctor::add_baseline_size(uint64_t add_size) 
{ 
  baseline_size_ += add_size; 
  expect_threshold_ = baseline_size_ * get_expected_multiple(type_); 
}

int MultimodeAlloctor::add_baseline_size(ObDatum *datum, bool has_lob_header, uint32_t multiple)
{
  INIT_SUCC(ret);
  int64_t byte_len = datum->len_;
  if (has_lob_header) {
    ObLobLocatorV2 locator(datum->get_string());
    if (OB_FAIL(locator.get_lob_data_byte_len(byte_len))) {
      LOG_WARN("get lob data byte length failed", K(ret));
    } else {
      add_baseline_size(byte_len * multiple);
    }
  } else {
    add_baseline_size(byte_len * multiple);
  }

  return ret;
}

int MultimodeAlloctor::add_baseline_size(const ObExpr *expr, ObEvalCtx &ctx, uint32_t multiple)
{
  INIT_SUCC(ret);
  ObDatum *datum = nullptr;
  ObObjType val_type;
  int64_t byte_len = 0;
  if (OB_ISNULL(expr)) {
  } else if (OB_FALSE_IT(val_type = expr->datum_meta_.type_)) {
  } else if (OB_FAIL(eval_arg(expr, ctx, datum))) {
    LOG_WARN("eval json arg failed", K(ret), K(val_type));
  } else if (OB_FAIL(add_baseline_size(datum, ob_is_user_defined_sql_type(val_type) || expr->obj_meta_.has_lob_header(), multiple))) {
    LOG_WARN("failed to add base line size.", K(ret), K(val_type), KPC(datum));
  }

  return ret;
}

void MultimodeAlloctor::mem_check(float multiple)
{
  INIT_SUCC(ret);
  if (has_reached_specified_threshold(multiple)) {
    LOG_INFO("[Multi-mode ALARM] Allocator has reached level.", K(ret), K(*this), K(multiple), K(expect_threshold_), K(lbt()));
  }
}

uint64_t MultimodeAlloctor::get_expected_multiple(uint64_t type)
{
  uint64_t expected_multiple = 0;
  switch (type) {
    case T_FUN_SYS_JSON_OBJECT:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_EXTRACT:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_CONTAINS:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_CONTAINS_PATH:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_DEPTH:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_KEYS:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_ARRAY:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_QUOTE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_UNQUOTE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_OVERLAPS:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_REMOVE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_SEARCH:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_VALID:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_ARRAY_APPEND:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_ARRAY_INSERT:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_REPLACE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_TYPE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_LENGTH:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_INSERT:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_STORAGE_SIZE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_STORAGE_FREE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_MERGE_PRESERVE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_MERGE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_MERGE_PATCH:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_PRETTY:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_SET:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_MEMBER_OF:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_VALUE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_JSON_ARRAYAGG:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_JSON_OBJECTAGG:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_IS_JSON:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_EQUAL:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_QUERY:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_ORA_JSON_ARRAYAGG:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_ORA_JSON_OBJECTAGG:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_MAKEXML:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_XML_ELEMENT:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_XMLPARSE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_ORA_XMLAGG:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_XML_ATTRIBUTES:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_XML_EXTRACTVALUE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_XML_EXTRACT:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_XML_SERIALIZE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_XMLCAST:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_UPDATE_XML:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_PRIV_MAKE_XML_BINARY:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_EXISTS:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_OBJECT_WILD_STAR:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_SCHEMA_VALID:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_SCHEMA_VALIDATION_REPORT:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_JSON_APPEND:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_INSERTCHILDXML:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_DELETEXML:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_XMLSEQUENCE:
      expected_multiple = ObMultiModeDefaultMagnification;
      break;
    case T_FUN_SYS_ST_ASTEXT:
    case T_FUN_SYS_ST_ASWKT:
    case T_FUN_SYS_PRIV_ST_ASEWKT:
      expected_multiple = ObMultiModeSingleMagnification;
      break;
    
    default:
      expected_multiple = ObMultiModeDefaultMagnification;
  }

  return expected_multiple;
}

const double MultimodeAlloctor::MEM_THRESHOLD_LEVEL[LEVEL_NUMBERS][LEVEL_CLASS] = 
{
  // level = 0
  {
    0,
    0,
    0
  },
  // level = 1
  {
    0.2,
    0.6,
    0.8
  },
  // level = 2
  {
    0.4,
    0.8,
    1.2
  }
};

bool MultimodeAlloctor::has_reached_specified_threshold(double threshold)
{
  double current_threshold = (double)expect_threshold_ * threshold;
  return (double)cur_func_used() > current_threshold;
}

void MultimodeAlloctor::memory_usage_check_if_need()
{
  if (is_open_trace() && baseline_size_ > 0 && cur_func_used() > OB_MEM_TRACK_MIN_FOOT) {
    memory_usage_check();
  }
}

void MultimodeAlloctor::memory_usage_check()
{
  INIT_SUCC(ret);
  bool has_reached = true;
  for (uint8_t i = 0; has_reached && i < LEVEL_CLASS; i++) {
    if (!has_level_mem_threshold(i)) {
      if (has_reached_specified_threshold(MEM_THRESHOLD_LEVEL[check_level_][i])) {
        set_level_mem_threshold(i);
        LOG_INFO("[Multi-mode ALARM] Allocator has reached level.", K(ret), K(i),
                  K(*this), K(expect_threshold_), K(lbt()));
      } else {
        has_reached = false;
      }
    }
  }

  if (!has_reached_threshold() && has_reached_specified_threshold(1.0)) {
    set_reached_threshold();
    LOG_INFO("[Multi-mode ALARM] Allocator has reached threshold.", K(ret), K(3),
              K(*this), K(expect_threshold_), K(lbt()));
  }
}

void *MultimodeAlloctor::alloc(const int64_t sz)
{
  void* ptr = arena_.alloc(sz); 
  memory_usage_check_if_need();
  return ptr;
}

void *MultimodeAlloctor::alloc(const int64_t size, const ObMemAttr &attr)
{
  void* ptr = arena_.alloc(size, attr);
  memory_usage_check_if_need();
  return ptr;
}


int MultimodeAlloctor::eval_arg(const ObExpr *arg, ObEvalCtx &ctx, common::ObDatum *&datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null expr argument", K(ret), K(arg));
  } else {
    int64_t last_used = used();
    if (OB_FAIL(arg->eval(ctx, datum))) {
      LOG_WARN("eval geo arg failed", K(ret));
    } else {
      children_used_ += used() - last_used;
    }
  }
  return ret;
}

void MultimodeAlloctor::add_ext_used(uint64_t add) {
  ext_used_ += add;
  memory_usage_check_if_need();
}

};
};
