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
#define USING_LOG_PREFIX SHARE

#include "ob_mview_args.h"

namespace oceanbase
{
using namespace share::schema;
namespace obrpc
{

bool ObAlterMViewArg::is_valid() const
{
  bool is_valid = true;
  if (is_alter_refresh_method_ && ObMVRefreshMethod::MAX <= refresh_method_) {
    is_valid = false;
  } else if (is_alter_refresh_dop_ && refresh_dop_ <= 0) {
    is_valid = false;
  } else if (is_alter_refresh_start_ && !start_time_.is_timestamp()) {
    is_valid = false;
  } else if (is_alter_refresh_next_ && next_time_expr_.empty()) {
    is_valid = false;
  }
  return is_valid;
}

void ObAlterMViewArg::reset()
{
  exec_env_.reset();
  is_alter_on_query_computation_ = false;
  enable_on_query_computation_ = false;
  is_alter_query_rewrite_ = false;
  enable_query_rewrite_ = false;
  is_alter_refresh_method_ = false;
  refresh_method_ = ObMVRefreshMethod::MAX;
  is_alter_refresh_dop_ = false;
  refresh_dop_ = 0;
  is_alter_refresh_start_ = false;
  start_time_.reset();
  is_alter_refresh_next_ = false;
  next_time_expr_.reset();
}

int ObAlterMViewArg::assign(const ObAlterMViewArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    exec_env_ = other.exec_env_;
    is_alter_on_query_computation_ = other.is_alter_on_query_computation_;
    enable_on_query_computation_ = other.enable_on_query_computation_;
    is_alter_query_rewrite_ = other.is_alter_query_rewrite_;
    enable_query_rewrite_ = other.enable_query_rewrite_;
    is_alter_refresh_method_ = other.is_alter_refresh_method_;
    refresh_method_ = other.refresh_method_;
    is_alter_refresh_dop_ = other.is_alter_refresh_dop_;
    refresh_dop_ = other.refresh_dop_;
    is_alter_refresh_start_ = other.is_alter_refresh_start_;
    start_time_ = other.start_time_;
    is_alter_refresh_next_ = other.is_alter_refresh_next_;
    next_time_expr_ = other.next_time_expr_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAlterMViewArg,
                    exec_env_,
                    is_alter_on_query_computation_,
                    enable_on_query_computation_,
                    is_alter_query_rewrite_,
                    enable_query_rewrite_,
                    is_alter_refresh_method_,
                    refresh_method_,
                    is_alter_refresh_dop_,
                    refresh_dop_,
                    is_alter_refresh_start_,
                    start_time_,
                    is_alter_refresh_next_,
                    next_time_expr_);

bool ObAlterMLogArg::is_valid() const
{
  bool is_valid = true;
  if (is_alter_table_dop_ && table_dop_ <= 0) {
    is_valid = false;
  } else if (is_alter_purge_start_ && !start_time_.is_timestamp()) {
    is_valid = false;
  } else if (is_alter_purge_next_ && next_time_expr_.empty()) {
    is_valid = false;
  } else if (is_alter_lob_threshold_ && lob_threshold_ <= 0) {
    is_valid = false;
  }
  return is_valid;
}

void ObAlterMLogArg::reset()
{
  exec_env_.reset();
  is_alter_table_dop_ = false;
  table_dop_ = 0;
  is_alter_purge_start_ = false;
  start_time_.reset();
  is_alter_purge_next_ = false;
  next_time_expr_.reset();
  is_alter_lob_threshold_ = false;
  lob_threshold_ = 0;
}

int ObAlterMLogArg::assign(const ObAlterMLogArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    exec_env_ = other.exec_env_;
    is_alter_table_dop_ = other.is_alter_table_dop_;
    table_dop_ = other.table_dop_;
    is_alter_purge_start_ = other.is_alter_purge_start_;
    start_time_ = other.start_time_;
    is_alter_purge_next_ = other.is_alter_purge_next_;
    next_time_expr_ = other.next_time_expr_;
    is_alter_lob_threshold_ = other.is_alter_lob_threshold_;
    lob_threshold_ = other.lob_threshold_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAlterMLogArg,
                    exec_env_,
                    is_alter_table_dop_,
                    table_dop_,
                    is_alter_purge_start_,
                    start_time_,
                    is_alter_purge_next_,
                    next_time_expr_,
                    is_alter_lob_threshold_,
                    lob_threshold_);

} // namespace obrpc
} // namespace oceanbase
