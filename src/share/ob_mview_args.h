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

#ifndef OCEANBASE_SHARE_OB_MVIEW_ARGS_H_
#define OCEANBASE_SHARE_OB_MVIEW_ARGS_H_

#include "lib/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "ob_ddl_args.h"

namespace oceanbase
{
namespace obrpc
{
struct ObMViewCompleteRefreshArg final : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObMViewCompleteRefreshArg()
    : ObDDLArg(),
      tenant_id_(OB_INVALID_TENANT_ID),
      table_id_(OB_INVALID_ID),
      session_id_(OB_INVALID_ID),
      sql_mode_(0),
      last_refresh_scn_(),
      allocator_("MVRefDDL"),
      tz_info_(),
      tz_info_wrap_(),
      nls_formats_(),
      parent_task_id_(0)
  {
  }
  ~ObMViewCompleteRefreshArg() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObMViewCompleteRefreshArg &other);
  INHERIT_TO_STRING_KV("ObDDLArg", ObDDLArg,
                       K_(tenant_id),
                       K_(table_id),
                       K_(session_id),
                       K_(sql_mode),
                       K_(last_refresh_scn),
                       K_(tz_info),
                       K_(tz_info_wrap),
                       "nls_formats", common::ObArrayWrap<common::ObString>(nls_formats_, common::ObNLSFormatEnum::NLS_MAX),
                       K_(parent_task_id));
public:
  uint64_t tenant_id_;
  uint64_t table_id_; // mview table id
  uint64_t session_id_;
  ObSQLMode sql_mode_;
  share::SCN last_refresh_scn_;
  common::ObArenaAllocator allocator_;
  common::ObTimeZoneInfo tz_info_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
  int64_t parent_task_id_;
};

struct ObMViewCompleteRefreshRes final
{
  OB_UNIS_VERSION(1);
public:
  ObMViewCompleteRefreshRes() : task_id_(0), trace_id_() {}
  ~ObMViewCompleteRefreshRes() = default;
  void reset()
  {
    task_id_ = 0;
    trace_id_.reset();
  }
  int assign(const ObMViewCompleteRefreshRes &other)
  {
    if (this != &other) {
      task_id_ = other.task_id_;
      trace_id_ = other.trace_id_;
    }
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(task_id), K_(trace_id));
public:
  int64_t task_id_;
  share::ObTaskId trace_id_;
};

struct ObMViewRefreshInfo
{
  OB_UNIS_VERSION(1);
public:
  ObMViewRefreshInfo()
    : mview_table_id_(OB_INVALID_ID),
      last_refresh_scn_(),
      refresh_scn_(),
      start_time_(OB_INVALID_TIMESTAMP),
      is_mview_complete_refresh_(false)
  {
  }
  ~ObMViewRefreshInfo() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObMViewRefreshInfo &other);
  TO_STRING_KV(K_(mview_table_id),
               K_(last_refresh_scn),
               K_(refresh_scn),
               K_(start_time),
               K_(is_mview_complete_refresh));
public:
  uint64_t mview_table_id_;
  share::SCN last_refresh_scn_;
  share::SCN refresh_scn_;
  int64_t start_time_;
  bool is_mview_complete_refresh_;
};

struct ObAlterMViewArg
{
  OB_UNIS_VERSION(1);
public:
  ObAlterMViewArg():
    exec_env_(),
    is_alter_on_query_computation_(false),
    enable_on_query_computation_(false),
    is_alter_query_rewrite_(false),
    enable_query_rewrite_(false),
    is_alter_refresh_method_(false),
    refresh_method_(share::schema::ObMVRefreshMethod::MAX),
    is_alter_refresh_dop_(false),
    refresh_dop_(0),
    is_alter_refresh_start_(false),
    start_time_(),
    is_alter_refresh_next_(false),
    next_time_expr_()
  {
  }
  ~ObAlterMViewArg() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObAlterMViewArg &other);

  TO_STRING_KV(K_(exec_env),
               K_(is_alter_on_query_computation),
               K_(enable_on_query_computation),
               K_(is_alter_query_rewrite),
               K_(enable_query_rewrite),
               K_(is_alter_refresh_method),
               K_(refresh_method),
               K_(is_alter_refresh_dop),
               K_(refresh_dop),
               K_(is_alter_refresh_start),
               K_(start_time),
               K_(is_alter_refresh_next),
               K_(next_time_expr));
public:
  void set_exec_env(const ObString &exec_env)
  {
    exec_env_ = exec_env;
  }
  void set_enable_on_query_computation(bool enable)
  {
    is_alter_on_query_computation_ = true;
    enable_on_query_computation_ = enable;
  }
  void set_enable_query_rewrite(bool enable)
  {
    is_alter_query_rewrite_ = true;
    enable_query_rewrite_ = enable;
  }
  void set_refresh_method(share::schema::ObMVRefreshMethod refresh_method)
  {
    is_alter_refresh_method_ = true;
    refresh_method_ = refresh_method;
  }
  void set_refresh_dop(int64_t refresh_dop)
  {
    is_alter_refresh_dop_ = true;
    refresh_dop_ = refresh_dop;
  }
  void set_start_time(int64_t start_time)
  {
    is_alter_refresh_start_ = true;
    start_time_.set_timestamp(start_time);
  }
  void set_next_time_expr(const ObString &next_time_expr)
  {
    is_alter_refresh_next_ = true;
    next_time_expr_ = next_time_expr;
  }
  const ObString &get_exec_env() const { return exec_env_; }
  bool is_alter_on_query_computation() const { return is_alter_on_query_computation_; }
  bool get_enable_on_query_computation() const { return enable_on_query_computation_; }
  bool is_alter_query_rewrite() const { return is_alter_query_rewrite_; }
  bool get_enable_query_rewrite() const { return enable_query_rewrite_; }
  bool is_alter_refresh_method() const { return is_alter_refresh_method_; }
  share::schema::ObMVRefreshMethod get_refresh_method() const { return refresh_method_; }
  bool is_alter_refresh_dop() const { return is_alter_refresh_dop_; }
  int64_t get_refresh_dop() const { return refresh_dop_; }
  bool is_alter_refresh_start() const { return is_alter_refresh_start_; }
  const common::ObObj &get_start_time() const { return start_time_; }
  bool is_alter_refresh_next() const { return is_alter_refresh_next_; }
  const ObString &get_next_time_expr() const { return next_time_expr_; }
private:
  ObString exec_env_;
  bool is_alter_on_query_computation_;
  bool enable_on_query_computation_;
  bool is_alter_query_rewrite_;
  bool enable_query_rewrite_;
  bool is_alter_refresh_method_;
  share::schema::ObMVRefreshMethod refresh_method_;
  bool is_alter_refresh_dop_;
  int64_t refresh_dop_;
  bool is_alter_refresh_start_;
  common::ObObj start_time_;
  bool is_alter_refresh_next_;
  ObString next_time_expr_;
};

struct ObAlterMLogArg
{
  OB_UNIS_VERSION(1);
public:
  ObAlterMLogArg() :
    exec_env_(),
    is_alter_table_dop_(false),
    table_dop_(0),
    is_alter_purge_start_(false),
    start_time_(),
    is_alter_purge_next_(false),
    next_time_expr_(),
    is_alter_lob_threshold_(false),
    lob_threshold_(0)
  {
  }
  ~ObAlterMLogArg() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObAlterMLogArg &other);

  TO_STRING_KV(K_(exec_env),
               K_(is_alter_table_dop),
               K_(table_dop),
               K_(is_alter_purge_start),
               K_(start_time),
               K_(is_alter_purge_next),
               K_(next_time_expr),
               K_(is_alter_lob_threshold),
               K_(lob_threshold));
public:
  void set_exec_env(const ObString &exec_env)
  {
    exec_env_ = exec_env;
  }
  void set_table_dop(int64_t dop)
  {
    is_alter_table_dop_ = true;
    table_dop_ = dop;
  }
  void set_start_time(int64_t start_time)
  {
    is_alter_purge_start_ = true;
    start_time_.set_timestamp(start_time);
  }
  void set_next_time_expr(const ObString &next_time_expr)
  {
    is_alter_purge_next_ = true;
    next_time_expr_ = next_time_expr;
  }
  void set_lob_threshold(int64_t lob_threshold)
  {
    is_alter_lob_threshold_ = true;
    lob_threshold_ = lob_threshold;
  }
  const ObString &get_exec_env() const { return exec_env_; }
  bool is_alter_table_dop() const { return is_alter_table_dop_; }
  int64_t get_table_dop() const { return table_dop_; }
  bool is_alter_purge_start() const { return is_alter_purge_start_; }
  const common::ObObj &get_start_time() const { return start_time_; }
  bool is_alter_purge_next() const { return is_alter_purge_next_; }
  const ObString &get_next_time_expr() const { return next_time_expr_; }
  bool is_alter_lob_threshold() const { return is_alter_lob_threshold_; }
  int64_t get_lob_threshold() const { return lob_threshold_; }
private:
  ObString exec_env_;
  bool is_alter_table_dop_;
  int64_t table_dop_;
  bool is_alter_purge_start_;
  common::ObObj start_time_;
  bool is_alter_purge_next_;
  ObString next_time_expr_;
  bool is_alter_lob_threshold_;
  int64_t lob_threshold_;
};

} // namespace obrpc
} // namespace oceanbase
#endif