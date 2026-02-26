
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
#ifndef OCEANBASE_CREATE_HIDDEN_TABLE_RPC_OB_RPC_STRUCT_H_
#define OCEANBASE_CREATE_HIDDEN_TABLE_RPC_OB_RPC_STRUCT_H_

#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace obrpc
{
// should use ObCreateHiddenTableArgV2 now
struct ObCreateHiddenTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(exec_tenant_id),
               K_(tenant_id),
               K_(table_id),
               K_(consumer_group_id),
               K_(dest_tenant_id),
               K_(session_id),
               K_(parallelism),
               K_(ddl_type),
               K_(ddl_stmt_str),
               K_(sql_mode),
               K_(tz_info_wrap),
               "nls_formats", common::ObArrayWrap<common::ObString>(nls_formats_, common::ObNLSFormatEnum::NLS_MAX),
               K_(tablet_ids),
               K_(need_reorder_column_id),
               K_(foreign_key_checks));
  ObCreateHiddenTableArg() :
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    table_id_(common::OB_INVALID_ID),
    consumer_group_id_(0),
    dest_tenant_id_(common::OB_INVALID_ID),
    session_id_(common::OB_INVALID_ID),
    ddl_type_(share::DDL_INVALID),
    ddl_stmt_str_(),
    sql_mode_(0),
    tz_info_wrap_(),
    nls_formats_{},
    tablet_ids_(),
    need_reorder_column_id_(false),
    foreign_key_checks_(true)
    {}
  ~ObCreateHiddenTableArg()
  {
    allocator_.clear();
  }
  bool is_valid() const;
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    consumer_group_id_ = 0;
    dest_tenant_id_ = common::OB_INVALID_ID;
    session_id_ = common::OB_INVALID_ID;
    ddl_type_ = share::DDL_INVALID;
    ddl_stmt_str_.reset();
    sql_mode_ = 0;
    tablet_ids_.reset();
    need_reorder_column_id_ = false;
    foreign_key_checks_ = true;
  }
  int assign(const ObCreateHiddenTableArg &arg);
  int init(const uint64_t tenant_id, const uint64_t dest_tenant_id, uint64_t exec_tenant_id,
           const uint64_t table_id, const int64_t consumer_group_id, const uint64_t session_id,
           const int64_t parallelism, const share::ObDDLType ddl_type,
           const ObSQLMode sql_mode, const ObTimeZoneInfo &tz_info,
           const common::ObString &local_nls_date, const common::ObString &local_nls_timestamp,
           const common::ObString &local_nls_timestamp_tz, const ObTimeZoneInfoWrap &tz_info_wrap,
           const common::ObIArray<common::ObTabletID> &tablet_ids, const bool need_reorder_column_id,
           const bool foreign_key_checks);
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_table_id() const { return table_id_; }
  int64_t get_consumer_group_id() const { return consumer_group_id_; }
  uint64_t get_exec_tenant_id() const { return exec_tenant_id_; }
  uint64_t get_dest_tenant_id() const { return dest_tenant_id_; }
  uint64_t get_session_id() const { return session_id_; }
  uint64_t get_parallelism() const { return parallelism_; }
  share::ObDDLType get_ddl_type() const { return ddl_type_; }
  const common::ObString &get_ddl_stmt_str() const { return ddl_stmt_str_; }
  ObSQLMode get_sql_mode() const { return sql_mode_; }
  common::ObArenaAllocator &get_allocator() { return allocator_; }
  common::ObTimeZoneInfo get_tz_info() const { return tz_info_; }
  const common::ObTimeZoneInfoWrap &get_tz_info_wrap() const { return tz_info_wrap_; }
  const common::ObString *get_nls_formats() const { return nls_formats_; }
  const common::ObIArray<common::ObTabletID> &get_tablet_ids() const { return tablet_ids_; }
  bool get_need_reorder_column_id() const { return need_reorder_column_id_; }
  bool get_foreign_key_checks() const { return foreign_key_checks_; }
private:
  uint64_t tenant_id_;
  int64_t table_id_;
  int64_t consumer_group_id_;
  uint64_t dest_tenant_id_;
  uint64_t session_id_;
  uint64_t parallelism_;
  share::ObDDLType ddl_type_;
  common::ObString ddl_stmt_str_;
  ObSQLMode sql_mode_;
  common::ObArenaAllocator allocator_;
  common::ObTimeZoneInfo tz_info_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
  common::ObSArray<common::ObTabletID> tablet_ids_;
  bool need_reorder_column_id_;
  bool foreign_key_checks_;
};

struct ObCreateHiddenTableArgV2 : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(exec_tenant_id),
               K_(tenant_id),
               K_(table_id),
               K_(consumer_group_id),
               K_(dest_tenant_id),
               K_(session_id),
               K_(parallelism),
               K_(ddl_type),
               K_(ddl_stmt_str),
               K_(sql_mode),
               K_(tz_info_wrap),
               K_(nls_formats),
               K_(tablet_ids),
               K_(foreign_key_checks),
               K_(enable_partition_pruning));
  ObCreateHiddenTableArgV2() :
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    table_id_(common::OB_INVALID_ID),
    consumer_group_id_(0),
    dest_tenant_id_(common::OB_INVALID_ID),
    session_id_(common::OB_INVALID_ID),
    ddl_type_(share::DDL_INVALID),
    sql_mode_(0),
    tz_info_wrap_(),
    nls_formats_(ObNLSFormatEnum::NLS_MAX),
    tablet_ids_(),
    foreign_key_checks_(true),
    enable_partition_pruning_(false)
    {}
  ~ObCreateHiddenTableArgV2()
  {
  }
  bool is_valid() const;
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    consumer_group_id_ = 0;
    dest_tenant_id_ = common::OB_INVALID_ID;
    session_id_ = common::OB_INVALID_ID;
    ddl_type_ = share::DDL_INVALID;
    ddl_stmt_str_.reset();
    sql_mode_ = 0;
    nls_formats_.reset();
    tablet_ids_.reset();
    foreign_key_checks_ = true;
    enable_partition_pruning_ = false;
  }
  int assign(const ObCreateHiddenTableArgV2 &arg);
  int assign(const ObCreateHiddenTableArg &arg);
  int init(const uint64_t tenant_id, const uint64_t dest_tenant_id, uint64_t exec_tenant_id,
           const uint64_t table_id, const int64_t consumer_group_id, const uint64_t session_id,
           const int64_t parallelism, const share::ObDDLType ddl_type,
           const ObSQLMode sql_mode, const ObTimeZoneInfo &tz_info,
           const common::ObString &local_nls_date, const common::ObString &local_nls_timestamp,
           const common::ObString &local_nls_timestamp_tz, const ObTimeZoneInfoWrap &tz_info_wrap,
           const common::ObIArray<common::ObTabletID> &tablet_ids, const bool foreign_key_checks,
           const bool enable_partition_pruning = false);
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_table_id() const { return table_id_; }
  int64_t get_consumer_group_id() const { return consumer_group_id_; }
  uint64_t get_exec_tenant_id() const { return exec_tenant_id_; }
  uint64_t get_dest_tenant_id() const { return dest_tenant_id_; }
  uint64_t get_session_id() const { return session_id_; }
  uint64_t get_parallelism() const { return parallelism_; }
  share::ObDDLType get_ddl_type() const { return ddl_type_; }
  const common::ObString &get_ddl_stmt_str() const { return ddl_stmt_str_; }
  ObSQLMode get_sql_mode() const { return sql_mode_; }
  common::ObTimeZoneInfo get_tz_info() const { return tz_info_; }
  const common::ObTimeZoneInfoWrap &get_tz_info_wrap() const { return tz_info_wrap_; }
  const common::ObIArray<common::ObString> &get_nls_formats() const { return nls_formats_; }
  const common::ObIArray<common::ObTabletID> &get_tablet_ids() const { return tablet_ids_; }
  bool get_foreign_key_checks() const { return foreign_key_checks_; }
  bool get_enable_partition_pruning() const { return enable_partition_pruning_; }
private:
  uint64_t tenant_id_;
  int64_t table_id_;
  int64_t consumer_group_id_;
  uint64_t dest_tenant_id_;
  uint64_t session_id_;
  uint64_t parallelism_;
  share::ObDDLType ddl_type_;
  ObSQLMode sql_mode_;
  common::ObTimeZoneInfo tz_info_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  common::ObSArray<common::ObString> nls_formats_;
  common::ObSArray<common::ObTabletID> tablet_ids_;
  bool foreign_key_checks_;
  bool enable_partition_pruning_;
};

}// end namespace obrpc
}// end namespace oceanbase
#endif