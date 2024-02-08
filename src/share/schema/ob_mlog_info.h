/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{
class ObMLogInfo : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  ObMLogInfo();
  explicit ObMLogInfo(common::ObIAllocator *allocator);
  ObMLogInfo(const ObMLogInfo &src_schema);
  virtual ~ObMLogInfo();

  ObMLogInfo &operator=(const ObMLogInfo &src_schema);
  int assign(const ObMLogInfo &other);

  bool is_valid() const override;
  void reset() override;
  int64_t get_convert_size() const override;

#define DEFINE_GETTER_AND_SETTER(type, name)            \
  OB_INLINE type get_##name() const { return name##_; } \
  OB_INLINE void set_##name(type name) { name##_ = name; }

#define DEFINE_STRING_GETTER_AND_SETTER(name)                      \
  OB_INLINE const ObString &get_##name() const { return name##_; } \
  OB_INLINE int set_##name(const ObString &name) { return deep_copy_str(name, name##_); }

  DEFINE_GETTER_AND_SETTER(uint64_t, tenant_id);
  DEFINE_GETTER_AND_SETTER(uint64_t, mlog_id);
  DEFINE_GETTER_AND_SETTER(ObMLogPurgeMode, purge_mode);
  DEFINE_GETTER_AND_SETTER(int64_t, purge_start);
  DEFINE_STRING_GETTER_AND_SETTER(purge_next);
  DEFINE_STRING_GETTER_AND_SETTER(purge_job);
  DEFINE_GETTER_AND_SETTER(uint64_t, last_purge_scn);
  DEFINE_GETTER_AND_SETTER(int64_t, last_purge_date);
  DEFINE_GETTER_AND_SETTER(int64_t, last_purge_time);
  DEFINE_GETTER_AND_SETTER(int64_t, last_purge_rows);
  DEFINE_STRING_GETTER_AND_SETTER(last_purge_trace_id);
  DEFINE_GETTER_AND_SETTER(int64_t, schema_version);

#undef DEFINE_GETTER_AND_SETTER
#undef DEFINE_STRING_GETTER_AND_SETTER

  int gen_insert_mlog_dml(const uint64_t exec_tenant_id, ObDMLSqlSplicer &dml) const;
  int gen_update_mlog_attribute_dml(const uint64_t exec_tenant_id, ObDMLSqlSplicer &dml) const;
  int gen_update_mlog_last_purge_info_dml(const uint64_t exec_tenant_id,
                                          ObDMLSqlSplicer &dml) const;

  static int insert_mlog_info(ObISQLClient &sql_client, const ObMLogInfo &mlog_info);
  static int update_mlog_attribute(ObISQLClient &sql_client, const ObMLogInfo &mlog_info);
  static int update_mlog_last_purge_info(ObISQLClient &sql_client, const ObMLogInfo &mlog_info);
  static int drop_mlog_info(ObISQLClient &sql_client, const ObMLogInfo &mlog_info);
  static int drop_mlog_info(ObISQLClient &sql_client, const uint64_t tenant_id,
                            const uint64_t mlog_id);
  static int fetch_mlog_info(ObISQLClient &sql_client, uint64_t tenant_id, uint64_t mlog_id,
                             ObMLogInfo &mlog_info, bool for_update = false, bool nowait = false);
  static int batch_fetch_mlog_ids(ObISQLClient &sql_client, uint64_t tenant_id,
                                  uint64_t last_mlog_id, ObIArray<uint64_t> &mlog_ids,
                                  int64_t limit = -1);

  TO_STRING_KV(K_(tenant_id),
               K_(mlog_id),
               K_(purge_mode),
               K_(purge_start),
               K_(purge_next),
               K_(purge_job),
               K_(last_purge_scn),
               K_(last_purge_date),
               K_(last_purge_time),
               K_(last_purge_rows),
               K_(last_purge_trace_id),
               K_(schema_version));

public:
  static constexpr char *MLOG_PURGE_JOB_PREFIX = const_cast<char *>("MLOG_PURGE$J_");

private:
  uint64_t tenant_id_;
  uint64_t mlog_id_;
  ObMLogPurgeMode purge_mode_;
  int64_t purge_start_;
  ObString purge_next_;
  ObString purge_job_;
  uint64_t last_purge_scn_;
  int64_t last_purge_date_;
  int64_t last_purge_time_;
  int64_t last_purge_rows_;
  ObString last_purge_trace_id_;
  int64_t schema_version_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase
