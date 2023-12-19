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
class ObMViewInfo : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  ObMViewInfo();
  explicit ObMViewInfo(common::ObIAllocator *allocator);
  ObMViewInfo(const ObMViewInfo &src_schema);
  virtual ~ObMViewInfo();

  ObMViewInfo &operator=(const ObMViewInfo &src_schema);
  int assign(const ObMViewInfo &other);

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
  DEFINE_GETTER_AND_SETTER(uint64_t, mview_id);
  DEFINE_GETTER_AND_SETTER(ObMViewBuildMode, build_mode);
  DEFINE_GETTER_AND_SETTER(ObMVRefreshMode, refresh_mode);
  DEFINE_GETTER_AND_SETTER(ObMVRefreshMethod, refresh_method);
  DEFINE_GETTER_AND_SETTER(int64_t, refresh_start);
  DEFINE_STRING_GETTER_AND_SETTER(refresh_next);
  DEFINE_STRING_GETTER_AND_SETTER(refresh_job);
  DEFINE_GETTER_AND_SETTER(uint64_t, last_refresh_scn);
  DEFINE_GETTER_AND_SETTER(ObMVRefreshType, last_refresh_type);
  DEFINE_GETTER_AND_SETTER(int64_t, last_refresh_date);
  DEFINE_GETTER_AND_SETTER(int64_t, last_refresh_time);
  DEFINE_STRING_GETTER_AND_SETTER(last_refresh_trace_id);
  DEFINE_GETTER_AND_SETTER(int64_t, schema_version);

#undef DEFINE_GETTER_AND_SETTER
#undef DEFINE_STRING_GETTER_AND_SETTER

  int gen_insert_mview_dml(const uint64_t exec_tenant_id, ObDMLSqlSplicer &dml) const;
  int gen_update_mview_attribute_dml(const uint64_t exec_tenant_id, ObDMLSqlSplicer &dml) const;
  int gen_update_mview_last_refresh_info_dml(const uint64_t exec_tenant_id,
                                             ObDMLSqlSplicer &dml) const;

  static int insert_mview_info(ObISQLClient &sql_client, const ObMViewInfo &mview_info);
  static int update_mview_attribute(ObISQLClient &sql_client, const ObMViewInfo &mview_info);
  static int update_mview_last_refresh_info(ObISQLClient &sql_client,
                                            const ObMViewInfo &mview_info);
  static int drop_mview_info(ObISQLClient &sql_client, const ObMViewInfo &mview_info);
  static int drop_mview_info(ObISQLClient &sql_client, const uint64_t tenant_id,
                             const uint64_t mview_id);
  static int fetch_mview_info(ObISQLClient &sql_client, uint64_t tenant_id, uint64_t mview_id,
                              ObMViewInfo &mview_info, bool for_update = false,
                              bool nowait = false);
  static int batch_fetch_mview_ids(ObISQLClient &sql_client, uint64_t tenant_id,
                                   uint64_t last_mview_id, ObIArray<uint64_t> &mview_ids,
                                   int64_t limit = -1);

  TO_STRING_KV(K_(tenant_id),
               K_(mview_id),
               K_(build_mode),
               K_(refresh_mode),
               K_(refresh_method),
               K_(refresh_start),
               K_(refresh_next),
               K_(refresh_job),
               K_(last_refresh_scn),
               K_(last_refresh_type),
               K_(last_refresh_date),
               K_(last_refresh_time),
               K_(last_refresh_trace_id),
               K_(schema_version));

public:
  static constexpr char *MVIEW_REFRESH_JOB_PREFIX = const_cast<char *>("MVIEW_REFRESH$J_");

private:
  uint64_t tenant_id_;
  uint64_t mview_id_;
  ObMViewBuildMode build_mode_;
  ObMVRefreshMode refresh_mode_;
  ObMVRefreshMethod refresh_method_;
  int64_t refresh_start_;
  ObString refresh_next_;
  ObString refresh_job_;
  uint64_t last_refresh_scn_;
  ObMVRefreshType last_refresh_type_;
  int64_t last_refresh_date_;
  int64_t last_refresh_time_;
  ObString last_refresh_trace_id_;
  int64_t schema_version_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase
