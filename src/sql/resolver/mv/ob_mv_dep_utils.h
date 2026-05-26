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

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MV_DEP_UTILS_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MV_DEP_UTILS_H_

#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_isql_client.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
}
namespace share
{
namespace schema
{
class ObDependencyInfo;
}
}
namespace sql
{
class ObMVDepInfo
{
public:
  ObMVDepInfo() : tenant_id_(OB_INVALID_TENANT_ID),
                  mview_id_(OB_INVALID_ID),
                  p_order_(0),
                  p_obj_(OB_INVALID_ID),
                  p_type_(0),
                  qbcid_(0),
                  flags_(0) {}
  virtual ~ObMVDepInfo() {}
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(mview_id), K_(p_order), K_(p_obj), K_(p_type), K_(qbcid), K_(flags));

public:
  static const int64_t IS_COMPLETE_REFRESH_ONLY_FLAG = 1 << 0;

public:
  uint64_t tenant_id_;
  uint64_t mview_id_;
  int64_t p_order_;
  uint64_t p_obj_;
  int64_t p_type_;
  int64_t qbcid_;
  union {
    int64_t flags_;
    struct {
      int64_t is_complete_refresh_only_ : 1;
      int64_t reserved_                 : 63;
    };
  };
};

class ObMVDepUtils
{
public:
  static int get_mview_dep_infos(common::ObISQLClient &sql_client,
                                 const uint64_t tenant_id,
                                 const uint64_t mview_table_id,
                                 common::ObIArray<ObMVDepInfo> &dep_infos,
                                 bool ignore_udt_udf = false);
  static int insert_mview_dep_infos(common::ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    const uint64_t mview_table_id,
                                    const common::ObIArray<ObMVDepInfo> &dep_infos);
  static int delete_mview_dep_infos(common::ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    const uint64_t mview_table_id);
  static int convert_to_mview_dep_infos(
      const common::ObIArray<share::schema::ObDependencyInfo> &deps,
      common::ObIArray<ObMVDepInfo> &mv_deps);
  static int get_table_ids_only_referenced_by_given_mv(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const uint64_t mview_table_id,
      common::ObIArray<uint64_t> &ref_table_ids);
  static int get_table_ids_only_referenced_by_given_fast_lsm_mv(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const uint64_t mview_table_id,
      common::ObIArray<uint64_t> &ref_table_ids);
  static int get_referring_mv_of_base_table(ObISQLClient &sql_client, const uint64_t tenant_id,
                                            const uint64_t base_table_id,
                                            ObIArray<uint64_t> &mview_ids,
                                            bool &exists_nested_mv);
  static int get_mview_ids_in_topo_refresh_order(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const uint64_t target_mview_id,
      common::ObIArray<uint64_t> &topo_ordered_mview_ids);
  static int get_mview_ids_in_topo_refresh_order(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const uint64_t target_mview_id,
      common::ObIArray<uint64_t> &topo_ordered_mview_ids,
      common::ObIArray<common::ObSEArray<uint64_t, 4>> &topo_ordered_dep_mview_ids);
  static int get_mds_locked_mview_ids(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const uint64_t target_mview_id,
      const int64_t unrefreshed_mv_count,
      common::ObIArray<uint64_t> &mds_locked_mview_ids);
  static int update_mview_dep_base_table(common::ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    const uint64_t old_p_obj,
                                    const uint64_t new_p_obj);

private:
  static int fetch_mview_ids_by_sql(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const common::ObSqlString &sql,
      common::ObIArray<uint64_t> &mview_ids);
  static int build_mview_dep_recursive_cte(const uint64_t target_mview_id,
      const char *cte_name,
      common::ObSqlString &cte_sql);
};
} // end of sql
} // end of oceanbase
#endif