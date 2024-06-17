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
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_isql_client.h"

namespace oceanbase
{
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
  uint64_t tenant_id_;
  uint64_t mview_id_;
  int64_t p_order_;
  uint64_t p_obj_;
  int64_t p_type_;
  int64_t qbcid_;
  int64_t flags_;
};

class ObMVDepUtils
{
public:
  static int get_mview_dep_infos(common::ObISQLClient &sql_client,
                                 const uint64_t tenant_id,
                                 const uint64_t mview_table_id,
                                 common::ObIArray<ObMVDepInfo> &dep_infos);
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
};
} // end of sql
} // end of oceanbase
#endif