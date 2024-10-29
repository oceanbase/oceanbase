/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_INDEX_OB_IVF_INDEX_SAMPLE_CACHE_H_
#define SRC_SHARE_VECTOR_INDEX_OB_IVF_INDEX_SAMPLE_CACHE_H_

#include "share/vector_index/ob_index_sample_cache.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/vector/ob_vector.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "observer/ob_inner_sql_connection.h"

namespace oceanbase
{
namespace share
{
struct ObMysqlResultIterator
{
  explicit ObMysqlResultIterator(sqlclient::ObMySQLResult &result)
    : result_(result)
  {}

  int get_next_vector(ObTypeVector &vector);

  sqlclient::ObMySQLResult &result_;

};

class ObIvfFixSampleCache : public ObIndexSampleCache
{
public:
  ObIvfFixSampleCache(ObISQLClient::ReadResult &result, common::ObISQLClient &sql_client)
    : ObIndexSampleCache(),
      limit_memory_size_(0),
      select_str_(),
      result_(result),
      sql_proxy_(&sql_client),
      res_(nullptr),
      allocator_(),
      inner_conn_(nullptr)
  {}
  ObIvfFixSampleCache(ObISQLClient::ReadResult &result, observer::ObInnerSQLConnection *inner_conn)
    : ObIndexSampleCache(),
      limit_memory_size_(0),
      select_str_(),
      result_(result),
      sql_proxy_(nullptr),
      res_(nullptr),
      allocator_(),
      inner_conn_(inner_conn)
  {}
  ~ObIvfFixSampleCache() { destroy(); }
  int init(
      const int64_t tenant_id,
      const int64_t lists,
      ObSqlString &select_sql_string,
      ObLabel allocator_label,
      ObLabel samples_label,
      int64_t sample_cnt);
  void destroy() override;
  int read() override;
  int get_next_vector(ObTypeVector &vector) override;
  int get_random_vector(ObTypeVector &vector) override;
  DECLARE_TO_STRING override;
private:
  int init_cache();
  int init_reservoir_samples();
  int get_next_vector_by_sql(ObTypeVector &vector);
  int get_next_vector_by_cache(ObTypeVector &vector) override;
  int alloc_and_copy_vector(const ObTypeVector& other, ObTypeVector *&vector);
  void destory_vector(ObTypeVector *&vector);
private:
  static const int64_t MAX_CACHE_MEMORY_RATIO = 20; // 20 % tenant_memory // TODO(@jingshui)
private:
  int64_t limit_memory_size_;
  ObSqlString select_str_;
  ObISQLClient::ReadResult &result_;
  common::ObISQLClient *sql_proxy_;
  sqlclient::ObMySQLResult *res_;
  ObArenaAllocator allocator_;
  observer::ObInnerSQLConnection *inner_conn_;
};
} // share
} // oceanbase

#endif
