/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_INDEX_OB_IVFFLAT_INDEX_SAMPLE_CACHE_H_
#define SRC_SHARE_VECTOR_INDEX_OB_IVFFLAT_INDEX_SAMPLE_CACHE_H_

#include "lib/ob_define.h"
#include "lib/container/ob_heap.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/vector/ob_vector.h"
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/schema/ob_table_schema.h"
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

class ObIvfflatFixSampleCache
{
public:
  ObIvfflatFixSampleCache(ObISQLClient::ReadResult &result, common::ObISQLClient &sql_client)
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      total_cnt_(0),
      sample_cnt_(0),
      limit_memory_size_(0),
      cur_idx_(0),
      select_str_(),
      result_(result),
      sql_proxy_(&sql_client),
      res_(nullptr),
      allocator_(),
      samples_(),
      inner_conn_(nullptr)
  {}
  ObIvfflatFixSampleCache(ObISQLClient::ReadResult &result, observer::ObInnerSQLConnection *inner_conn)
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      total_cnt_(0),
      limit_memory_size_(0),
      cur_idx_(0),
      select_str_(),
      result_(result),
      sql_proxy_(nullptr),
      res_(nullptr),
      allocator_(),
      samples_(),
      inner_conn_(inner_conn)
  {}
  ~ObIvfflatFixSampleCache() { destroy(); }
  int init(
      const int64_t tenant_id,
      const int64_t lists,
      ObSqlString &select_sql_string);
  void destroy();
  bool is_inited() const { return is_inited_; }
  int read();
  int get_next_vector(ObTypeVector &vector);
  int get_random_vector(ObTypeVector &vector);

  int64_t get_total_cnt() const { return total_cnt_; }
  int64_t get_sample_cnt() const { return 0 == sample_cnt_ ? total_cnt_ : samples_.count(); }
  DECLARE_TO_STRING;
private:
  int init_cache();
  int init_reservoir_samples();
  int get_next_vector_by_sql(ObTypeVector &vector);
  int get_next_vector_by_cache(ObTypeVector &vector);
  int alloc_and_copy_vector(const ObTypeVector& other, ObTypeVector *&vector);
  void destory_vector(ObTypeVector *&vector);
private:
  static const int64_t MAX_CACHE_MEMORY_RATIO = 20; // 20 % tenant_memory // TODO(@jingshui)
private:
  bool is_inited_;
  int64_t tenant_id_;
  int64_t total_cnt_;
  int64_t sample_cnt_; // expect sample cnt
  int64_t limit_memory_size_;
  int64_t cur_idx_;
  ObSqlString select_str_;
  ObISQLClient::ReadResult &result_;
  common::ObISQLClient *sql_proxy_;
  sqlclient::ObMySQLResult *res_;
  ObArenaAllocator allocator_;
  ObArray<ObTypeVector*> samples_;
  observer::ObInnerSQLConnection *inner_conn_;
};
} // share
} // oceanbase

#endif
