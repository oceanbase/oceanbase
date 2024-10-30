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

#ifndef SRC_SHARE_VECTOR_INDEX_OB_IVF_INDEX_SEARCH_HELPER_H_
#define SRC_SHARE_VECTOR_INDEX_OB_IVF_INDEX_SEARCH_HELPER_H_

#include "lib/ob_define.h"
#include "lib/container/ob_heap.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/vector/ob_vector.h"
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/schema/ob_table_schema.h"
#include "share/vector_index/ob_tenant_ivf_center_cache.h"
#include "share/ob_i_tablet_scan.h"
#include "sql/engine/expr/ob_expr.h"


namespace oceanbase
{
namespace sql
{
class ObTableScanOp;
}
namespace share
{

struct ObIvfRow
{
  ObObj *projector_objs_;
};

class ObIvfIndexSearchHelper
{
public:
  ObIvfIndexSearchHelper(const char* label)
    : basic_label_(),
      scan_label_(),
      is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      index_table_id_(OB_INVALID_ID),
      ann_k_(0),
      row_cnt_(0),
      projector_cnt_(0),
      n_probes_(0),
      distance_type_(INVALID_DISTANCE_TYPE),
      allocator_(),
      rows_(nullptr),
      centers_(nullptr),
      center_heap_(compare_),
      row_heap_(compare_),
      row_item_pool_(),
      partition_name_(),
      partition_idx_(-1)
  {
    MEMSET(basic_label_, '\0', sizeof(basic_label_));
    MEMSET(scan_label_, '\0', sizeof(scan_label_));
    (void)snprintf(basic_label_, sizeof(basic_label_), "%s", label);
    (void)snprintf(scan_label_, sizeof(scan_label_), "%s%s", basic_label_, "Scan");
  }
  virtual ~ObIvfIndexSearchHelper() { destroy(); }

  virtual int init(
    const int64_t tenant_id,
    const int64_t index_table_id,
    const int64_t ann_k,
    const int64_t probes,
    const ObTypeVector &qvector,
    const common::ObIArray<int32_t> &output_projector,
    common::sqlclient::ObISQLConnection *conn,
    sql::ObSQLSessionInfo *session);
  virtual void destroy();
  bool is_inited() const { return is_inited_; }
  int get_row(const int64_t idx, ObIvfRow *&row);
  void reuse();
  int reset_centers();
  int reload_centers(const common::ObIArray<ObTypeVector *> &array);
  virtual int reset_pq_centers() { return OB_NOT_SUPPORTED; }
  virtual int set_partition_name(common::ObTabletID &tablet_id);  // TODO(@wangmiao): index_table partition & container_table partition
  int generate_centers_by_storage(
      const sql::ExprFixedArray *output_exprs,
      sql::ObEvalCtx *eval_ctx,
      ObIAllocator &allocator,
      ObIArray<ObTypeVector *> &centers); // one by one
  virtual int generate_pq_centers_by_storage(
      const sql::ExprFixedArray *output_exprs,
      sql::ObEvalCtx *eval_ctx) { return OB_NOT_SUPPORTED; } ; // one by one
  virtual int get_rows_by_storage(
      const sql::ExprFixedArray *extra_access_exprs,
      const sql::ExprFixedArray *output_exprs,
      sql::ObEvalCtx *eval_ctx); // one by one
  int get_rows_by_storage(); // alloc and set rows_
  int prepare_rowkey_ranges(
      ObIArray<ObNewRange> &range_array,
      const int64_t rowkey_cnt,
      const uint64_t table_id,
      ObIAllocator &allocator);

  int get_centers_by_cache();
  bool has_cached_centers() const { return centers_ != nullptr; }
  virtual int get_pq_centers_by_cache() { return OB_NOT_SUPPORTED; }
  virtual bool has_cached_pq_centers() const { return false; }
protected:
  struct HeapItem
  {
    HeapItem() : distance_(DBL_MAX) {}
    HeapItem(const double distance) : distance_(distance) {}
    double distance_;
  };
  struct HeapCompare
  {
    bool operator()(const HeapItem &lhs, const HeapItem &rhs)
    {
      return lhs.distance_ < rhs.distance_ ? true : false;
    }
    int get_error_code() const { return OB_SUCCESS; }
  };
  struct HeapCenterItem : public HeapItem
  {
    HeapCenterItem() : HeapItem(), center_idx_(-1) {}
    HeapCenterItem(const double distance, const int64_t center_idx) : HeapItem(distance), center_idx_(center_idx) {}
    int64_t center_idx_;
    TO_STRING_KV(K_(distance), K_(center_idx));
  };
  struct HeapRowItem : public HeapItem
  {
    HeapRowItem() : HeapItem(), projector_objs_(nullptr) {}
    HeapRowItem(const double distance, ObObj *projector_objs) : HeapItem(distance), projector_objs_(projector_objs) {}
    ObObj *projector_objs_;
    TO_STRING_KV(K_(distance), K_(projector_objs));
  };
  typedef common::ObSEArray<HeapRowItem, 64> HeapRowItemPool;
protected:
  int parse_output_expr(
    const sql::ExprFixedArray *output_exprs,
    sql::ObEvalCtx *eval_ctx, ObArray<int64_t> &idx_array, ObTypeVector &vector);
  int update_center_heap(common::ObBinaryHeap<HeapCenterItem, HeapCompare, 64> &heap, const double center_idx,
      const ObTypeVector &tmp_vec, const ObTypeVector &qvector, double distance, const ObVectorDistanceType distance_type);
  int alloc_rows(const bool need_objs);
  int get_centers_by_sql(
    schema::ObSchemaGetterGuard &schema_guard,
    const schema::ObTableSchema &container_table_schema,
    common::sqlclient::ObISQLConnection &conn);
  int prepare_rowkey_range(
      ObNewRange &key_range,
      const int64_t rowkey_cnt,
      const int64_t center_idx,
      const uint64_t table_id,
      ObIAllocator &allocator);
protected:
  int alloc_row_item(
      const sql::ExprFixedArray *output_exprs,
      const double distance,
      sql::ObEvalCtx *eval_ctx,
      HeapRowItem &item);
  int update_row_heap(const HeapRowItem &item, const double distance);
  int init_item_pool(const HeapRowItem &item);
  int alloc_item_from_pool(HeapRowItem &item);
protected:
  static const int64_t PAGE_SIZE = (1 << 12); // 4KB
  static const int64_t MAX_MEMORY_SIZE = (1 << 30); // 1GB // TODO(@jingshui) limit the max memory size
protected:
  virtual int get_vector_probes(const sql::ObSQLSessionInfo *session, uint64_t &probes) const = 0;
protected:
  char basic_label_[lib::AOBJECT_LABEL_SIZE + 1];
  char scan_label_[lib::AOBJECT_LABEL_SIZE + 1];
  bool is_inited_;// keep
  int64_t tenant_id_;// keep
  int64_t index_table_id_;// keep
  int64_t ann_k_;// keep
  int64_t row_cnt_;
  int64_t projector_cnt_;// keep
  uint64_t n_probes_;// keep
  ObVectorDistanceType distance_type_;// keep
  common::ObArenaAllocator allocator_;// keep
  // common::ObFIFOAllocator allocator_;
  ObIvfRow *rows_;
  ObTableIvfCenters *centers_;
  HeapCompare compare_;
  common::ObBinaryHeap<HeapCenterItem, HeapCompare, 64> center_heap_;
  common::ObBinaryHeap<HeapRowItem, HeapCompare, 64> row_heap_;
  HeapRowItemPool row_item_pool_;
  ObTypeVector qvector_;// keep
  common::sqlclient::ObISQLConnection *conn_;// keep
  ObString partition_name_;
  int64_t partition_idx_;
};
} // share
} // oceanbase
#endif
