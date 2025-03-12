/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_REDIS_ITERATOR_
#define OCEANBASE_OBSERVER_OB_REDIS_ITERATOR_
#include "observer/table/ob_table_scan_executor.h"
#include "observer/table/ttl/ob_table_ttl_task.h"

namespace oceanbase
{
namespace table
{

class ObRedisCellEntity
{
public:
  explicit ObRedisCellEntity(common::ObNewRow &ob_row, ObRedisModel model)
      : ob_row_(&ob_row), model_(model)
  {}
  virtual ~ObRedisCellEntity()
  {}

  void set_ob_row(common::ObNewRow &ob_row)
  {
    ob_row_ = &ob_row;
  }
  const common::ObNewRow *get_ob_row() const
  {
    return ob_row_;
  }
  int get_expire_ts(int64_t &expire_ts) const;
  int get_insert_ts(int64_t &insert_ts) const;
  int get_is_data(bool &is_data) const;
  TO_STRING_KV(KPC(ob_row_), K(model_));

private:
  int get_cell(int64_t idx, ObObj *&obj) const;

  common::ObNewRow *ob_row_;
  ObRedisModel model_;
  DISALLOW_COPY_AND_ASSIGN(ObRedisCellEntity);
};

class ObRedisRowIterator : public ObTableTTLRowIterator
{
public:
  ObRedisRowIterator();
  ~ObRedisRowIterator()
  {}
  virtual int get_next_row(ObNewRow *&row);
  int init_ttl(const schema::ObTableSchema &table_schema, uint64_t del_row_limit);
  int init_scan(const ObKVAttr &kv_attribute, const ObRedisTTLCtx *ttl_ctx);
  virtual int close() override;

private:
  int get_next_row_ttl(ObNewRow *&row);
  int get_next_row_scan(ObNewRow *&row);
  int update_expire_ts_with_last_row();
  int update_expire_ts_with_meta();
  int is_last_row_expired(bool &is_expired);
  int is_meta_row_expired(bool &is_expired);
  int deep_copy_last_meta_row();

public:
  // for TTL
  uint64_t limit_del_rows_;  // maximum delete row
  // bool is_end_; // stop iter row (reach time or row count limit)
  ObNewRow *meta_row_; // last meta row
  ObArenaAllocator row_allocator_;
  bool is_finished_;

  // both TTL and Scan
  bool return_expired_row_;  // 'true' for TTL delete, 'false' for scan/get executor
  int64_t cur_ts_;           // timestamp that ObRedisRowIterator is created
  int64_t meta_expire_ts_;   // timestamp of last meta expire ts
  int64_t meta_insert_ts_;   // timestamp of last meta insert ts
  int64_t expire_ts_;        // depends on meta_expire_ts_ and meta_insert_ts_
  ObRedisModel model_;
  bool is_ttl_table_;
  const ObRedisMeta *meta_;        // scan may need meta
  bool return_redis_meta_;
};
}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_ITERATOR_