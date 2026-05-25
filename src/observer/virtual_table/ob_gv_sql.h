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

#ifndef _OB_GV_SQL_H
#define _OB_GV_SQL_H 1

#include "observer/virtual_table/ob_all_plan_cache_stat.h"
#include "sql/plan_cache/ob_cache_object.h"

namespace oceanbase
{
namespace observer
{

class ObGVSql : public ObAllPlanCacheBase
{
public:
  struct ReuseableBuffer {
    ReuseableBuffer(): inner_allocator_(), buff_(NULL), len_(0) {}
    void reset()
    {
      if (OB_NOT_NULL(buff_)) {
        inner_allocator_.free(buff_);
        buff_ = NULL;
      }
      inner_allocator_.reset();
    }
    char *alloc(int64_t &len)
    {
      if (len > len_) {
        if (buff_ != NULL) {
          inner_allocator_.free(buff_);
          buff_ = NULL;
        }
        buff_ = static_cast<char *>(inner_allocator_.alloc(len));
        len_ = buff_ == NULL ? 0 : len;
      } else {
        len = len_;
      }
      return buff_;
    }
    ObMalloc inner_allocator_;
    char *buff_;
    int64_t len_;
  };
public:
  ObGVSql();
  virtual ~ObGVSql();
  void reset();
  virtual int inner_open();
  int inner_get_next_row() { return get_row_from_tenants(); }
protected:
  int get_row_from_tenants();
  int fill_cells(const sql::ObILibCacheObject *cache_obj, const sql::ObPlanCache &plan_cache);
  int get_row_from_specified_tenant(uint64_t tenant_id, bool &is_end);
private:
  common::ObSEArray<uint64_t, 1024> plan_id_array_;
  int64_t plan_id_array_idx_;
  sql::ObPlanCache *plan_cache_;
  ReuseableBuffer tmp_buff_;
  DISALLOW_COPY_AND_ASSIGN(ObGVSql);
};

}
}

#endif /* _OB_GV_SQL_H */


