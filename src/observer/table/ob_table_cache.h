/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_CACHE_H_
#define OCEANBASE_OBSERVER_OB_TABLE_CACHE_H_
#include "ob_table_executor.h"
#include "sql/plan_cache/ob_i_lib_cache_key.h"
#include "sql/plan_cache/ob_i_lib_cache_node.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_i_lib_cache_context.h"
#include "lib/utility/utility.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "ob_table_cg_service.h"
namespace oceanbase
{

namespace table
{

struct ObTableApiCacheKey: public ObILibCacheKey
{
  ObTableApiCacheKey()
      : ObILibCacheKey(ObLibCacheNameSpace::NS_TABLEAPI),
        table_id_(common::OB_INVALID_ID),
        index_table_id_(common::OB_INVALID_ID),
        schema_version_(-1),
        operation_type_(ObTableOperationType::Type::INVALID),
        is_ttl_table_(false)
  {}
  void reset();
  virtual int deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other);
  virtual uint64_t hash() const;
  virtual bool is_equal(const ObILibCacheKey &other) const;

  TO_STRING_KV(K_(table_id),
              K_(schema_version),
              K_(is_ttl_table),
              K_(operation_type),
              K_(index_table_id),
              K_(op_column_ids),
              K_(namespace));

  common::ObTableID table_id_;
  common::ObTableID index_table_id_;
  int64_t schema_version_;
  ObTableOperationType::Type operation_type_;
  bool is_ttl_table_;
  common::ObSEArray<uint64_t, 32> op_column_ids_;
};

class ObTableApiCacheNode: public ObILibCacheNode
{
public:
  ObTableApiCacheNode(ObPlanCache *lib_cache, lib::MemoryContext &mem_context)
      : ObILibCacheNode(lib_cache, mem_context),
        cache_obj_(nullptr) {}
  virtual ~ObTableApiCacheNode() {}
  virtual int inner_get_cache_obj(ObILibCacheCtx &ctx,
                                ObILibCacheKey *key,
                                ObILibCacheObject *&cache_obj) override;
  virtual int inner_add_cache_obj(ObILibCacheCtx &ctx,
                                ObILibCacheKey *key,
                                ObILibCacheObject *cache_obj) override;
private:
  ObILibCacheObject *cache_obj_;
};

class ObTableApiCacheObj: public ObILibCacheObject
{
public:
  ObTableApiCacheObj(lib::MemoryContext &mem_context)
  : ObILibCacheObject(ObLibCacheNameSpace::NS_TABLEAPI, mem_context),
    expr_info_(allocator_),
    spec_(NULL) {}

  virtual ~ObTableApiCacheObj()
  {
    if (OB_NOT_NULL(spec_)) {
      spec_->~ObTableApiSpec();
    }
  }
  OB_INLINE ObTableApiSpec* get_spec() { return spec_; }
  OB_INLINE void set_spec(ObTableApiSpec* spec) { spec_ = spec; }
  OB_INLINE sql::ObExprFrameInfo* get_expr_frame_info() { return &expr_info_; }
  OB_INLINE const sql::ObExprFrameInfo* get_expr_frame_info() const { return &expr_info_; }
private:
  sql::ObExprFrameInfo expr_info_;
  ObTableApiSpec *spec_;
};

class ObTableApiCacheGuard
{
public:
  ObTableApiCacheGuard()
    : is_use_cache_(false),
      lib_cache_(nullptr),
      cache_guard_(CacheRefHandleID::TABLEAPI_NODE_HANDLE) {}
  ~ObTableApiCacheGuard() {}
  int init(ObTableCtx *tb_ctx);
  void reset();
  int get_expr_info(ObTableCtx *tb_ctx, ObExprFrameInfo *&exp_frame_info);
  template<int TYPE>
  int get_spec(ObTableCtx *tb_ctx, ObTableApiSpec *&spec)
  {
    int ret = OB_SUCCESS;
    ObTableApiCacheObj *cache_obj = nullptr;
    ObTableApiSpec *tmp_spec = nullptr;
    if (OB_ISNULL(cache_obj = static_cast<ObTableApiCacheObj *>(cache_guard_.get_cache_obj()))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cache obj is null", K(ret));
    } else if (OB_ISNULL(tmp_spec = cache_obj->get_spec())) {
      if (OB_FAIL(ObTableSpecCgService::generate<TYPE>(cache_obj->get_allocator(),
                                                      *tb_ctx,
                                                      tmp_spec))) {
        SERVER_LOG(WARN, "fail to generate spec", K(ret));
      } else {
        cache_obj->set_spec(tmp_spec);
        if (OB_FAIL(lib_cache_->add_cache_obj(cache_ctx_, &cache_key_, cache_obj))) {
          // spec生成后直接加入的lib cache
          SERVER_LOG(WARN, "fail to add cache obj to lib cache", K(ret), K(cache_key_));
        }
      }
    }
    spec = tmp_spec;
    return ret;
  }
  OB_INLINE bool is_use_cache() { return is_use_cache_; }
private:
  int create_cache_key(ObTableCtx *tb_ctx);
  int append_column_ids(const ObITableEntity *entity,
                        const ObTableSchema *table_schema,
                        common::ObArray<uint64_t> &op_column_ids);
  int get_or_create_cache_obj();

private:
  bool is_use_cache_;
  sql::ObPlanCache *lib_cache_;
  ObTableApiCacheKey cache_key_;
  ObCacheObjGuard cache_guard_;
  ObILibCacheCtx cache_ctx_;
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_CACHE_H_ */