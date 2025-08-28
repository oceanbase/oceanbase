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

#ifndef OB_HP_INFRASTRUCTURE_MANAGER_H_
#define OB_HP_INFRASTRUCTURE_MANAGER_H_

#include "lib/list/ob_dlist.h"
#include "sql/engine/basic/ob_hash_partitioning_infrastructure_op.h"

namespace oceanbase
{
namespace sql
{
template<typename HashCol, typename HashRowStore>
class ObHashPartInfrastructureGroup
{
using HashPartInfras = ObHashPartInfrastructure<HashCol, HashRowStore>;
using HashPartInfrasList = common::ObDList<HashPartInfras>;
public:
  ObHashPartInfrastructureGroup(common::ObIAllocator &allocator)
    : allocator_(allocator), est_bucket_num_(0), initial_hp_size_(0), hp_infras_buffer_(nullptr),
      hp_infras_buffer_idx_(MAX_HP_INFRAS_CNT)
  {
  }
  ~ObHashPartInfrastructureGroup()
  {
    reset();
  }
  void reset();
  OB_INLINE int64_t get_hp_infras_count() const
  { return hp_infras_list_.get_size(); }
  OB_INLINE int reserve(const int64_t size)
  {
    int ret = OB_SUCCESS;
    initial_hp_size_ = size;
    return ret;
  }
  int init_one_hp_infras(
    HashPartInfras *&hp_infras, uint64_t tenant_id, bool enable_sql_dumped,
    bool unique, int64_t ways, int64_t batch_size, int64_t est_rows, int64_t width,
    ObSqlMemMgrProcessor *sql_mem_processor, bool need_rewind);
  int free_one_hp_infras(HashPartInfras *&hp_infras);
private:
  template<typename R, typename ...Args>
  struct _func_traits_base {
      using func_type = std::function<R(Args...)>;
      using result_type = R;
  };
  template<typename F>
  struct _func_traits;
  template<typename F>
  struct _func_traits<std::reference_wrapper<F>> : public _func_traits<F> {};
  template<typename R, typename ...Args>
  struct _func_traits<R(*)(Args...)> : public _func_traits_base<R, Args...> {};
  template<typename R, typename C, typename ...Args>
  struct _func_traits<R(C::*)(Args...)> : public _func_traits_base<R, Args...> {};
  template<typename R, typename C, typename ...Args>
  struct _func_traits<R(C::*)(Args...) const> : public _func_traits_base<R, Args...> {};
  template<typename F>
  struct _func_traits : public _func_traits<decltype(&F::operator())> {};
  template<typename F>
  struct func_traits : public _func_traits<typename std::decay<F>::type> {};
  
  static const int64_t MAX_HP_INFRAS_CNT = 16;
  int alloc_hp_infras(HashPartInfras *&hp_infras);
  template<typename F,
           typename ...Args,
           typename std::enable_if<std::is_same<typename func_traits<F>::result_type, int>::value, bool>::type = true>
  int foreach_call(HashPartInfrasList &hp_infras_list, F &&func, Args&& ...args)
  {
    int ret = OB_SUCCESS;
    DLIST_FOREACH(hp_infras, hp_infras_list) {
      // It has been checked whether it is nullptr when it is added, so it will not be checked here
      if (hp_infras->is_destroyed()) {
        // do nothing
      } else if (OB_FAIL((hp_infras->*std::forward<F>(func))(std::forward<Args>(args)...))) {
        SQL_ENG_LOG(WARN, "failed to execute function", K(ret));
      }
    }
    return ret;
  }
  template<typename F,
           typename ...Args,
           typename std::enable_if<!std::is_same<typename func_traits<F>::result_type, int>::value, bool>::type = false>
  void foreach_call(HashPartInfrasList &hp_infras_list, F &&func, Args&& ...args)
  {
    DLIST_FOREACH_NORET(hp_infras, hp_infras_list) {
      // It has been checked whether it is nullptr when it is added, so it will not be checked here
      if (hp_infras->is_destroyed()) {
        // do nothing
      } else {
        (hp_infras->*std::forward<F>(func))(std::forward<Args>(args)...);
      }
    }
  }
  int try_get_hp_infras_from_free_list(HashPartInfras *&hp_infras);
  int init_hash_table(HashPartInfras *&hp_infras, const int64_t est_rows, const int64_t width);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHashPartInfrastructureGroup);

private:
  const static int64_t RATIO = 20;
  common::ObIAllocator &allocator_;
  int64_t est_bucket_num_;
  int64_t initial_hp_size_;
  HashPartInfras *hp_infras_buffer_;
  int64_t hp_infras_buffer_idx_;
  HashPartInfrasList hp_infras_list_;
  HashPartInfrasList hp_infras_free_list_;
};

template<typename HashCol, typename HashRowStore>
class ObHashPartInfrastructureMgr
{
  using HashPartInfrasGroup = ObHashPartInfrastructureGroup<HashCol, HashRowStore>;
public:
  static const int64_t MIN_BUCKET_COUNT = 1L << 1;  //2;
  static const int64_t MAX_BUCKET_COUNT = 1L << 19; //524288;
  ObHashPartInfrastructureMgr(const uint64_t tenant_id)
    : arena_alloc_("HPInfrasGroup", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id), inited_(false),
      tenant_id_(tenant_id), enable_sql_dumped_(false), est_rows_(0), width_(0),
      unique_(false), ways_(1), eval_ctx_(nullptr), sql_mem_processor_(nullptr),
      io_event_observer_(nullptr), hp_infras_group_(arena_alloc_)
  {
  }
  ~ObHashPartInfrastructureMgr()
  {
    destroy();
  }
  int init(uint64_t tenant_id, bool enable_sql_dumped, const int64_t est_rows, const int64_t width,
    const bool unique, const int64_t ways, ObEvalCtx *eval_ctx,
    ObSqlMemMgrProcessor *sql_mem_processor, ObIOEventObserver *io_event_observer);
  int reserve_hp_infras(const int64_t capacity)
  { return hp_infras_group_.reserve(capacity); }
  int init_one_hp_infras(const bool need_rewind, const ObSortCollations *sort_collations,
    const ObSortFuncs *sort_cmp_funcs, const ObHashFuncs *hash_funcs, HashPartInfras *&hp_infras);
  int free_one_hp_infras(HashPartInfras *&hp_infras);

  OB_INLINE bool is_inited() const { return inited_; }
  void destroy()
  {
    inited_ = false;
    tenant_id_ = UINT64_MAX;
    enable_sql_dumped_ = false;
    est_rows_ = 0;
    width_ = 0;
    unique_ = false;
    ways_ = 1;
    eval_ctx_ = nullptr;
    sql_mem_processor_ = nullptr;
    io_event_observer_ = nullptr;
    hp_infras_group_.reset();
    arena_alloc_.reset();
  }

  OB_INLINE uint64_t get_tenant_id()
  { return tenant_id_; }
  OB_INLINE ObSqlMemMgrProcessor *get_sql_mem_mgr_processor()
  { return sql_mem_processor_; }
  OB_INLINE void set_io_event_observer(ObIOEventObserver *io_event_observer)
  { io_event_observer_ = io_event_observer; }
  OB_INLINE ObIOEventObserver *get_io_event_observer()
  { return io_event_observer_; }
  OB_INLINE bool enable_sql_dumped() const
  { return enable_sql_dumped_; }
  OB_INLINE int64_t get_max_batch_size() const
  { return eval_ctx_->max_batch_size_; }
  
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHashPartInfrastructureMgr);

private:
  common::ObArenaAllocator arena_alloc_;
  bool inited_;
  uint64_t tenant_id_;
  bool enable_sql_dumped_;
  int64_t est_rows_;
  int64_t width_;
  bool unique_;
  int64_t ways_;
  ObEvalCtx *eval_ctx_;
  ObSqlMemMgrProcessor *sql_mem_processor_;
  ObIOEventObserver *io_event_observer_;
  HashPartInfrasGroup hp_infras_group_;
};

using HashPartInfrasMgr = ObHashPartInfrastructureMgr<ObHashPartCols, ObHashPartStoredRow>;

}  // namespace sql
}  // namespace oceanbase

#include "ob_hp_infrastructure_manager.ipp"

#endif /* OB_HP_INFRASTRUCTURE_MANAGER_H_ */