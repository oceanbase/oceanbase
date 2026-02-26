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

#ifndef OB_HP_INFRASTRUCTURE_VEC_MANAGER_H_
#define OB_HP_INFRASTRUCTURE_VEC_MANAGER_H_

#include "lib/list/ob_dlist.h"
#include "sql/engine/basic/ob_hp_infras_vec_op.h"

namespace oceanbase
{
namespace sql
{
using HashPartInfrasVec = ObHashPartInfrastructureVecImpl;

class ObHashPartInfrasVecGroup
{
using HashPartInfrasVecList = common::ObDList<HashPartInfrasVec>;
public:
  ObHashPartInfrasVecGroup(common::ObIAllocator &allocator, const uint64_t tenant_id) :
    allocator_(allocator), tenant_id_(tenant_id), enable_sql_dumped_(false), est_rows_(0),
    width_(0), unique_(false), ways_(1), eval_ctx_(nullptr), compressor_type_(NONE_COMPRESSOR),
    sql_mem_processor_(nullptr), io_event_observer_(nullptr), est_bucket_num_(0),
    initial_hp_size_(0), hp_infras_buffer_(nullptr), hp_infras_buffer_idx_(MAX_HP_INFRAS_CNT)
  {}
  ~ObHashPartInfrasVecGroup()
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
  int init(uint64_t tenant_id, bool enable_sql_dumped, const int64_t est_rows, const int64_t width,
           const bool unique, const int64_t ways, ObEvalCtx *eval_ctx,
           ObSqlMemMgrProcessor *sql_mem_processor, ObIOEventObserver *io_event_observer,
           common::ObCompressorType compressor_type);
  int init_one_hp_infras(HashPartInfrasVec *&hp_infras, const common::ObIArray<ObExpr *> &exprs,
                         const common::ObIArray<ObSortFieldCollation> *sort_collations,
                         bool need_rewind);
  int free_one_hp_infras(HashPartInfrasVec *&hp_infras);
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
  int alloc_hp_infras(HashPartInfrasVec *&hp_infras);
  template<typename F,
           typename ...Args,
           typename std::enable_if<std::is_same<typename func_traits<F>::result_type, int>::value, bool>::type = true>
  int foreach_call(HashPartInfrasVecList &hp_infras_list, F &&func, Args&& ...args)
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
  void foreach_call(HashPartInfrasVecList &hp_infras_list, F &&func, Args&& ...args)
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
  int try_get_hp_infras_from_free_list(HashPartInfrasVec *&hp_infras,
                                       const common::ObIArray<ObExpr *> &exprs);
  int init_hash_table(HashPartInfrasVec *&hp_infras, const int64_t est_rows, const int64_t width);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHashPartInfrasVecGroup);

private:
  const static int64_t RATIO = 30;
  common::ObIAllocator &allocator_;
  uint64_t tenant_id_;
  bool enable_sql_dumped_;
  int64_t est_rows_;
  int64_t width_;
  bool unique_;
  int64_t ways_;
  ObEvalCtx *eval_ctx_;
  common::ObCompressorType compressor_type_;
  ObSqlMemMgrProcessor *sql_mem_processor_;
  ObIOEventObserver *io_event_observer_;
  int64_t est_bucket_num_;
  int64_t initial_hp_size_;
  HashPartInfrasVec *hp_infras_buffer_;
  int64_t hp_infras_buffer_idx_;
  HashPartInfrasVecList hp_infras_list_;
  HashPartInfrasVecList hp_infras_free_list_;
};

class ObHashPartInfrasVecMgr
{
public:
  static const int64_t MIN_BUCKET_COUNT = 1L << 1;  //2;
  static const int64_t MAX_BUCKET_COUNT = 1L << 19; //524288;
  ObHashPartInfrasVecMgr(const uint64_t tenant_id) :
    arena_alloc_("HPInfrasVec", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id), inited_(false),
    hp_infras_group_(arena_alloc_, tenant_id)
  {}
  ~ObHashPartInfrasVecMgr()
  {
    destroy();
  }
  int init(uint64_t tenant_id, bool enable_sql_dumped, const int64_t est_rows, const int64_t width,
           const bool unique, const int64_t ways, ObEvalCtx *eval_ctx,
           ObSqlMemMgrProcessor *sql_mem_processor, ObIOEventObserver *io_event_observer,
           common::ObCompressorType compressor_type);
  int reserve_hp_infras(const int64_t capacity)
  { return hp_infras_group_.reserve(capacity); }
  int init_one_hp_infras(const bool need_rewind, const ObSortCollations *sort_collations,
                         const common::ObIArray<ObExpr *> &exprs,
                         HashPartInfrasVec *&hp_infras);
  int free_one_hp_infras(HashPartInfrasVec *&hp_infras);

  OB_INLINE bool is_inited() const { return inited_; }
  void destroy()
  {
    inited_ = false;
    hp_infras_group_.reset();
    arena_alloc_.reset();
  }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHashPartInfrasVecMgr);

private:
  common::ObArenaAllocator arena_alloc_;
  bool inited_;
  ObHashPartInfrasVecGroup hp_infras_group_;
};


}  // namespace sql
}  // namespace oceanbase


#endif /* OB_HP_INFRASTRUCTURE_VEC_MANAGER_H_ */