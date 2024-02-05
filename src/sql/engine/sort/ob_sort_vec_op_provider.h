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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_PROVIDER_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_PROVIDER_H_

#include "sql/engine/sort/ob_sort_vec_op_impl.h"
#include "sql/engine/sort/ob_prefix_sort_vec_op_impl.h"

namespace oceanbase {
namespace sql {
// Vectorization 2.0 sort implementation provider class
class ObSortVecOpProvider final
{
public:
  enum SortType : int32_t
  {
    SORT_TYPE_INVALID = -1,
    SORT_TYPE_GENERAL,
    SORT_TYPE_PREFIX,
    SORT_TYPE_MAX
  };
  enum SortKeyType : int32_t
  {
    SK_TYPE_INVALID = -1,
    SK_TYPE_GENERAL,
    SK_TYPE_TOPN,
    SK_TYPE_MAX
  };

  explicit ObSortVecOpProvider(ObMonitorNode &op_monitor_info) :
    mem_context_(nullptr), mem_entify_guard_(mem_context_), op_monitor_info_(op_monitor_info),
    alloc_(nullptr), is_inited_(false), has_addon_(false), is_basic_cmp_(false),
    sort_type_(SORT_TYPE_INVALID), sort_key_type_(SK_TYPE_INVALID), sort_op_impl_(nullptr)
  {}
  ~ObSortVecOpProvider();
  void check_status() const
  {
#ifndef NDEBUG
    abort_unless(sort_op_impl_ != nullptr);
#endif
  }
  bool is_inited() const
  {
    return is_inited_;
  }
  void reset();
  int sort();
  int init(ObSortVecOpContext &context);
  int add_batch(const ObBatchRows &input_brs, bool &sort_need_dump);
  int get_next_batch(const int64_t max_cnt, int64_t &read_rows);
  int add_batch_stored_row(int64_t &row_size, const ObCompactRow **sk_stored_rows,
                           const ObCompactRow **addon_stored_rows);
  int64_t get_extra_size(bool is_sort_key);
  void unregister_profile();
  void unregister_profile_if_necessary();
  void collect_memory_dump_info(ObMonitorNode &info);
  void set_input_rows(int64_t input_rows);
  void set_input_width(int64_t input_width);
  void set_operator_type(ObPhyOperatorType op_type);
  void set_operator_id(uint64_t op_id);
  void set_io_event_observer(ObIOEventObserver *observer);
  common::ObIAllocator *get_malloc_allocator() { return alloc_; }

private:
  int init_mem_context(uint64_t tenant_id);
  int init_sort_impl(ObSortVecOpContext &context, ObISortVecOpImpl *&sort_op_impl);
  int decide_sort_key_type(ObSortVecOpContext &context);
  bool is_basic_cmp_type(VecValueTypeClass vec_tc);
  template <typename SORT_CLASS>
  int alloc_sort_impl_instance(ObISortVecOpImpl *&sort_impl);
  template <SortType sort_type, SortKeyType sk_type, bool has_addon>
  int init_sort_impl_instance(ObISortVecOpImpl *&sort_op_impl);
  class MemEntifyFreeGuard
  {
  public:
    explicit MemEntifyFreeGuard(lib::MemoryContext &entify) : entify_(entify)
    {}
    ~MemEntifyFreeGuard()
    {
      if (nullptr != entify_) {
        DESTROY_CONTEXT(entify_);
        entify_ = nullptr;
      }
    }
    lib::MemoryContext &entify_;
  };

private:
  lib::MemoryContext mem_context_;
  MemEntifyFreeGuard mem_entify_guard_;
  ObMonitorNode &op_monitor_info_;
  common::ObIAllocator *alloc_;
  bool is_inited_;
  bool has_addon_;
  bool is_basic_cmp_;
  SortType sort_type_;
  SortKeyType sort_key_type_;
  ObISortVecOpImpl *sort_op_impl_;
};

using SortType = ObSortVecOpProvider::SortType;
using SortKeyType = ObSortVecOpProvider::SortKeyType;

template <SortKeyType sk_type, bool has_addon>
struct RTSortKeyTraits
{
  using SKType = ObSortKeyStore<true>;
};

template <bool has_addon>
struct RTSortKeyTraits<ObSortVecOpProvider::SK_TYPE_GENERAL, has_addon>
{
  using SKType = ObSortKeyStore<has_addon>;
};

template <bool has_addon>
struct RTSortKeyTraits<ObSortVecOpProvider::SK_TYPE_TOPN, has_addon>
{
  using SKType = ObTopNSortKey<has_addon>;
};

template <SortKeyType sk_type, bool has_addon>
using RTSKType = typename RTSortKeyTraits<sk_type, has_addon>::SKType;

template <bool is_basic_cmp, SortKeyType sk_type, bool has_addon>
struct RTSortCmpTraits
{
  using SortCmpType = GeneralCompare<RTSKType<sk_type, has_addon>, has_addon>;
};

template <SortKeyType sk_type, bool has_addon>
struct RTSortCmpTraits<false, sk_type, has_addon>
{
  using SortCmpType = GeneralCompare<RTSKType<sk_type, has_addon>, has_addon>;
};

template <SortKeyType sk_type, bool has_addon>
struct RTSortCmpTraits<true, sk_type, has_addon>
{
  using SortCmpType = FixedCompare<RTSKType<sk_type, has_addon>, has_addon>;
};

template <bool is_basic_cmp, SortKeyType sk_type, bool has_addon>
using RTSortCmpType =
  typename RTSortCmpTraits<is_basic_cmp, sk_type, has_addon>::SortCmpType;

template <SortType sort_type, bool is_basic_cmp, SortKeyType sk_type, bool has_addon>
struct RTSortImplTraits
{
  using SortImplType = ObSortVecOpImpl<RTSortCmpType<is_basic_cmp, sk_type, has_addon>,
                                       RTSKType<sk_type, has_addon>, has_addon>;
};

template <bool is_basic_cmp, SortKeyType sk_type, bool has_addon>
struct RTSortImplTraits<ObSortVecOpProvider::SORT_TYPE_GENERAL, is_basic_cmp, sk_type, has_addon>
{
  using SortImplType = ObSortVecOpImpl<RTSortCmpType<is_basic_cmp, sk_type, has_addon>,
                                       RTSKType<sk_type, has_addon>, has_addon>;
};

template <bool is_basic_cmp, SortKeyType sk_type, bool has_addon>
struct RTSortImplTraits<ObSortVecOpProvider::SORT_TYPE_PREFIX, is_basic_cmp, sk_type, has_addon>
{
  using SortImplType = ObPrefixSortVecImpl<RTSortCmpType<is_basic_cmp, sk_type, has_addon>,
                                           RTSKType<sk_type, has_addon>, has_addon>;
};

template <SortType sort_type, bool is_basic_cmp, SortKeyType sk_type, bool has_addon>
using RTSortImplType =
  typename RTSortImplTraits<sort_type, is_basic_cmp, sk_type, has_addon>::SortImplType;

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_PROVIDER_H_ */
