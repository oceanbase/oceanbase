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

#pragma once
#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"

namespace oceanbase
{
namespace sql
{

struct ObTopNFilterCmpMeta final
{
  OB_UNIS_VERSION_V(1);
public:
  union
  {
    NullSafeRowCmpFunc cmp_func_;
    sql::serializable_function ser_cmp_func_;
  };
  ObObjMeta obj_meta_;
  TO_STRING_KV(K(obj_meta_), K(ser_cmp_func_));
};

struct ObTopNFilterCompare final
{
  OB_UNIS_VERSION_V(1);
public:
  // in join scene, the join cond is T1.a=T2.b, sometimes a and b are not same type but not need to
  // cast if the sql with order by T1.a, the topn filter can pushdown to T2.b, but the compare info
  // is differnet in build stage and filter stage
  ObTopNFilterCmpMeta build_meta_;
  ObTopNFilterCmpMeta filter_meta_;
  bool is_ascending_;
  common::ObCmpNullPos null_pos_;
  TO_STRING_KV(K(build_meta_), K(filter_meta_), K(is_ascending_), K(null_pos_));
};
typedef common::ObFixedArray<ObTopNFilterCompare, common::ObIAllocator> ObTopNFilterCompares;

struct ObPushDownTopNFilterInfo
{
  OB_UNIS_VERSION(1);

public:
  explicit ObPushDownTopNFilterInfo(common::ObIAllocator &alloc)
      : enabled_(false), p2p_dh_id_(OB_INVALID), effective_sk_cnt_(0), total_sk_cnt_(0),
        cmp_metas_(alloc), dh_msg_type_(ObP2PDatahubMsgBase::ObP2PDatahubMsgType::NOT_INIT),
        expr_ctx_id_(UINT32_MAX /*INVALID_EXP_CTX_ID*/), is_shared_(false), is_shuffle_(false),
        max_batch_size_(0), adaptive_filter_ratio_(0.5)
  {}
public:
  bool enabled_;
  int64_t p2p_dh_id_;
  int64_t effective_sk_cnt_;
  int64_t total_sk_cnt_;
  ObFixedArray<ObTopNFilterCmpMeta, common::ObIAllocator> cmp_metas_;
  ObP2PDatahubMsgBase::ObP2PDatahubMsgType dh_msg_type_;
  uint32_t expr_ctx_id_;
  bool is_shared_; // whether the filter is shared in sql level
  bool is_shuffle_; // whether need shuffle topn msg between differnet dfos
  int64_t max_batch_size_;
  double adaptive_filter_ratio_;
  TO_STRING_KV(K(enabled_), K(p2p_dh_id_), K(dh_msg_type_), K(expr_ctx_id_), K(is_shared_),
               K(is_shuffle_), K(max_batch_size_), K(adaptive_filter_ratio_));
};

} // end namespace sql
} // end namespace oceanbase
