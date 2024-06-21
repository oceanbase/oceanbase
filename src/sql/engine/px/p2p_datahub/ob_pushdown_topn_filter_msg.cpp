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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/px/p2p_datahub/ob_pushdown_topn_filter_msg.h"

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER(ObTopNFilterCmpMeta, ser_cmp_func_, obj_meta_);
OB_SERIALIZE_MEMBER(ObTopNFilterCompare, build_meta_, filter_meta_, is_ascending_, null_pos_);
OB_SERIALIZE_MEMBER(ObPushDownTopNFilterInfo, enabled_, p2p_dh_id_, effective_sk_cnt_,
                    total_sk_cnt_, cmp_metas_, dh_msg_type_, expr_ctx_id_, is_shared_, is_shuffle_,
                    max_batch_size_, adaptive_filter_ratio_);
} // end namespace sql
} // end namespace oceanbase
