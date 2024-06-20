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

#define USING_LOG_PREFIX CLIENT

#include "ob_table_load_define.h"

namespace oceanbase
{
namespace table
{

OB_SERIALIZE_MEMBER(ObTableLoadConfig,
                    parallel_,
                    batch_size_,
                    max_error_row_count_,
                    dup_action_,
                    is_need_sort_);

OB_SERIALIZE_MEMBER(ObTableLoadSegmentID,
                    id_);

OB_SERIALIZE_MEMBER(ObTableLoadTransId,
                    segment_id_,
                    trans_gid_);

OB_SERIALIZE_MEMBER(ObTableLoadPartitionId,
                    partition_id_,
                    tablet_id_);

OB_SERIALIZE_MEMBER(ObTableLoadLSIdAndPartitionId,
                    ls_id_,
                    part_tablet_id_);

OB_SERIALIZE_MEMBER(ObTableLoadResultInfo,
                    rows_affected_,
                    records_,
                    deleted_,
                    skipped_,
                    warnings_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableLoadSequenceNo, sequence_no_);

}  // namespace table
}  // namespace oceanbase
