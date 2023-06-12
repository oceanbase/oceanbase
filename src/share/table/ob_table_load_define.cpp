// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#define USING_LOG_PREFIX CLIENT

#include "ob_table_load_define.h"

namespace oceanbase
{
namespace table
{

OB_SERIALIZE_MEMBER_SIMPLE(ObTableLoadFlag, flag_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableLoadConfig,
                           session_count_,
                           batch_size_,
                           max_error_row_count_,
                           flag_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableLoadSegmentID,
                           id_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableLoadTransId,
                           segment_id_,
                           trans_gid_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableLoadPartitionId,
                           partition_id_,
                           tablet_id_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableLoadLSIdAndPartitionId,
                           ls_id_,
                           part_tablet_id_);

OB_SERIALIZE_MEMBER_SIMPLE(ObTableLoadResultInfo,
                           rows_affected_,
                           records_,
                           deleted_,
                           skipped_,
                           warnings_);

}  // namespace table
}  // namespace oceanbase
