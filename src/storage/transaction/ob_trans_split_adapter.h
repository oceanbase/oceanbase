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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_SPLIT_ADAPTER_H
#define OCEANBASE_TRANSACTION_OB_TRANS_SPLIT_ADAPTER_H

//#include "share/ob_partition_modify.h"
#include "common/ob_partition_key.h"

namespace oceanbase {

namespace common {
template <typename T>
class ObIArray;
class ObMaskSet;
}  // namespace common

namespace storage {
class ObPartitionService;
}
namespace transaction {

class ObCoordTransCtx;
class ObPartTransCtx;
class ObTrx2PCPrepareResponse;

class ObTransSplitInfo {
  OB_UNIS_VERSION(1);

public:
  int init(const common::ObPartitionKey& src_pk, const common::ObIArray<common::ObPartitionKey>& dest_pks);
  bool is_valid() const
  {
    return src_pk_.is_valid() && dest_pks_.count() > 0;
  }
  void reset()
  {
    src_pk_.reset();
    dest_pks_.reset();
  }
  const common::ObPartitionArray& get_dest_array() const
  {
    return dest_pks_;
  }
  int assign(const ObTransSplitInfo& other);
  TO_STRING_KV(K_(src_pk), K_(dest_pks));

private:
  common::ObPartitionKey src_pk_;
  common::ObPartitionArray dest_pks_;
};

typedef common::ObSEArray<ObTransSplitInfo, 2> ObTransSplitInfoArray;

class ObTransSplitAdapter {
public:
  static int update_participant_list(ObCoordTransCtx* ctx, common::ObPartitionArray& ctx_participants,
      common::ObMaskSet& ctx_mask_set, const ObTransSplitInfoArray& split_info_arr,
      common::ObPartitionArray& new_participants);
  static int set_split_info_for_prepare_response(const storage::ObPartitionService* partition_service,
      const common::ObPartitionKey& pk, ObTrx2PCPrepareResponse& res);
  static int find_corresponding_split_info(
      const common::ObPartitionKey& dest_pk, const ObTransSplitInfoArray& split_info_arr, ObTransSplitInfo& split_info);
};

}  // namespace transaction
}  // namespace oceanbase
#endif