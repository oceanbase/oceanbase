/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_table_range.h"


namespace oceanbase
{

namespace share
{
const SCN ObScnRange::MIN_SCN = SCN::min_scn();
const SCN ObScnRange::MAX_SCN = SCN::max_scn();

ObScnRange::ObScnRange()
  : start_scn_(MIN_SCN),
    end_scn_(MIN_SCN)
{
}

OB_DEF_SERIALIZE(ObScnRange)
{
  using oceanbase::common::serialization::encode;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, start_scn_, end_scn_);
  return ret;
}

OB_DEF_DESERIALIZE(ObScnRange)
{
  using oceanbase::common::serialization::decode;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, start_scn_, end_scn_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObScnRange)
{
  using oceanbase::common::serialization::encoded_length;
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, start_scn_, end_scn_);
  return len;
}

int64_t ObScnRange::hash() const
{
  int64_t hash_value = 0;
  hash_value = common::murmurhash(&start_scn_, sizeof(start_scn_), hash_value);
  hash_value = common::murmurhash(&end_scn_, sizeof(end_scn_), hash_value);
  return hash_value;
}


} //share
} //oceanbase
