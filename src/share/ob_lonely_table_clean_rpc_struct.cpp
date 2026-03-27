/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_lonely_table_clean_rpc_struct.h"

namespace oceanbase
{
namespace obrpc
{

OB_SERIALIZE_MEMBER((ObForceDropLonelyLobAuxTableArg, ObDDLArg), tenant_id_, data_table_id_, aux_lob_meta_table_id_, aux_lob_piece_table_id_);

}//end namespace obrpc
}//end namespace oceanbase
