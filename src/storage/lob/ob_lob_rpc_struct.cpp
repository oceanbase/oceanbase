// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "share/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_task_define.h"
#include "share/ob_force_print_log.h"
#include "ob_lob_rpc_struct.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
using namespace storage;

namespace obrpc
{

ObLobQueryBlock::ObLobQueryBlock()
  : size_(0)
{
}

void ObLobQueryBlock::reset()
{
  size_ = 0;
}

bool ObLobQueryBlock::is_valid() const
{
  return size_ > 0;
}

OB_SERIALIZE_MEMBER(ObLobQueryBlock, size_);


OB_DEF_SERIALIZE_SIZE(ObLobQueryArg)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              offset_,
              len_,
              cs_type_,
              scan_backward_,
              qtype_,
              lob_locator_);
  return len;
}

OB_DEF_SERIALIZE(ObLobQueryArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              offset_,
              len_,
              cs_type_,
              scan_backward_,
              qtype_,
              lob_locator_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLobQueryArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              offset_,
              len_,
              cs_type_,
              scan_backward_,
              qtype_,
              lob_locator_);
  return ret;
}

ObLobQueryArg::ObLobQueryArg()
  : tenant_id_(0),
    offset_(0),
    len_(0),
    cs_type_(common::ObCollationType::CS_TYPE_INVALID),
    scan_backward_(false),
    qtype_(QueryType::READ),
    lob_locator_()
{}

ObLobQueryArg::~ObLobQueryArg()
{
}


} // obrpc

} // oceanbase