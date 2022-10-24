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

#define USING_LOG_PREFIX STORAGE
#include "ob_tablets_transfer.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

/******************ObTabletsTransferDagNet*********************/
ObTabletsTransferDagNet::ObTabletsTransferDagNet()
 : ObIDagNet(ObDagNetType::DAG_NET_TYPE_TRANSFER),
   is_inited_(false),
   param_(),
   ctx_(nullptr)
{
}

ObTabletsTransferDagNet::~ObTabletsTransferDagNet()
{
}

int ObTabletsTransferDagNet::init_by_param(const share::ObIDagInitParam *param)
{
  UNUSED(param);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObTabletsTransferDagNet::start_running()
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

bool ObTabletsTransferDagNet::operator == (const ObIDagNet &other) const
{
  UNUSED(other);
  bool is_same = true;
  return is_same;
}

int64_t ObTabletsTransferDagNet::hash() const
{
  int64_t hash_value = 0;
  return hash_value;
}

int ObTabletsTransferDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObTabletsTransferDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

}
}

