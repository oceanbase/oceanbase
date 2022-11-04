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

#include "sql/executor/ob_task_location.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObTaskLocation::ObTaskLocation(const ObAddr &server, const ObTaskID &ob_task_id) :
    server_(server),
    ob_task_id_(ob_task_id)
{
}

ObTaskLocation::ObTaskLocation() :
    server_(),
    ob_task_id_()
{
}

void ObTaskLocation::reset()
{
  server_.reset();
  ob_task_id_.reset();
}

ObTaskLocation& ObTaskLocation::operator=(const ObTaskLocation &task_location)
{
  server_ = task_location.server_;
  ob_task_id_ = task_location.ob_task_id_;
  return *this;
}

OB_SERIALIZE_MEMBER(ObTaskLocation, server_, ob_task_id_)

}/* ns sql*/
}/* ns oceanbase */




