/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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




