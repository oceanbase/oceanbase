/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_tenant_meta_obj_pool.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

constexpr const char RPMetaObjLabel::LABEL[];


TryWashTabletFunc::TryWashTabletFunc(ObTenantMetaMemMgr &t3m)
  : t3m_(t3m)
{
}

TryWashTabletFunc::~TryWashTabletFunc()
{
}

int TryWashTabletFunc::operator()(const std::type_info &type_info, void *&free_obj)
{
  free_obj = nullptr;
  return t3m_.try_wash_tablet(type_info, free_obj);
}

ObMetaObjBufferNode &ObMetaObjBufferHelper::get_linked_node(char *obj)
{
  ObMetaObjBufferNode *header = reinterpret_cast<ObMetaObjBufferNode *>(obj - sizeof(ObMetaObjBufferNode));
  abort_unless(nullptr != header);
  abort_unless(ObMetaObjBufferHeader::MAGIC_NUM == header->get_data().magic_num_);
  return *header;
}

ObMetaObjBufferHeader &ObMetaObjBufferHelper::get_buffer_header(char *obj)
{
  return get_linked_node(obj).get_data();
}

char *ObMetaObjBufferHelper::get_obj_buffer(ObMetaObjBufferNode *node)
{
  abort_unless(nullptr != node);
  abort_unless(ObMetaObjBufferHeader::MAGIC_NUM == node->get_data().magic_num_);
  return reinterpret_cast<char *>(node) + sizeof(ObMetaObjBufferNode);
}

void *ObMetaObjBufferHelper::get_meta_obj_buffer_ptr(char *obj)
{
  return static_cast<void *>(&get_linked_node(obj));
}

} // end namespace storage
} // end namespace oceanbase
