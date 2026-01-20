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

#include "share/catalog/rest/client/ob_base_rest_client.h"

#define USING_LOG_PREFIX SHARE

namespace oceanbase
{
namespace share
{

int ObBaseRestClient::init(const ObRestCatalogProperties &properties)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("base rest client already initialized");
  } else if (OB_FAIL(ob_write_string(allocator_, properties.uri_, uri_, true /*c_style*/))) {
    LOG_WARN("failed to write uri", K(ret), K(properties.uri_));
  } else if (OB_FAIL(do_init(properties))) {
    LOG_WARN("failed to init client", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBaseRestClient::destroy()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("base rest client not initialized", K(ret));
  } else if (OB_FAIL(do_destroy())) {
    LOG_WARN("failed to destroy", K(ret));
  } else {
    is_inited_ = false;
    client_pool_ = nullptr;
    allocator_.free(uri_.ptr());
    uri_.reset();
  }
  return ret;
}

int ObBaseRestClient::reuse()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("base rest client not initialized", K(ret));
  } else if (OB_FAIL(do_reuse())) {
    LOG_WARN("failed to reuse", K(ret));
  }
  return ret;
}

}
}
