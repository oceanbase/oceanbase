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

#define USING_LOG_PREFIX SHARE
#include "share/catalog/odps/ob_odps_catalog_utils.h"

#include "share/catalog/ob_catalog_properties.h"

namespace oceanbase
{
namespace share
{

#ifdef OB_BUILD_CPP_ODPS
int ObODPSCatalogUtils::create_odps_conf(const ObODPSCatalogProperties &odps_format, apsara::odps::sdk::Configuration &conf)
{
  int ret = OB_SUCCESS;
  apsara::odps::sdk::Account account;

  if (0 == odps_format.access_type_.case_compare("aliyun") || odps_format.access_type_.empty()) {
    account = apsara::odps::sdk::Account(std::string(apsara::odps::sdk::ACCOUNT_ALIYUN),
                                         std::string(odps_format.access_id_.ptr(), odps_format.access_id_.length()),
                                         std::string(odps_format.access_key_.ptr(), odps_format.access_key_.length()));
  } else if (0 == odps_format.access_type_.case_compare("sts")) {
    account = apsara::odps::sdk::Account(std::string(apsara::odps::sdk::ACCOUNT_STS),
                                         std::string(odps_format.sts_token_.ptr(), odps_format.sts_token_.length()));
  } else if (0 == odps_format.access_type_.case_compare("token")) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported access type", K(ret), K(odps_format.access_type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS access type: token");
  } else if (0 == odps_format.access_type_.case_compare("domain")) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported access type", K(ret), K(odps_format.access_type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS access type: domain");
  } else if (0 == odps_format.access_type_.case_compare("taobao")) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported access type", K(ret), K(odps_format.access_type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS access type: taobao");
  } else if (0 == odps_format.access_type_.case_compare("app")) {
    account = apsara::odps::sdk::Account(std::string(apsara::odps::sdk::ACCOUNT_APPLICATION),
                                         std::string(odps_format.access_id_.ptr(), odps_format.access_id_.length()),
                                         std::string(odps_format.access_key_.ptr(), odps_format.access_key_.length()));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS access type");
  }
  conf.SetAccount(account);
  conf.SetEndpoint(std::string(odps_format.endpoint_.ptr(), odps_format.endpoint_.length()));
  if (!odps_format.tunnel_endpoint_.empty()) {
    LOG_TRACE("set tunnel endpoint", K(ret), K(odps_format.tunnel_endpoint_));
    conf.SetTunnelEndpoint(std::string(odps_format.tunnel_endpoint_.ptr(), odps_format.tunnel_endpoint_.length()));
  }
  conf.SetUserAgent("OB_ACCESS_ODPS");
  conf.SetTunnelQuotaName(std::string(odps_format.quota_.ptr(), odps_format.quota_.length()));
  if (0 == odps_format.compression_code_.case_compare("zlib")) {
    conf.SetCompressOption(apsara::odps::sdk::CompressOption::ZLIB_COMPRESS);
  } else if (0 == odps_format.compression_code_.case_compare("zstd")) {
    conf.SetCompressOption(apsara::odps::sdk::CompressOption::ZSTD_COMPRESS);
  } else if (0 == odps_format.compression_code_.case_compare("lz4")) {
    conf.SetCompressOption(apsara::odps::sdk::CompressOption::LZ4_COMPRESS);
  } else if (0 == odps_format.compression_code_.case_compare("odps_lz4")) {
    conf.SetCompressOption(apsara::odps::sdk::CompressOption::ODPS_LZ4_COMPRESS);
  } else {
    conf.SetCompressOption(apsara::odps::sdk::CompressOption::NO_COMPRESS);
  }

  return ret;
}
#endif

} // namespace share
} // namespace oceanbase