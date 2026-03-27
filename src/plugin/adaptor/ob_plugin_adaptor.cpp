/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "plugin/adaptor/ob_plugin_adaptor.h"

namespace oceanbase {
namespace plugin {

ObPluginVersionAdaptor::ObPluginVersionAdaptor(uint16_t major, uint16_t minor, uint16_t patch)
    : version_(OBP_MAKE_VERSION(major, minor, patch))
{}

ObPluginVersionAdaptor::ObPluginVersionAdaptor(int64_t version)
    : ObPluginVersionAdaptor(static_cast<ObPluginVersion>(version))
{}
ObPluginVersionAdaptor::ObPluginVersionAdaptor(ObPluginVersion version)
    : version_(version)
{}

ObPluginVersion ObPluginVersionAdaptor::version() const
{
  return version_;
}

ObPluginVersion ObPluginVersionAdaptor::major() const
{
  return version_ / OBP_VERSION_FIELD_NUMBER / OBP_VERSION_FIELD_NUMBER;
}

ObPluginVersion ObPluginVersionAdaptor::minor() const
{
  return (version_ / OBP_VERSION_FIELD_NUMBER) % OBP_VERSION_FIELD_NUMBER;
}

ObPluginVersion ObPluginVersionAdaptor::patch() const
{
  return version_ % OBP_VERSION_FIELD_NUMBER;
}

int64_t ObPluginVersionAdaptor::to_string(char buf[], int64_t buf_len) const
{
  return snprintf(buf, buf_len, "%lu.%lu.%lu", major(), minor(), patch());
}

} // namespace plugin
} // namespace oceanbase
