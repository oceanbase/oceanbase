/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "plugin/share/ob_properties.h"

namespace oceanbase {

namespace json {
struct Value;
};

namespace plugin {

class ObIExternalDescriptor;
class ObExternalDataEngine;

/**
 * @brief Helper for external file format and caching data engine
 * @details see @class ObExternalFileFormat for details.
 */
class ObPluginFormat final
{
public:
  ObPluginFormat() = default;
  ~ObPluginFormat() { destroy(); }

  int init(ObIAllocator &allocator, bool encrypt_mode = true);
  void destroy();

  /**
   * Helper function: create external data engine
   */
  int create_engine(ObIAllocator &allocator, ObExternalDataEngine *&engine) const;

  int set_parameters(const ObString &parameters);
  const ObString &parameters() const { return parameters_; }

  int64_t to_json_string(char buf[], int64_t len) const;
  int load_from_json_node(json::Pair *node);

  int set_type_name(const ObString &name);

  const ObString &type_name() const { return type_name_; }

private:
  int set_string_value(const ObString &src, ObString &dst);
  int encrypt(const ObString &src, ObString &dst);
  int decrypt(const ObString &src, ObString &dst);
private:
  bool inited_ = false;

  ObString  parameters_; // the source parameters string
  ObString  type_name_;

  // The parameters string to be stored, encrypted and hex.
  ObString  stored_parameters_;

  ObIAllocator *        allocator_ = nullptr;

  bool encrypt_mode_ = true; // for unittest
};

} // namespace plugin
} // namespace oceanbase
