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

#include "share/catalog/rest/responses/ob_rest_http_response.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"

namespace oceanbase
{
namespace share
{
int ObRestHttpResponse::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_reset())) {
    LOG_WARN("failed to reset", K(ret));
  } else {
    body_.reset();
    status_code_ = 0;
    content_length_ = 0;
    content_type_.reset();
  }
  return ret;
}

int ObRestHttpResponse::do_reset()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObRestHttpResponse::parse_from_response()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObRestHttpResponse::parse_json_to_map(const ObIJsonBase *json_node,
                                          common::hash::ObHashMap<ObString, ObString> &map,
                                          ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(json_node) || ObJsonNodeType::J_OBJECT != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json node", K(ret));
  } else {
    const ObJsonObject *json_object = down_cast<const ObJsonObject *>(json_node);
    JsonObjectIterator iter = json_object->object_iterator();
    while (OB_SUCC(ret) && !iter.end()) {
      ObString key;
      ObIJsonBase *value = NULL;
      if (OB_FAIL(iter.get_key(key))) {
        LOG_WARN("failed to get key from json node", K(ret));
      } else if (OB_FAIL(iter.get_value(value))) {
        LOG_WARN("failed to get value from json node", K(ret));
      } else if (OB_ISNULL(value) || ObJsonNodeType::J_STRING != value->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid json node", K(ret));
      } else {
        ObString str_value(value->get_data_length(), value->get_data());
        ObString copy_key;
        ObString copy_value;
        if (OB_FAIL(ob_write_string(allocator, key, copy_key, true/*c_style*/))) {
          LOG_WARN("failed to copy key", K(key), K(ret));
        } else if (OB_FAIL(ob_write_string(allocator, str_value, copy_value, true/*c_style*/))) {
          LOG_WARN("failed to copy value", K(str_value), K(ret));
        } else if (OB_FAIL(map.set_refactored(copy_key, copy_value, 1))) {
          LOG_WARN("failed to set map", K(copy_key), K(copy_value), K(ret));
        }
      }
      iter.next();
    }
  }
  return ret;
}

int ObRestHttpResponse::parse_json_to_array(const ObIJsonBase *json_node,
                                            ObArray<ObString> &array,
                                            ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(json_node) || ObJsonNodeType::J_ARRAY != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json node", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < json_node->element_count(); i++) {
      ObIJsonBase *value_node = NULL;
      if (OB_FAIL(json_node->get_array_element(i, value_node))) {
        LOG_WARN("failed to get array element", K(i), K(ret));
      } else if (OB_ISNULL(value_node) || ObJsonNodeType::J_STRING != value_node->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid json node", K(i), K(ret));
      } else {
        ObString str_value(value_node->get_data_length(), value_node->get_data());
        ObString copy_str_value;
        if (OB_FAIL(ob_write_string(allocator, str_value, copy_str_value, true/*c_style*/))) {
          LOG_WARN("failed to copy string", K(str_value), K(ret));
        } else if (OB_FAIL(array.push_back(copy_str_value))) {
          LOG_WARN("failed to add string to array", K(copy_str_value), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRestHttpResponse::parse_json_to_string(const ObIJsonBase *json_node,
                                             ObString &string,
                                             ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(json_node) || ObJsonNodeType::J_STRING != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json node", K(ret));
  } else {
    ObString tmp_str = ObString(json_node->get_data_length(), json_node->get_data());
    if (OB_ISNULL(allocator)) {
      string.assign(tmp_str.ptr(), tmp_str.length());
    } else if (OB_FAIL(ob_write_string(*allocator, tmp_str, string, true/*c_style*/))) {
      LOG_WARN("failed to copy string", K(tmp_str), K(ret));
    }
  }
  return ret;
}

int ObRestConfigResponse::parse_from_response()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(default_.create(DEFAULT_MAP_BUCKET_NUM, lib::ObLabel("Default")))) {
    LOG_WARN("failed to init default", K(ret));
  } else if (OB_FAIL(override_.create(DEFAULT_MAP_BUCKET_NUM, lib::ObLabel("Override")))) {
    LOG_WARN("failed to init override", K(ret));
  }

  ObJsonNode *json_node = NULL;
  ObJsonObject *json_object = NULL;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObJsonParser::get_tree(&allocator_, body_.string(), json_node))) {
    LOG_WARN("failed to get tree", K(ret));
  } else if (OB_ISNULL(json_node) || ObJsonNodeType::J_OBJECT != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json value", K(ret));
  } else {
    json_object = down_cast<ObJsonObject *>(json_node);
  }

  // Parse defaults section, required
  if (OB_SUCC(ret)) {
    const ObJsonNode *json_defaults = json_object->get_value(DEFAULT_CONFIG_KEY);
    if (OB_ISNULL(json_defaults)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("defaults is null", K(ret));
    } else if (OB_FAIL(parse_json_to_map(json_defaults, default_, allocator_))) {
      LOG_WARN("failed to parse defaults", K(ret));
    }
  }

  // Parse overrides section, required
  if (OB_SUCC(ret)) {
    const ObJsonNode *json_overrides = json_object->get_value(OVERRIDE_CONFIG_KEY);
    if (OB_ISNULL(json_overrides)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("overrides is null", K(ret));
    } else if (OB_FAIL(parse_json_to_map(json_overrides, override_, allocator_))) {
      LOG_WARN("failed to parse overrides", K(ret));
    }
  }

  // Parse endpoints section, not required
  if (OB_SUCC(ret)) {
    const ObJsonNode *json_endpoints = json_object->get_value(ENDPOINTS_KEY);
    if (OB_NOT_NULL(json_endpoints) && OB_FAIL(parse_json_to_array(json_endpoints, endpoints_, allocator_))) {
      LOG_WARN("failed to parse endpoints", K(ret));
    }
  }
  return ret;
}

int ObRestConfigResponse::do_reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(default_.destroy())) {
    LOG_WARN("failed to destroy default", K(ret));
  } else if (OB_FAIL(override_.destroy())) {
    LOG_WARN("failed to destroy override", K(ret));
  } else {
    endpoints_.reset();
  }
  return ret;
}

int ObRestListNamespacesResponse::parse_from_response()
{
  int ret = OB_SUCCESS;
  ObJsonNode *json_node = NULL;
  ObJsonObject *json_object = NULL;

  if (OB_FAIL(ObJsonParser::get_tree(&allocator_, body_.string(), json_node))) {
    LOG_WARN("failed to get tree", K(ret));
  } else if (OB_ISNULL(json_node) || ObJsonNodeType::J_OBJECT != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json value", K(ret));
  } else {
    json_object = down_cast<ObJsonObject *>(json_node);
  }

  // Parse namespaces section, required
  if (OB_SUCC(ret)) {
    const ObJsonNode *json_namespaces = json_object->get_value(NAMESPACES_KEY);
    if (OB_ISNULL(json_namespaces) || ObJsonNodeType::J_ARRAY != json_namespaces->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid namespaces json value", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < json_namespaces->element_count(); i++) {
        ObIJsonBase *namespace_item = NULL;
        if (OB_FAIL(json_namespaces->get_array_element(i, namespace_item))) {
          LOG_WARN("failed to get array element", K(i), K(ret));
        } else if (OB_ISNULL(namespace_item) || ObJsonNodeType::J_ARRAY != namespace_item->json_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid namespace element", K(i), K(ret));
        } else {
          ObSqlString full_namespace_str;
          for (int64_t j = 0; OB_SUCC(ret) && j < namespace_item->element_count(); j++) {
            ObIJsonBase *namespace_item_element = NULL;
            ObString tmp_str;
            if (OB_FAIL(namespace_item->get_array_element(j, namespace_item_element))) {
              LOG_WARN("failed to get array element", K(i), K(j), K(ret));
            } else if (OB_FAIL(parse_json_to_string(namespace_item_element, tmp_str))) {
              LOG_WARN("failed to parse json to string", K(i), K(j), K(ret));
            } else if (!full_namespace_str.empty() && OB_FAIL(full_namespace_str.append("."))) {
              LOG_WARN("failed to append separator to string", K(ret));
            } else if (OB_FAIL(full_namespace_str.append(tmp_str))) {
              LOG_WARN("failed to append namespace to string", K(tmp_str), K(ret));
            }
          }

          ObString copy_full_namespace_str;
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(ob_write_string(allocator_, full_namespace_str.string(), copy_full_namespace_str, true/*c_style*/))) {
            LOG_WARN("failed to copy full namespace string", K(full_namespace_str), K(ret));
          } else if (OB_FAIL(namespaces_.push_back(copy_full_namespace_str))) {
            LOG_WARN("failed to add namespace to array", K(copy_full_namespace_str), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRestListNamespacesResponse::do_reset()
{
  int ret = OB_SUCCESS;
  namespaces_.reset();
  return ret;
}

int ObRestGetNamespaceResponse::parse_from_response()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(properties_.create(DEFAULT_MAP_BUCKET_NUM, lib::ObLabel("Properties")))) {
    LOG_WARN("failed to create properties", K(ret));
  }

  ObJsonNode *json_node = NULL;
  ObJsonObject *json_object = NULL;

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObJsonParser::get_tree(&allocator_, body_.string(), json_node))) {
    LOG_WARN("failed to get tree", K(ret));
  } else if (OB_ISNULL(json_node) || ObJsonNodeType::J_OBJECT != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json value", K(ret));
  } else {
    json_object = down_cast<ObJsonObject *>(json_node);
  }

  // Parse database name section, required
  if (OB_SUCC(ret)) {
    const ObJsonNode *json_database = json_object->get_value(DATABASE_KEY);
    if (OB_ISNULL(json_database) || ObJsonNodeType::J_ARRAY != json_database->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid database name json value", K(ret));
    } else {
      ObSqlString database_name_str;
      for (int64_t i = 0; OB_SUCC(ret) && i < json_database->element_count(); i++) {
        ObIJsonBase *database_item = NULL;
        ObString tmp_str;
        if (OB_FAIL(json_database->get_array_element(i, database_item))) {
          LOG_WARN("failed to get array element", K(i), K(ret));
        } else if (OB_FAIL(parse_json_to_string(database_item, tmp_str))) {
          LOG_WARN("failed to parse json to string", K(i), K(ret));
        } else if (!database_name_str.empty() && OB_FAIL(database_name_str.append("."))) {
            LOG_WARN("failed to append separator to string", K(ret));
        } else if (OB_FAIL(database_name_str.append(tmp_str))) {
          LOG_WARN("failed to append database name", K(tmp_str), K(ret));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(ob_write_string(allocator_, database_name_str.string(), database_name_, true/*c_style*/))) {
        LOG_WARN("failed to copy database name", K(database_name_str), K(ret));
      }
    }
  }

  // Parse properties section, not required
  if (OB_SUCC(ret)) {
    const ObJsonNode *json_properties = json_object->get_value(PROPERTIES_KEY);
    if (OB_ISNULL(json_properties)) {
    } else if (OB_FAIL(parse_json_to_map(json_properties, properties_, allocator_))) {
      LOG_WARN("failed to parse properties", K(ret));
    }
  }
  return ret;
}

int ObRestGetNamespaceResponse::do_reset()
{
  int ret = OB_SUCCESS;
  database_name_.reset();
  OZ (properties_.destroy());
  return ret;
}

int ObRestListTablesResponse::parse_from_response()
{
  int ret = OB_SUCCESS;
  ObJsonNode *json_node = NULL;
  ObJsonObject *json_object = NULL;

  if (OB_FAIL(ObJsonParser::get_tree(&allocator_, body_.string(), json_node))) {
    LOG_WARN("failed to get tree", K(ret));
  } else if (OB_ISNULL(json_node) || ObJsonNodeType::J_OBJECT != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json value", K(ret));
  } else {
    json_object = down_cast<ObJsonObject *>(json_node);
  }

  // Parse identifiers section, required
  if (OB_SUCC(ret)) {
    const ObJsonNode *json_identifiers = json_object->get_value(IDENTIFIERS_KEY);
    if (OB_ISNULL(json_identifiers) || ObJsonNodeType::J_ARRAY != json_identifiers->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid identifiers json value", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < json_identifiers->element_count(); i++) {
        ObIJsonBase *identifier_item = NULL;
        if (OB_FAIL(json_identifiers->get_array_element(i, identifier_item))) {
          LOG_WARN("failed to get array element", K(i), K(ret));
        } else if (OB_ISNULL(identifier_item) || ObJsonNodeType::J_OBJECT != identifier_item->json_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("identifier element is null", K(i), K(ret));
        } else {
          // Parse namespace array
          ObJsonObject *identifier_object = down_cast<ObJsonObject *>(identifier_item);
          const ObJsonNode *json_namespace = identifier_object->get_value(NAMESPACE_KEY);  // 未解析, OB不支持多级目录
          const ObJsonNode *json_name = identifier_object->get_value(NAME_KEY);
          ObString name_str;
          if (OB_ISNULL(json_namespace)
              || OB_ISNULL(json_name)
              || ObJsonNodeType::J_ARRAY != json_namespace->json_type()
              || ObJsonNodeType::J_STRING != json_name->json_type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("namespace or name is null in identifier", K(i), K(ret));
          } else if (OB_FAIL(parse_json_to_string(json_name, name_str, &allocator_))) {
            LOG_WARN("failed to parse json to string", K(i), K(ret));
          } else if (OB_FAIL(tables_.push_back(name_str))) {
            LOG_WARN("failed to add table identifier to array", K(name_str), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRestListTablesResponse::do_reset()
{
  int ret = OB_SUCCESS;
  tables_.reset();
  return ret;
}

int ObRestLoadTableResponse::parse_from_response()
{
  int ret = OB_SUCCESS;
  ObJsonNode *json_node = NULL;
  ObJsonObject *json_object = NULL;

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObJsonParser::get_tree(&allocator_, body_.string(), json_node))) {
    LOG_WARN("failed to get tree", K(ret));
  } else if (OB_ISNULL(json_node) || ObJsonNodeType::J_OBJECT != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json value", K(ret));
  } else {
    json_object = down_cast<ObJsonObject *>(json_node);
  }

  // metadata-location, required
  if (OB_SUCC(ret)) {
    const ObJsonNode *json_metadata_location = json_object->get_value(METADATA_LOCATION_KEY);
    if (OB_ISNULL(json_metadata_location) || ObJsonNodeType::J_STRING != json_metadata_location->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid metadata location json value", K(ret));
    } else if (OB_FAIL(parse_json_to_string(json_metadata_location, metadata_location_, &allocator_))) {
      LOG_WARN("failed to parse json to string", K(ret));
    }
  }

  // metadata, required
  if (OB_SUCC(ret)) {
    const ObJsonNode *json_metadata = json_object->get_value(METADATA_KEY);
    if (OB_ISNULL(json_metadata) || ObJsonNodeType::J_OBJECT != json_metadata->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid metadata json value", K(ret));
    } else if (OB_FAIL(table_metadata_.init_from_json(*down_cast<const ObJsonObject *>(json_metadata)))) {
      LOG_WARN("failed to init table metadata", K(ret));
    }
  }

  // should first check credentials, not required
  // todo@lekou, parse storage credentials, gravitino都是在config提供credentials

  // then config, not required
  if (OB_SUCC(ret)) {
    const ObJsonNode *json_config = json_object->get_value(CONFIG_KEY);
    if (OB_NOT_NULL(json_config)) {
      if (ObJsonNodeType::J_OBJECT != json_config->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("config is not object type", K(ret));
      } else {
        const ObJsonObject *config_object = down_cast<const ObJsonObject *>(json_config);
        JsonObjectIterator iter = config_object->object_iterator();
        while (OB_SUCC(ret) && !iter.end()) {
          ObString key;
          ObIJsonBase *value = NULL;
          if (OB_FAIL(iter.get_key(key))) {
            LOG_WARN("failed to get key from config", K(ret));
          } else if (OB_FAIL(iter.get_value(value))) {
            LOG_WARN("failed to get value from config", K(ret));
          } else if (OB_ISNULL(value)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("config value is null", K(ret));
          } else if (ObJsonNodeType::J_STRING != value->json_type()) {
            LOG_WARN("config value is not string type", K(ret), K(key));
          } else {
            ObString str_value(value->get_data_length(), value->get_data());
            if (key.suffix_match(ACCESS_ID_KEY_SUFFIX)) {
              if (OB_FAIL(ob_write_string(allocator_, str_value, accessid_, true/*c_style*/))) {
                LOG_WARN("failed to copy accessid", K(str_value), K(ret));
              }
            } else if (key.suffix_match(SECRET_ACCESS_KEY_SUFFIX_1) || key.suffix_match(SECRET_ACCESS_KEY_SUFFIX_2)) {
              if (OB_FAIL(ob_write_string(allocator_, str_value, accesskey_, true/*c_style*/))) {
                LOG_WARN("failed to copy accesskey", K(str_value), K(ret));
              }
            } else if (key.suffix_match(ENDPOINT_KEY_SUFFIX)) {
              if (OB_FAIL(ob_write_string(allocator_, str_value, endpoint_, true/*c_style*/))) {
                LOG_WARN("failed to copy endpoint", K(str_value), K(ret));
              }
            } else if (key.suffix_match(REGION_KEY_SUFFIX)) {
              if (OB_FAIL(ob_write_string(allocator_, str_value, region_, true/*c_style*/))) {
                LOG_WARN("failed to copy region", K(str_value), K(ret));
              }
            } else {
              LOG_WARN("unknown key", K(key), K(ret));
            }

            if (OB_SUCC(ret)) {
              iter.next();
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRestLoadTableResponse::do_reset()
{
  int ret = OB_SUCCESS;
  table_metadata_.reset();
  metadata_location_.reset();
  accessid_.reset();
  accesskey_.reset();
  endpoint_.reset();
  region_.reset();
  return ret;
}

}
}