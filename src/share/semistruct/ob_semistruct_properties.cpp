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

#define USING_LOG_PREFIX SHARE
#include "ob_semistruct_properties.h"
#include "common/ob_version_def.h"
#include "lib/json_type/ob_json_tree.h"

namespace oceanbase
{
namespace share
{

int ObSemistructProperties::merge_new_semistruct_properties(ObIAllocator &allocator,
                                            const common::ObString &alter_properties,
                                            const common::ObString &origin_properties,
                                            common::ObString &new_properties)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *j_alter_tree = nullptr;
  ObIJsonBase *j_origin_tree = nullptr;
  if (alter_properties.empty()) {
  } else if (origin_properties.empty()) {
    if (OB_FAIL(ob_write_string(allocator, alter_properties, new_properties))) {
      LOG_WARN("fail to write string", K(ret));
    }
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                      alter_properties,
                                                      ObJsonInType::JSON_TREE,
                                                      ObJsonInType::JSON_TREE, j_alter_tree))) {
    LOG_WARN("fail to get json base", K(ret), K(alter_properties));
  } else if (OB_ISNULL(j_alter_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("j_alter_tree is null", K(ret));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                      origin_properties,
                                                      ObJsonInType::JSON_TREE,
                                                      ObJsonInType::JSON_TREE, j_origin_tree))) {
    LOG_WARN("fail to get json base", K(ret), K(origin_properties));
  } else if (OB_ISNULL(j_origin_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("j_origin_tree is null", K(ret));
  } else if (j_origin_tree->json_type() != ObJsonNodeType::J_OBJECT || j_alter_tree->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected json type", K(ret), K(j_origin_tree->json_type()), K(j_alter_tree->json_type()));
  } else if (OB_FAIL(static_cast<ObJsonObject*>(j_origin_tree)->merge_patch(&allocator, static_cast<ObJsonObject*>(j_alter_tree)))) {
    LOG_WARN("fail to merge tree", K(ret));
  } else {
    ObJsonBuffer j_buf(&allocator);
    ObIJsonBase *encoding_type = nullptr;
    if (OB_FAIL(j_origin_tree->print(j_buf, false))) {
      LOG_WARN("Fail to print json", K(ret));
    } else if (OB_FAIL(j_buf.get_result_string(new_properties))) {
      LOG_WARN("Fail to get result string", K(ret));
    }
  }
  return ret;
}

int ObSemistructProperties::check_alter_encoding_type(const common::ObString &origin_properties, bool &contain_encoding_type, uint64_t &encoding_type)
{
  int ret = OB_SUCCESS;
  encoding_type = 0;
  contain_encoding_type = true;
  ObArenaAllocator allocator;
  ObIJsonBase *j_origin_tree = nullptr;
  ObIJsonBase *encoding_node = nullptr;
  if (!origin_properties.empty()) {
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                origin_properties,
                                                ObJsonInType::JSON_TREE,
                                                ObJsonInType::JSON_TREE, j_origin_tree))) {
      LOG_WARN("fail to get json base", K(ret), K(origin_properties));
    } else if (OB_ISNULL(j_origin_tree) || j_origin_tree->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("j_origin_tree is null or json type is not object", K(ret), K(j_origin_tree));
    } else if (OB_FAIL(j_origin_tree->get_object_value("encoding_type", encoding_node))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        contain_encoding_type = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get json base", K(ret));
      }
    } else if (OB_ISNULL(encoding_node) || encoding_node->json_type() != ObJsonNodeType::J_INT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encoding_node is null or json type is not int", K(ret), K(encoding_node));
    } else {
      encoding_type = encoding_node->get_int();
    }
  } else {
    contain_encoding_type = false;
  }
  return ret;
}


int ObSemistructProperties::resolve_semistruct_properties(uint8_t mode, const common::ObString &semistruct_properties)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *j_tree = nullptr;
  ObArenaAllocator allocator;
  if (semistruct_properties.empty()) {
    reset();
    mode_ = mode;
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(
                 &allocator, semistruct_properties, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree))) {
    LOG_WARN("fail to get json base", K(ret), K(semistruct_properties));
  } else if (OB_ISNULL(j_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("j_tree is null", K(ret));
  } else {
    int obj_num = j_tree->element_count();
    ObIJsonBase *node = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < obj_num; i++) {
      ObString key;
      if (OB_FAIL(j_tree->get_object_value(i, key, node))) {
        LOG_WARN("fail to get json base", K(ret));
      } else if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", K(ret));
      } else if (key.case_compare_equal("encoding_type")) {
        // if (node->json_type() != ObJsonNodeType::J_INT) {
        //   ret = OB_ERR_UNEXPECTED;
        //   LOG_WARN("unexpected encoding_type node type", K(ret), K(key), K(node->json_type()));
        // } else if (!ObSemistructProperties::is_mode_valid(node->get_int())) {
        //   ret = OB_ERR_UNEXPECTED;
        //   LOG_WARN("unexpected encoding_type value", K(ret), K(key), K(node->get_int()));
        // } else {
        //   mode_ = node->get_int();
        // }
        mode_ = mode; // compat old version
      } else if (key.case_compare_equal("freq_threshold")) {
        if (node->json_type() != ObJsonNodeType::J_INT) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected freq_threshold node type", K(ret), K(key), K(node->json_type()));
        } else if (node->get_int() <= 0 || node->get_int() > 100) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected freq_threshold value", K(ret), K(key), K(node->get_int()));
        } else {
          freq_threshold_ = node->get_int();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected key", K(ret), K(key));
      }
    }
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase