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

#ifdef OB_BUILD_ORACLE_PL
#define USING_LOG_PREFIX PL
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_json_type.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace jit;
using namespace obmysql;
using namespace sql;

namespace pl
{

int ObPLJsonBaseType::deep_copy(ObPLOpaque *dst)
{
  int ret = OB_SUCCESS;
  ObPlJsonNode* pl_node = data_;
  ObJsonNode* ref_node = nullptr;
  ObJsonNode* data_node = nullptr;
  ObPlJsonTypeManager* pl_manager = nullptr;
  if (OB_NOT_NULL(data_)) {
    ref_node = pl_node->get_ref_node();
    data_node = pl_node->get_data_node();
    pl_manager = pl_node->get_manager();
  }

  ObPLJsonBaseType *copy = NULL;
  OZ (ObPLOpaque::deep_copy(dst));
  CK (OB_NOT_NULL(copy = new(dst)ObPLJsonBaseType()));
  OX (copy->set_err_behavior(static_cast<int32_t>(behavior_)));
  if (OB_SUCC(ret) && OB_NOT_NULL(data_)) {
    ObPlJsonNode* dup = nullptr;
    if (OB_FAIL(pl_manager->create_empty_node(dup))) {
      LOG_WARN("fail to create empty node", K(ret), K(pl_manager->get_map_count()),
               K(pl_manager->get_list_count()),  K(pl_manager->get_alloc_count()),
               K(pl_manager->get_free_count()), K(pl_manager->get_holding_count()));
    } else if (OB_FAIL(dup->assign(pl_node))) {
      LOG_WARN("fail to assign node", K(ret));
    } else {
      copy->set_data(dup);
      if (OB_FAIL(pl_manager->check_candidate_list())) {
        LOG_WARN("fail to checkin candidate list node.", K(ret));
      }
    }
  }

  return ret;
}

ObPlJsonNode::ObPlJsonNode(ObPlJsonTypeManager *pl_handle, ObJsonNode* json_node)
  : ObPlJsonNode(pl_handle)
{
  set_data_node(json_node);
  increase_ref();
}


int ObPlJsonNode::assign(ObPlJsonNode* from)
{
  int ret = OB_SUCCESS;

  if (from->get_ref_node()) {
    ObJsonNode* origin = from->get_ref_node();
    ObPlJsonNode* from_origin = nullptr;
    if (OB_FAIL(pl_manager_->get_node(origin, from_origin))) {
      LOG_WARN("fail to get node from manger", K(ret));
    } else {
      from_origin->increase_ref();
      set_ref_node(from_origin->get_data_node());
      set_data_node(from->get_data_node());
    }
  } else {
    from->increase_ref();
    set_ref_node(from->get_data_node());
    set_data_node(from->get_data_node());
  }

  return ret;
}

int ObPlJsonNode::clone(ObPlJsonNode* other, bool is_deep_copy)
{
  return clone(other->get_data_node(), is_deep_copy);
}

int ObPlJsonNode::clone(ObJsonNode* other, bool is_deep_copy)
{
  int ret = OB_SUCCESS;
  ObJsonNode *json_dst = other->clone(&allocator_, true);
  if (OB_ISNULL(json_dst)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for clone json node failed", K(ret));
  } else {
    set_data_node(json_dst);
    increase_ref();
  }

  return ret;
}

int ObPlJsonNode::parse_tree(const ObString& text, ObJsonInType in_type)
{
  int ret = OB_SUCCESS;
  ObJsonNode* tree = nullptr;
  if (OB_FAIL(ObJsonBaseFactory::get_json_tree(&allocator_, text, in_type, tree, ObJsonParser::JSN_RELAXED_FLAG))) {
    LOG_WARN("fail to get json base", K(ret), K(in_type));
  } else {
    set_data_node(tree);
    increase_ref();
  }

  return ret;
}

int ObPlJsonNode::unref()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(get_ref_node())) {
    ObPlJsonNode* dom_node = nullptr;
    if (OB_FAIL(pl_manager_->get_node(get_ref_node(), dom_node))) {
      LOG_WARN("fail to get node from manger", K(ret));
    } else if (OB_ISNULL(dom_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to unassign get dom node is null", K(ret));
    } else if (dom_node->decrease_ref() == 0) {
      if (OB_FAIL(pl_manager_->remove_node(dom_node, false))) {
        LOG_WARN("fail to remove node from manger", K(ret));
      }
    }
  } else if (decrease_ref() == 0) {
    if (OB_FAIL(pl_manager_->remove_node(this, false))) {
      LOG_WARN("fail to remove node from manger", K(ret));
    }
  }

  return ret;
}

void ObPlJsonNode::reuse()
{
  data_ = nullptr;
  origin_ = nullptr;
  ref_count_ = 0;
  ref_type_ = 0;
  allocator_.reset();
}

void ObPlJsonNode::free()
{
  reuse();
}

int ObPlJsonTypeManager::destroy_node(ObPlJsonNode* node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(node->unref())) {
    LOG_WARN("failed to unref current node", K(ret));
  } else if (OB_FAIL(check_candidate_list())) {
    LOG_WARN("failed to eliminate node", K(ret));
  } else if (node->get_ref_node()) {
    free_empty_node(node);
  }
  return ret;
}

ObPlJsonTypeManager::ObPlJsonTypeManager(uint64_t tenant_id)
  : dom_node_allocator_(sizeof(ObPlJsonNode),
                        common::OB_MALLOC_NORMAL_BLOCK_SIZE - 32,
                        ObMalloc(lib::ObMemAttr(tenant_id, "JsonPlDom"))),
    candidates_(dom_node_allocator_),
    json_dom_map_(),
    tenant_id_(tenant_id),
    is_init_(false)
{
}

void ObPlJsonTypeManager::free_empty_node(ObPlJsonNode* node)
{
  dom_node_allocator_.free(node);
  ++free_count_;
}


int ObPlJsonTypeManager::create_empty_node(ObPlJsonNode*& res)
{
  int ret = OB_SUCCESS;
  ObPlJsonNode* tmp = nullptr;

  if (OB_ISNULL(tmp = static_cast<ObPlJsonNode*>(dom_node_allocator_.alloc(PL_JSON_DOM_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc empty dom node", K(ret), K(*this));
  } else {
    res = new (tmp) ObPlJsonNode(this);
    ++alloc_count_;
  }

  LOG_DEBUG("json pl manager statistic:", K(get_map_count()), K(get_list_count()),
            K(get_alloc_count()), K(get_free_count()), K(get_holding_count()));

  return ret;
}

int ObPlJsonTypeManager::create_new_node(ObJsonNode* data, ObPlJsonNode*& res)
{
  int ret = OB_SUCCESS;
  ObPlJsonNode* tmp = nullptr;
  ObJsonNode* clone = nullptr;

  if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(create_empty_node(tmp))) {
    LOG_WARN("failed to create empty node", K(ret), K(get_map_count()), K(get_list_count()),
             K(get_alloc_count()), K(get_free_count()), K(get_holding_count()));
  } else if (OB_ISNULL(clone = data->clone(&tmp->get_allocator(), true))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to clone node", K(ret));
  } else {
    tmp->set_data_node(clone);
    tmp->increase_ref();

    if (OB_FAIL(add_node(tmp))) {
      LOG_WARN("failed to add tree", K(ret), K(get_map_count()), K(get_list_count()),
               K(get_alloc_count()), K(get_free_count()), K(get_holding_count()));
    } else {
      res = tmp;
    }
  }
  return ret;
}

int ObPlJsonTypeManager::create_ref_node(ObJsonNode* data, ObJsonNode* ref, ObPlJsonNode*& res)
{
  int ret = OB_SUCCESS;
  ObPlJsonNode* tmp = nullptr;
  ObPlJsonNode* origin = nullptr;

  if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(get_node(ref, origin))) {
    LOG_WARN("failed to get node", K(ret));
  } else if (OB_FAIL(create_empty_node(tmp))) {
    LOG_WARN("failed to create empty node", K(ret), K(get_map_count()), K(get_list_count()),
             K(get_alloc_count()), K(get_free_count()), K(get_holding_count()));
  } else {
    tmp->set_data_node(data);
    tmp->set_ref_node(origin->get_data_node());
    origin->increase_ref();

    res = tmp;
  }
  return ret;
}

int ObPlJsonTypeManager::create_new_node(const ObString& text, ObJsonInType in_type, ObPlJsonNode*& res)
{
  int ret = OB_SUCCESS;

  ObPlJsonNode* tmp = nullptr;
  if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(create_empty_node(tmp))) {
    LOG_WARN("failed to create empty node", K(ret), K(get_map_count()), K(get_list_count()),
             K(get_alloc_count()), K(get_free_count()), K(get_holding_count()));
  } else if (OB_FAIL(tmp->parse_tree(text, in_type))) {
    LOG_WARN("failed to parse tree", K(ret));
  } else if (OB_FAIL(add_node(tmp))) {
    LOG_WARN("failed to add tree",  K(ret), K(get_map_count()), K(get_list_count()),
             K(get_alloc_count()), K(get_free_count()), K(get_holding_count()));
  } else {
    res = tmp;
  }

  return ret;
}

int ObPlJsonTypeManager::init()
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ObMemAttr bucket_attr(tenant_id_, "jsonPlBucket");
    ObMemAttr node_attr(tenant_id_, "jsonPlBuckNode");
    if (OB_FAIL(json_dom_map_.create(JSON_PL_BUCKET_NUM, bucket_attr, node_attr))) {
      LOG_WARN("failed to create json bucket num", K(ret));
    } else {
      is_init_ = true;
    }
  }

  return ret;
}

int ObPlJsonTypeManager::add_node(ObPlJsonNode* node)
{
  int ret = OB_SUCCESS;
  ObPlJsonNode* value = nullptr;

  if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_NOT_NULL(node->data_)
    && OB_FAIL(json_dom_map_.get_refactored(reinterpret_cast<uint64_t>(node->data_), value))) {
    if (ret == OB_HASH_NOT_EXIST) {
      if (OB_FAIL(json_dom_map_.set_refactored(reinterpret_cast<uint64_t>(node->data_), node))) {
        LOG_WARN("failed to set json pl object into bucket.", K(ret));
      }
    }
  }

  return ret;
}

int ObPlJsonTypeManager::remove_node(ObPlJsonNode* node, bool do_force)
{
  int ret = OB_SUCCESS;
  ObPlJsonNode* dom_value = nullptr;

  if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(json_dom_map_.get_refactored(reinterpret_cast<uint64_t>(node->data_), dom_value))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to set json pl object into bucket.", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (!do_force && OB_FAIL(candidates_.push_front(node))) {
    LOG_WARN("failed to add into candidates list.", K(ret));
  } else if (OB_FAIL(json_dom_map_.erase_refactored(reinterpret_cast<uint64_t>(node->data_)))) {
    LOG_WARN("failed to remove from candidates list.", K(ret));
  }

  return ret;
}

int ObPlJsonTypeManager::get_node(ObJsonNode* node, ObPlJsonNode*& value)
{
  int ret = OB_SUCCESS;
  value = nullptr;
  ObPlJsonNode* dom_value = nullptr;

  if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(json_dom_map_.get_refactored(reinterpret_cast<uint64_t>(node), dom_value))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to set json pl object into bucket.", K(ret));
    } else {
      ObList<ObPlJsonNode*, ObIAllocator>::iterator it = candidates_.begin();
      for (; it != candidates_.end(); ++it) {
        ObPlJsonNode* temp = *it;
        if (temp->data_ == node) {
          value = temp;
          break;
        }
      }

      if (OB_NOT_NULL(value)) {
        ret = OB_SUCCESS;
      }
    }
  } else if (OB_ISNULL(dom_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get json node from hash.", K(ret));
  } else  {
    value = dom_value;
  }

  return ret;
}

int ObPlJsonTypeManager::check_candidate_list()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  } else {
    ObList<ObPlJsonNode*, ObIAllocator>::iterator it = candidates_.begin();
    for (; it != candidates_.end(); ) {
      ObPlJsonNode* temp = *it;
      if (temp->ref_count()) {
        if (OB_FAIL(json_dom_map_.set_refactored(reinterpret_cast<uint64_t>(temp->data_), temp))) {
          LOG_WARN("failed to init", K(ret));
        } else {
          ++it;
        }
      } else if (!temp->ref_count()) {
        ++it;
        free(temp);
      }
    }
  }

  return ret;
}

void ObPlJsonTypeManager::free(ObPlJsonNode* node)
{
  node->free();
  candidates_.erase(node);
  dom_node_allocator_.free(node);
}

void ObPlJsonTypeManager::destroy()
{
  ObJsonDomMap::iterator iter = json_dom_map_.begin();
  for (; iter != json_dom_map_.end(); ) {
    ObPlJsonNode* node = iter->second;
    if (OB_NOT_NULL(node)) {
      iter++;
      node->free();
    } else {
      ++iter;
    }
  }

  candidates_.clear();
  json_dom_map_.destroy();
  dom_node_allocator_.reset();
  is_init_ = false;
}
uint64_t ObPlJsonTypeManager::get_map_count()
{
  return json_dom_map_.size();
}

uint64_t ObPlJsonTypeManager::get_list_count()
{
  return candidates_.size();
}

uint64_t ObPlJsonTypeManager::get_alloc_count()
{
  return alloc_count_;
}

uint64_t ObPlJsonTypeManager::get_free_count()
{
  return free_count_;
}

uint64_t ObPlJsonTypeManager::get_holding_count()
{
  return alloc_count_ - free_count_;
}

void ObPlJsonTypeManager::release(intptr_t handle)
{
  ObPlJsonTypeManager* manager = reinterpret_cast<ObPlJsonTypeManager*>(handle);
  if (manager) {
    manager->destroy();
  }
}

void ObPlJsonTypeManager::release_useless_resource(intptr_t handle)
{
  ObPlJsonTypeManager* manager = reinterpret_cast<ObPlJsonTypeManager*>(handle);
  if (manager) {
    manager->check_candidate_list();
  }
}

}  // namespace pl
}  // namespace oceanbase
#endif