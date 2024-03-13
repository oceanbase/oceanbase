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
#ifndef DEV_SRC_PL_OB_PL_JSON_TYPE_H_
#define DEV_SRC_PL_OB_PL_JSON_TYPE_H_
#include "pl/ob_pl_type.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_parse.h"
#include "pl/ob_pl_user_type.h"


namespace oceanbase
{
namespace pl
{

struct ObPlJsonNode;

class ObPlJsonTypeManager {
public:
  const uint32_t JSON_PL_BUCKET_NUM = 1000;
  typedef common::hash::ObHashMap<uint64_t, ObPlJsonNode*, common::hash::NoPthreadDefendMode> ObJsonDomMap;

  ObPlJsonTypeManager(uint64_t tenant_id);
  int create_new_node(const ObString& text, ObJsonInType in_type, ObPlJsonNode*& res);
  int create_ref_node(ObJsonNode* data, ObJsonNode* ref, ObPlJsonNode*& res);
  int create_new_node(ObJsonNode* data, ObPlJsonNode*& res);
  int create_empty_node(ObPlJsonNode*& res);
  void free_empty_node(ObPlJsonNode* node);
  int destroy_node(ObPlJsonNode* node);
  int add_node(ObPlJsonNode*);
  int remove_node(ObPlJsonNode*, bool force = true);
  int get_node(ObJsonNode*, ObPlJsonNode*&);
  int init();
  int check_candidate_list();
  void destroy();
  void free(ObPlJsonNode* node);
  common::ObIAllocator* get_dom_node_allocator() { return &dom_node_allocator_; }

  static void release(intptr_t handle);
  static void release_useless_resource(intptr_t handle);

  uint64_t get_map_count();
  uint64_t get_list_count();
  uint64_t get_alloc_count();
  uint64_t get_free_count();
  uint64_t get_holding_count();

  common::ObSmallBlockAllocator<> dom_node_allocator_;
  ObList<ObPlJsonNode*, common::ObIAllocator> candidates_;
  ObJsonDomMap json_dom_map_;
  uint64_t tenant_id_;
  bool is_init_;

  uint64_t alloc_count_;
  uint64_t free_count_;

  TO_STRING_KV("alloc total size", dom_node_allocator_.get_total_mem_size(),
              "node map count", json_dom_map_.size(),
              "list count", candidates_.size(),
              K_(tenant_id),
              K_(alloc_count),
              K_(free_count));
};

struct ObPlJsonNode {
  ObPlJsonNode(ObPlJsonTypeManager *pl_handle)
   : data_(nullptr),
     origin_(nullptr),
     ref_count_(0),
     ref_type_(0),
     allocator_(ObMemAttr(pl_handle->tenant_id_, "JsonPlManager"), OB_MALLOC_NORMAL_BLOCK_SIZE),
     pl_manager_(pl_handle) {}

  ObPlJsonNode(ObPlJsonTypeManager *pl_handle,
              ObJsonNode* json_node);

  int parse_tree(const ObString& text, ObJsonInType in_type);
  common::ObIAllocator& get_allocator() { return allocator_; }
  int clone(ObPlJsonNode* other, bool is_deep_copy = true);
  int clone(ObJsonNode* other, bool is_deep_copy = true);

  int32_t increase_ref() { return ++ref_count_; }
  int32_t decrease_ref() { return --ref_count_; }
  void set_data_node(ObJsonNode* node) { data_ = node; }
  void set_ref_node(ObJsonNode* node) { origin_ = node; }
  ObJsonNode* get_ref_node() { return origin_; }
  ObJsonNode* get_data_node() { return data_; }
  int32_t ref_count() { return ref_count_; }


  ObPlJsonTypeManager* get_manager() { return pl_manager_; }

  int unref();
  int assign(ObPlJsonNode* from);

  void reuse();
  void free();

  ObJsonNode* data_; // for save current using obj
  ObJsonNode* origin_; // for save reference original obj
  int32_t ref_count_;
  int32_t ref_type_;
  common::ObArenaAllocator allocator_;
  ObPlJsonTypeManager *pl_manager_;

  TO_STRING_KV(KPC(data_), KPC(origin_), K_(ref_count), KPC(pl_manager_));
};

static uint32_t PL_JSON_DOM_LEN = sizeof(ObPlJsonNode);

class ObPLJsonBaseType : public ObPLOpaque
{
public:
  enum JSN_ERR_BEHAVIOR {
    JSN_PL_NULL_ON_ERR,
    JSN_PL_ERR_ON_ERR,
    JSN_PL_ERR_ON_EMP,
    JSN_PL_ERR_ON_MISMATCH,
    JSN_PL_ERR_ON_INVALID = 7
  };

  ObPLJsonBaseType()
    : ObPLOpaque(ObPLOpaqueType::PL_JSON_TYPE),
      data_(NULL),
      behavior_(0)
      {}

  void destroy()
  {
    if (OB_NOT_NULL(data_)) {
      ObPlJsonTypeManager* manager = data_->get_manager();
      manager->destroy_node(data_);
    }

    data_ = NULL;
    behavior_ = 0;
  }

  virtual ~ObPLJsonBaseType()
  {
    destroy();
  }

public:
  virtual int deep_copy(ObPLOpaque *dst);
  void set_data(ObPlJsonNode *data) { data_ = data; }
  void set_err_behavior(int behavior) { behavior_ = behavior; }
  int get_err_behavior() { return behavior_ ; }
  ObPlJsonNode* get_data() { return data_; }

  TO_STRING_KV(KPC(data_), K_(behavior));

private:
  ObPlJsonNode *data_;
  int behavior_;
};

}  // namespace pl
}  // namespace oceanbase
#endif /* DEV_SRC_PL_OB_PL_JSON_TYPE_H_ */
#endif