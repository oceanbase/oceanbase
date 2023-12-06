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
 * This file contains interface support for the xml bin abstraction.
 */

#ifndef OCEANBASE_SQL_OB_XML_BIN
#define OCEANBASE_SQL_OB_XML_BIN

#include "lib/xml/ob_multi_mode_interface.h"
#include "ob_multi_mode_bin.h"
#include "ob_tree_base.h"
#include "ob_xml_tree.h"

namespace oceanbase {
namespace common {

/**
 * xml binary format as following
 *
 * | common_header | doc_header | element header | key entry | value_entry | real key | real value |
 *
*/

/**
 * element header:
 * |uint16_t flags | prefix_ | standalone(uint16_t) |
*/
struct ObXmlElementBinHeader {
  ObXmlElementBinHeader()
    : flags_(0),
      prefix_len_size_(0),
      prefix_len_(0),
      prefix_() {}

  ObXmlElementBinHeader(uint16_t is_unparsed,
                        const ObString& prefix)
    : ObXmlElementBinHeader()
  {
    prefix_ = prefix;
    if (prefix_.empty()) {
      is_prefix_ = 0;
    } else {
      is_prefix_ = 1;
      prefix_len_ = prefix_.length();
      prefix_len_size_ = serialization::encoded_length_vi64(prefix.length());
    }
    is_unparse_ = is_unparsed;
  }

public:
  union {
    struct {
      uint8_t is_unparse_ : 1;
      uint8_t is_prefix_ : 1;
      uint8_t reserved_: 7;
    };

    uint8_t flags_;
  };

  uint8_t prefix_len_size_;
  uint16_t prefix_len_;
  ObString prefix_;

  uint32_t header_size();
  ObString get_prefix() { return prefix_; }
  uint16_t get_unparse() { return is_unparse_; }

  int serialize(ObStringBuffer& buffer);
  int deserialize(const char* data, int64_t length);
};

/**
 *  header:
 * |uint8_t flags | prefix_ |
*/
struct ObXmlAttrBinHeader {
  ObXmlAttrBinHeader(const ObString& prefix, ObMulModeNodeType type)
  {
    type_ = type;
    prefix_ = prefix;
    if (prefix_.empty()) {
      is_prefix_ = 0;
    } else {
      is_prefix_ = 1;
      prefix_len_ = prefix_.length();
      prefix_len_size_ = serialization::encoded_length_vi64(prefix.length());
    }
  }

  ObXmlAttrBinHeader()
    : flags_(0),
      prefix_len_(0),
      prefix_(0) {}

  union {
    uint8_t flags_;
    struct {
      uint8_t is_prefix_ : 1;
      uint8_t reserved_ : 7;
    };
  };

  uint8_t prefix_len_size_;
  uint16_t prefix_len_;
  ObString prefix_;
  ObMulModeNodeType type_;

  uint32_t header_size();
  ObString get_prefix() { return prefix_; }
  int serialize(ObStringBuffer* buffer);
  int deserialize(const char* data, int64_t length);
};

class ObXmlAttributeSerializer {
public:
  ObXmlAttributeSerializer(ObIMulModeBase* root, ObStringBuffer& buffer);
  ObXmlAttributeSerializer(const char* data, int64_t length, ObMulModeMemCtx* ctx);

  int serialize();
  int deserialize(ObIMulModeBase*& handle);

private:
  // for serialzie
  ObIMulModeBase* root_;
  ObStringBuffer* buffer_;
  ObXmlAttrBinHeader header_;

  // for deserialize
  const char* data_;
  int64_t data_len_;
  ObIAllocator* allocator_;
  ObMulModeMemCtx* ctx_;
};

/**
 * text header:
 * | type | value |
*/
class ObXmlTextSerializer {
public:
  ObXmlTextSerializer(ObIMulModeBase* root, ObStringBuffer& buffer);
  ObXmlTextSerializer(const char* data, int64_t length, ObMulModeMemCtx* ctx);
  int serialize();
  int deserialize(ObIMulModeBase*& handle);
  int64_t header_size() { return is_extend_type(type_) ? sizeof(uint8) * 2 : sizeof(uint8_t); }
private:
 // for serialzie
  ObIMulModeBase* root_;
  ObStringBuffer* buffer_;
  ObMulModeNodeType type_;
  // for deserialize
  const char* data_;
  int64_t data_len_;
  ObIAllocator* allocator_;
  ObMulModeMemCtx* ctx_;
};

/**
 * | flags_ | version | is_encoding | element_header |
*/
struct ObXmlDocBinHeader {
  ObXmlDocBinHeader()
    : flags_(0),
      version_len_(0),
      encode_len_(0),
      standalone_(0),
      version_(),
      encoding_(),
      elem_header_(0, "") {}

  ObXmlDocBinHeader(int32_t sort_flag)
    : flags_(sort_flag),
      version_len_(0),
      encode_len_(0),
      standalone_(0),
      version_(),
      encoding_(),
      elem_header_(0, "") {}

  ObXmlDocBinHeader(const ObString& version,
                    const ObString& encoding,
                    uint16_t encoding_empty,
                    uint16_t standalone,
                    uint16_t is_xml_decl)
    : flags_(0),
      standalone_(standalone),
      version_(version),
      encoding_(encoding),
      elem_header_(0, "")
  {
    encode_len_ = encoding.length();
    version_len_ = version.length();
    is_version_ = version_len_ > 0;
    is_encoding_ = encode_len_ > 0;
    is_standalone_ = standalone > 0;
    is_encoding_empty_ = encoding_empty > 0;
    is_xml_decl_ = is_xml_decl;
  }

  union {
    uint16_t flags_; //
    struct {
      uint16_t is_version_ : 1;
      uint16_t is_encoding_ : 1;
      uint16_t is_int_dtd_ : 1;
      uint16_t is_ext_dtd_ : 1;

      uint16_t is_standalone_ : 1;
      uint16_t is_xml_decl_ : 1;
      uint16_t is_encoding_empty_ : 1;
      uint16_t reserved_ : 9;
    };
  };

  // 如果is_prefix, is_standalone_, is_encoding_是0, 则不会序列化相应的成员
  uint8_t version_len_;
  uint8_t encode_len_;
  uint16_t standalone_;
  ObString version_;
  ObString encoding_;

  ObXmlElementBinHeader elem_header_;

  uint64_t header_size();
  ObString get_version() { return version_; }
  ObString get_encoding() { return encoding_; }
  uint8_t has_xml_decl() { return is_xml_decl_; }
  uint8_t get_encoding_empty() { return is_encoding_empty_; }
  uint16_t get_standalone() { return standalone_; }

  int serialize(ObStringBuffer& buffer);
  int deserialize(const char* data, int64_t length);
};

/**
 * Element value
 * | element header | index-array | key_entry | value entry | key | value |
 * key-entry ： | key_offset | key_len      |
 * value-entry  |   type     | value_offset |
*/
class ObXmlElementSerializer : public ObMulModeContainerSerializer {
public:
  static const int64_t MAX_RETRY_TIME = 2;
  // root must be ObXmlElement or ObXmlDocument
  ObXmlElementSerializer(ObIMulModeBase* root, ObStringBuffer* buffer, bool serialize_key = false);
  ObXmlElementSerializer(const char* data, int64_t length, ObMulModeMemCtx* ctx);
  int serialize_value(int idx, int64_t depth);
  int serialize_key(int idx, int64_t depth);
  int serialize(int64_t depth);
  int deserialize(ObIMulModeBase*& node);
  int reserve_meta();
  void set_key_entry(int64_t entry_idx,  int64_t key_offset, int64_t key_len);
  void set_index_entry(int64_t origin_index, int64_t sort_index);

  int64_t size() { return attr_count_ + child_count_; }
  int serialize_child_key(const ObString& key, int64_t idx);
  void set_value_entry(int64_t entry_idx,  uint8_t type, int64_t value_offset);

  struct MemberArray {
    int64_t g_start_;
    int64_t g_last_;

    int64_t l_start_;
    int64_t l_last_;

    ObIMulModeBase* entry_;

    bool is_valid() { return l_start_ != -1; }
    int64_t size() { return l_last_ - l_start_ + 1; }

    MemberArray()
      : g_start_(-1),
        g_last_(-1),
        l_start_(-1),
        l_last_(-1),
        entry_(nullptr) {}
  };

private:
  int64_t attr_count_;
  int64_t child_count_;

  int64_t index_start_;
  int8_t index_entry_size_;
  int64_t key_entry_start_;
  int8_t key_entry_size_;
  int64_t key_start_;

  MemberArray child_arr_[2];

  union {
    ObXmlDocBinHeader doc_header_;
    ObXmlElementBinHeader ele_header_;
  };

  bool serialize_key_;

  // for deserialize
  const char* data_;
  int64_t data_len_;
  ObIAllocator* allocator_;
  ObMulModeMemCtx* ctx_;

  int64_t serialize_try_time_;
};

struct ObXmlBinIndexMeta {
  ObXmlBinIndexMeta(const char* index_entry, int64_t idx, int64_t var_size);
  ObXmlBinIndexMeta(const char* index_entry, int64_t idx, uint8_t var_type);
  int64_t get_index();
  int64_t pos_;
};

struct ObXmlBinKeyMeta {
  ObXmlBinKeyMeta() : offset_(-1), len_(-1) {}
  ObXmlBinKeyMeta(const char* cur_entry, uint8_t var_type);
  ObXmlBinKeyMeta(const char* cur_entry, int64_t var_size);
  ObXmlBinKeyMeta(const char* key_entry, int64_t idx, uint8_t var_type);
  ObXmlBinKeyMeta(const char* key_entry, int64_t idx, int64_t var_size);

  ObXmlBinKeyMeta(int64_t offset, int32_t len);
  ObXmlBinKeyMeta(const ObXmlBinKeyMeta& meta);

  void read(const char* cur_entry, int64_t var_size);
  void read(const char* cur_entry, uint8_t var_size);

  int64_t offset_;
  int64_t len_;
};

struct ObXmlBinKey {
  ObXmlBinKey() : meta_(), key_() {}
  ObXmlBinKey(const char* data, int64_t cur_entry, uint8_t var_type);
  ObXmlBinKey(const char* data, int64_t cur_entry, int64_t var_size);
  ObXmlBinKey(const char* data, int64_t key_entry, int64_t idx, uint8_t var_type);
  ObXmlBinKey(const char* data, int64_t key_entry, int64_t idx, int64_t var_size);
  ObXmlBinKey(const char* data, int64_t offset, int32_t len);
  ObXmlBinKey(const ObString& key);
  ObXmlBinKey(const ObXmlBinKey& other);

  ObString get_key() { return key_; }

  ObXmlBinKeyMeta meta_;
  ObString key_;
};

struct ObXmlBinMetaParser {
  ObXmlBinMetaParser()
  : data_(nullptr),
    len_(0),      // bin_len (without extend)
    total_(0),    // cur_obj_size
    count_(0),
    key_len_(0),
    version_len_(0),
    encoding_len_(0),
    prefix_len_(0),
    value_len_(0),
    key_ptr_(nullptr),
    version_ptr_(nullptr),
    encoding_ptr_(nullptr),
    prefix_ptr_(nullptr),
    value_ptr_(nullptr),
    index_entry_(0),
    key_entry_(0),
    value_entry_(0),
    child_pos_(-1),
    idx_(-1),
    sort_idx_(-1),
    parsed_(0),
    is_empty_(0),
    is_unparse_(0),
    has_xml_decl_(0),
    encoding_val_empty_(0),
    standalone_(0),
    index_entry_size_(0),
    index_entry_size_type_(0),
    key_entry_size_(0),
    key_entry_size_type_(0),
    value_entry_size_(0),
    value_entry_size_type_(0) {}

  ObXmlBinMetaParser(const char* data, int64_t len)
    : ObXmlBinMetaParser()
  {
    data_ = data;
    len_ = len;
  }

  ObXmlBinMetaParser(const ObXmlBinMetaParser& other)
    : data_(other.data_),
      len_(other.len_),
      total_(other.total_),
      count_(other.count_),
      key_len_(other.key_len_),
      version_len_(other.version_len_),
      encoding_len_(other.encoding_len_),
      prefix_len_(other.prefix_len_),
      value_len_(other.value_len_),
      key_ptr_(other.key_ptr_),
      version_ptr_(other.version_ptr_),
      encoding_ptr_(other.encoding_ptr_),
      prefix_ptr_(other.prefix_ptr_),
      value_ptr_(other.value_ptr_),
      index_entry_(other.index_entry_),
      key_entry_(other.key_entry_),
      value_entry_(other.value_entry_),
      type_(other.type_),
      child_pos_(other.child_pos_),
      idx_(other.idx_),
      sort_idx_(other.sort_idx_),
      parsed_(other.parsed_),
      is_empty_(other.is_empty_),
      is_unparse_(other.is_unparse_),
      has_xml_decl_(other.has_xml_decl_),
      encoding_val_empty_(other.encoding_val_empty_),
      standalone_(other.standalone_),
      index_entry_size_(other.index_entry_size_),
      index_entry_size_type_(other.index_entry_size_type_),
      key_entry_size_(other.key_entry_size_),
      key_entry_size_type_(other.key_entry_size_type_),
      value_entry_size_(other.value_entry_size_),
      value_entry_size_type_(other.value_entry_size_type_) {}


  bool operator==(const ObXmlBinMetaParser& other) {
    return data_ == other.data_ && len_ == other.len_;
  }

  bool operator<(const ObXmlBinMetaParser& other) {
    return data_ < other.data_;
  }
  ObXmlBinMetaParser& operator=(const ObXmlBinMetaParser& other) {
    new (this) ObXmlBinMetaParser(other);
    return *this;
  }

  int parser();
  int parser(const char* data, int64_t len);

  ObString get_version();
  ObString get_encoding();
  uint16_t get_standalone();
  ObString get_prefix();
  ObString get_key();
  ObString get_value();
  uint8_t get_key_entry_size();
  uint8_t get_key_entry_size_type();
  uint8_t get_value_entry_size();
  uint8_t get_value_entry_size_type();

  int64_t get_key_offset(int64_t index);
  int64_t get_value_offset(int64_t index);
  int64_t get_index(int64_t index);
  bool is_empty() { return is_empty_; }
  char* get_data() { return const_cast<char*>(data_);}

  const char* data_;
  int64_t len_;
  int64_t total_;

  int32_t count_;

  uint16_t key_len_;
  uint8_t version_len_;
  uint8_t encoding_len_;

  uint32_t prefix_len_;
  uint32_t value_len_;

  char* key_ptr_;
  char* version_ptr_;
  char* encoding_ptr_;
  char* prefix_ptr_;

  char* value_ptr_;
  int32_t index_entry_;
  int32_t key_entry_;
  int32_t value_entry_;

  ObMulModeNodeType type_;
  int32_t child_pos_;

  int32_t idx_;
  int32_t sort_idx_;
  bool parsed_;

  bool is_empty_;
  bool is_unparse_;
  bool has_xml_decl_;
  bool encoding_val_empty_;

  uint8_t standalone_;

  uint8_t index_entry_size_;
  uint8_t index_entry_size_type_;

  uint8_t key_entry_size_;
  uint8_t key_entry_size_type_;

  uint8_t value_entry_size_;
  uint8_t value_entry_size_type_;

  TO_STRING_KV(K(len_),
               K(total_),
               K(count_),
               K(key_len_),
               K(version_len_),
               K(encoding_len_),
               K(prefix_len_),
               K(value_len_),
               K(index_entry_),
               K(key_entry_),
               K(value_entry_),
               K(type_),
               K(idx_),
               K(standalone_),
               K(index_entry_size_),
               K(index_entry_size_type_),
               K(key_entry_size_),
               K(key_entry_size_type_),
               K(value_entry_size_),
               K(value_entry_size_type_));
};

typedef PageArena<ObXmlBinMetaParser, ModulePageAllocator> LibModuleArena;
typedef common::ObArray<ObXmlBinMetaParser, LibModuleArena> ObXmlBinMetaArray;

const int64_t OB_XMLBIN_META_SIZE = (1LL << 10);                 // 1KB

class ObXmlBin : public ObIMulModeBase {
public:
  ObStringBuffer buffer_;
  ObXmlBinMetaParser meta_;
  ObMulModeMemCtx* ctx_;
  // true, when buffer used for record exrend
  bool buffer_for_extend_;
public:
  friend class ObXmlBinMerge;
  ObXmlBin()
    : ObIMulModeBase(ObNodeMemType::BINARY_TYPE, ObNodeDataType::OB_XML_TYPE),
      buffer_(),
      meta_(),
      ctx_(nullptr),
      buffer_for_extend_(false) {}

  ObXmlBin(const ObXmlBin& other, ObMulModeMemCtx* ctx)
    : ObIMulModeBase(ObNodeMemType::BINARY_TYPE, ObNodeDataType::OB_XML_TYPE),
      buffer_(),
      meta_(other.meta_),
      ctx_(ctx),
      buffer_for_extend_(false)
  {
    if (ctx && ctx->allocator_) {
      ObIMulModeBase::set_allocator(ctx->allocator_);
      buffer_.set_allocator(allocator_);
    }
  }

  ObXmlBin(const ObXmlBin& other)
    : ObXmlBin(other, other.ctx_)
  {
  }

  ObXmlBin(ObMulModeMemCtx* ctx)
    : ObXmlBin()
  {
    if (OB_NOT_NULL(ctx)) {
      ctx_ = ctx;
      buffer_.set_allocator(ctx->allocator_);
      set_allocator(ctx->allocator_);
    }
  }

  ObXmlBin(const ObString &data, ObMulModeMemCtx* ctx = nullptr)
    : ObXmlBin(ctx)
  {
    new (&meta_) ObXmlBinMetaParser(data.ptr(), data.length());
  }

  ObXmlBin(const char* data, size_t len)
    : ObXmlBin(ObString(len, data), nullptr)
  {
  }

  ObXmlBin& operator=(const ObXmlBin& rhs)
  {
    new (this) ObXmlBin(rhs);
    return *this;
  }

  void reset() {
    if (ctx_ && ctx_->allocator_) {
      buffer_.reset();
      buffer_for_extend_ = false;
    }
  }

  int get_ns_value(ObStack<ObIMulModeBase*>& stk, ObString& value, ObIMulModeBase* extend);
  int node_ns_value(ObString& prefix, ObString& ns_value);

  ObMulModeMemCtx* get_mem_ctx() { return ctx_; }

  int get_ns_value(ObString& ns_value) {
    return 0;
  }

  int get_ns_value(const ObString& prefix, ObString& ns_value, int& ans_idx);

  virtual bool is_equal_node(const ObIMulModeBase* other);
  virtual bool is_node_before(const ObIMulModeBase* other);
  virtual bool check_extend();
  virtual bool check_if_defined_ns();

  virtual int get_attribute(ObIArray<ObIMulModeBase*>& res, ObMulModeNodeType filter_type, int32_t flags = 0);

  virtual int get_attribute(ObIMulModeBase*& res, ObMulModeNodeType filter_type, const ObString& key1, const ObString &key2 = ObString());

  int deep_copy(ObXmlBin& from);

  int64_t child_size();

  int64_t low_bound(const ObString& key);
  int64_t up_bound(const ObString& key);

  friend class ObXmlBinIterator;

  typedef class ObXmlBinIterator iterator;

  iterator begin();
  iterator end();

  int64_t size();

  int64_t count();

  int64_t attribute_size();

  int64_t attribute_count();
  int64_t origin_bin_len() { return meta_.total_;}

  ObMulModeNodeType type() const { return meta_.type_; }

  virtual ObNodeMemType get_internal_type() { return ObNodeMemType::BINARY_TYPE; }

  // namespace, attribute, children all together
  int set_at(int64_t pos);

  int set_sorted_at(int64_t sort_idx);

  // just child
  int set_child_at(int64_t pos);

  int construct(ObXmlBin*& res, ObIAllocator *allocator_);

  int get_value_start(int64_t &value_start);

  /**
   * for search
   * current node key name is consistent with input key string
  */
  virtual int compare(const ObString& key, int& res);
  // seek interface

  /**
   * under current node
   * get specified element's value string
  */

  virtual int get_value(ObIMulModeBase*& value, int64_t index = -1);


  virtual int get_value(ObString& value, int64_t index = -1);

  ObString get_prefix();

  ObString get_version();

  uint16_t get_encoding_flag() { return meta_.encoding_val_empty_;}

  uint16_t has_xml_decl() { return meta_.has_xml_decl_;}
  uint16_t is_unparse() { return meta_.is_unparse_;}
  ObIMulModeBase* get_attribute_handle() { return nullptr; }

  ObString get_encoding();

  uint16_t get_standalone();

  void set_standalone(uint16_t standalone) { meta_.standalone_ = standalone; }

  bool get_unparse();

  bool get_is_empty();

  bool has_flags(ObMulModeNodeFlag flag);

  /**
   * under current node
   * get specified element's key string
  */
  int get_key(ObString& res, int64_t index = -1);
  int get_total_value(ObString& res, int64_t value_start);
  ObString get_element_buffer();
  int get_text_value(ObString &value);
  int get_index_key(ObString& res, int64_t &origin_index, int64_t &value_offset, int64_t index = -1);
  int get_value_entry_type(uint8_t &type, int64_t index = -1);

  /**
   * get speicified element pointer under current node
  */

  int32_t get_child_start();

  ObIMulModeBase* at(int64_t pos, ObIMulModeBase* buffer = nullptr);

  ObIMulModeBase* attribute_at(int64_t pos, ObIMulModeBase* buffer = nullptr);
  ObIMulModeBase* sorted_at(int64_t pos, ObIMulModeBase* buffer = nullptr);

  int get_range(int64_t start, int64_t end, ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter = nullptr);

  // the index is sorted index
  int get_index_content(int64_t index, int64_t &index_content);
  int get_sorted_key_info(int64_t index, int64_t &key_len, int64_t &key_offset);
  int get_key_info(int64_t text_index, int64_t& sorted_index, int64_t &value_offset, int64_t &key_len);
  int get_value_info(int64_t index, uint8_t &type, int64_t &value_offset, int64_t &value_len);
  int get_child_value_start(int64_t &value_start);
  int get_node_count(ObMulModeNodeType node_type, int &count);
  int get_children(ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter = nullptr);

  int get_after(ObIArray<ObIMulModeBase*>& nodes, ObMulModeFilter* filter = nullptr);

  int get_before(ObIArray<ObIMulModeBase*>& nodes, ObMulModeFilter* filter = nullptr);

  int get_descendant(ObIArray<ObIMulModeBase*>& nodes, scan_type type, ObMulModeFilter* filter = nullptr);

  /**
   * get all childrent member under current node, whose key string is equal with input key stirng
  */
  int get_children(const ObString& key, ObIArray<ObIMulModeBase*>& res, ObMulModeFilter* filter = nullptr);

  /**
   * current node's binary
  */
  virtual int get_raw_binary(common::ObString &out, ObIAllocator *allocator = NULL);

  /**
   * for node compare
   * current json use
   */
  virtual int compare(const ObIMulModeBase &other, int &res) { return 0; }

  /**
   * serialize tree into binary
  */
  int parse_tree(ObIMulModeBase* root, bool set_alter_member = true);

  /**
   * binary type tranform to tree
  */
  int to_tree(ObIMulModeBase*& root);

  int parse(const char* data, int64_t len);

  int parse();
  int append_extend(ObXmlElement* ele);
  int append_extend(char* start, int64_t len);
  int remove_extend();
  int merge_extend(ObXmlBin& res);
  int get_extend(ObXmlBin& extend);
  int get_extend(char*& start, int64_t& len);
  // not support modify on  binary
  int append(ObIMulModeBase* node) { return OB_NOT_SUPPORTED; }

  int insert(int64_t pos, ObIMulModeBase* node) { return OB_NOT_SUPPORTED; }

  int remove(int64_t pos) { return OB_NOT_SUPPORTED; }

  int remove(ObIMulModeBase* node) { return OB_NOT_SUPPORTED; }

  int update(int64_t pos, ObIMulModeBase* new_node) { return OB_NOT_SUPPORTED; }

  int update(ObIMulModeBase* old_node, ObIMulModeBase* new_node) { return OB_NOT_SUPPORTED; }
};

class ObXmlBinIterator {
public:
  friend class ObXmlBin;

  ObXmlBinIterator()
    : is_valid_(false),
      is_sorted_iter_(false),
      cur_pos_(-1),
      total_(-1),
      cur_node_(),
      ctx_(nullptr)
  {}

  // construct
  ObXmlBinIterator(ObXmlBin* bin, bool is_sorted_iter = false)
    : is_valid_(true),
      is_sorted_iter_(is_sorted_iter),
      cur_pos_(0),
      total_(bin->meta_.count_),
      meta_header_(bin->meta_),
      cur_node_(*bin),
      ctx_(bin->ctx_)
  {
    cur_node_.meta_.parsed_ = false;
  }

  // construct
  ObXmlBinIterator(const ObXmlBinIterator& iter)
    : is_valid_(iter.is_valid_),
      is_sorted_iter_(iter.is_sorted_iter_),
      cur_pos_(iter.cur_pos_),
      total_(iter.total_),
      meta_header_(iter.meta_header_),
      ctx_(iter.ctx_)
  {
    new (&cur_node_) ObXmlBin(iter.cur_node_);
  }

  ObXmlBinIterator& operator=(const ObXmlBinIterator& from) {
    new(this) ObXmlBinIterator(from);

    return *this;
  }

  ObXmlBin* current();
  ObXmlBin* operator*();
  ObXmlBin* operator->();
  ObXmlBin* operator[](int64_t pos);

  bool end();
  bool begin();
  bool is_valid() { return is_valid_; }
  int error_code() { return error_code_; }
  ObXmlBinIterator& next();
  ObXmlBinIterator& operator++();
  ObXmlBinIterator& operator--();
  ObXmlBinIterator operator++(int);
  ObXmlBinIterator operator--(int);
  bool operator<(const ObXmlBinIterator& iter);
  bool operator>(const ObXmlBinIterator& iter);
  ObXmlBinIterator operator-(int size);
  ObXmlBinIterator operator+(int size);
  ObXmlBinIterator& operator+=(int size);
  ObXmlBinIterator& operator-=(int size);
  void set_range(int64_t start, int64_t finish);
  int64_t operator-(const ObXmlBinIterator& iter);
  bool operator==(const ObXmlBinIterator& rhs);
  bool operator!=(const ObXmlBinIterator& rhs);
  bool operator<=(const ObXmlBinIterator& rhs);

  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "cur_pos = %ld, total_=%ld", cur_pos_, total_);
    return pos;
  }

protected:

  bool is_valid_;
  bool is_sorted_iter_;
  int error_code_;
  int64_t cur_pos_;
  int64_t total_;

  ObXmlBinMetaParser meta_header_;
  ObXmlBin cur_node_;
  ObMulModeMemCtx* ctx_;
};

typedef struct ObBinMergeKeyInfo {
  ObBinMergeKeyInfo()
    : key_ptr_(nullptr),
     key_len_(0),
     origin_index_(0),
     text_index_(0),
     is_origin_(false) {}
  ObBinMergeKeyInfo(char* key_ptr, int64_t key_len, int64_t origin_index, int64_t text_index, bool is_origin)
   : key_ptr_(key_ptr),
     key_len_(key_len),
     origin_index_(origin_index),
     text_index_(text_index),
     is_origin_(is_origin) {}
  ObBinMergeKeyInfo(const ObBinMergeKeyInfo& other)
   : key_ptr_(other.key_ptr_),
     key_len_(other.key_len_),
     origin_index_(other.origin_index_),
     text_index_(other.text_index_),
     is_origin_(other.is_origin_) {}
  char* key_ptr_;
  int64_t key_len_;
  int64_t origin_index_; // origin_sorted_index
  int64_t text_index_;
  bool is_origin_;
  TO_STRING_KV(K(key_len_),
                K(key_ptr_),
                K(text_index_),
                K(origin_index_),
                K(is_origin_));
} ObBinMergeKeyInfo;

typedef struct ObBinMergeKeyCompare {
  int operator()(const ObBinMergeKeyInfo& left, const ObBinMergeKeyInfo& right) {
    ObString left_str = ObString(left.key_len_, left.key_ptr_);
    ObString right_str = ObString(right.key_len_, right.key_ptr_);
    return (left_str.compare(right_str) < 0);
  }
} ObBinMergeKeyCompare;

class ObXmlBinMergeMeta {
public:
  int init_merge_meta(ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObMulBinHeaderSerializer& header, bool with_patch);
  void set_key_entry(int64_t entry_idx,  int64_t key_offset, int64_t key_len);
  void set_index_entry(int64_t origin_index, int64_t sort_index);
  void set_value_entry(int64_t entry_idx, uint8_t type, int64_t value_offset);
  void set_value_offset(int64_t entry_idx, int64_t value_offset);
  ObMulBinHeaderSerializer* header_;
  int64_t header_start_;
  int64_t attr_count_;
  int64_t child_count_;

  int64_t index_start_;
  int8_t index_entry_size_;
  int64_t key_entry_start_;
  int8_t key_entry_size_;
  int64_t value_entry_start_;
  int64_t value_entry_size_;
  int64_t key_start_;
  ObXmlDocBinHeader doc_header_;
  ObXmlElementBinHeader ele_header_;
};

// use for xml binary merge, make sure is xml binary, checked in function: init_merge_info
class ObXmlBinMerge : public ObMulModeBinMerge {
protected:
  friend class ObXmlBin;
  virtual int init_merge_info(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                              ObIMulModeBase& patch, ObIMulModeBase& res);
  virtual int if_need_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                            ObIMulModeBase& patch, ObIMulModeBase& res, bool& need_merge);
  virtual bool if_need_append_key(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                  ObIMulModeBase& patch, ObIMulModeBase& res);
  virtual int append_res_without_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                      ObIMulModeBase& patch, ObIMulModeBase& res);
  virtual int append_value_without_merge(ObBinMergeCtx& ctx, ObIMulModeBase& value, ObIMulModeBase& res);
  virtual int append_key_without_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                      ObMulBinHeaderSerializer& header, ObIMulModeBase& res);
  virtual int append_merge_key(ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch,
                              ObMulBinHeaderSerializer& header, ObIMulModeBase& res);
  virtual int append_value_by_idx(bool is_origin, int idx, ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                  ObIMulModeBase& patch, ObMulBinHeaderSerializer& header, ObIMulModeBase& res);
  virtual int set_value_offset(int idx, uint64_t offset, ObBinMergeCtx& ctx, ObIMulModeBase& res);
  virtual uint64_t estimated_length(bool retry, ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch);
  virtual uint64_t estimated_count(bool retry, ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch);
  virtual ObMulModeNodeType get_res_type(const ObMulModeNodeType &origin_type, const ObMulModeNodeType &res_type) { return origin_type;}
  int collect_merge_key(ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch,
                      ObMulBinHeaderSerializer& header, ObArray<ObBinMergeKeyInfo>& attr_vec);
  int reserve_meta(ObMulBinHeaderSerializer& header);
  void do_sort(ObArray<ObBinMergeKeyInfo>& attr_vec);

  ObXmlBinMergeMeta merge_meta_;
};

} // namespace common
} // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_XML_BIN