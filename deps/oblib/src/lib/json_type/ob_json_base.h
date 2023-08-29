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
 * This file contains interface support for the json base abstraction.
 */

#ifndef OCEANBASE_SQL_OB_JSON_BASE
#define OCEANBASE_SQL_OB_JSON_BASE

#include "ob_json_path.h"
#include "lib/number/ob_number_v2.h" // for number::ObNumber
#include "lib/timezone/ob_time_convert.h" // for ObTime
#include "lib/timezone/ob_timezone_info.h"

namespace oceanbase {
namespace common {

class ObIJsonBase;
class ObJsonNode;
class ObOTimestampData;

enum class ObJsonInType
{
  JSON_TREE,
  JSON_BIN,
};

enum class ObJsonNodeType
{
  J_NULL, // 0 oracle & mysql
  J_DECIMAL,
  J_INT,
  J_UINT,
  J_DOUBLE,
  J_STRING, // 5 oracle & mysql
  J_OBJECT, // oracle & mysql
  J_ARRAY, // oracle & mysql
  J_BOOLEAN,
  J_DATE,
  J_TIME, // 10
  J_DATETIME,
  J_TIMESTAMP,
  J_OPAQUE,

  /* oracle json extended types: type(), scalar type */
  J_OFLOAT = 15, //  number float
  J_ODOUBLE = 16,  //  number double
  J_ODECIMAL = 17, // number number
  J_OINT = 18, // number number
  J_OLONG = 19,  // number number
  J_OBINARY = 20, // binary string
  J_OOID = 21, // binary string
  J_ORAWHEX = 22, // binary string
  J_ORAWID = 23, // binary string
  J_ORACLEDATE = 24, // date
  J_ODATE = 25,  // timestamp string
  J_OTIMESTAMP = 26, // timestamp string
  J_OTIMESTAMPTZ = 27, // timestamptz string
  J_ODAYSECOND = 28, // daySecondInterval string
  J_OYEARMONTH = 29, // yearMonthInterval string
  J_MAX_TYPE,

  J_ERROR = 200 // 14
};

// sub-types of J_OPAQUE.
enum class JsonOpaqueType
{
  J_OPAQUE_BLOB = static_cast<uint32_t>(ObJsonNodeType::J_OPAQUE) + 1,
  J_OPAQUE_BIT,
  J_OPAQUE_GEOMETRY // not support now
};

#define FROM_BLOB_FLAG 1
#define HAS_BLOB_FLAG(flags) ((flags) & 1)

typedef common::ObVector<ObIJsonBase *> ObJsonBaseVector;
typedef common::ObSortedVector<ObIJsonBase *> ObJsonBaseSortedVector;
typedef std::pair<ObString, ObIJsonBase*> ObJsonObjPair;
typedef common::hash::ObHashMap<ObString, ObIJsonBase*> PassingMap;

typedef struct ObSeekParentInfo
{
  ObIJsonBase *parent_jb_;
  JsonPathIterator parent_path_;
  bool is_subpath_;

  int path_size_;
} ObSeekParentInfo;

class JsonObjectIterator
{
public:
  /**
    @param[in] wrapper the JSON object wrapper to iterate over
  */
  explicit JsonObjectIterator(const ObIJsonBase *wrapper);
  ~JsonObjectIterator() {}

  bool end() const;
  int get_elem(ObJsonObjPair &elem);
  int get_key(ObString &key);
  int get_value(ObIJsonBase *&value);
  int get_value(ObString &key, ObIJsonBase *&value);
  void next();
private:
  uint64_t curr_element_;
  uint64_t element_count_;
  const ObIJsonBase *json_object_;
};

class ObIJsonBase
{
public:
  explicit ObIJsonBase(ObIAllocator *allocator)
      : allocator_(allocator) 
  {
  }
  virtual ~ObIJsonBase() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "is_tree = %d", is_tree());
    return pos;
  }
  OB_INLINE bool is_tree() const { return get_internal_type() == ObJsonInType::JSON_TREE; }
  OB_INLINE bool is_bin() const { return get_internal_type() == ObJsonInType::JSON_BIN; }
  OB_INLINE ObIAllocator *get_allocator() { return allocator_; }
  OB_INLINE void set_allocator(ObIAllocator *allocator) { allocator_ = allocator; }
public:
  // Get internal json type(tree or binary).
  //
  // @return Returns ObJsonInType.
  virtual ObJsonInType get_internal_type() const = 0;

  // Get element count of json.
  //
  // @return json containner returns the capacity, json scalar return 1.
  virtual uint64_t element_count() const = 0;

  // Get json node type.
  //
  // @return see ObJsonNodeType.
  virtual ObJsonNodeType json_type() const = 0;

  // Get field type of ObJsonOpaque or ObJsonDatetime.
  //
  // @return see ObObjType.
  virtual ObObjType field_type() const = 0;

  // Gey key by index from json node array.
  //
  // @param [in] index    The index of json object node array.
  // @param [out] key_out The result.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int get_key(uint64_t index, common::ObString &key_out) const = 0;

  // Gey array element by index.
  //
  // @param [in] index    The index of json array.
  // @param [out] value   The result.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int get_array_element(uint64_t index, ObIJsonBase *&value) const = 0;

  // Gey object value by index.
  //
  // @param [in] index    The index of json object node array.
  // @param [out] value   The result.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int get_object_value(uint64_t index, ObIJsonBase *&value) const = 0;

  // Gey object value by key.
  //
  // @param [in] key       The key to find.
  // @param [out] key_out  The result.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int get_object_value(const ObString &key, ObIJsonBase *&value) const = 0;

  // Appends the array element to the end of the array.
  //
  // @param [in] value The array element.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int array_append(ObIJsonBase *value) = 0;

  // Adds an array element at the specified location.
  //
  // @param [in] index The index of the array to be inserted.
  // @param [in] value The array element to be inserted.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int array_insert(uint64_t index, ObIJsonBase *value) = 0;

  // Removes the array element with the specified index.
  //
  // @param [in] index  Index of the array to be deleted.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int array_remove(uint64_t index) = 0;

  // Adds a key-value pair to the current object. 
  //
  // @param [in] key    The key.
  // @param [in] value  The value.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int object_add(const common::ObString &key, ObIJsonBase *value) = 0;

  // Delete Json node with key.
  //
  // @param [in] key The key.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int object_remove(const common::ObString &key) = 0;

  // Replace the old node with the new node.
  //
  // @param [in] old_wr The old node to be replaced.
  // @param [in] new_wr The new node.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int replace(const ObIJsonBase *old_node, ObIJsonBase *new_node) = 0;

  // Merge two tree.
  //
  // @param [in]  other    The other json tree.
  // @param [out] result   The result of two tree after merged.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int merge_tree(ObIAllocator *allocator, ObIJsonBase *other, ObIJsonBase *&result);

  // Search json node by path. (allocator can not be null)
  //
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual bool is_real_json_null(const ObIJsonBase* ptr) const;
  virtual int seek(const ObJsonPath &path, uint32_t node_cnt, bool is_auto_wrap,
                   bool only_need_one, ObJsonBaseVector &res, PassingMap* sql_var = NULL) const;
  virtual int seek(ObIAllocator* allocator, const ObJsonPath &path, uint32_t node_cnt, bool is_auto_wrap,
                   bool only_need_one, bool is_lax, ObJsonBaseVector &res, PassingMap* sql_var = NULL) const;
  // Change json to string
  //
  // @param [in, out] j_buf   The dest buf.
  // @param [in] is_quoted  Whether append double quotes or not.
  // @param [in] is_pretty  Whether from JSON_PRETTY function or not.
  // @param [in]  depth      The depth of json tree.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int print(ObJsonBuffer &j_buf, bool is_quoted,
                    bool is_pretty = false, uint64_t depth = 0) const;
  
  // calculate json hash value
  //
  // @param [in] val        The initialized hash value.
  // @param [in] hash_func  The hash function.
  // @param [in] res        The result after calculate.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int calc_json_hash_value(uint64_t val, hash_algo hash_func, uint64_t &res) const;

  // Compare with other ObIJsonBase.
  //
  // @param [in] other  Another ObIJsonBase.
  // @param [out] res   Less than other returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int compare(const ObIJsonBase &other, int &res, bool is_path = false) const;

  // Get depth of current json document.
  //
  // @return uint32_t.
  virtual uint32_t depth();

  // Returns a string in json path form from the root node to the current location.
  //
  // @param [out] path The string in json path form
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int get_location(ObJsonBuffer &path);

  // Gets the number of bytes used for serialization.
  //
  // @param [in, out] size  The number of bytes taken to serialize.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int get_used_size(uint64_t &size);

  // Get the json binary free space.
  //
  // @param [in, out] size The json binary free space.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int get_free_space(uint64_t &size);

  // Get string after serializing the json doc.
  //
  // @param [out] out The string of json binary.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int get_raw_binary(common::ObString &out, ObIAllocator *allocator = NULL);

  // get object_iterator
  //
  // @return Returns object_iterator
  virtual JsonObjectIterator object_iterator() const;

  // getter
  virtual bool get_boolean() const = 0;
  virtual double get_double() const = 0;
  virtual float get_float() const = 0;
  virtual int64_t get_int() const = 0;
  virtual uint64_t get_uint() const = 0;
  virtual const char *get_data() const = 0;
  virtual uint64_t get_data_length() const = 0;
  virtual number::ObNumber get_decimal_data() const = 0;
  virtual ObPrecision get_decimal_precision() const = 0;
  virtual ObScale get_decimal_scale() const = 0;
  virtual int get_obtime(ObTime &t) const = 0;
  bool is_json_number(ObJsonNodeType json_type) const;
  bool is_json_scalar(ObJsonNodeType json_type) const;
  bool is_json_string(ObJsonNodeType json_type) const;
  bool is_json_date(ObJsonNodeType json_type) const;

  // for cast
  int to_int(int64_t &value, bool check_range = false, bool force_convert = false) const;
  int to_uint(uint64_t &value, bool fail_on_negative = false, bool check_range = false) const;
  int to_double(double &value) const;
  int to_number(ObIAllocator *allocator, number::ObNumber &number) const;
  int to_datetime(int64_t &value, ObTimeConvertCtx *cvrt_ctx_t = nullptr) const;
  int to_date(int32_t &value) const;
  int to_otimestamp(common::ObOTimestampData &value, ObTimeConvertCtx *cvrt_ctx = nullptr) const;
  int to_time(int64_t &value) const;
  int to_bit(uint64_t &value) const;
protected:
  OB_INLINE void *alloc(const int64_t size)
  {
    return OB_NOT_NULL(allocator_) ? allocator_->alloc(size) : NULL;
  }
  OB_INLINE void free(void *ptr)
  {
    if (OB_NOT_NULL(ptr)) {
      allocator_->free(ptr);
    }
  }
private:
  // Change json time to string.
  //
  // @param [in, out] j_buf  The dest buf.
  // @param [in] is_quoted  Whether append double quotes or not.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int print_jtime(ObJsonBuffer &j_buf, bool is_quoted) const;

  // Change json array to string.
  //
  // @param [in, out] j_buf      The dest buf.
  // @param [in]      depth     The depth of json tree.
  // @param [in]      is_pretty Whether is from json funcion JSON_PRETTY or not.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int print_array(ObJsonBuffer &j_buf, uint64_t depth, bool is_pretty) const;

  // Change json object to string.
  //
  // @param [in, out] j_buf      The dest buf.
  // @param [in]      depth     The depth of json tree.
  // @param [in]      is_pretty Whether is from json funcion JSON_PRETTY or not.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int print_object(ObJsonBuffer &j_buf, uint64_t depth, bool is_pretty) const;

  // Change json decimal to string.
  //
  // @param [in, out] j_buf      The dest buf.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int print_decimal(ObJsonBuffer &j_buf) const;

  // Change json double to string.
  //
  // @param [in, out] j_buf      The dest buf.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int print_double(ObJsonBuffer &j_buf) const;

  // Change json float to string.
  //
  // @param [in, out] j_buf      The dest buf.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int print_float(ObJsonBuffer &j_buf) const;

  // Change json opaque to string.
  //
  // @param [in, out] j_buf      The dest buf.
  // @param [in]      depth     The depth of json tree.
  // @param [in]      is_quoted Whether append double quotes or not.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int print_opaque(ObJsonBuffer &j_buf, uint64_t depth, bool is_quoted) const;

  // Compare two ObIJsonBase which are ObJsonArray.
  //
  // @param [in] other  Another ObIJsonBase which is ObJsonArray.
  // @param [out] res Less than other returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int compare_array(const ObIJsonBase &other, int &res) const;

  // Compare two ObIJsonBase which are ObJsonObject.
  //
  // @param [in] other  Another ObIJsonBase which is ObJsonObject.
  // @param [out] res Less than other returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int compare_object(const ObIJsonBase &other, int &res) const;

  // Compare two ObIJsonBase and this json base is ObJsonInt.
  //
  // @param [in] other  Another ObIJsonBase which is other ObJsonNodeType.
  // @param [out] res Less than other returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int compare_int(const ObIJsonBase &other, int &res) const;

  // Compare two ObIJsonBase and this json base is ObJsonUint.
  //
  // @param [in] other  Another ObIJsonBase which is other ObJsonNodeType.
  // @param [out] res Less than other returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int compare_uint(const ObIJsonBase &other, int &res) const;

  // Compare two ObIJsonBase and this json base is ObJsonDouble.
  //
  // @param [in] other  Another ObIJsonBase which is other ObJsonNodeType.
  // @param [out] res Less than other returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int compare_double(const ObIJsonBase &other, int &res) const;

  // Compare two ObIJsonBase and this json base is J_DECIMAL.
  //
  // @param [in] other  Another ObIJsonBase which is other ObJsonNodeType.
  // @param [out] res Less than other returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int compare_decimal(const ObIJsonBase &other, int &res) const;

  // Compare two ObIJsonBase which are J_DATETIME/J_DATE/J_TIME/J_TIMESTAMP.
  //
  // @param [in] other Another ObIJsonBase which is other ObJsonNodeType.
  // @param [out] res Less than other returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int compare_datetime(ObDTMode dt_mode_a, const ObIJsonBase &other, int &res) const;

  int path_compare_string(const ObString &str_l, const ObString &str_r, int &res) const;
  // Check whether the search is complete.
  //
  // @param [in] res             The result.
  // @param [in] only_need_one   Whether only need one result or not.
  // @return Returns true if finished, false otherwise.
  OB_INLINE bool is_seek_done(ObJsonBaseVector &res, bool only_need_one) const
  {
    return (only_need_one && res.size() > 0);
  }
  // Store the seeking results of all path statements to hits.
  //
  // @param [in] dup  The answer found in the current path expression, preventing repeated additions.
  // @param [in] res  The result of seeking.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int add_if_missing(ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // Find in ellipsis.
  //
  // @param [in] cur_node          The current path node.
  // @param [in] next_node         The next path node.
  // @param [in,out] last_node     The last path node of path.
  // @param [in,out] is_auto_wrap  Is auto wrap or not.
  // @param [in,out] only_need_one Whether finish when get one result or not.
  // @param [in,out] dup           The answer found in the current path expression, preventing repeated additions.
  // @param [in,out] res           The result of seeking.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_ellipsis(const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                    const JsonPathIterator &next_node, bool is_auto_wrap, bool only_need_one,
                    ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;

  // Find in array range.
  //
  // @param [in] cur_node          The current path node.
  // @param [in,out] last_node     The last path node of path.
  // @param [in,out] is_auto_wrap  Is auto wrap or not.
  // @param [in,out] only_need_one Whether finish when get one result or not.
  // @param [in,out] dup           The answer found in the current path expression, preventing repeated additions.
  // @param [in,out] res           The result of seeking.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_array_range(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                      bool only_need_one, bool is_lax, ObJsonBaseSortedVector &dup,
                      ObJsonBaseVector &res, PassingMap* sql_var = NULL) const;
  // Find in array cell.
  //
  // @param [in] cur_node          The current path node.
  // @param [in,out] last_node     The last path node of path.
  // @param [in,out] is_auto_wrap  Is auto wrap or not.
  // @param [in,out] only_need_one Whether finish when get one result or not.
  // @param [in,out] dup           The answer found in the current path expression, preventing repeated additions.
  // @param [in,out] res           The result of seeking.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_array_cell(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                      bool only_need_one, bool is_lax, ObJsonBaseSortedVector &dup,
                      ObJsonBaseVector &res, PassingMap* sql_var = NULL) const;

  // Find in member wildcard.
  //
  // @param [in] cur_node          The current path node.
  // @param [in,out] last_node     The last path node of path.
  // @param [in,out] is_auto_wrap  Is auto wrap or not.
  // @param [in,out] only_need_one Whether finish when get one result or not.
  // @param [in,out] dup           The answer found in the current path expression, preventing repeated additions.
  // @param [in,out] res           The result of seeking.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_member_wildcard(const JsonPathIterator &next_node, 
                           const JsonPathIterator &last_node, bool is_auto_wrap,
                           bool only_need_one, ObJsonBaseSortedVector &dup,
                           ObJsonBaseVector &res) const;

  // Find in member.
  //
  // @param [in] cur_node          The current path node.
  // @param [in,out] last_node     The last path node of path.
  // @param [in,out] is_auto_wrap  Is auto wrap or not.
  // @param [in,out] only_need_one Whether finish when get one result or not.
  // @param [in,out] dup           The answer found in the current path expression, preventing repeated additions.
  // @param [in,out] res           The result of seeking.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_member(const JsonPathIterator &next_node, const JsonPathIterator &last_node,
                  const ObJsonPathBasicNode *path_node, bool is_auto_wrap, bool only_need_one,
                  ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;

  // find in oracle wildcard: ..  (allocator can not be null)
  //
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  int find_ellipsis(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                    const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                    const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                    bool only_need_one, bool is_lax, ObJsonBaseSortedVector &dup,
                    ObJsonBaseVector &res, PassingMap* sql_var) const;
  // find in oracle array wildcard:[*] (allocator can not be null)
  //
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  int find_array_wildcard(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                          const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                          const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                          bool only_need_one, bool is_lax, ObJsonBaseSortedVector &dup,
                          ObJsonBaseVector &res, PassingMap* sql_var = NULL) const;
  // find in oracle array_range:[index, index1 to index2, last-index3,...]
  //
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  int find_multi_array_ranges(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                              const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                              const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                              bool only_need_one, bool is_lax, ObJsonBaseSortedVector &dup,
                              ObJsonBaseVector &res, PassingMap* sql_var = NULL) const;
  // Find in member wildcard: .*
  //
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  int find_member_wildcard(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                          const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                          const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                          bool only_need_one, bool is_lax, ObJsonBaseSortedVector &dup,
                          ObJsonBaseVector &res, PassingMap* sql_var = NULL) const;
  // Find in member: .keyname
  //
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  int find_member(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                  const ObJsonPathBasicNode *path_node, bool is_auto_wrap,
                  bool only_need_one, bool is_lax, ObJsonBaseSortedVector &dup,
                  ObJsonBaseVector &res, PassingMap* sql_var = NULL) const;
  // Find in basic_node, including: .., .*, [*], .keyname, [index, index1 to index2, last-index3,...]
  //
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  int find_basic_child(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                        const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                        bool is_auto_wrap, bool only_need_one, bool is_lax,
                        ObJsonBaseSortedVector &dup, ObJsonBaseVector &res,
                        PassingMap* sql_var = NULL) const;
  int find_array_child(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      bool is_auto_wrap, bool only_need_one, bool is_lax,
                      ObJsonBaseSortedVector &dup, ObJsonBaseVector &res,
                      PassingMap* sql_var = NULL) const;
  // item_function:ceiling(). (allocator can not be null)
  // legal for number
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_ceiling_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;// According to the path node, recursively query results dow.
  // item_function:floor(). (allocator can not be null)
  // legal for number
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_floor_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // item_function:abs(). (allocator can not be null)
  // legal for number
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_abs_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // numeric_item_method:abs(), floor(), ceiling(). (allocator can not be null)
  // legal for number
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_numeric_item_method (ObIAllocator* allocator, ObSeekParentInfo &parent_info, const JsonPathIterator &cur_node,
                                const JsonPathIterator &last_node,const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                                bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // item_function:type(). (allocator can not be null)
  /* returning J_STRING:
        "null" for a value of null.
        "boolean" for a value of true or false.
        "number" for a number.
        "string" for a string.
        "array" for an array.
        "object" for an object.
  */
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_type_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // item_function:size(). (allocator can not be null)
  // returning uint: the number of elements in an array, or 1 for a scalar or an object
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_size_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // item_function:length(). (allocator can not be null)
  // returning uint: the number of characters in the targeted JSON string, interpreted as a SQL NUMBER.
  //                 returng J_NULL(is_null_ = false) for other json_type()
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_length_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // item_function:boolean()/booleanOnly(). (allocator can not be null)
  // for booleanOnly() :return boolean for J_BOOL, returng J_NULL(is_null_ = false) for other json_type()
  // for boolean() :return boolean for J_BOOL,
  //                return J_String for J_String
  //                return J_NULL(is_null_ = false) for other json_type()
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_boolean_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  bool check_legal_ora_date(const ObString date) const;
  // item_function: date()
  // legal for J_String
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_date_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // item_function: timestamp()
  // legal for J_String
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_timestamp_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // item_function: double()
  // legal for J_Number & J_String, but retun J_NULL when str_to_double failed.
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_double_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // item_function: number()
  // legal for J_Number & J_String, but retun J_NULL when str_to_number failed.
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_number_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // item_function: number()
  // legal for JSON Scalar.
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_string_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                      const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                      bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // item_function: upper() & lower()
  // legal for JSON String.
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @param [in] node_cnt      The count of json node.
  // @param [in] is_auto_wrap  Whether is auto wrap or not.
  // @param [in] only_need_one Whether only one result or not.
  // @param [in] lax_mode      Whether lax_mode or not.
  // @param [out] res          The result of seek.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_trans_method(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                        const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                        const ObJsonPathFuncNode *path_node, bool is_auto_wrap, bool only_need_one,
                        bool is_lax, ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // Used to determine whether type conversion is required
  // is left and right the same josn type, divided into three categories: string, number and date
  // @param [in] allocator     The json allocator.
  // @param [in] path          The json path.
  // @return                   boolean
  bool is_same_type(ObIJsonBase* left, ObIJsonBase* right) const;
  int trans_to_json_number(ObIAllocator* allocator, ObString num_str, ObIJsonBase* &origin) const;
  int trans_to_boolean(ObIAllocator* allocator, ObString num_str, ObIJsonBase* &origin) const;
  int trans_to_date_timestamp(ObIAllocator* allocator, ObString num_str,
                              ObIJsonBase* &origin, bool is_date) const;
  int trans_json_node (ObIAllocator* allocator, ObIJsonBase* &left, ObIJsonBase* &right) const;
  // Used to compare with multiple right_arg, when left_arg is the results found by the sub_path
  //
  // @param [in] allocator       The json allocator.
  // @param [in] hit             The results found by the sub_path.
  // @param [in] node_type       The node_type, could be: >, >=, ==...
  // @param [in] right_arg       The right_arg, could be: scalar or sql_var
  // @param [in] only_need_one   Whether only one result or not.
  // @param [in] lax_mode        Whether lax_mode or not.
  // @param [out] filter_result  The result of compare.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int cmp_to_right_recursively(ObIAllocator* allocator, const ObJsonBaseVector& hit,
                              const ObJsonPathNodeType node_type,
                              ObIJsonBase* right_arg, bool& filter_result) const;
  // Used to compare with multiple left_arg, when right_arg is the results found by the sub_path
  //
  // @param [in] allocator       The json allocator.
  // @param [in] hit             The results found by the sub_path.
  // @param [in] node_type       The node_type, could be: >, >=, ==...
  // @param [in] left_arg        The right_arg, could be: scalar or sql_var
  // @param [in] only_need_one   Whether only one result or not.
  // @param [in] lax_mode        Whether lax_mode or not.
  // @param [out] filter_result  The result of compare.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int cmp_to_left_recursively(ObIAllocator* allocator, const ObJsonBaseVector& hit,
                              const ObJsonPathNodeType node_type,
                              ObIJsonBase* right_arg, bool& filter_result) const;
  // Used to compare with the right_arg
  // when left_arg is the results found by the sub_path and the last path node is func_node
  //
  // @param [in] allocator       The json allocator.
  // @param [in] hit             The results found by the sub_path.
  // @param [in] node_type       The node_type, could be: >, >=, ==...
  // @param [in] right_arg       The right_arg, could be: scalar or sql_var
  // @param [in] only_need_one   Whether only one result or not.
  // @param [in] lax_mode        Whether lax_mode or not.
  // @param [out] filter_result  The result of compare.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int cmp_to_right_strictly(ObIAllocator* allocator, ObIJsonBase* hit,
                            const ObJsonPathNodeType node_type,
                            const ObJsonPathNodeType last_sub_path_node_type,
                            ObIJsonBase* right_arg, bool& filter_result) const;
  // Used to compare with the left_arg
  // when left_arg is the results found by the sub_path and the last path node is func_node
  //
  // @param [in] allocator       The json allocator.
  // @param [in] hit             The results found by the sub_path.
  // @param [in] node_type       The node_type, could be: >, >=, ==...
  // @param [in] right_arg       The right_arg, could be: scalar or sql_var
  // @param [in] only_need_one   Whether only one result or not.
  // @param [in] lax_mode        Whether lax_mode or not.
  // @param [out] filter_result  The result of compare.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int cmp_to_left_strictly(ObIAllocator* allocator, ObIJsonBase* hit,
                          const ObJsonPathNodeType node_type,
                          const ObJsonPathNodeType last_sub_path_node_type,
                          ObIJsonBase* right_arg, bool& filter_result) const;
  int cmp_based_on_node_type(ObJsonPathNodeType node_type, int res, bool& ret_bool) const;
  int get_sign_result_right_subpath(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                    const ObJsonPathFilterNode *path_node, bool& filter_result,
                                    ObIJsonBase* left_arg, PassingMap* sql_var = NULL) const;
  int get_sign_result_left_subpath(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                                  const ObJsonPathFilterNode *path_node, bool& filter_result,
                                  ObIJsonBase* right_arg, PassingMap* sql_var = NULL) const;
  int get_scalar(ObIAllocator* allocator, const ObJsonPathNodeType type,
                const ObPathScalar scalar_content, ObIJsonBase* &scalar) const;
  int compare_scalar(ObIAllocator* allocator, const ObJsonPathFilterNode *path_node,
                     bool& filter_result) const;
  int get_sign_comp_result(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                          const ObJsonPathFilterNode *path_node,
                          bool& filter_result, PassingMap* sql_var = NULL) const;
  bool has_sub_string(const ObString& left, const ObString& right) const;
  int str_cmp_autowrap(ObIAllocator* allocator, const ObString& right_str,
                        const ObJsonPathFilterNode *path_node,
                        bool autowrap, bool& filter_result) const;
  int str_comp_predicate(const ObString& left, const ObString& right,
                         const ObJsonPathFilterNode *path_node, bool& filter_result) const;
  int get_str_comp_result(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                          const ObJsonPathFilterNode *path_node,
                          bool& filter_result, PassingMap* sql_var = NULL) const;
  int find_func_child(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                       bool is_auto_wrap, bool only_need_one, bool is_lax,
                      ObJsonBaseSortedVector &dup, ObJsonBaseVector &res) const;
  // According to the path node, recursively query results dow.
  //
  // @param [in] cur_node          The current path node.
  // @param [in,out] last_node     The last path node of path.
  // @param [in,out] is_auto_wrap  Is auto wrap or not.
  // @param [in,out] only_need_one Whether finish when get one result or not.
  // @param [in,out] dup           The answer found in the current path expression, preventing repeated additions.
  // @param [in,out] res           The result of seeking.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_comp_result(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const ObJsonPathFilterNode *path_node,
                      bool& filter_result, PassingMap* sql_var = NULL) const;
  int get_half_ans(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                  ObJsonPathNodeType node_type, const ObJsonPathFilterNode *path_node,
                  bool& filter_result, PassingMap* sql_var) const;
  // According to the path node, recursively query results dow.
  //
  // @param [in] cur_node          The current path node.
  // @param [in,out] last_node     The last path node of path.
  // @param [in,out] is_auto_wrap  Is auto wrap or not.
  // @param [in,out] only_need_one Whether finish when get one result or not.
  // @param [in,out] dup           The answer found in the current path expression, preventing repeated additions.
  // @param [in,out] res           The result of seeking.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_cond_result(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                      const ObJsonPathFilterNode *path_node,
                      bool& filter_result, PassingMap* sql_var = NULL) const;
  // According to the path node, recursively query results dow.
  //
  // @param [in] cur_node          The current path node.
  // @param [in,out] last_node     The last path node of path.
  // @param [in,out] is_auto_wrap  Is auto wrap or not.
  // @param [in,out] only_need_one Whether finish when get one result or not.
  // @param [in,out] dup           The answer found in the current path expression, preventing repeated additions.
  // @param [in,out] res           The result of seeking.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_filter_child(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                        const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                        bool is_auto_wrap, bool only_need_one, bool is_lax,
                        ObJsonBaseSortedVector &dup, ObJsonBaseVector &res,
                        PassingMap* sql_var) const;
  // According to the path node, recursively query results dow.
  //
  // @param [in] cur_node          The current path node.
  // @param [in,out] last_node     The last path node of path.
  // @param [in,out] is_auto_wrap  Is auto wrap or not.
  // @param [in,out] only_need_one Whether finish when get one result or not.
  // @param [in,out] dup           The answer found in the current path expression, preventing repeated additions.
  // @param [in,out] res           The result of seeking.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int find_child(ObIAllocator* allocator, ObSeekParentInfo &parent_info,
                  const JsonPathIterator &cur_node, const JsonPathIterator &last_node,
                  bool is_auto_wrap, bool only_need_one, bool is_lax,
                  ObJsonBaseSortedVector &dup, ObJsonBaseVector &res,
                  PassingMap* sql_var = NULL) const;
private:
  ObIAllocator *allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObIJsonBase);
};

/// Predicate that checks if one array element is less than another.
struct Array_less
{
  bool operator() (const ObIJsonBase *idx1, const ObIJsonBase *idx2) const
  {
    int result;
    idx1->compare(*idx2, result);
    return result < 0;
  }
};

struct Array_equal
{
  bool operator() (const ObIJsonBase *idx1, const ObIJsonBase *idx2) const
  {
    int result;
    idx1->compare(*idx2, result);
    return result == 0;
  }
};

class ObJsonBaseFactory
{
public:
  ObJsonBaseFactory() {};
  virtual ~ObJsonBaseFactory() {};
  static int get_json_base(ObIAllocator *allocator, const ObString &buf,
                           ObJsonInType in_type, ObJsonInType expect_type,
                           ObIJsonBase *&out, uint32_t parse_flag = 0);
  static int get_json_tree(ObIAllocator *allocator, const ObString &str,
                           ObJsonInType in_type, ObJsonNode *&out, uint32_t parse_flag = 0);
  static int get_json_base(ObIAllocator *allocator, const char *ptr, uint64_t length,
                           ObJsonInType in_type, ObJsonInType expect_type,
                           ObIJsonBase *&out, uint32_t parse_flag = 0);
  static int transform(ObIAllocator *allocator, ObIJsonBase *src,
                       ObJsonInType expect_type, ObIJsonBase *&out);
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonBaseFactory);
};

class ObJsonBaseUtil final
{
public:
  ObJsonBaseUtil() {}
  virtual ~ObJsonBaseUtil() {}

  // get is valid json datetime type.
  //
  // @param [in] jtype The json node type.
  // @return Returns true on time type, false otherwise.
  static ObObjType get_time_type(ObJsonNodeType json_type);

  // get is valid json datetime type.
  //
  // @param [in] jtype The json node type.
  // @return Returns true on time type, false otherwise.
  static bool is_time_type(ObJsonNodeType j_type);

  // Get ObDTMode by json datetime type.
  //
  // @param [in] jtype The json node type.
  // @param [out] tmode ObDTMode
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int get_dt_mode_by_json_type(ObJsonNodeType j_type, ObDTMode &dt_mode);

  // Append comma(', ') to buf. 
  //
  // @param [in, out] j_buf       The buf that appending comma.
  // @param [in]      is_pretty JSON_PRETTY function sets true, false otherwise.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int append_comma(ObJsonBuffer &j_buf, bool is_pretty);

  // Append newline and indent to buf.
  //
  // @param [in, out] j_buf    The buf that appending newline and indent.
  // @param [in]      level  Number of nested layers to indent.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int append_newline_and_indent(ObJsonBuffer &j_buf, uint64_t level);

  // Handle escape characters and fill into buf.
  //
  // @param [in]      c    The escape character.
  // @param [in, out] j_buf  The buf that appending escape characters.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int escape_character(char c, ObJsonBuffer &j_buf);

  // Add double quotes to the source string, and handle the escape characters, then write them to buf.
  //
  // @param [in, out] j_buf    The dest buf. 
  // @param [in]      cptr   The pointer of source string.
  // @param [in]      length The length of source string.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int add_double_quote(ObJsonBuffer &j_buf, const char *cptr, uint64_t length);

  // Append data to buf, add double quotes if is_quoted is true.
  //
  // @param [in, out] j_buf     The buf that appending data.
  // @param [in] is_quoted    Whether append double quotes or not.
  // @param [in] data         The source string.
  // @param [in] length       The length of source string.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int append_string(ObJsonBuffer &j_buf, bool is_quoted, const char *data, uint64_t length);

  // Compare two numbers.
  //
  // @param [in] a The first value to be compared.
  // @param [in] b The decond value to be compared.
  // @return Less than returns -1, greater than 1, equal returns 0.
  template <class T>
  static inline int compare_numbers(T a, T b) {
    return a < b ? -1 : (a == b ? 0 : 1);
  }

  // Compare int64 with uint64_t.
  //
  // @param [in] a The first value with int64 to be compared.
  // @param [in] b The decond value with uint64_t to be compared.
  // @return Less than returns -1, greater than 1, equal returns 0.
  static int compare_int_uint(int64_t a, uint64_t b);

  // Compare json decimal with uint64_t.
  //
  // @param [in]  a       The first value with ObNumber to be compared.
  // @param [in]  b       The decond value with uint64_t to be compared.
  // @param [out] res     Less than returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int compare_decimal_uint(const number::ObNumber &a, uint64_t b, int &res);

  // Compare int with json.
  //
  // @param [in]  a       The first value with double to be compared.
  // @param [in]  other   The decond value with json type to be compared.
  // @param [out] result  Less than returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int compare_int_json(int a, ObIJsonBase* other, int& result);

  // Compare json decimal with int64_t.
  //
  // @param [in]  a       The first value with ObNumber to be compared.
  // @param [in]  b       The decond value with int64_t to be compared.
  // @param [out] res     Less than returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int compare_decimal_int(const number::ObNumber &a, int64_t b, int &res);

  // Compare json decimal with double.
  //
  // @param [in]  a       The first value with ObNumber to be compared.
  // @param [in]  b       The decond value with double to be compared.
  // @param [out] res     Less than returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int compare_decimal_double(const number::ObNumber &a, double b, int &res);

  // Compare double with int.
  //
  // @param [in]  a       The first value with double to be compared.
  // @param [in]  b       The decond value with int64_t to be compared.
  // @param [out] res     Less than returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int compare_double_int(double a, int64_t b, int &res);

  // Compare double with uint.
  //
  // @param [in]  a       The first value with double to be compared.
  // @param [in]  b       The decond value with uint64_t to be compared.
  // @param [out] res     Less than returns -1, greater than 1, equal returns 0.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int compare_double_uint(double a, uint64_t b, int &res);

  // Change double to number
  //
  // @param [in]  d     The double type.
  // @param [out] num   The result of change.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  template<class T>
  static int double_to_number(double d, T &allocator, number::ObNumber &num);

  // Change number to uint
  //
  // @param [in]  nmb   ObNumber
  // @param [out] value The result of change.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int number_to_uint(number::ObNumber &nmb, uint64_t &value);

  // Change double to uint
  //
  // @param [in]  d     The double type.
  // @param [out] value The result of change.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int double_to_uint(double d, uint64_t &value, bool check_range = false);

  // Get bit length from ObString.
  //
  // @param [in]  str       The ObString.
  // @param [out] bit_len   The bit length of ObString
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int get_bit_len(const ObString &str, int32_t &bit_len);

  // Get bit length from int64.
  //
  // @param [in]  int       The int64_t.
  // @param [out] bit_len   The bit length of int
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int get_bit_len(uint64_t value, int32_t &bit_len);

  // ObString to uint64_t
  //
  // @param [in]  str       The ObString.
  // @return The result of ObString to uint64_t.
  static uint64_t hex_to_uint64(const ObString &str);

  // Change string to bit
  //
  // @param [in]  string  The ObString type.
  // @param [out] value   The result of change.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int string_to_bit(ObString &str, uint64_t &value);

  // Sort array index from orig
  //
  // @param [in]  string  The input jsonBase for sorting.
  // @param [out] vec     The vector of sorted array pointers of input jsonBase.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int sort_array_pointer(ObIJsonBase *orig, ObSortedVector<ObIJsonBase *> &vec);

  static int str_up_to_low(ObIAllocator* allocator, const ObString &src, ObString &dst);
  static int str_low_to_up(ObIAllocator* allocator, const ObString &src, ObString &dst);
  // Binary lookup of an sorted array index.
  //
  // @param [in] vec   The vector of sorted array pointers.
  // @param [in] value The element to be searched.
  // @return  Return true if it exists, return false if it does not.
  static bool binary_search(ObSortedVector<ObIJsonBase *> &vec, ObIJsonBase *value);
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonBaseUtil);
};

class ObJsonHashValue final
{
public:
  ObJsonHashValue()
      : hash_value_(0),
        calc_(NULL)
  {
  }
  explicit ObJsonHashValue(uint64_t hash_val, hash_algo hash_func)
      : hash_value_(hash_val),
        calc_(hash_func)
  {
  }
  virtual ~ObJsonHashValue() {}
  OB_INLINE uint64_t get_hash_value() const { return hash_value_; }
  OB_INLINE void calc_character(uchar c) { calc_hash_value(&c, 1); }
  OB_INLINE void calc_uint64(uint64_t number)
  {
    calc_hash_value(reinterpret_cast<void*>(&number), sizeof(number));
  }
  OB_INLINE void calc_int64(int64_t number)
  {
    calc_hash_value(reinterpret_cast<void*>(&number), sizeof(number));
  }
  OB_INLINE void calc_double(double number)
  {
    calc_hash_value(reinterpret_cast<void*>(&number), sizeof(number));
  }
  OB_INLINE void calc_string(const ObString &str)
  {
    calc_hash_value(str.ptr(), str.length());
  }
  OB_INLINE void calc_num(const number::ObNumber &num)
  { 
    calc_hash_value(reinterpret_cast<void*>(num.get_digits()), num.get_length() < 0 ? 0 : num.get_length());
  }
  int calc_time(ObDTMode dt_mode, const ObIJsonBase *jb);

  static const uchar JSON_ARRAY_FLAG = '\x30';
  static const uchar JSON_OBJ_FLAG = '\x31';
  static const uchar JSON_NULL_FLAG = '\x32';
  static const uchar JSON_BOOL_TRUE = '\x33';
  static const uchar JSON_BOOL_FALSE = '\x34';

private:
  inline void calc_hash_value(const void *data, uint64_t len) { hash_value_ = (calc_ == NULL) ?
      murmurhash64A(data, static_cast<int32_t>(len), hash_value_) : calc_(data, len, hash_value_); }
  uint64_t hash_value_;
  hash_algo calc_;
};

inline bool is_mysql_unsupported_json_column_conversion(ObObjType type)
{
  bool bool_res = false;
  ObObjTypeClass tc = ob_obj_type_class(type);
  if (tc != ObStringTC && tc != ObTextTC && tc != ObJsonTC) {
    bool_res = true;
  }
  return bool_res;
}
} // namespace common
} // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_JSON_BASE