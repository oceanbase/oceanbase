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

#include "sql/parser/parse_node.h"
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "lib/alloc/alloc_assist.h"
#include "sql/parser/parse_malloc.h"
#include "sql/parser/parse_node_hash.h"
#include "sql/parser/parse_define.h"
#include "sql/parser/sql_parser_base.h"
extern const char* get_type_name(int type);

#ifdef SQL_PARSER_COMPILATION
#include "sql/parser/parser_proxy_func.h"
#endif  // SQL_PARSER_COMPILATION

#define WINDOW_FUNCTION_NUM 41
struct FuncPair {
  char* func_name_;
  int yytokentype_;
};

int count_child(ParseNode* root, void* malloc_pool, int* count);

// merge_child:if succ ,return 0, else return 1
int merge_child(ParseNode* node, void* malloc_pool, ParseNode* source_tree, int* index);

void destroy_tree(ParseNode* root)
{
  (void)root;
  /*
  if (OB_UNLIKELY(NULL == root)) {
    //do nothing
  } else {
    int64_t i = 0;
    if (root->num_child_ > 0) {
      if (OB_UNLIKELY(NULL == root->children_)) {
        (void)fprintf(stderr, "ERROR children of root is NULL\n");
      } else {
        for (; i < root->num_child_; ++i) {
          destroy_tree(root->children_[i]);
          root->children_[i] = NULL;
        }
        parse_free(root->children_);
        root->children_ = NULL;
      }
    }
    if (NULL != root->str_value_) {
      parse_free((char *)root->str_value_);
      root->str_value_ = NULL;
    }
    parse_free(root);
    }*/
}

ParseNode* new_node(void* malloc_pool, ObItemType type, int num)
{
  // the mem alloced by parse_malloc has been memset;
  ParseNode* node = (ParseNode*)parse_malloc(sizeof(ParseNode), malloc_pool);
  if (OB_UNLIKELY(NULL == node)) {
    (void)printf("malloc memory failed\n");
  } else {
    node->type_ = type;
    node->num_child_ = num;
    node->param_num_ = 0;
    node->is_neg_ = 0;
    node->is_hidden_const_ = 0;
    node->is_date_unit_ = 0;
    node->is_tree_not_param_ = 0;
    node->length_semantics_ = 0;
    node->is_change_to_char_ = 0;
    node->is_val_paramed_item_idx_ = 0;
    node->is_copy_raw_text_ = 0;
    node->is_column_varchar_ = 0;
    node->is_trans_from_minus_ = 0;
    node->is_assigned_from_child_ = 0;
    node->is_num_must_be_pos_ = 0;
    node->value_ = INT64_MAX;
    node->str_len_ = 0;
    node->str_value_ = NULL;
    node->text_len_ = 0;
    node->raw_text_ = NULL;
    node->pos_ = 0;
#ifdef SQL_PARSER_COMPILATION
    node->token_off_ = -1;
    node->token_len_ = -1;
#endif
    if (num > 0) {
      int64_t alloc_size = sizeof(ParseNode*) * num;
      node->children_ = (ParseNode**)parse_malloc(alloc_size, malloc_pool);
      if (OB_UNLIKELY(NULL == node->children_)) {
        parse_free(node);
        node = NULL;
      }
    } else {
      node->children_ = NULL;
    }
  }
  return node;
}

int count_child(ParseNode* root, void* malloc_pool, int* count)
{
  int ret = OB_PARSER_SUCCESS;
  ParserLinkNode* stack_top = NULL;
  if (NULL == count) {
    ret = OB_PARSER_ERR_UNEXPECTED;
    (void)fprintf(stderr, "ERROR invalid parameter ret=%d\n", ret);
  } else if (NULL == root) {
    *count = 0;
  } else if (NULL == (stack_top = new_link_node(malloc_pool))) {
    ret = OB_PARSER_ERR_NO_MEMORY;
    (void)fprintf(stderr, "ERROR failed to malloc memory\n");
  } else {
    *count = 0;
    stack_top->val_ = root;

    do {
      ParseNode *tree = NULL;
      if (NULL == stack_top || NULL == (tree = (ParseNode *)stack_top->val_)) {
        ret = OB_PARSER_ERR_UNEXPECTED;
        (void)fprintf(stderr, "ERROR invalid null argument\n");
      } else {
        stack_top = stack_top->next_;
      }

      if (OB_PARSER_SUCCESS != ret) {
      } else if (T_LINK_NODE != tree->type_) {
        *count += 1;
      } else if (tree->num_child_ <= 0) {
        // do nothing
      } else {
        if (NULL == tree->children_) {
          ret = OB_PARSER_ERR_UNEXPECTED;
          (void)fprintf(stderr, "ERROR invalid null children\n");
        }
        ParserLinkNode* tmp_node = NULL;
        for (int64_t i = tree->num_child_ - 1; OB_PARSER_SUCCESS == ret && i >= 0; i--) {
          ParseNode* child = tree->children_[i];
          if (NULL == child) {
            // do nothing
          } else if (NULL == (tmp_node = new_link_node(malloc_pool))) {
            ret = OB_PARSER_ERR_NO_MEMORY;
            (void)fprintf(stderr, "ERROR failed to allocate memory\n");
          } else {
            tmp_node->val_ = child;
            tmp_node->next_ = stack_top;
            stack_top = tmp_node;
          }
        }  // for end
      }
    } while (OB_PARSER_SUCCESS == ret && NULL != stack_top);
  }
  return ret;
}

// merge_child:if succ ,return 0, else return 1
int merge_child(ParseNode* node, void* malloc_pool, ParseNode* source_tree, int* index)
{
  int ret = 0;
  ParserLinkNode* stack_top = NULL;
  if (OB_UNLIKELY(NULL == node || NULL == index)) {
    ret = OB_PARSER_ERR_UNEXPECTED;
    (void)fprintf(stderr, "ERROR node%p or index:%p is NULL\n", node, index);
  } else if (NULL == source_tree) {
    // do nothing
  } else if (NULL == (stack_top = new_link_node(malloc_pool))) {
    ret = OB_PARSER_ERR_NO_MEMORY;
    (void)fprintf(stderr, "ERROR failed to malloc memory\n");
  } else {
    stack_top->val_ = source_tree;

    do {
      ParseNode *tree = NULL;
      if (NULL == stack_top || NULL == (tree = (ParseNode *)stack_top->val_)) {
        ret = OB_PARSER_ERR_UNEXPECTED;
        (void)fprintf(stderr, "ERROR invalid null argument\n");
      } else {
        // pop stack
        stack_top = stack_top->next_;
      }
      if (OB_PARSER_SUCCESS != ret) {
        // do nothing
      } else if (T_LINK_NODE != tree->type_) {
        if (OB_UNLIKELY(*index < 0 || *index >= node->num_child_)) {
          ret = OB_PARSER_ERR_UNEXPECTED;
          (void)fprintf(
              stderr, "ERROR invalid index: %d, num_child:%d\n tree: %d", *index, node->num_child_, tree->type_);
        } else if (NULL == node->children_) {
          ret = OB_PARSER_ERR_UNEXPECTED;
          (void)fprintf(stderr, "ERROR invalid null children pointer\n");
        } else {
          node->children_[*index] = tree;
          ++(*index);
        }
      } else if (tree->num_child_ <= 0) {
        // do nothing
      } else if (NULL == tree->children_) {
        ret = OB_PARSER_ERR_UNEXPECTED;
        (void)fprintf(stderr, "ERROR invalid children pointer\n");
      } else {
        ParserLinkNode* tmp_node = NULL;
        for (int64_t i = tree->num_child_ - 1; OB_PARSER_SUCCESS == ret && i >= 0; i--) {
          if (NULL == tree->children_[i]) {
            // do nothing
          } else if (NULL == (tmp_node = new_link_node(malloc_pool))) {
            ret = OB_PARSER_ERR_NO_MEMORY;
            (void)fprintf(stderr, "ERROR failed to malloc memory\n");
          } else {
            // push stack
            tmp_node->val_ = tree->children_[i];
            tmp_node->next_ = stack_top;
            stack_top = tmp_node;
          }
        }  // for end
      }
    } while (OB_PARSER_SUCCESS == ret && (stack_top != NULL));
  }
  return ret;
}

ParseNode* merge_tree(void* malloc_pool, int* fatal_error, ObItemType node_tag, ParseNode* source_tree)
{
  ParseNode* node = NULL;
  ParseNode* ret_node = NULL;
  if (OB_UNLIKELY(NULL == malloc_pool) || OB_UNLIKELY(NULL == fatal_error)) {
    (void)fprintf(stderr, "ERROR parser result is NULL\n");
  } else if (NULL == source_tree) {
    // source_tree may be NULL, do nothing
  } else {
    int index = 0;
    int num = 0;
    int tmp_ret = 0;
    if (OB_UNLIKELY(OB_PARSER_SUCCESS != (tmp_ret = count_child(source_tree, malloc_pool, &num)))) {
      (void)fprintf(stderr, "ERROR fail to , count child num code : %d\n", tmp_ret);
      *fatal_error = tmp_ret;
    } else if (OB_LIKELY(NULL != (node = new_node(malloc_pool, node_tag, num)))) {
      if (OB_UNLIKELY(OB_PARSER_SUCCESS != (tmp_ret = merge_child(node, malloc_pool, source_tree, &index)))) {
        (void)fprintf(stderr, "ERROR fail to merge_child, error code : %d\n", tmp_ret);
        *fatal_error = tmp_ret;
      } else if (index != num) {
        (void)fprintf(stderr, "ERROR index:%d is not equal to num:%d\n", index, num);
      } else {
        ret_node = node;
      }
    } else {
      *fatal_error = OB_PARSER_ERR_NO_MEMORY;
    }
  }
  return ret_node;
}

ParseNode* new_terminal_node(void* malloc_pool, ObItemType type)
{
  int children_num = 0;
  return new_node(malloc_pool, type, children_num);
}

ParseNode* new_non_terminal_node(void* malloc_pool, ObItemType node_tag, int num, ...)
{
  ParseNode* ret_node = NULL;
  if (OB_UNLIKELY(num <= 0)) {
    (void)fprintf(stderr, "ERROR invalid num:%d\n", num);
  } else {
    int32_t i = 0;
    va_list va;
    ret_node = new_node(malloc_pool, node_tag, num);
    if (OB_LIKELY(NULL != ret_node)) {
      va_start(va, num);
      for (; i < num; ++i) {
        ret_node->children_[i] = va_arg(va, ParseNode*);
      }
      va_end(va);
    }
  }
  return ret_node;
}

char* copy_expr_string(ParseResult* p, int expr_start, int expr_end)
{
  char* expr_string = NULL;
  if (OB_UNLIKELY(NULL == p)) {
    (void)fprintf(stderr, "ERROR parser result is NULL\n");
  } else if (OB_UNLIKELY(NULL == p->input_sql_)) {
    (void)fprintf(stderr, "ERROR input sql is NULL\n");
  } else if (OB_UNLIKELY(expr_start < 0 || expr_end < 0 || expr_end < expr_start)) {
    (void)fprintf(stderr, "ERROR invalid argument, expr_start:%d, expr_end:%d\n", expr_start, expr_end);
  } else {
    int len = expr_end - expr_start + 1;
    expr_string = (char*)parse_malloc(len + 1, p->malloc_pool_);
    if (OB_UNLIKELY(NULL == expr_string)) {
      (void)printf("malloc memory failed\n");
    } else {
      memmove(expr_string, p->input_sql_ + expr_start - 1, len);
      expr_string[len] = '\0';
    }
  }
  return expr_string;
}

unsigned char escaped_char(unsigned char c, int* with_back_slash)
{
  *with_back_slash = 0;
  switch (c) {
    case 'n':
      return '\n';
    case 't':
      return '\t';
    case 'r':
      return '\r';
    case 'b':
      return '\b';
    case '0':
      return '\0';
    case 'Z':
      return '\032';
    case '_':
    case '%':
      *with_back_slash = 1;
      return c;
    default:
      return c;
  }
}

///* quote_type: 0 - single quotes; 1 - double quotation marks */
// int64_t ob_parse_string(const char *src, char *dest, int64_t len, int quote_type)
//{
//  int64_t i;
//  int64_t index = 0;
//  int with_back_slash = 1;
//  for (i = 0; i < len; ++i) {
//    unsigned char c = src[i];
//    if (c == '\\') {
//      if (i < len - 1) {
//        c = src[++i];
//      } else {
//        break;
//      }
//      c = escaped_char(c, &with_back_slash);
//      if (with_back_slash)
//      {
//        dest[index++] = '\\';
//      }
//    } else if (quote_type == 0 && c == '\'' && i + 1 < len && src[i + 1] == '\'') {
//      ++i;
//    } else if (quote_type == 1 && c == '"' && i + 1 < len && src[i + 1] == '"') {
//      ++i;
//    }
//    dest[index++] = c;
//  }
//  assert(index <= len);
//  dest[index] = '\0';
//  return index;
//}

static char char_int(char c)
{
  return (c >= '0' && c <= '9' ? c - '0' : (c >= 'A' && c <= 'Z' ? c - 'A' + 10 : c - 'a' + 10));
}

int64_t ob_parse_binary_len(int64_t len)
{
  return (len + 1) / 2;
}

void ob_parse_binary(const char* src, int64_t len, char* dest)
{
  if (OB_UNLIKELY(NULL == src || len <= 0 || NULL == dest)) {
    // do nothing
  } else {
    if (len > 0 && len % 2 != 0) {
      *dest = char_int(src[0]);
      ++src;
      ++dest;
    }
    const char* end = src + len - 1;
    for (; src <= end; src += 2) {
      *dest = (char)(16 * char_int(src[0]) + char_int(src[1]));
      ++dest;
    }
  }
}

int64_t ob_parse_bit_string_len(int64_t len)
{
  return (len + 7) / 8;
}

void ob_parse_bit_string(const char* src, int64_t len, char* dest)
{
  if (OB_UNLIKELY(NULL == src || len <= 0 || NULL == dest)) {
    // do nothing
  } else {
    const char* end = src + len - 1;
    char* dest_end = dest + ob_parse_bit_string_len(len) - 1;
    if (len > 0) {
      unsigned char c = 0;
      unsigned int one_bit = 1;
      for (; end >= src; --end) {
        if (256 == one_bit) /* one byte ready */
        {
          *dest_end = c;
          --dest_end;
          c = 0;
          one_bit = 1;
        }
        if ('1' == *end) {
          c |= one_bit;
        }
        one_bit <<= 1;
      }              /* end for */
      *dest_end = c; /* the first byte */
    }
  }
}

char* str_tolower(char* buff, int64_t len)
{
  if (OB_LIKELY(NULL != buff)) {
    char* ptr = buff;
    char* end = buff + len;
    unsigned char ch = *ptr;
    while (ptr != end) {
      ch = *ptr;
      if (ch >= 'A' && ch <= 'Z') {
        ch += 'a' - 'A';
      } else if (ch >= 0x80 && isupper(ch)) {
        ch = tolower(ch);
      }
      *ptr = ch;
      ptr++;
    }
  }
  return buff;
}

char* str_toupper(char* buff, int64_t len)
{
  if (OB_LIKELY(NULL != buff)) {
    char* ptr = buff;
    char* end = buff + len;
    unsigned char ch = *ptr;
    while (ptr != end) {
      ch = *ptr;
      if (ch >= 'a' && ch <= 'z') {
        ch -= 'a' - 'A';
      } else if (ch >= 0x80 && islower(ch)) {
        ch = toupper(ch);
      }
      *ptr = ch;
      ptr++;
    }
  }
  return buff;
}

int64_t str_remove_space(char* buff, int64_t len)
{
  int64_t length = 0;
  if (OB_LIKELY(NULL != buff)) {
    for (int i = 0; i < len; i++) {
      if (!isspace(buff[i])) {
        buff[length++] = buff[i];
      }
    }
  }
  return length;
}

// calculate hash value of syntax tree recursively
// every member of ParseNode is calculated using murmurhash
uint64_t parsenode_hash(const ParseNode* node)
{
  uint64_t hash_val = 0;
  if (check_stack_overflow_c()) {
    (void)fprintf(stderr, "ERROR stack overflow in recursive function\n");
  } else if (OB_LIKELY(NULL != node)) {
    hash_val = murmurhash(&node->type_, sizeof(node->type_), hash_val);
    hash_val = murmurhash(&node->value_, sizeof(node->value_), hash_val);
    hash_val = murmurhash(&node->str_len_, sizeof(node->str_len_), hash_val);
    if (NULL != node->str_value_) {
      hash_val = murmurhash(node->str_value_, node->str_len_, hash_val);
    }

    uint64_t child_hash_val = 0;
    int32_t i = 0;
    if (node->num_child_ > 0) {
      if (OB_UNLIKELY(NULL == node->children_)) {
        (void)fprintf(stderr, "ERROR children of node is NULL\n");
      } else {
        for (; i < node->num_child_; ++i) {
          if (NULL != node->children_[i]) {
            child_hash_val = parsenode_hash(node->children_[i]);
            hash_val = murmurhash(&child_hash_val, sizeof(child_hash_val), hash_val);
          }
        }
      }
    }
  }
  return hash_val;
}

// compare syntax tree recursively
// every member of ParseNode is compared
bool parsenode_equal(const ParseNode* lnode, const ParseNode* rnode)
{
  bool result = true;
  if (check_stack_overflow_c()) {
    (void)fprintf(stderr, "ERROR stack overflow in recursive function\n");
  } else if (NULL == lnode && NULL == rnode) {
    result = true;
  } else if ((NULL == lnode && NULL != rnode) || (NULL != lnode && NULL == rnode)) {
    result = false;
  } else {
    if (lnode->type_ != rnode->type_ || lnode->value_ != rnode->value_ || lnode->str_len_ != rnode->str_len_ ||
        lnode->num_child_ != rnode->num_child_) {
      result = false;
    } else {
      if (NULL == lnode->str_value_ && NULL == rnode->str_value_) {
        result = true;
      } else if ((NULL == lnode->str_value_ && NULL != rnode->str_value_) ||
                 (NULL != lnode->str_value_ && NULL == rnode->str_value_)) {
        result = false;
      } else if (lnode->str_len_ != rnode->str_len_) {
        result = false;
      } else {
        // T_VARCHAR type: value_ is length, str_value_ is ptr
        // @ref ob_raw_expr.cpp
        if (0 != strncmp(lnode->str_value_, rnode->str_value_, lnode->str_len_)) {
          result = false;
        }
      }
      if (result) {
        if (lnode->num_child_ > 0) {
          if (NULL == lnode->children_ || NULL == rnode->children_) {
            result = false;
          } else {
            int32_t i = 0;
            for (; result && i < lnode->num_child_; ++i) {
              result = parsenode_equal(lnode->children_[i], rnode->children_[i]);
            }
          }
        }
      }
    }
  }
  return result;
}

// Search according to the name, return the subscript of the name when found, add the name and return the subscript when
// found
int64_t get_question_mark(ObQuestionMarkCtx* ctx, void* malloc_pool, const char* name)
{
  int64_t idx = -1;
  if (OB_UNLIKELY(NULL == ctx || NULL == name)) {
    (void)fprintf(stderr, "ERROR question mark ctx or name is NULL\n");
  } else {
    if (NULL == ctx->name_ && 0 == ctx->capacity_) {
      ctx->capacity_ = MAX_QUESTION_MARK;
      // the errocde will be ignored here. TO BE FIXED.
      ctx->name_ = (char **)parse_malloc(sizeof(char*) * MAX_QUESTION_MARK, malloc_pool);
    }
    if (ctx->name_ != NULL) {
      bool valid_name = true;
      for (int64_t i = 0; valid_name && -1 == idx && i < ctx->count_; ++i) {
        if (NULL == ctx->name_[i]) {
          (void)fprintf(stderr, "ERROR name_ in question mark ctx is null\n");
          valid_name = false;
        } else if (0 == STRCASECMP(ctx->name_[i], name)) {
          idx = i;
        }
      }
      if (-1 == idx && valid_name) {
        if (ctx->count_ >= ctx->capacity_) {
          void *buf = parse_malloc(sizeof(char*) * (ctx->capacity_ * 2), malloc_pool);
          if (OB_UNLIKELY(NULL == buf)) {
            ctx->name_ = NULL;
            (void)printf("ERROR malloc memory failed\n");
          } else {
            MEMCPY(buf, ctx->name_, sizeof(char*) * ctx->capacity_);
            ctx->capacity_ *= 2;
            ctx->name_ = (char **)buf;
          }
        }
        if (ctx->name_ != NULL) {
          int64_t len = 0;
          ctx->name_[ctx->count_] = parse_strdup(name, malloc_pool, &len);
          idx = ctx->count_++;
        }
      }
    } else {
      (void)fprintf(stderr, "ERROR question mark name buffer is null\n");
    }
  }
  return idx;
}

ParserLinkNode* new_link_node(void* malloc)
{
  ParserLinkNode* new_node = (ParserLinkNode*)parse_malloc(sizeof(ParserLinkNode), malloc);
  if (NULL == new_node) {
    (void)printf("ERROR malloc memory failed\n");
  } else {
    new_node->next_ = NULL;
    new_node->prev_ = NULL;
    new_node->val_ = NULL;
  }
  return new_node;
}

bool nodename_equal(const ParseNode* node, const char* pattern, int64_t pat_len)
{
  bool result = true;
  if (NULL == node || NULL == node->str_value_ || NULL == pattern || node->str_len_ != pat_len) {
    result = false;
  } else {
    result = true;
    for (int64_t i = 0; result && i < pat_len; ++i) {
      if (toupper(node->str_value_[i]) != toupper(pattern[i])) {
        result = false;
      }
    }
  }
  return result;
}

int binary_search(const struct FuncPair* window_func, int64_t begin, int64_t end, const char* value)
{
  int result = -1;
  bool need_break = false;
  while (!need_break && begin <= end) {
    int mid = begin + (end - begin) / 2;
    int cmp = strcmp(window_func[mid].func_name_, value);
    if (cmp > 0) {
      end = mid - 1;
    } else if (cmp < 0) {
      begin = mid + 1;
    } else {
      result = window_func[mid].yytokentype_;
      need_break = true;
    }
  }
  return result;
}
