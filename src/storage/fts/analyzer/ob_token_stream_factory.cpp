/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/fts/analyzer/ob_token_stream_factory.h"
#include "lib/allocator/page_arena.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "storage/fts/analyzer/tokenizer/ob_legacy_parser_tokenizer.h"
#include "storage/fts/analyzer/tokenizer/ob_standard_tokenizer.h"
#include "storage/fts/analyzer/tokenizer/ob_keyword_tokenizer.h"
#include "storage/fts/analyzer/char_filter/ob_legacy_char_filter.h"
#include "storage/fts/analyzer/filter/ob_legacy_token_filter.h"
#include "storage/fts/analyzer/filter/ob_stop_word_filter.h"
#include "storage/fts/analyzer/filter/ob_lower_case_filter.h"
#include "storage/fts/analyzer/filter/ob_decimal_digit_filter.h"
#include "storage/fts/analyzer/filter/ob_possessive_english_filter.h"
#include "storage/fts/analyzer/filter/ob_snowball_filter.h"
#include "storage/fts/analyzer/filter/ob_icu_normalizer2_filter.h"
#include "storage/fts/analyzer/filter/ob_charset_convert_filter.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/fts/ob_fts_literal.h"
#include "storage/fts/ob_fts_plugin_helper.h"

namespace oceanbase
{
namespace storage
{

int ObAnalyzerSpecFactory::create_analyzer_spec(
    const common::ObString &analysis_json,
    common::ObIAllocator &allocator,
    ObAnalyzerSpec *&analyzer_spec)
{
  int ret = OB_SUCCESS;
  analyzer_spec = nullptr;
  if (OB_UNLIKELY(analysis_json.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("analysis json is empty", K(ret));
  } else {
    common::ObJsonNode *root = nullptr;
    if (OB_FAIL(common::ObJsonParser::get_tree(&allocator, analysis_json, root))) {
      LOG_WARN("fail to parse analysis json", K(ret), K(analysis_json));
    } else if (OB_ISNULL(root)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json root is null", K(ret));
    } else {
      common::ObIJsonBase *analyzer_val = nullptr;
      common::ObString analyzer_key("analyzer");
      if (OB_FAIL(root->get_object_value(analyzer_key, analyzer_val))) {
        LOG_WARN("invalid analysis json: missing analyzer field", K(ret), K(analysis_json));
      } else if (OB_ISNULL(analyzer_val)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("analyzer value is null after successful lookup", K(ret), K(analysis_json));
      } else if (analyzer_val->json_type() == common::ObJsonNodeType::J_STRING) {
        common::ObString analyzer_type(static_cast<int32_t>(analyzer_val->get_data_length()),
                                       analyzer_val->get_data());
        if (OB_FAIL(create_builtin_analyzer_spec_(analyzer_type, allocator, analyzer_spec))) {
          LOG_WARN("failed to create builtin analyzer spec", K(ret), K(analyzer_type));
        }
      } else if (analyzer_val->json_type() == common::ObJsonNodeType::J_OBJECT) {
#ifdef OB_BUILD_PACKAGE
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("custom analyzer is not supported in package build", K(ret), K(analysis_json));
#else
        if (OB_FAIL(create_custom_analyzer_spec_(*root, allocator, analyzer_spec))) {
          LOG_WARN("failed to create custom analyzer spec", K(ret), K(analysis_json));
        }
#endif
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid analysis json: analyzer field must be string or object",
                 K(ret), K(analysis_json));
      }
    }
  }

  if (OB_FAIL(ret)) {
    destroy_analyzer_spec(allocator, analyzer_spec);
  }
  return ret;
}

int ObAnalyzerSpecFactory::check_analyzer_use_ik_tokenizer(
    const common::ObString &analysis_json,
    common::ObIAllocator &allocator,
    bool &use_ik_tokenizer)
{
  int ret = OB_SUCCESS;
  common::ObJsonNode *root_json = nullptr;
  common::ObIJsonBase *analyzer_val = nullptr;
  use_ik_tokenizer = false;

  if (OB_FAIL(common::ObJsonParser::get_tree(&allocator, analysis_json, root_json))) {
    LOG_WARN("fail to parse analysis json", K(ret), K(analysis_json));
  } else if (OB_ISNULL(root_json)
      || root_json->json_type() != common::ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("analysis json root must be an object", K(ret), K(analysis_json));
  } else {
    common::ObString analyzer_key("analyzer");
    if (OB_FAIL(root_json->get_object_value(analyzer_key, analyzer_val))) {
      LOG_WARN("missing analyzer field in analysis json", K(ret), K(analysis_json));
    } else if (OB_ISNULL(analyzer_val)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("analyzer value is null", K(ret), K(analysis_json));
    } else if (analyzer_val->json_type() == common::ObJsonNodeType::J_STRING) {
      // Built-in analyzers do not use the IK tokenizer directly.
    } else if (analyzer_val->json_type() != common::ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("analyzer field must be string or object", K(ret), K(analysis_json));
    } else {
      common::ObIJsonBase *analyzer_def = nullptr;
      if (OB_UNLIKELY(analyzer_val->element_count() != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("exactly one analyzer definition is expected", K(ret),
                 K(analyzer_val->element_count()));
      } else if (OB_FAIL(analyzer_val->get_object_value(static_cast<uint64_t>(0),
                                                        analyzer_def))) {
        LOG_WARN("failed to get analyzer definition", K(ret), K(analysis_json));
      } else if (OB_ISNULL(analyzer_def)
          || analyzer_def->json_type() != common::ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("analyzer definition must be an object", K(ret), K(analysis_json));
      } else {
        common::ObIJsonBase *tokenizer_val = nullptr;
        common::ObString tokenizer_key("tokenizer");
        if (OB_FAIL(analyzer_def->get_object_value(tokenizer_key, tokenizer_val))) {
          LOG_WARN("missing tokenizer field in analyzer definition", K(ret), K(analysis_json));
        } else if (OB_ISNULL(tokenizer_val)
            || tokenizer_val->json_type() != common::ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tokenizer field must be a string reference", K(ret), K(analysis_json));
        } else {
          common::ObString tokenizer_name(static_cast<int32_t>(tokenizer_val->get_data_length()),
                                          tokenizer_val->get_data());
          if (0 == tokenizer_name.case_compare(ObFTSLiteral::PARSER_NAME_IK)) {
            use_ik_tokenizer = true;
          } else if (is_builtin_tokenizer_type_(tokenizer_name)) {
            // Other built-in tokenizer types do not need IK dictionary loading.
          } else {
            common::ObIJsonBase *tokenizer_section = nullptr;
            common::ObIJsonBase *tokenizer_def = nullptr;
            common::ObIJsonBase *type_val = nullptr;
            common::ObString type_key("type");
            if (OB_FAIL(root_json->get_object_value(tokenizer_key, tokenizer_section))) {
              LOG_WARN("tokenizer name not recognized and no tokenizer section in JSON",
                       K(ret), K(tokenizer_name));
            } else if (OB_ISNULL(tokenizer_section)
                || tokenizer_section->json_type() != common::ObJsonNodeType::J_OBJECT) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("tokenizer section must be an object", K(ret), K(analysis_json));
            } else if (OB_FAIL(tokenizer_section->get_object_value(tokenizer_name,
                                                                   tokenizer_def))) {
              LOG_WARN("tokenizer definition not found", K(ret), K(tokenizer_name));
            } else if (OB_ISNULL(tokenizer_def)
                || tokenizer_def->json_type() != common::ObJsonNodeType::J_OBJECT) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("tokenizer definition must be an object", K(ret), K(tokenizer_name));
            } else if (OB_FAIL(tokenizer_def->get_object_value(type_key, type_val))) {
              LOG_WARN("missing type field in tokenizer definition", K(ret), K(tokenizer_name));
            } else if (OB_ISNULL(type_val)
                || type_val->json_type() != common::ObJsonNodeType::J_STRING) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("tokenizer type must be a string", K(ret), K(tokenizer_name));
            } else {
              common::ObString tokenizer_type(static_cast<int32_t>(type_val->get_data_length()),
                                              type_val->get_data());
              use_ik_tokenizer = (0 == tokenizer_type.case_compare(ObFTSLiteral::PARSER_NAME_IK));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObAnalyzerSpecFactory::create_builtin_analyzer_spec_(
    const common::ObString &analyzer_type,
    common::ObIAllocator &allocator,
    ObAnalyzerSpec *&analyzer_spec)
{
  int ret = OB_SUCCESS;
  void *spec_buf = nullptr;
  if (OB_ISNULL(spec_buf = allocator.alloc(sizeof(ObAnalyzerSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate analyzer spec", K(ret));
  } else if (FALSE_IT(analyzer_spec = new (spec_buf) ObAnalyzerSpec(allocator))) {
  } else if (OB_FAIL(resolve_builtin_analyzer_type_(analyzer_type, analyzer_spec->analyzer_type_))) {
    LOG_WARN("failed to resolve builtin analyzer type", K(ret), K(analyzer_type));
  } else {
    switch (analyzer_spec->analyzer_type_) {
      case ObAnalyzerType::ANALYZER_TYPE_STANDARD:
        ret = build_standard_analyzer_spec_(allocator, *analyzer_spec);
        break;
      case ObAnalyzerType::ANALYZER_TYPE_ENGLISH:
        ret = build_english_analyzer_spec_(allocator, *analyzer_spec);
        break;
      case ObAnalyzerType::ANALYZER_TYPE_THAI:
        ret = build_thai_analyzer_spec_(allocator, *analyzer_spec);
        break;
      case ObAnalyzerType::ANALYZER_TYPE_VIETNAMESE:
        ret = build_vietnamese_analyzer_spec_(allocator, *analyzer_spec);
        break;
      case ObAnalyzerType::ANALYZER_TYPE_INDONESIAN:
        ret = build_indonesian_analyzer_spec_(allocator, *analyzer_spec);
        break;
      case ObAnalyzerType::ANALYZER_TYPE_MALAY:
        ret = build_malay_analyzer_spec_(allocator, *analyzer_spec);
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported builtin analyzer type", K(ret), K(analyzer_type));
        break;
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::create_custom_analyzer_spec_(
    common::ObJsonNode &root_json,
    common::ObIAllocator &allocator,
    ObAnalyzerSpec *&analyzer_spec)
{
  int ret = OB_SUCCESS;
  void *spec_buf = nullptr;

  // 1. Extract the "analyzer" object from root
  common::ObIJsonBase *analyzer_section = nullptr;
  common::ObString analyzer_key("analyzer");
  if (OB_FAIL(root_json.get_object_value(analyzer_key, analyzer_section))) {
    LOG_WARN("missing analyzer field in custom analyzer JSON", K(ret));
  } else if (OB_ISNULL(analyzer_section)
      || analyzer_section->json_type() != common::ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("analyzer field must be an object for custom analyzer", K(ret));
  } else if (OB_UNLIKELY(analyzer_section->element_count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("exactly one analyzer definition is expected", K(ret),
             K(analyzer_section->element_count()));
  }

  // 2. Get the single analyzer definition (the value of the first key)
  common::ObIJsonBase *analyzer_def = nullptr;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(analyzer_section->get_object_value(static_cast<uint64_t>(0), analyzer_def))) {
      LOG_WARN("failed to get analyzer definition", K(ret));
    } else if (OB_ISNULL(analyzer_def)
        || analyzer_def->json_type() != common::ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("analyzer definition must be an object", K(ret));
    }
  }

  // 3. Validate "type" == "custom"
  if (OB_SUCC(ret)) {
    common::ObIJsonBase *type_val = nullptr;
    common::ObString type_key("type");
    if (OB_FAIL(analyzer_def->get_object_value(type_key, type_val))) {
      LOG_WARN("missing type field in analyzer definition", K(ret));
    } else if (OB_ISNULL(type_val)
        || type_val->json_type() != common::ObJsonNodeType::J_STRING) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("type field must be a string", K(ret));
    } else {
      common::ObString type_str(static_cast<int32_t>(type_val->get_data_length()),
                                type_val->get_data());
      if (0 != type_str.case_compare("custom")) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("only custom type is supported for user-defined analyzer", K(ret), K(type_str));
      }
    }
  }

  // 4. Allocate ObAnalyzerSpec
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(spec_buf = allocator.alloc(sizeof(ObAnalyzerSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate analyzer spec", K(ret));
    } else {
      analyzer_spec = new (spec_buf) ObAnalyzerSpec(allocator);
      analyzer_spec->analyzer_type_ = ObAnalyzerType::ANALYZER_TYPE_CUSTOM;
    }
  }

  // 5. Resolve tokenizer: try built-in type name first, then lookup in root definition
  if (OB_SUCC(ret)) {
    common::ObIJsonBase *tok_ref_val = nullptr;
    common::ObString tok_key("tokenizer");
    if (OB_FAIL(analyzer_def->get_object_value(tok_key, tok_ref_val))) {
      LOG_WARN("missing tokenizer field in custom analyzer", K(ret));
    } else if (OB_ISNULL(tok_ref_val)
        || tok_ref_val->json_type() != common::ObJsonNodeType::J_STRING) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tokenizer field must be a string reference", K(ret));
    } else {
      common::ObString tok_ref_name(static_cast<int32_t>(tok_ref_val->get_data_length()),
                                    tok_ref_val->get_data());
      if (is_builtin_tokenizer_type_(tok_ref_name)) {
        // Check naming conflict: root "tokenizer" section must not contain the built-in name.
        // The section/key may be absent; other lookup failures should be propagated.
        common::ObIJsonBase *tokenizer_section = nullptr;
        int tmp_ret = root_json.get_object_value(tok_key, tokenizer_section);
        if (OB_SEARCH_NOT_FOUND == tmp_ret) {
          tmp_ret = OB_SUCCESS;
        } else if (OB_FAIL(tmp_ret)) {
          LOG_WARN("failed to get tokenizer section", K(ret), K(tok_ref_name));
        } else if (OB_ISNULL(tokenizer_section)
            || tokenizer_section->json_type() != common::ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tokenizer section must be an object", K(ret), K(tok_ref_name));
        } else {
          common::ObIJsonBase *conflict_def = nullptr;
          tmp_ret = tokenizer_section->get_object_value(tok_ref_name, conflict_def);
          if (OB_SEARCH_NOT_FOUND == tmp_ret) {
            tmp_ret = OB_SUCCESS;
          } else if (OB_FAIL(tmp_ret)) {
            LOG_WARN("failed to get tokenizer definition for conflict check",
                     K(ret), K(tok_ref_name));
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("user-defined tokenizer name conflicts with built-in type name",
                     K(ret), K(tok_ref_name));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(resolve_builtin_tokenizer_spec_(tok_ref_name, allocator,
                                                       analyzer_spec->tokenizer_spec_))) {
            LOG_WARN("failed to resolve builtin tokenizer spec", K(ret), K(tok_ref_name));
          }
        }
      } else {
        // Non-built-in name: lookup in root "tokenizer" section
        common::ObIJsonBase *tokenizer_section = nullptr;
        common::ObIJsonBase *tok_def = nullptr;
        if (OB_FAIL(root_json.get_object_value(tok_key, tokenizer_section))) {
          LOG_WARN("tokenizer name not recognized and no tokenizer section in JSON",
                   K(ret), K(tok_ref_name));
        } else if (OB_ISNULL(tokenizer_section)
            || tokenizer_section->json_type() != common::ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tokenizer section must be an object", K(ret));
        } else if (OB_FAIL(tokenizer_section->get_object_value(tok_ref_name, tok_def))) {
          LOG_WARN("tokenizer definition not found", K(ret), K(tok_ref_name));
        } else if (OB_ISNULL(tok_def)
            || tok_def->json_type() != common::ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tokenizer definition must be an object", K(ret), K(tok_ref_name));
        } else if (OB_FAIL(resolve_custom_tokenizer_spec_(*tok_def, allocator,
                                                           analyzer_spec->tokenizer_spec_))) {
          LOG_WARN("failed to resolve custom tokenizer spec", K(ret), K(tok_ref_name));
        }
      }
    }
  }

  // 6. Count user-defined char filters and token filters for capacity reservation
  int64_t user_char_filter_count = 0;
  int64_t user_token_filter_count = 0;
  if (OB_SUCC(ret)) {
    common::ObIJsonBase *cf_array = nullptr;
    common::ObString cf_key("char_filter");
    int tmp_ret = analyzer_def->get_object_value(cf_key, cf_array);
    if (OB_SEARCH_NOT_FOUND == tmp_ret) {
      tmp_ret = OB_SUCCESS;
    } else if (OB_FAIL(tmp_ret)) {
      LOG_WARN("failed to get char_filter field", K(ret));
    } else if (OB_ISNULL(cf_array)
        || cf_array->json_type() != common::ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("char_filter field must be an array", K(ret));
    } else {
      user_char_filter_count = static_cast<int64_t>(cf_array->element_count());
    }
  }
  if (OB_SUCC(ret)) {
    common::ObIJsonBase *filter_array = nullptr;
    common::ObString filter_key("filter");
    int tmp_ret = analyzer_def->get_object_value(filter_key, filter_array);
    if (OB_SEARCH_NOT_FOUND == tmp_ret) {
      tmp_ret = OB_SUCCESS;
    } else if (OB_FAIL(tmp_ret)) {
      LOG_WARN("failed to get filter field", K(ret));
    } else if (OB_ISNULL(filter_array)
        || filter_array->json_type() != common::ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("filter field must be an array", K(ret));
    } else {
      user_token_filter_count = static_cast<int64_t>(filter_array->element_count());
    }
  }

  // 7. Init default filters (utf8mb4_bin char filter + optional MinMax token filter)
  if (OB_SUCC(ret)) {
    bool need_min_max_token_filter = true;
    if (OB_NOT_NULL(analyzer_spec->tokenizer_spec_)
        && ObTokenizerType::TOKENIZER_TYPE_STANDARD == analyzer_spec->tokenizer_spec_->type_) {
      const ObStandardTokenizerSpec &standard_tokenizer_spec =
          static_cast<const ObStandardTokenizerSpec &>(*analyzer_spec->tokenizer_spec_);
      if (standard_tokenizer_spec.max_token_length_
          <= ObLegacyMinMaxTokenFilter::MAX_CHAR_COUNT_PER_TOKEN) {
        need_min_max_token_filter = false;
      }
    }
    if (OB_FAIL(init_default_filter_specs_(user_char_filter_count, user_token_filter_count,
                                            need_min_max_token_filter,
                                            allocator, *analyzer_spec))) {
      LOG_WARN("failed to init default filter specs for custom analyzer", K(ret));
    }
  }

  // 8. Resolve user-defined char filters (phase 1: no user char filters supported)
  if (OB_SUCC(ret) && user_char_filter_count > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("user-defined char filters are not supported yet", K(ret), K(user_char_filter_count));
  }

  // 9. Resolve token filters from "filter" array
  if (OB_SUCC(ret) && user_token_filter_count > 0) {
    common::ObIJsonBase *filter_array = nullptr;
    common::ObString filter_key("filter");
    if (OB_FAIL(analyzer_def->get_object_value(filter_key, filter_array))) {
      LOG_WARN("failed to re-fetch filter array", K(ret));
    }
    // Get the root "token_filter" section for custom filter lookups
    common::ObIJsonBase *token_filter_section = nullptr;
    common::ObString tf_section_key("filter");
    if (OB_SUCC(ret)) {
      // Not mandatory: only needed if user references custom-defined filters
      int tmp_ret = root_json.get_object_value(tf_section_key, token_filter_section);
      if (OB_SEARCH_NOT_FOUND == tmp_ret) {
        tmp_ret = OB_SUCCESS;
      } else if (OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to get token filter section", K(ret));
      } else if (OB_ISNULL(token_filter_section)
          || token_filter_section->json_type() != common::ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("token filter section must be an object", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < user_token_filter_count; ++i) {
      common::ObIJsonBase *filter_elem = nullptr;
      if (OB_FAIL(filter_array->get_array_element(static_cast<uint64_t>(i), filter_elem))) {
        LOG_WARN("failed to get filter array element", K(ret), K(i));
      } else if (OB_ISNULL(filter_elem)
          || filter_elem->json_type() != common::ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("filter array element must be a string", K(ret), K(i));
      } else {
        common::ObString filter_name(static_cast<int32_t>(filter_elem->get_data_length()),
                                     filter_elem->get_data());
        ObTokenFilterSpec *tf_spec = nullptr;
        if (is_builtin_token_filter_name_(filter_name)) {
          // Check naming conflict: root "filter" section must not redefine built-in names.
          // Use plain comparison instead of OB_SUCC() to avoid polluting ret when
          // the built-in name is simply absent from the user-defined section.
          if (OB_NOT_NULL(token_filter_section)) {
            common::ObIJsonBase *conflict_def = nullptr;
            int tmp_ret = token_filter_section->get_object_value(filter_name, conflict_def);
            if (OB_SUCCESS == tmp_ret) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("user-defined token filter name conflicts with built-in name",
                       K(ret), K(filter_name));
            } else if (OB_SEARCH_NOT_FOUND != tmp_ret) {
              ret = tmp_ret;
              LOG_WARN("failed to check token filter naming conflict",
                       K(ret), K(filter_name));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(resolve_builtin_token_filter_spec_(filter_name, allocator, tf_spec))) {
              LOG_WARN("failed to resolve builtin token filter", K(ret), K(filter_name));
            } else if (OB_FAIL(analyzer_spec->token_filter_specs_.push_back(tf_spec))) {
              LOG_WARN("failed to push back builtin token filter spec", K(ret), K(filter_name));
              tf_spec->~ObTokenFilterSpec();
              allocator.free(tf_spec);
            }
          }
        } else {
          // Non-built-in name: look up in root "token_filter" section
          common::ObIJsonBase *tf_def = nullptr;
          if (OB_ISNULL(token_filter_section)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("filter name not recognized and no token_filter section in JSON",
                     K(ret), K(filter_name));
          } else if (OB_FAIL(token_filter_section->get_object_value(filter_name, tf_def))) {
            LOG_WARN("token filter definition not found", K(ret), K(filter_name));
          } else if (OB_ISNULL(tf_def)
              || tf_def->json_type() != common::ObJsonNodeType::J_OBJECT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("token filter definition must be an object", K(ret), K(filter_name));
          } else if (OB_FAIL(resolve_custom_token_filter_spec_(*tf_def, allocator, tf_spec))) {
            LOG_WARN("failed to resolve custom token filter spec", K(ret), K(filter_name));
          } else if (OB_FAIL(analyzer_spec->token_filter_specs_.push_back(tf_spec))) {
            LOG_WARN("failed to push back custom token filter spec", K(ret), K(filter_name));
            tf_spec->~ObTokenFilterSpec();
            allocator.free(tf_spec);
          }
        }
      }
    }
  }

  // 10. Append the trailing charset_convert token filter as the LAST filter, so the
  //     analyzer's outgoing tokens are converted from utf8mb4_bin back to source.
  if (OB_SUCC(ret) && OB_NOT_NULL(analyzer_spec)) {
    if (OB_FAIL(append_charset_convert_token_filter_spec_(allocator, *analyzer_spec))) {
      LOG_WARN("failed to append charset convert filter spec", K(ret));
    }
  }

  return ret;
}

bool ObAnalyzerSpecFactory::is_builtin_tokenizer_type_(const common::ObString &name)
{
  return 0 == name.case_compare("standard")
      || 0 == name.case_compare("ik")
      || 0 == name.case_compare("keyword");
}

bool ObAnalyzerSpecFactory::is_builtin_token_filter_name_(const common::ObString &name)
{
  // Built-in token filters have default specs and can be referenced directly by name
  // in the analyzer "filter" array without an explicit top-level filter definition.
  return 0 == name.case_compare("lowercase")
      || 0 == name.case_compare("possessive_english")
      || 0 == name.case_compare("stop")
      || 0 == name.case_compare("decimal_digit")
      || 0 == name.case_compare("icu_normalizer")
      || 0 == name.case_compare("icu_folding")
      || 0 == name.case_compare("porter_stem")
      || 0 == name.case_compare("snowball");
}

int ObAnalyzerSpecFactory::resolve_builtin_tokenizer_spec_(
    const common::ObString &type_name,
    common::ObIAllocator &allocator,
    ObTokenizerSpec *&tokenizer_spec)
{
  int ret = OB_SUCCESS;
  tokenizer_spec = nullptr;
  void *buf = nullptr;
  if (0 == type_name.case_compare("standard")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStandardTokenizerSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate standard tokenizer spec", K(ret));
    } else {
      tokenizer_spec = new (buf) ObStandardTokenizerSpec();
    }
  } else if (0 == type_name.case_compare("ik")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObIKTokenizerSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate ik tokenizer spec", K(ret));
    } else {
      ObIKTokenizerSpec *ik_spec = new (buf) ObIKTokenizerSpec();
      ik_spec->main_dict_id_ = share::OB_FT_DICT_IK_UTF8_TID;
      ik_spec->quan_dict_id_ = share::OB_FT_QUANTIFIER_IK_UTF8_TID;
      ik_spec->stopword_dict_id_ = share::OB_FT_STOPWORD_IK_UTF8_TID;
      ik_spec->main_dict_name_ = common::ObString(ObFTSLiteral::FT_DEFAULT_IK_DICT_UTF8_TABLE);
      ik_spec->quan_dict_name_ = common::ObString(ObFTSLiteral::FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE);
      ik_spec->stopword_dict_name_ = common::ObString(ObFTSLiteral::FT_DEFAULT_IK_STOPWORD_UTF8_TABLE);
      tokenizer_spec = ik_spec;
    }
  } else if (0 == type_name.case_compare("keyword")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObKeywordTokenizerSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate keyword tokenizer spec", K(ret));
    } else {
      tokenizer_spec = new (buf) ObKeywordTokenizerSpec();
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unrecognized builtin tokenizer type", K(ret), K(type_name));
  }
  return ret;
}

int ObAnalyzerSpecFactory::resolve_custom_tokenizer_spec_(
    const common::ObIJsonBase &tok_def_json,
    common::ObIAllocator &allocator,
    ObTokenizerSpec *&tokenizer_spec)
{
  int ret = OB_SUCCESS;
  tokenizer_spec = nullptr;
  common::ObIJsonBase *type_val = nullptr;
  common::ObString type_key("type");
  if (OB_FAIL(tok_def_json.get_object_value(type_key, type_val))) {
    LOG_WARN("missing type field in tokenizer definition", K(ret));
  } else if (OB_ISNULL(type_val)
      || type_val->json_type() != common::ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tokenizer type must be a string", K(ret));
  } else {
    common::ObString type_str(static_cast<int32_t>(type_val->get_data_length()),
                              type_val->get_data());
    if (0 == type_str.case_compare("standard")) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStandardTokenizerSpec)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate standard tokenizer spec", K(ret));
      } else {
        tokenizer_spec = new (buf) ObStandardTokenizerSpec();
      }
    } else if (0 == type_str.case_compare("ik")) {
      bool ik_mode_smart = true;
      common::ObIJsonBase *ik_mode_val = nullptr;
      common::ObString ik_mode_key("ik_mode");
      int tmp_ret = tok_def_json.get_object_value(ik_mode_key, ik_mode_val);
      if (OB_SEARCH_NOT_FOUND == tmp_ret) {
        tmp_ret = OB_SUCCESS;
      } else if (OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to get ik_mode field", K(ret));
      } else if (OB_ISNULL(ik_mode_val)
          || ik_mode_val->json_type() != common::ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ik_mode field must be a string", K(ret));
      } else {
        common::ObString mode_str(static_cast<int32_t>(ik_mode_val->get_data_length()),
                                  ik_mode_val->get_data());
        if (0 == mode_str.case_compare("smart")) {
          ik_mode_smart = true;
        } else if (0 == mode_str.case_compare("max_word")) {
          ik_mode_smart = false;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unsupported ik_mode value", K(ret), K(mode_str));
        }
      }
      if (OB_SUCC(ret)) {
        void *buf = nullptr;
        if (OB_ISNULL(buf = allocator.alloc(sizeof(ObIKTokenizerSpec)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate ik tokenizer spec", K(ret));
        } else {
          ObIKTokenizerSpec *ik_spec = new (buf) ObIKTokenizerSpec();
          ik_spec->ik_mode_smart_ = ik_mode_smart;
          ik_spec->main_dict_id_ = share::OB_FT_DICT_IK_UTF8_TID;
          ik_spec->quan_dict_id_ = share::OB_FT_QUANTIFIER_IK_UTF8_TID;
          ik_spec->stopword_dict_id_ = share::OB_FT_STOPWORD_IK_UTF8_TID;
          ik_spec->main_dict_name_ = common::ObString(ObFTSLiteral::FT_DEFAULT_IK_DICT_UTF8_TABLE);
          ik_spec->quan_dict_name_ = common::ObString(ObFTSLiteral::FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE);
          ik_spec->stopword_dict_name_ = common::ObString(ObFTSLiteral::FT_DEFAULT_IK_STOPWORD_UTF8_TABLE);
          tokenizer_spec = ik_spec;
        }
      }
    } else if (0 == type_str.case_compare("keyword")) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObKeywordTokenizerSpec)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate keyword tokenizer spec", K(ret));
      } else {
        tokenizer_spec = new (buf) ObKeywordTokenizerSpec();
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported tokenizer type for custom analyzer", K(ret), K(type_str));
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::resolve_custom_token_filter_spec_(
    const common::ObIJsonBase &tf_def_json,
    common::ObIAllocator &allocator,
    ObTokenFilterSpec *&token_filter_spec)
{
  int ret = OB_SUCCESS;
  token_filter_spec = nullptr;
  common::ObIJsonBase *type_val = nullptr;
  common::ObString type_key("type");
  if (OB_FAIL(tf_def_json.get_object_value(type_key, type_val))) {
    LOG_WARN("missing type field in token filter definition", K(ret));
  } else if (OB_ISNULL(type_val)
      || type_val->json_type() != common::ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("token filter type must be a string", K(ret));
  } else {
    common::ObString type_str(static_cast<int32_t>(type_val->get_data_length()),
                              type_val->get_data());
    if (0 == type_str.case_compare("stop")) {
      ObStopWordLanguageKind language = ObStopWordLanguageKind::LANGUAGE_ENGLISH;
      common::ObIJsonBase *sw_val = nullptr;
      common::ObString sw_key("stopwords");
      if (OB_FAIL(tf_def_json.get_object_value(sw_key, sw_val))) {
        if (OB_SEARCH_NOT_FOUND == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get mode parameter", K(ret));
        }
      } else if (OB_ISNULL(sw_val)
          || sw_val->json_type() != common::ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("stopwords field must be a string", K(ret));
      } else {
        common::ObString sw_str(static_cast<int32_t>(sw_val->get_data_length()),
                                sw_val->get_data());
        if (0 == sw_str.case_compare("_english_")) {
          language = ObStopWordLanguageKind::LANGUAGE_ENGLISH;
        } else if (0 == sw_str.case_compare("_thai_")) {
          language = ObStopWordLanguageKind::LANGUAGE_THAI;
        } else if (0 == sw_str.case_compare("_indonesian_")) {
          language = ObStopWordLanguageKind::LANGUAGE_INDONESIAN;
        } else if (0 == sw_str.case_compare("_none_")) {
          language = ObStopWordLanguageKind::LANGUAGE_NONE;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported stopwords language", K(ret), K(sw_str));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Unsupported value for parameter 'stopwords' in stop filter");
        }
      }
      if (OB_SUCC(ret)) {
        void *buf = nullptr;
        if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStopWordFilterSpec)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate stop word filter spec", K(ret));
        } else {
          ObStopWordFilterSpec *stop_spec = new (buf) ObStopWordFilterSpec();
          stop_spec->language_ = language;
          token_filter_spec = stop_spec;
        }
      }
    } else if (0 == type_str.case_compare("snowball")) {
      // Align with Elasticsearch: when "language" is omitted, snowball defaults to English.
      ObSnowballFilterSpec::Algorithm lang = ObSnowballFilterSpec::Algorithm::ENGLISH;
      common::ObIJsonBase *lang_val = nullptr;
      common::ObString lang_key = "language";
      if (OB_FAIL(tf_def_json.get_object_value(lang_key, lang_val))) {
        if (OB_SEARCH_NOT_FOUND == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get language parameter", K(ret));
        }
      } else if (OB_ISNULL(lang_val) || common::ObJsonNodeType::J_STRING != lang_val->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("language paramter must be a string", K(ret));
      } else {
        common::ObString lang_str(static_cast<int32_t>(lang_val->get_data_length()),
                                  lang_val->get_data());
        if (0 == lang_str.case_compare("arabic")) {
          lang = ObSnowballFilterSpec::Algorithm::ARABIC;
        } else if (0 == lang_str.case_compare("armenian")) {
          lang = ObSnowballFilterSpec::Algorithm::ARMENIAN;
        } else if (0 == lang_str.case_compare("basque")) {
          lang = ObSnowballFilterSpec::Algorithm::BASQUE;
        } else if (0 == lang_str.case_compare("catalan")) {
          lang = ObSnowballFilterSpec::Algorithm::CATALAN;
        } else if (0 == lang_str.case_compare("danish")) {
          lang = ObSnowballFilterSpec::Algorithm::DANISH;
        } else if (0 == lang_str.case_compare("dutch")) {
          lang = ObSnowballFilterSpec::Algorithm::DUTCH;
        } else if (0 == lang_str.case_compare("english")) {
          lang = ObSnowballFilterSpec::Algorithm::ENGLISH;
        } else if (0 == lang_str.case_compare("esperanto")) {
          lang = ObSnowballFilterSpec::Algorithm::ESPERANTO;
        } else if (0 == lang_str.case_compare("estonian")) {
          lang = ObSnowballFilterSpec::Algorithm::ESTONIAN;
        } else if (0 == lang_str.case_compare("finnish")) {
          lang = ObSnowballFilterSpec::Algorithm::FINNISH;
        } else if (0 == lang_str.case_compare("french")) {
          lang = ObSnowballFilterSpec::Algorithm::FRENCH;
        } else if (0 == lang_str.case_compare("german")) {
          lang = ObSnowballFilterSpec::Algorithm::GERMAN;
        } else if (0 == lang_str.case_compare("greek")) {
          lang = ObSnowballFilterSpec::Algorithm::GREEK;
        } else if (0 == lang_str.case_compare("hindi")) {
          lang = ObSnowballFilterSpec::Algorithm::HINDI;
        } else if (0 == lang_str.case_compare("hungarian")) {
          lang = ObSnowballFilterSpec::Algorithm::HUNGARIAN;
        } else if (0 == lang_str.case_compare("indonesian")) {
          lang = ObSnowballFilterSpec::Algorithm::INDONESIAN;
        } else if (0 == lang_str.case_compare("irish")) {
          lang = ObSnowballFilterSpec::Algorithm::IRISH;
        } else if (0 == lang_str.case_compare("italian")) {
          lang = ObSnowballFilterSpec::Algorithm::ITALIAN;
        } else if (0 == lang_str.case_compare("lithuanian")) {
          lang = ObSnowballFilterSpec::Algorithm::LITHUANIAN;
        } else if (0 == lang_str.case_compare("nepali")) {
          lang = ObSnowballFilterSpec::Algorithm::NEPALI;
        } else if (0 == lang_str.case_compare("norwegian")) {
          lang = ObSnowballFilterSpec::Algorithm::NORWEGIAN;
        } else if (0 == lang_str.case_compare("portuguese")) {
          lang = ObSnowballFilterSpec::Algorithm::PORTUGUESE;
        } else if (0 == lang_str.case_compare("romanian")) {
          lang = ObSnowballFilterSpec::Algorithm::ROMANIAN;
        } else if (0 == lang_str.case_compare("russian")) {
          lang = ObSnowballFilterSpec::Algorithm::RUSSIAN;
        } else if (0 == lang_str.case_compare("serbian")) {
          lang = ObSnowballFilterSpec::Algorithm::SERBIAN;
        } else if (0 == lang_str.case_compare("spanish")) {
          lang = ObSnowballFilterSpec::Algorithm::SPANISH;
        } else if (0 == lang_str.case_compare("swedish")) {
          lang = ObSnowballFilterSpec::Algorithm::SWEDISH;
        } else if (0 == lang_str.case_compare("tamil")) {
          lang = ObSnowballFilterSpec::Algorithm::TAMIL;
        } else if (0 == lang_str.case_compare("turkish")) {
          lang = ObSnowballFilterSpec::Algorithm::TURKISH;
        } else if (0 == lang_str.case_compare("yiddish")) {
          lang = ObSnowballFilterSpec::Algorithm::YIDDISH;
        } else if (0 == lang_str.case_compare("porter")) {
          lang = ObSnowballFilterSpec::Algorithm::PORTER;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported language in snowball filter", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Unsupported value for parameter 'language' in snowball filter");
        }
      }
      if (OB_SUCC(ret)) {
        void *buf = nullptr;
        if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSnowballFilterSpec)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate snowball filter spec", K(ret));
        } else {
          ObSnowballFilterSpec *snowball_spec = new (buf) ObSnowballFilterSpec(lang);
          token_filter_spec = snowball_spec;
        }
      }
    } else if (0 == type_str.case_compare("icu_normalizer")) {
      ObICUNormalizer2FilterSpec::Name name = ObICUNormalizer2FilterSpec::Name::NFKC_CF;
      UNormalization2Mode mode = UNormalization2Mode::UNORM2_COMPOSE;
      common::ObIJsonBase *name_val = nullptr;
      common::ObString name_key("name");
      if (OB_FAIL(tf_def_json.get_object_value(name_key, name_val))) {
        if (OB_SEARCH_NOT_FOUND == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get mode parameter", K(ret));
        }
      } else if (OB_ISNULL(name_val)
          || common::ObJsonNodeType::J_STRING != name_val->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("name parameter must be a string", K(ret));
      } else {
        common::ObString name_str(static_cast<int32_t>(name_val->get_data_length()),
                                  name_val->get_data());
        if (0 == name_str.case_compare("nfc")) {
          name = ObICUNormalizer2FilterSpec::Name::NFC;
        } else if (0 == name_str.case_compare("nfkc")) {
          name = ObICUNormalizer2FilterSpec::Name::NFKC;
        } else if (0 == name_str.case_compare("nfkc_cf")) {
          name = ObICUNormalizer2FilterSpec::Name::NFKC_CF;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unsupported name in icu_normalizer filter", K(ret), K(name_str));
        }
      }
      common::ObIJsonBase *mode_val = nullptr;
      common::ObString mode_key("mode");
      if (FAILEDx(tf_def_json.get_object_value(mode_key, mode_val))) {
        if (OB_SEARCH_NOT_FOUND == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get mode parameter", K(ret));
        }
      } else if (OB_ISNULL(mode_val)
          || common::ObJsonNodeType::J_STRING != mode_val->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("mode parameter must be a string", K(ret));
      } else {
        common::ObString mode_str(static_cast<int32_t>(mode_val->get_data_length()),
                                  mode_val->get_data());
        if (0 == mode_str.case_compare("compose")) {
          mode = UNormalization2Mode::UNORM2_COMPOSE;
        } else if (0 == mode_str.case_compare("decompose")) {
          mode = UNormalization2Mode::UNORM2_DECOMPOSE;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unsupported mode in icu_normalizer filter", K(ret), K(mode_str));
        }
      }
      if (OB_SUCC(ret)) {
        void *buf = nullptr;
        if (OB_ISNULL(buf = allocator.alloc(sizeof(ObICUNormalizer2FilterSpec)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate icu normalizer filter spec", K(ret));
        } else {
          ObICUNormalizer2FilterSpec *icu_normalizer_spec =
              new (buf) ObICUNormalizer2FilterSpec(name, mode);
          token_filter_spec = icu_normalizer_spec;
        }
      }
    } else if (0 == type_str.case_compare("lowercase")
        || 0 == type_str.case_compare("possessive_english")
        || 0 == type_str.case_compare("decimal_digit")
        || 0 == type_str.case_compare("icu_folding")
        || 0 == type_str.case_compare("porter_stem")) {
      if (OB_FAIL(resolve_builtin_token_filter_spec_(type_str, allocator, token_filter_spec))) {
        LOG_WARN("failed to resolve builtin filter by type name", K(ret), K(type_str));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported token filter type for custom analyzer", K(ret), K(type_str));
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::resolve_builtin_token_filter_spec_(
    const common::ObString &filter_name,
    common::ObIAllocator &allocator,
    ObTokenFilterSpec *&token_filter_spec)
{
  int ret = OB_SUCCESS;
  token_filter_spec = nullptr;
  void *buf = nullptr;
  if (0 == filter_name.case_compare("lowercase")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObLowerCaseFilterSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for lowercase filter spec", K(ret));
    } else {
      token_filter_spec = new (buf) ObLowerCaseFilterSpec();
    }
  } else if (0 == filter_name.case_compare("possessive_english")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObPossessiveEnglishFilterSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for english possessive filter spec", K(ret));
    } else {
      token_filter_spec = new (buf) ObPossessiveEnglishFilterSpec();
    }
  } else if (0 == filter_name.case_compare("stop")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStopWordFilterSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for stop filter spec", K(ret));
    } else {
      ObStopWordFilterSpec *stop_spec = new (buf) ObStopWordFilterSpec();
      stop_spec->language_ = ObStopWordLanguageKind::LANGUAGE_ENGLISH;
      token_filter_spec = stop_spec;
    }
  } else if (0 == filter_name.case_compare("decimal_digit")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObDecimalDigitFilterSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for decimal digit filter spec", K(ret));
    } else {
      token_filter_spec = new (buf) ObDecimalDigitFilterSpec();
    }
  } else if (0 == filter_name.case_compare("icu_normalizer")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObICUNormalizer2FilterSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for icu normalizer2 filter spec", K(ret));
    } else {
      token_filter_spec = new (buf) ObICUNormalizer2FilterSpec(
          ObICUNormalizer2FilterSpec::Name::NFKC_CF,
          UNormalization2Mode::UNORM2_COMPOSE);
    }
  } else if (0 == filter_name.case_compare("icu_folding")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObICUNormalizer2FilterSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for icu normalizer2 filter spec", K(ret));
    } else {
      token_filter_spec = new (buf) ObICUNormalizer2FilterSpec(
          ObTokenFilterType::TOKEN_FILTER_TYPE_ICU_FOLDING);
    }
  } else if (0 == filter_name.case_compare("porter_stem")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSnowballFilterSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for snowball filter spec", K(ret));
    } else {
      token_filter_spec = new (buf) ObSnowballFilterSpec(ObSnowballFilterSpec::Algorithm::PORTER);
    }
  } else if (0 == filter_name.case_compare("snowball")) {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSnowballFilterSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for snowball filter spec", K(ret));
    } else {
      token_filter_spec = new (buf) ObSnowballFilterSpec(ObSnowballFilterSpec::Algorithm::ENGLISH);
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unrecognized builtin token filter name", K(ret), K(filter_name));
  }
  return ret;
}

void ObAnalyzerSpecFactory::destroy_analyzer_spec(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec *&analyzer_spec)
{
  if (OB_NOT_NULL(analyzer_spec)) {
    if (OB_NOT_NULL(analyzer_spec->tokenizer_spec_)) {
      analyzer_spec->tokenizer_spec_->~ObTokenizerSpec();
      allocator.free(analyzer_spec->tokenizer_spec_);
      analyzer_spec->tokenizer_spec_ = nullptr;
    }
    for (int64_t i = 0; i < analyzer_spec->char_filter_specs_.count(); ++i) {
      if (OB_NOT_NULL(analyzer_spec->char_filter_specs_.at(i))) {
        analyzer_spec->char_filter_specs_.at(i)->~ObCharFilterSpec();
        allocator.free(analyzer_spec->char_filter_specs_.at(i));
      }
    }
    for (int64_t i = 0; i < analyzer_spec->token_filter_specs_.count(); ++i) {
      if (OB_NOT_NULL(analyzer_spec->token_filter_specs_.at(i))) {
        analyzer_spec->token_filter_specs_.at(i)->~ObTokenFilterSpec();
        allocator.free(analyzer_spec->token_filter_specs_.at(i));
      }
    }
    analyzer_spec->~ObAnalyzerSpec();
    allocator.free(analyzer_spec);
    analyzer_spec = nullptr;
  }
}

int ObAnalyzerSpecFactory::resolve_builtin_analyzer_type_(
    const common::ObString &analyzer_type,
    ObAnalyzerType &type)
{
  int ret = OB_SUCCESS;
  if (0 == analyzer_type.case_compare("standard")) {
    type = ObAnalyzerType::ANALYZER_TYPE_STANDARD;
  } else if (0 == analyzer_type.case_compare("english")) {
    type = ObAnalyzerType::ANALYZER_TYPE_ENGLISH;
  } else if (0 == analyzer_type.case_compare("thai")) {
    type = ObAnalyzerType::ANALYZER_TYPE_THAI;
  } else if (0 == analyzer_type.case_compare("vietnamese")) {
    type = ObAnalyzerType::ANALYZER_TYPE_VIETNAMESE;
  } else if (0 == analyzer_type.case_compare("indonesian")) {
    type = ObAnalyzerType::ANALYZER_TYPE_INDONESIAN;
  } else if (0 == analyzer_type.case_compare("malay")) {
    type = ObAnalyzerType::ANALYZER_TYPE_MALAY;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported analyzer type", K(ret), K(analyzer_type));
  }
  return ret;
}

int ObAnalyzerSpecFactory::create_standard_tokenizer_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStandardTokenizerSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate standard tokenizer spec", K(ret));
  } else {
    analyzer_spec.tokenizer_spec_ = new (buf) ObStandardTokenizerSpec();
  }
  return ret;
}

int ObAnalyzerSpecFactory::init_default_filter_specs_(
    int64_t extra_char_filter_count,
    int64_t extra_token_filter_count,
    bool need_min_max_token_filter,
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  // Prepend the mandatory utf8mb4_bin char filter so every non-legacy analyzer
  // normalises input to utf8mb4_bin before tokenization.
  // src_collation_ is left as CS_TYPE_INVALID here; create_analyzer() patches it
  // with the runtime source collation before instantiating the filter.
  void *cf_buf = nullptr;
  if (OB_FAIL(analyzer_spec.char_filter_specs_.init(extra_char_filter_count + 1))) {
    LOG_WARN("failed to init char filter specs", K(ret), K(extra_char_filter_count));
  } else if (OB_ISNULL(cf_buf = allocator.alloc(sizeof(ObUtf8mb4BinCharFilterSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate utf8mb4 bin char filter spec", K(ret));
  } else {
    ObUtf8mb4BinCharFilterSpec *utf8_spec = new (cf_buf) ObUtf8mb4BinCharFilterSpec();
    if (OB_FAIL(analyzer_spec.char_filter_specs_.push_back(utf8_spec))) {
      LOG_WARN("failed to push back utf8mb4 bin char filter spec", K(ret));
      utf8_spec->~ObUtf8mb4BinCharFilterSpec();
      allocator.free(utf8_spec);
    }
  }
  // Prepend the optional MinMax token filter and reserve room for extra filters
  // plus a trailing charset_convert token filter that callers append at the end of
  // the pipeline.
  if (OB_SUCC(ret)) {
    int64_t min_max_token_filter_count = need_min_max_token_filter ? 1 : 0;
    if (OB_FAIL(analyzer_spec.token_filter_specs_.init(
            extra_token_filter_count + min_max_token_filter_count + 1))) {
      LOG_WARN("failed to init token filter specs", K(ret), K(extra_token_filter_count));
    } else if (need_min_max_token_filter
        && OB_FAIL(append_min_max_token_filter_spec_(allocator, analyzer_spec))) {
      LOG_WARN("failed to append min max filter spec", K(ret));
    }
  }
  return ret;
}

// Pipeline: StandardTokenizer + LowerCaseFilter
int ObAnalyzerSpecFactory::build_standard_analyzer_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_standard_tokenizer_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to create standard tokenizer spec", K(ret));
  } else if (OB_FAIL(init_default_filter_specs_(0, 1, false, allocator, analyzer_spec))) {
    LOG_WARN("failed to init default filter specs", K(ret));
  } else if (OB_FAIL(append_lowercase_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append lowercase filter spec", K(ret));
  } else if (OB_FAIL(append_charset_convert_token_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append charset convert filter spec", K(ret));
  }
  return ret;
}

// Pipeline: StandardTokenizer + PossessiveEnglishFilter + LowerCaseFilter + StopFilter(english) + SnowballFilter(english)
int ObAnalyzerSpecFactory::build_english_analyzer_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_standard_tokenizer_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to create standard tokenizer spec", K(ret));
  } else if (OB_FAIL(init_default_filter_specs_(0, 4, false, allocator, analyzer_spec))) {
    LOG_WARN("failed to init default filter specs", K(ret));
  } else if (OB_FAIL(append_possessive_english_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append english possessive filter spec", K(ret));
  } else if (OB_FAIL(append_lowercase_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append lowercase filter spec", K(ret));
  } else if (OB_FAIL(append_stop_filter_spec_(
      allocator, analyzer_spec, ObStopWordLanguageKind::LANGUAGE_ENGLISH))) {
    LOG_WARN("failed to append english stop filter spec", K(ret));
  } else if (OB_FAIL(append_snowball_filter_spec_(
      allocator, analyzer_spec, ObSnowballFilterSpec::Algorithm::ENGLISH))) {
    LOG_WARN("failed to append english stem filter spec", K(ret));
  } else if (OB_FAIL(append_charset_convert_token_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append charset convert filter spec", K(ret));
  }
  return ret;
}

// Pipeline: StandardTokenizer(ICU) + LowerCaseFilter + DecimalDigitFilter + StopFilter(thai)
int ObAnalyzerSpecFactory::build_thai_analyzer_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_standard_tokenizer_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to create standard tokenizer spec", K(ret));
  } else if (OB_FAIL(init_default_filter_specs_(0, 3, false, allocator, analyzer_spec))) {
    LOG_WARN("failed to init default filter specs", K(ret));
  } else if (OB_FAIL(append_lowercase_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append lowercase filter spec", K(ret));
  } else if (OB_FAIL(append_decimal_digit_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append decimal digit filter spec", K(ret));
  } else if (OB_FAIL(append_stop_filter_spec_(allocator, analyzer_spec,
                                              ObStopWordLanguageKind::LANGUAGE_THAI))) {
    LOG_WARN("failed to append thai stop filter spec", K(ret));
  } else if (OB_FAIL(append_charset_convert_token_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append charset convert filter spec", K(ret));
  }
  return ret;
}

// Pipeline: StandardTokenizer + IcuFoldingFilter
int ObAnalyzerSpecFactory::build_vietnamese_analyzer_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_standard_tokenizer_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to create standard tokenizer spec", K(ret));
  } else if (OB_FAIL(init_default_filter_specs_(0, 1, false, allocator, analyzer_spec))) {
    LOG_WARN("failed to init default filter specs", K(ret));
  } else if (OB_FAIL(append_icu_folding_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append icu folding filter spec", K(ret));
  } else if (OB_FAIL(append_charset_convert_token_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append charset convert filter spec", K(ret));
  }
  return ret;
}

// Pipeline: StandardTokenizer + LowerCaseFilter + StopFilter(indonesian) + SnowballFilter(Indonesian)
int ObAnalyzerSpecFactory::build_indonesian_analyzer_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_standard_tokenizer_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to create standard tokenizer spec", K(ret));
  } else if (OB_FAIL(init_default_filter_specs_(0, 3, false, allocator, analyzer_spec))) {
    LOG_WARN("failed to init default filter specs", K(ret));
  } else if (OB_FAIL(append_lowercase_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append lowercase filter spec", K(ret));
  } else if (OB_FAIL(append_stop_filter_spec_(
      allocator, analyzer_spec, ObStopWordLanguageKind::LANGUAGE_INDONESIAN))) {
    LOG_WARN("failed to append indonesian stop filter spec", K(ret));
  } else if (OB_FAIL(append_snowball_filter_spec_(
      allocator, analyzer_spec, ObSnowballFilterSpec::Algorithm::INDONESIAN))) {
    LOG_WARN("failed to append indonesian snowball filter spec", K(ret));
  } else if (OB_FAIL(append_charset_convert_token_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append charset convert filter spec", K(ret));
  }
  return ret;
}

// Pipeline: StandardTokenizer + LowerCaseFilter
int ObAnalyzerSpecFactory::build_malay_analyzer_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_standard_tokenizer_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to create standard tokenizer spec", K(ret));
  } else if (OB_FAIL(init_default_filter_specs_(0, 1, false, allocator, analyzer_spec))) {
    LOG_WARN("failed to init default filter specs", K(ret));
  } else if (OB_FAIL(append_lowercase_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append lowercase filter spec", K(ret));
  } else if (OB_FAIL(append_charset_convert_token_filter_spec_(allocator, analyzer_spec))) {
    LOG_WARN("failed to append charset convert filter spec", K(ret));
  }
  return ret;
}

int ObAnalyzerSpecFactory::append_min_max_token_filter_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObLegacyMinMaxTokenFilterSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate min max token filter spec", K(ret));
  } else {
    ObLegacyMinMaxTokenFilterSpec *mm_spec = new (buf) ObLegacyMinMaxTokenFilterSpec();
    mm_spec->min_token_size_ = 0;
    mm_spec->max_token_size_ = ObLegacyMinMaxTokenFilter::MAX_CHAR_COUNT_PER_TOKEN;
    mm_spec->coll_type_ = common::CS_TYPE_UTF8MB4_BIN;
    if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(mm_spec))) {
      LOG_WARN("failed to append min max filter spec", K(ret));
      mm_spec->~ObLegacyMinMaxTokenFilterSpec();
      allocator.free(mm_spec);
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::append_possessive_english_filter_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObPossessiveEnglishFilterSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate english possessive filter spec", K(ret));
  } else {
    ObPossessiveEnglishFilterSpec *english_spec = new (buf) ObPossessiveEnglishFilterSpec();
    if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(english_spec))) {
      LOG_WARN("failed to append english possessive filter spec", K(ret));
      english_spec->~ObPossessiveEnglishFilterSpec();
      allocator.free(english_spec);
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::append_lowercase_filter_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObLowerCaseFilterSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate lowercase filter spec", K(ret));
  } else {
    ObLowerCaseFilterSpec *lowercase_spec = new (buf) ObLowerCaseFilterSpec();
    if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(lowercase_spec))) {
      LOG_WARN("failed to append lowercase filter spec", K(ret));
      lowercase_spec->~ObLowerCaseFilterSpec();
      allocator.free(lowercase_spec);
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::append_decimal_digit_filter_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObDecimalDigitFilterSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate decimal digit filter spec", K(ret));
  } else {
    ObDecimalDigitFilterSpec *decimal_digit_spec = new (buf) ObDecimalDigitFilterSpec();
    if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(decimal_digit_spec))) {
      LOG_WARN("failed to append decimal digit filter spec", K(ret));
      decimal_digit_spec->~ObDecimalDigitFilterSpec();
      allocator.free(decimal_digit_spec);
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::append_stop_filter_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec,
    ObStopWordLanguageKind language)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStopWordFilterSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate stop word filter spec", K(ret));
  } else {
    ObStopWordFilterSpec *stop_spec = new (buf) ObStopWordFilterSpec();
    stop_spec->language_ = language;
    if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(stop_spec))) {
      LOG_WARN("failed to append stop filter spec", K(ret));
      stop_spec->~ObStopWordFilterSpec();
      allocator.free(stop_spec);
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::append_snowball_filter_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec,
    ObSnowballFilterSpec::Algorithm algo)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSnowballFilterSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate snowball filter spec", K(ret), K(algo));
  } else {
    ObSnowballFilterSpec *snowball_spec = new (buf) ObSnowballFilterSpec(algo);
    if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(snowball_spec))) {
      LOG_WARN("failed to append snowball filter spec", K(ret), K(algo));
      snowball_spec->~ObSnowballFilterSpec();
      allocator.free(snowball_spec);
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::append_icu_normalizer_filter_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec,
    ObICUNormalizer2FilterSpec::Name name,
    UNormalization2Mode mode)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObICUNormalizer2FilterSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate icu normalizer filter spec", K(ret), K(name), K(mode));
  } else {
    ObICUNormalizer2FilterSpec *normalizer_spec = new (buf) ObICUNormalizer2FilterSpec(name, mode);
    if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(normalizer_spec))) {
      LOG_WARN("failed to append icu normalizer filter spec", K(ret), K(name), K(mode));
      normalizer_spec->~ObICUNormalizer2FilterSpec();
      allocator.free(normalizer_spec);
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::append_icu_folding_filter_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObICUNormalizer2FilterSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate icu folding filter spec", K(ret));
  } else {
    ObICUNormalizer2FilterSpec *folding_spec = new (buf) ObICUNormalizer2FilterSpec(
        ObTokenFilterType::TOKEN_FILTER_TYPE_ICU_FOLDING);
    if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(folding_spec))) {
      LOG_WARN("failed to append icu folding filter spec", K(ret));
      folding_spec->~ObICUNormalizer2FilterSpec();
      allocator.free(folding_spec);
    }
  }
  return ret;
}

int ObAnalyzerSpecFactory::append_charset_convert_token_filter_spec_(
    common::ObIAllocator &allocator,
    ObAnalyzerSpec &analyzer_spec)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObCharsetConvertFilterSpec)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate charset convert filter spec", K(ret));
  } else {
    // target_collation_ is left as CS_TYPE_INVALID here; create_token_filter() patches
    // it with the runtime source collation before instantiating the filter.
    ObCharsetConvertFilterSpec *cc_spec = new (buf) ObCharsetConvertFilterSpec();
    if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(cc_spec))) {
      LOG_WARN("failed to append charset convert filter spec", K(ret));
      cc_spec->~ObCharsetConvertFilterSpec();
      allocator.free(cc_spec);
    }
  }
  return ret;
}

int ObTokenStreamFactory::create_analyzer(
    const ObAnalyzerSpec &spec,
    const common::ObCollationType source_collation,
    common::ObIAllocator &alloc,
    ObFTSAnalyzer *&analyzer)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  analyzer = nullptr;

  if (OB_UNLIKELY(!spec.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid analyzer spec", K(ret), K(spec));
  } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObFTSAnalyzer)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for analyzer", K(ret));
  } else {
    analyzer = new (buf) ObFTSAnalyzer(alloc);
    analyzer->analyzer_type_ = spec.analyzer_type_;
    analyzer->source_collation_ = source_collation;
    common::ObIAllocator &scratch_alloc = analyzer->scratch_alloc_;

    // create char filters from spec
    const int64_t char_filter_count = spec.char_filter_specs_.count();
    if (OB_SUCC(ret) && char_filter_count > 0) {
      if (OB_FAIL(analyzer->char_filters_.init(char_filter_count))) {
        LOG_WARN("failed to init char filters array", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < spec.char_filter_specs_.count(); ++i) {
      ObICharFilter *cf = nullptr;
      if (OB_ISNULL(spec.char_filter_specs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("char filter spec is null", K(ret), K(i));
      } else if (OB_FAIL(create_char_filter(
                     *spec.char_filter_specs_.at(i), source_collation, alloc, scratch_alloc, cf))) {
        LOG_WARN("failed to create char filter", K(ret), K(i));
      } else if (OB_FAIL(analyzer->char_filters_.push_back(cf))) {
        LOG_WARN("failed to push back char filter", K(ret), K(i));
        cf->~ObICharFilter();
        alloc.free(cf);
      }
    }

    // create tokenizer
    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_tokenizer(*spec.tokenizer_spec_, alloc, scratch_alloc, analyzer->tokenizer_))) {
        LOG_WARN("failed to create tokenizer", K(ret));
      } else {
        analyzer->tail_ = analyzer->tokenizer_;
      }
    }

    // init token filters array
    if (OB_SUCC(ret) && spec.token_filter_specs_.count() > 0) {
      if (OB_FAIL(analyzer->token_filters_.init(spec.token_filter_specs_.count()))) {
        LOG_WARN("failed to init token filters array", K(ret));
      }
    }

    // create token filters and chain them
    for (int64_t i = 0; OB_SUCC(ret) && i < spec.token_filter_specs_.count(); ++i) {
      ObITokenFilter *tf = nullptr;
      if (OB_ISNULL(spec.token_filter_specs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("token filter spec is null", K(ret), K(i));
      } else if (OB_FAIL(create_token_filter(*spec.token_filter_specs_.at(i),
                                              source_collation, alloc, scratch_alloc, tf))) {
        LOG_WARN("failed to create token filter", K(ret), K(i));
      } else if (OB_FAIL(analyzer->token_filters_.push_back(tf))) {
        LOG_WARN("failed to push back token filter", K(ret), K(i));
        tf->~ObITokenFilter();
        alloc.free(tf);
      } else {
        tf->set_input(analyzer->tail_);
        analyzer->tail_ = tf;
      }
    }

    if (OB_SUCC(ret)) {
      analyzer->is_inited_ = true;
    } else if (OB_NOT_NULL(analyzer)) {
      // cleanup on failure to avoid memory leak
      analyzer->~ObFTSAnalyzer();
      alloc.free(analyzer);
      analyzer = nullptr;
    }
  }

  return ret;
}

int ObTokenStreamFactory::create_analyzer_from_legacy_parser(
    const ObFTAnalyzerParam &param,
    ObFTSAnalyzer *&analyzer)
{
  int ret = OB_SUCCESS;
  analyzer = nullptr;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid analyzer param", K(ret), K(param));
  } else {
    common::ObIAllocator &alloc = *param.alloc_;
    const ObTokenizerType tokenizer_type = param.legacy_tokenizer_type_;
    const ObFTParserProperty &parser_property = *param.parser_property_;
    const ObProcessTokenFlag &process_token_flag = param.process_token_flag_;

    ObAnalyzerSpec analyzer_spec(alloc);
    analyzer_spec.analyzer_type_ = ObAnalyzerType::ANALYZER_TYPE_LEGACY;

    // build char filter specs
    ObLegacyLowercaseCharFilterSpec *lc_spec = nullptr;
    if (OB_SUCC(ret) && process_token_flag.casedown_token()) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = alloc.alloc(sizeof(ObLegacyLowercaseCharFilterSpec)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate char filter spec", K(ret));
      } else if (FALSE_IT(lc_spec = new (buf) ObLegacyLowercaseCharFilterSpec())) {
      } else if (FALSE_IT(lc_spec->coll_type_ = param.meta_.get_collation_type())) {
      } else if (OB_FAIL(analyzer_spec.char_filter_specs_.init(1))) {
        LOG_WARN("failed to init char filter specs array", K(ret));
      } else if (OB_FAIL(analyzer_spec.char_filter_specs_.push_back(lc_spec))) {
        LOG_WARN("failed to push back char filter spec", K(ret));
      }
    }

    // build tokenizer spec based on tokenizer type
    ObTokenizerSpec *tokenizer_spec = nullptr;
    if (OB_SUCC(ret)) {
      void *buf = nullptr;
      switch (tokenizer_type) {
        case ObTokenizerType::TOKENIZER_TYPE_SPACE:
          if (OB_ISNULL(buf = alloc.alloc(sizeof(ObSpaceTokenizerSpec)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate space tokenizer spec", K(ret));
          } else {
            tokenizer_spec = new (buf) ObSpaceTokenizerSpec();
            analyzer_spec.tokenizer_spec_ = tokenizer_spec;
          }
          break;
        case ObTokenizerType::TOKENIZER_TYPE_NGRAM:
          if (OB_ISNULL(buf = alloc.alloc(sizeof(ObNgramTokenizerSpec)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate ngram tokenizer spec", K(ret));
          } else {
            ObNgramTokenizerSpec *s = new (buf) ObNgramTokenizerSpec();
            s->ngram_token_size_ = parser_property.ngram_token_size_;
            tokenizer_spec = s;
            analyzer_spec.tokenizer_spec_ = tokenizer_spec;
          }
          break;
        case ObTokenizerType::TOKENIZER_TYPE_BENG:
          if (OB_ISNULL(buf = alloc.alloc(sizeof(ObBengTokenizerSpec)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate beng tokenizer spec", K(ret));
          } else {
            tokenizer_spec = new (buf) ObBengTokenizerSpec();
            analyzer_spec.tokenizer_spec_ = tokenizer_spec;
          }
          break;
        case ObTokenizerType::TOKENIZER_TYPE_IK:
          if (OB_ISNULL(buf = alloc.alloc(sizeof(ObIKTokenizerSpec)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate ik tokenizer spec", K(ret));
          } else {
            ObIKTokenizerSpec *s = new (buf) ObIKTokenizerSpec();
            s->ik_mode_smart_ = parser_property.ik_mode_smart_;
            s->main_dict_id_ = parser_property.dict_table_id_;
            s->quan_dict_id_ = parser_property.quantifier_table_id_;
            s->stopword_dict_id_ = parser_property.stopword_table_id_;
            s->main_dict_name_ = parser_property.dict_table_name_;
            s->quan_dict_name_ = parser_property.quantifier_table_name_;
            s->stopword_dict_name_ = parser_property.stopword_table_name_;
            s->is_ddl_mode_ = param.is_ddl_mode_;
            s->need_casedown_ = param.need_casedown_;
            tokenizer_spec = s;
            analyzer_spec.tokenizer_spec_ = tokenizer_spec;
          }
          break;
        case ObTokenizerType::TOKENIZER_TYPE_NGRAM2:
          if (OB_ISNULL(buf = alloc.alloc(sizeof(ObNgram2TokenizerSpec)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate ngram2 tokenizer spec", K(ret));
          } else {
            ObNgram2TokenizerSpec *s = new (buf) ObNgram2TokenizerSpec();
            s->min_ngram_size_ = parser_property.min_ngram_token_size_;
            s->max_ngram_size_ = parser_property.max_ngram_token_size_;
            tokenizer_spec = s;
            analyzer_spec.tokenizer_spec_ = tokenizer_spec;
          }
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported tokenizer type", K(ret), K(tokenizer_type));
          break;
      }
    }

    // build token filter specs
    // min_max filter is always added: it enforces the hard MAX_CHAR_COUNT_PER_TOKEN limit (1024)
    // even when the min_max_token flag is not set.
    int64_t tf_count = 0;
    if (OB_SUCC(ret)) {
      ++tf_count; // min_max filter always present
      if (process_token_flag.stop_token()) { ++tf_count; }
      if (OB_FAIL(analyzer_spec.token_filter_specs_.init(tf_count))) {
        LOG_WARN("failed to init token filter specs array", K(ret));
      }
    }
    ObLegacyMinMaxTokenFilterSpec *mm_spec = nullptr;
    if (OB_SUCC(ret)) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = alloc.alloc(sizeof(ObLegacyMinMaxTokenFilterSpec)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate min max token filter spec", K(ret));
      } else if (FALSE_IT(mm_spec = new (buf) ObLegacyMinMaxTokenFilterSpec())) {
      } else if (process_token_flag.min_max_token()) {
        // use configured min/max sizes
        mm_spec->min_token_size_ = parser_property.min_token_size_;
        mm_spec->max_token_size_ = parser_property.max_token_size_;
      } else {
        // only enforce the hard limit (min=0 lets everything through, max=1024 is the hard cap)
        mm_spec->min_token_size_ = 0;
        mm_spec->max_token_size_ = ObLegacyMinMaxTokenFilter::MAX_CHAR_COUNT_PER_TOKEN;
      }
      if (OB_SUCC(ret)) {
        if (FALSE_IT(mm_spec->coll_type_ = param.meta_.get_collation_type())) {
        } else if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(mm_spec))) {
          LOG_WARN("failed to push back min max spec", K(ret));
        }
      }
    }
    ObLegacyStopTokenFilterSpec *stop_spec = nullptr;
    if (OB_SUCC(ret) && process_token_flag.stop_token()) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = alloc.alloc(sizeof(ObLegacyStopTokenFilterSpec)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate stop token filter spec", K(ret));
      } else if (FALSE_IT(stop_spec = new (buf) ObLegacyStopTokenFilterSpec())) {
      } else if (FALSE_IT(stop_spec->token_meta_ = param.meta_)) {
      } else if (OB_FAIL(analyzer_spec.token_filter_specs_.push_back(stop_spec))) {
        LOG_WARN("failed to push back stop spec", K(ret));
      }
    }

    // create analyzer from spec
    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_analyzer(analyzer_spec, param.meta_.get_collation_type(), alloc, analyzer))) {
        LOG_WARN("failed to create legacy analyzer", K(ret), K(tokenizer_type));
      }
    }

    // cleanup spec objects (analyzer owns the created components, not the specs)
    if (OB_NOT_NULL(tokenizer_spec)) {
      tokenizer_spec->~ObTokenizerSpec();
      alloc.free(tokenizer_spec);
      tokenizer_spec = nullptr;
    }
    if (OB_NOT_NULL(lc_spec)) {
      lc_spec->~ObLegacyLowercaseCharFilterSpec();
      alloc.free(lc_spec);
      lc_spec = nullptr;
    }
    if (OB_NOT_NULL(mm_spec)) {
      mm_spec->~ObLegacyMinMaxTokenFilterSpec();
      alloc.free(mm_spec);
      mm_spec = nullptr;
    }
    if (OB_NOT_NULL(stop_spec)) {
      stop_spec->~ObLegacyStopTokenFilterSpec();
      alloc.free(stop_spec);
      stop_spec = nullptr;
    }
  }
  return ret;
}

int ObTokenStreamFactory::create_char_filter(
    const ObCharFilterSpec &spec,
    const common::ObCollationType source_collation,
    common::ObIAllocator &alloc,
    common::ObIAllocator &scratch_alloc,
    ObICharFilter *&char_filter)
{
  int ret = OB_SUCCESS;
  const ObCharFilterType type = spec.type_;

  if (ObCharFilterType::CHAR_FILTER_TYPE_UTF8MB4_BIN == type) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = alloc.alloc(sizeof(ObUtf8mb4BinCharFilter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for utf8mb4 bin char filter", K(ret));
    } else {
      ObUtf8mb4BinCharFilter *filter = new (buf) ObUtf8mb4BinCharFilter();
      // The spec carries a placeholder src_collation_ (CS_TYPE_INVALID) set at DDL time;
      // patch it with the runtime source collation before init.
      ObUtf8mb4BinCharFilterSpec patched_spec =
          static_cast<const ObUtf8mb4BinCharFilterSpec &>(spec);
      patched_spec.src_collation_ = source_collation;
      if (OB_FAIL(filter->init(patched_spec, scratch_alloc))) {
        LOG_WARN("failed to init utf8mb4 bin char filter", K(ret));
        filter->~ObUtf8mb4BinCharFilter();
        alloc.free(filter);
        filter = nullptr;
      } else {
        char_filter = filter;
      }
    }
  } else if (ObCharFilterType::CHAR_FILTER_TYPE_LOWERCASE_LEGACY == type) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = alloc.alloc(sizeof(ObLegacyLowercaseCharFilter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for lowercase char filter", K(ret));
    } else {
      ObLegacyLowercaseCharFilter *filter = new (buf) ObLegacyLowercaseCharFilter();
      if (OB_FAIL(filter->init(spec, scratch_alloc))) {
        LOG_WARN("failed to init lowercase char filter", K(ret));
        filter->~ObLegacyLowercaseCharFilter();
        alloc.free(filter);
        filter = nullptr;
      } else {
        char_filter = filter;
      }
    }
  } else {
    // TODO: dispatch to other char filter implementations
    // e.g. "html_strip" -> ObHtmlStripCharFilter, "mapping" -> ObMappingCharFilter
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("char filter type not yet implemented", K(ret), K(type));
  }
  return ret;
}

#define CREATE_TOKENIZER(TokenizerClass)                                               \
  do {                                                                                 \
    void *buf = nullptr;                                                               \
    if (OB_ISNULL(buf = alloc.alloc(sizeof(TokenizerClass)))) {                        \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                 \
      LOG_WARN("failed to allocate tokenizer", K(ret), K(type));                       \
    } else {                                                                           \
      TokenizerClass *t = new (buf) TokenizerClass();                                  \
      if (OB_FAIL(t->init(spec, scratch_alloc))) {                                     \
        LOG_WARN("failed to init tokenizer", K(ret), K(type));                         \
        t->~TokenizerClass();                                                          \
        alloc.free(t);                                                                 \
      } else {                                                                         \
        tokenizer = t;                                                                 \
      }                                                                                \
    }                                                                                  \
  } while (0)

int ObTokenStreamFactory::create_tokenizer(
    const ObTokenizerSpec &spec,
    common::ObIAllocator &alloc,
    common::ObIAllocator &scratch_alloc,
    ObITokenizer *&tokenizer)
{
  int ret = OB_SUCCESS;
  const ObTokenizerType type = spec.type_;

  if (ObTokenizerType::TOKENIZER_TYPE_SPACE == type) {
    CREATE_TOKENIZER(ObSpaceTokenizer);
  } else if (ObTokenizerType::TOKENIZER_TYPE_NGRAM == type) {
    CREATE_TOKENIZER(ObNgramTokenizer);
  } else if (ObTokenizerType::TOKENIZER_TYPE_BENG == type) {
    CREATE_TOKENIZER(ObBengTokenizer);
  } else if (ObTokenizerType::TOKENIZER_TYPE_IK == type) {
    CREATE_TOKENIZER(ObIKTokenizer);
  } else if (ObTokenizerType::TOKENIZER_TYPE_NGRAM2 == type) {
    CREATE_TOKENIZER(ObNgram2Tokenizer);
  } else if (ObTokenizerType::TOKENIZER_TYPE_STANDARD == type) {
    CREATE_TOKENIZER(ObStandardTokenizer);
  } else if (ObTokenizerType::TOKENIZER_TYPE_KEYWORD == type) {
    CREATE_TOKENIZER(ObKeywordTokenizer);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tokenizer type not yet implemented", K(ret), K(type));
  }

  return ret;
}

#undef CREATE_TOKENIZER

#define CREATE_TOKEN_FILTER_WITH_SPEC(FilterClass, filter_name, init_spec)               \
  do {                                                                                   \
    void *buf = nullptr;                                                                 \
    if (OB_ISNULL(buf = alloc.alloc(sizeof(FilterClass)))) {                             \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                   \
      LOG_WARN("failed to allocate " filter_name, K(ret), K(type));                      \
    } else {                                                                             \
      FilterClass *filter = new (buf) FilterClass();                                     \
      if (OB_FAIL(filter->init(init_spec, scratch_alloc))) {                             \
        LOG_WARN("failed to init " filter_name, K(ret), K(type));                        \
        filter->~FilterClass();                                                          \
        alloc.free(filter);                                                              \
      } else {                                                                           \
        token_filter = filter;                                                           \
      }                                                                                  \
    }                                                                                    \
  } while (0)

#define CREATE_TOKEN_FILTER(FilterClass, filter_name)                                    \
  CREATE_TOKEN_FILTER_WITH_SPEC(FilterClass, filter_name, spec)

int ObTokenStreamFactory::create_token_filter(
    const ObTokenFilterSpec &spec,
    const common::ObCollationType source_collation,
    common::ObIAllocator &alloc,
    common::ObIAllocator &scratch_alloc,
    ObITokenFilter *&token_filter)
{
  int ret = OB_SUCCESS;
  const ObTokenFilterType type = spec.type_;

  if (ObTokenFilterType::TOKEN_FILTER_TYPE_MIN_MAX == type) {
    CREATE_TOKEN_FILTER(ObLegacyMinMaxTokenFilter, "min max token filter");
  } else if (ObTokenFilterType::TOKEN_FILTER_TYPE_LEGACY_STOP == type) {
    CREATE_TOKEN_FILTER(ObLegacyStopTokenFilter, "stop token filter");
  } else if (ObTokenFilterType::TOKEN_FILTER_TYPE_STOP == type) {
    CREATE_TOKEN_FILTER(ObStopWordFilter, "stop word filter");
  } else if (ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE == type) {
    CREATE_TOKEN_FILTER(ObLowerCaseFilter, "lowercase filter");
  } else if (ObTokenFilterType::TOKEN_FILTER_TYPE_DECIMAL_DIGIT == type) {
    CREATE_TOKEN_FILTER(ObDecimalDigitFilter, "decimal digit filter");
  } else if (ObTokenFilterType::TOKEN_FILTER_TYPE_POSSESSIVE_ENGLISH == type) {
    CREATE_TOKEN_FILTER(ObPossessiveEnglishFilter, "english possessive filter");
  } else if (ObTokenFilterType::TOKEN_FILTER_TYPE_SNOWBALL == type) {
    CREATE_TOKEN_FILTER(ObSnowballFilter, "snowball filter");
  } else if (ObTokenFilterType::TOKEN_FILTER_TYPE_ICU_NORMALIZATION == type) {
    CREATE_TOKEN_FILTER(ObICUNormalizer2Filter, "icu normalization filter");
  } else if (ObTokenFilterType::TOKEN_FILTER_TYPE_ICU_FOLDING == type) {
    // ICU folding shares the same Normalizer2-based filter implementation;
    // ObICUNormalizer2Filter::init() selects the folding ICU data by spec.type_.
    CREATE_TOKEN_FILTER(ObICUNormalizer2Filter, "icu folding filter");
  } else if (ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT == type) {
    // The spec carries a placeholder target_collation_ (CS_TYPE_INVALID) set at DDL
    // time; patch it with the runtime source collation before init.
    ObCharsetConvertFilterSpec patched_spec =
        static_cast<const ObCharsetConvertFilterSpec &>(spec);
    patched_spec.target_collation_ = source_collation;
    CREATE_TOKEN_FILTER_WITH_SPEC(ObCharsetConvertFilter, "charset convert filter", patched_spec);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("token filter type not yet implemented", K(ret), K(type));
  }
  return ret;
}

#undef CREATE_TOKEN_FILTER
#undef CREATE_TOKEN_FILTER_WITH_SPEC

void ObTokenStreamFactory::reset_analyzer(ObFTSAnalyzer *analyzer)
{
  if (OB_NOT_NULL(analyzer)) {
    analyzer->reset();
  }
}

} // namespace storage
} // namespace oceanbase
