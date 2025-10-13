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

#include "lib/string/ob_string.h"
#include <gtest/gtest.h>
#include <stdexcept>
#define private public
#define protected public
#include "share/hybrid_search/ob_query_parse.h"
#include "common/ob_smart_var.h"
#include "common/ob_clock_generator.h"
#undef private
#undef protected


using namespace std;

namespace oceanbase {
using namespace share;
namespace common {
class TestHybridSearch : public ::testing::Test
{
public:
  TestHybridSearch()
  {}
  ~TestHybridSearch()
  {}

private:
  ObArenaAllocator allocator_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestHybridSearch);
};

class TestHybridSearchHelp
{
public :
  static void runtest(const common::ObString &table_name, const common::ObString &req_str,
                      const common::ObString &expect, bool json_wrap = false,
                      common::ObString database_name = "", bool enable_es_mode = false)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator tmp_allocator;
    ObQueryReqFromJson *req = nullptr;
    ObESQueryParser parser(tmp_allocator, json_wrap, &table_name, &database_name, enable_es_mode);
    SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
      MEMSET(buf, 0, sizeof(buf));
      int64_t res_len = 0;
      int64_t start_ts = ObClockGenerator::getClock();
      ASSERT_EQ(OB_SUCCESS, parser.parse(req_str, req));
      ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
      int64_t translate_cost_time = ObClockGenerator::getClock() - start_ts;
      std::cout << "translate cost time: " << translate_cost_time << std::endl;
      std::cout << "translate sql:" << std::endl << buf << std::endl;
      std::cout << "expect sql:" << std::endl << expect.ptr() << std::endl;
      ObString trans_res(res_len, buf);
      ASSERT_EQ(0, expect.case_compare(trans_res));
    }
  }
};

TEST_F(TestHybridSearch, basic_match)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "bool": {
          "must": [
            {"match": {"query": "database or oceanBase"}},
            {"match": { "content": "Elasticsearch" }}
          ]
        }
      }
    })";

  common::ObString result("SELECT *, (match(query) against('database or oceanBase' in natural language mode) + match(content) against('Elasticsearch' in natural language mode)) as _score FROM doc_table WHERE match(query) against('database or oceanBase' in natural language mode) AND match(content) against('Elasticsearch' in natural language mode) ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}


TEST_F(TestHybridSearch, basic_term)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "must": [
         {"term": {"book_name": "c ++ programming"}},
         {"match": { "content": "Elasticsearch" }}
        ]
      }
    }
  })";
  common::ObString result("SELECT *, ((book_name = 'c ++ programming') + match(content) against('Elasticsearch' in natural language mode)) as _score FROM doc_table WHERE book_name = 'c ++ programming' AND match(content) against('Elasticsearch' in natural language mode) ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}


TEST_F(TestHybridSearch, must_not_single_match)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "must_not": [
          {"match": {"query": "database or oceanBase"}}
        ]
      }
    }
  })";
  common::ObString result("SELECT *, 0 as _score FROM doc_table WHERE NOT match(query) against('database or oceanBase' in natural language mode) ORDER BY __pk_increment LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, must_not_multiple_conditions)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "must_not": [
          {"match": {"query": "database or oceanBase"}},
          {"match": { "content": "Elasticsearch" }},
          {"range": {"price": {"gte": 100}}}
        ]
      }
    }
  })";
  common::ObString result("SELECT *, 0 as _score FROM doc_table WHERE NOT (match(query) against('database or oceanBase' in natural language mode) OR match(content) against('Elasticsearch' in natural language mode) OR price >= 100) ORDER BY __pk_increment LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, must_not_with_must)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "must_not": [
          {"match": {"query": "database or oceanBase"}},
          {"match": { "content": "Elasticsearch" }},
          {"range": {"price": {"gte": 100}}}
        ],
        "must": [
          {"match": {"book_name": "c ++ programming"}}
        ]
      }
    }
  })";
  common::ObString result("SELECT *, match(book_name) against('c ++ programming' in natural language mode) as _score FROM doc_table WHERE match(book_name) against('c ++ programming' in natural language mode) AND NOT (match(query) against('database or oceanBase' in natural language mode) OR match(content) against('Elasticsearch' in natural language mode) OR price >= 100) ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, must_not_with_bool_query)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "must_not": [
          {"match": {"query": "database or oceanBase"}},
          {"match": { "content": "Elasticsearch" }},
          {
            "bool": {
              "should": [
                {"term": {"book_id": "1"}},
                {"term": {"age": "2"}}
              ]
            }
          }
        ],
        "must": [
          {"match": {"book_name": "c ++ programming"}}
        ]
      }
    }
  })";
  common::ObString result(
    "SELECT *, match(book_name) against('c ++ programming' in natural language mode) as _score "
    "FROM doc_table WHERE match(book_name) against('c ++ programming' in natural language mode) AND "
    "NOT (match(query) against('database or oceanBase' in natural language mode) OR "
    "match(content) against('Elasticsearch' in natural language mode) OR "
    "book_id = '1' OR age = '2') ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}


TEST_F(TestHybridSearch, basic_filter)
{
  int ret = OB_SUCCESS;
  ObString table_name("doc_table");

  common::ObString req_str1 = R"({
    "query": {
      "bool": {
        "filter": [
          {"term": {"book_name": "c ++ programming"}}
        ]
      }
    }
  })";
  common::ObString result1("SELECT *, 0 as _score FROM doc_table WHERE book_name = 'c ++ programming' ORDER BY __pk_increment LIMIT 10");

  common::ObString req_str2 = R"({
      "query": {
        "bool": {
          "must": [
            {"match": {"query": "database or oceanBase"}},
            {"match": { "content": "Elasticsearch" }}
          ],
          "filter": [
          {
            "bool": {
              "must": [
                {"match": {"book_id": "1"}},
                {"match": {"age": "2"}}
                ]
              }
            },
            {"term": {"book_name": "c ++ programming"}
          }
          ]
        }
      }
    })";
  common::ObString result2("SELECT *, (match(query) against('database or oceanBase' in natural language mode) + match(content) against('Elasticsearch' in natural language mode)) as _score FROM doc_table WHERE match(query) against('database or oceanBase' in natural language mode) AND match(content) against('Elasticsearch' in natural language mode) AND match(book_id) against('1' in natural language mode) AND match(age) against('2' in natural language mode) AND book_name = 'c ++ programming' ORDER BY _score DESC LIMIT 10");

  common::ObString req_str3 = R"({
    "query": {
      "bool": {
        "filter": [
          {"term": {"book_name": "c ++ programming"}},
          {"term": {"book_id": 1}},
          {"term": {"price": 2.35}}
        ]
      }
    }
  })";
  common::ObString result3("SELECT *, 0 as _score FROM doc_table WHERE book_name = 'c ++ programming' AND book_id = 1 AND price = 2.35 ORDER BY __pk_increment LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str1, result1);
  TestHybridSearchHelp::runtest(table_name, req_str2, result2);
  TestHybridSearchHelp::runtest(table_name, req_str3, result3);
}

TEST_F(TestHybridSearch, should_with_minimum_should_match)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "should": [
          {"match": {"query": "database or oceanBase"}},
          {"match": { "content": "Elasticsearch" }},
          {"term": {"book_id": "1"}},
          {"term": {"age": "2"}}
        ],
        "minimum_should_match": 3
      }
    }
  })";
  common::ObString result(
    "SELECT *, _fts0._score FROM "
    "(SELECT *, "
    "match(query) against('database or oceanBase' in natural language mode) as _word_score_0, "
    "match(content) against('Elasticsearch' in natural language mode) as _word_score_1, "
    "book_id = '1' as _word_score_2, "
    "age = '2' as _word_score_3, "
    "(match(query) against('database or oceanBase' in natural language mode) + "
    "match(content) against('Elasticsearch' in natural language mode) + "
    "(book_id = '1') + "
    "(age = '2')) as _score "
    "FROM doc_table "
    "WHERE "
    "match(query) against('database or oceanBase' in natural language mode) OR "
    "match(content) against('Elasticsearch' in natural language mode) OR "
    "book_id = '1' "
    "OR age = '2'"
    ") _fts0 "
    "WHERE (_word_score_0 > 0) + (_word_score_1 > 0) + (_word_score_2 > 0) + (_word_score_3 > 0) >= 3 "
    "ORDER BY _score DESC "
    "LIMIT 10");

  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, should_with_must_and_minimum_should_match)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
      "minimum_should_match": 2,
        "should": [
          {"match": {"query": "database or oceanBase"}},
          {"match": {"content": "Elasticsearch"}}
        ],
        "must": [
          {"match": {"book_id": "1"}},
          {"match": {"age": "2"}}
        ]
      }
    }
  })";
  common::ObString result(
    "SELECT *, _fts0._score FROM "
    "(SELECT *, "
    "match(query) against('database or oceanBase' in natural language mode) as _word_score_0, "
    "match(content) against('Elasticsearch' in natural language mode) as _word_score_1, "
    "(match(book_id) against('1' in natural language mode) + "
    "match(age) against('2' in natural language mode) + "
    "match(query) against('database or oceanBase' in natural language mode) + "
    "match(content) against('Elasticsearch' in natural language mode)) as _score "
    "FROM doc_table "
    "WHERE "
    "match(book_id) against('1' in natural language mode) AND "
    "match(age) against('2' in natural language mode) AND "
    "match(query) against('database or oceanBase' in natural language mode) "
    "AND match(content) against('Elasticsearch' in natural language mode)) _fts0 WHERE (_word_score_0 > 0) + (_word_score_1 > 0) >= 2 ORDER BY _score DESC LIMIT 10");

  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, should_with_must_and_minimum_should_match_one)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "should": [
          {"match": {"query": "database or oceanBase"}},
          {"match": {"content": "Elasticsearch"}},
          {"term": {"book_id": "1"}},
        ],
        "must": [
          {"match": {"book_name": "c ++ programming"}}
        ],
        "minimum_should_match": 1
      }
    }
  })";
  common::ObString result(
    "SELECT *, (match(book_name) against('c ++ programming' in natural language mode) + "
    "match(query) against('database or oceanBase' in natural language mode) + "
    "match(content) against('Elasticsearch' in natural language mode) + (book_id = '1')) as _score "
    "FROM doc_table WHERE match(book_name) against('c ++ programming' in natural language mode) AND "
    "(match(query) against('database or oceanBase' in natural language mode) OR "
    "match(content) against('Elasticsearch' in natural language mode) OR "
    "book_id = '1') "
    "ORDER BY _score DESC LIMIT 10");

  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, should_with_must_no_minimum_should_match)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "should": [
          {"match": {"query": "database or oceanBase"}},
          {"match": {"content": "Elasticsearch"}}
        ],
        "must": [
          {"match": {"book_id": "1"}},
          {"match": {"age": "2"}}
        ]
      }
    }
  })";
  common::ObString result("SELECT *, (match(book_id) against('1' in natural language mode) + match(age) against('2' in natural language mode) + match(query) against('database or oceanBase' in natural language mode) + match(content) against('Elasticsearch' in natural language mode)) as _score FROM doc_table WHERE match(book_id) against('1' in natural language mode) AND match(age) against('2' in natural language mode) ORDER BY _score DESC LIMIT 10");

  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, should_with_nested_bool)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "should": [
          {"match": {"query": "database or oceanBase"}},
          {"match": { "content": "Elasticsearch" }},
          {
          "bool": {
            "must": [
              {"match": {"book_id": "1"}},
              {"match": { "age": "2" }}
            ]
          }
         }
        ]
      }
    }
  })";
  common::ObString result(
    "SELECT *, (match(query) against('database or oceanBase' in natural language mode) + "
    "match(content) against('Elasticsearch' in natural language mode) + "
    "match(book_id) against('1' in natural language mode) + match(age) against('2' in natural language mode)) as _score "
    "FROM doc_table WHERE "
    "match(query) against('database or oceanBase' in natural language mode) OR "
    "match(content) against('Elasticsearch' in natural language mode) OR "
    "(match(book_id) against('1' in natural language mode) AND match(age) against('2' in natural language mode)) "
    "ORDER BY _score DESC LIMIT 10");

  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, should_with_nested_bool_and_minimum_should_match)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "should": [
          {"match": {"query": "database or oceanBase"}},
          {"match": { "content": "Elasticsearch" }},
          {
          "bool": {
            "must": [
              {"match": {"book_id": "1"}},
              {"match": { "age": "2" }}
            ]
          }
         }
        ],
        "minimum_should_match": 2
      }
    }
  })";
  common::ObString result(
    "SELECT *, (match(query) against('database or oceanBase' in natural language mode) + "
    "match(content) against('Elasticsearch' in natural language mode) + "
    "match(book_id) against('1' in natural language mode) + match(age) against('2' in natural language mode)) as _score "
    "FROM doc_table WHERE "
    "(match(query) against('database or oceanBase' in natural language mode) AND match(content) against('Elasticsearch' in natural language mode)) OR "
    "(match(query) against('database or oceanBase' in natural language mode) AND match(book_id) against('1' in natural language mode) AND match(age) against('2' in natural language mode)) OR "
    "(match(content) against('Elasticsearch' in natural language mode) AND match(book_id) against('1' in natural language mode) AND match(age) against('2' in natural language mode)) "
    "ORDER BY _score DESC LIMIT 10");

  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, basic_range)
{
  common::ObString req_str = R"({
      "query": {
        "range" : {
          "c1" : {
            "gte" : 2,
            "lte" : 5
          }
        }
      }
    })";
  ObString table_name("doc_table");
  common::ObString result("SELECT *, (c1 >= 2 AND c1 <= 5) as _score FROM doc_table WHERE c1 >= 2 AND c1 <= 5 ORDER BY _score DESC LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str, result);

  common::ObString req_str1 = R"({
    "query": {
      "bool": {
        "must": [
          {
            "range" : {
              "c1" : {
                "gte" : 2,
                "lte" : 5
              }
            }
          }
        ]
      }
    }
  })";
  common::ObString result1("SELECT *, (c1 >= 2 AND c1 <= 5) as _score FROM doc_table WHERE c1 >= 2 AND c1 <= 5 ORDER BY _score DESC LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str1, result1);
}

TEST_F(TestHybridSearch, basic_knn)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "knn" : {
          "field": "vector",
          "k": 5,
          "num_candidates": 10,
          "query_vector": [1,2,3],
          "boost": 0.7
      }
    })";

  common::ObString req_str1 = R"({
    "knn" : {
      "field": "text",
      "K": 10,
      "num_candidates": 10,
      "query_vector": [1,2,3],
      "BOOST": 0.7,
      "similarity" : 0.5
    }
  })";

  common::ObString req_str_no_field = R"({
    "knn" : {
      "K": 10,
      "num_candidates": 10,
      "query_vector": [1,2,3],
      "BOOST": 0.7,
      "similarity" : 0.5
    }
  })";

  common::ObString req_str_no_k = R"({
    "knn" : {
      "field": "text",
      "num_candidates": 10,
      "query_vector": [1,2,3],
      "BOOST": 0.7,
      "similarity" : 0.5
    }
  })";
  common::ObString req_str_no_query_vector = R"({
    "knn" : {
      "field": "text",
      "num_candidates": 10,
      "BOOST": 0.7,
      "similarity" : 0.5
    }
  })";
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString table_name("doc_table");
  common::ObString result("SELECT *, l2_distance(vector, '[1, 2, 3]') as _distance, (round(1 / (1 + l2_distance(vector, '[1, 2, 3]')), 8) * 0.7) as _score FROM doc_table ORDER BY _distance APPROXIMATE LIMIT 5");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(allocator, &table_name);
  SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
    MEMSET(buf, 0, sizeof(buf));
    int64_t res_len = 0;
    int64_t start_ts = ObClockGenerator::getClock();
    ASSERT_EQ(OB_SUCCESS, parser.parse(req_str, req));
    ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
    int64_t translate_cost_time = ObClockGenerator::getClock() - start_ts;
    std::cout << "translate cost time: " << translate_cost_time << std::endl;
    std::cout << "translate sql:" << std::endl << buf << std::endl;
    std::cout << "expect sql:" << std::endl << result.ptr() << std::endl;
    ObString trans_res(res_len, buf);
    ASSERT_EQ(0, result.case_compare(trans_res));

    MEMSET(buf, 0, sizeof(buf));
    res_len = 0;
    common::ObString res1("SELECT * FROM (SELECT *, l2_distance(text, '[1, 2, 3]') as _distance, (round(1 / (1 + l2_distance(text, '[1, 2, 3]')), 8) * 0.7) as _score FROM doc_table ORDER BY _distance APPROXIMATE LIMIT 10) _vs0 WHERE _vs0._distance <= 0.5 LIMIT 10");
    start_ts = ObClockGenerator::getClock();

    ASSERT_EQ(OB_SUCCESS, parser.parse(req_str1, req));
    ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
    translate_cost_time = ObClockGenerator::getClock() - start_ts;
    std::cout << "translate cost time: " << translate_cost_time << std::endl;
    std::cout << "translate sql:" << std::endl << buf << std::endl;
    std::cout << "expect sql:" << std::endl << res1.ptr() << std::endl;
    trans_res.assign_ptr(buf, res_len);
    ASSERT_EQ(0, res1.case_compare(trans_res));

    ASSERT_EQ(OB_ERR_PARSER_SYNTAX, parser.parse(req_str_no_field, req));
    ASSERT_EQ(OB_ERR_PARSER_SYNTAX, parser.parse(req_str_no_k, req));
    ASSERT_EQ(OB_ERR_PARSER_SYNTAX, parser.parse(req_str_no_query_vector, req));
  }
}

TEST_F(TestHybridSearch, knn_filter)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "knn" : {
          "field": "vector",
          "k": 5,
          "num_candidates": 10,
          "query_vector": [1,2,3],
          "filter" : [
            {"range": {"c1": {"gte" : 2}}}
          ],
          "boost": 0.7
      }
    })";

  common::ObString req_str1 = R"({
    "knn" : {
      "field": "text",
      "K": 10,
      "num_candidates": 10,
      "query_vector": [1,2,3],
      "BOOST": 0.7,
      "filter" : [
        {"range": {"c1": {"gte" : 2}}}
      ],
      "similarity" : 0.5
    }
  })";
  ObString table_name("doc_table");
  common::ObString result("SELECT *, l2_distance(vector, '[1, 2, 3]') as _distance, (round(1 / (1 + l2_distance(vector, '[1, 2, 3]')), 8) * 0.7) as _score FROM doc_table WHERE c1 >= 2 ORDER BY _distance APPROXIMATE LIMIT 5");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
  common::ObString res1("SELECT * FROM (SELECT *, l2_distance(text, '[1, 2, 3]') as _distance, (round(1 / (1 + l2_distance(text, '[1, 2, 3]')), 8) * 0.7) as _score FROM doc_table WHERE c1 >= 2 ORDER BY _distance APPROXIMATE LIMIT 10) _vs0 WHERE _vs0._distance <= 0.5 LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str1, res1);
}

TEST_F(TestHybridSearch, multi_knn)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "knn" : [{
          "field": "vector",
          "k": 5,
          "num_candidates": 10,
          "query_vector": [1,2,3],
          "filter" : [
            {"range": {"c1": {"gte" : 2}}}
          ],
          "boost": 0.7
        },
        {
          "field": "semantic_text",
          "k": 5,
          "num_candidates": 10,
          "query_vector": [1,2,3]
        }
      ]
    })";
  ObString table_name("doc_table");
  common::ObString result("SELECT *, sum(_score) as _score FROM ((SELECT /*+ opt_param('hidden_column_visible', 'true') */*, l2_distance(vector, '[1, 2, 3]') as _distance, __pk_increment, (round(1 / (1 + l2_distance(vector, '[1, 2, 3]')), 8) * 0.7) as _score FROM doc_table WHERE c1 >= 2 ORDER BY _distance APPROXIMATE LIMIT 5)  UNION ALL (SELECT /*+ opt_param('hidden_column_visible', 'true') */*, l2_distance(semantic_text, '[1, 2, 3]') as _distance, __pk_increment, round(1 / (1 + l2_distance(semantic_text, '[1, 2, 3]')), 8) as _score FROM doc_table ORDER BY _distance APPROXIMATE LIMIT 5) ) GROUP BY __pk_increment ORDER BY _score DESC LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, rank_feature)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "rank_feature": {
          "field" : "pagerank",
          "sigmoid": {"pivot": 40, "exponent": 0.6}
        }
      }
    })";
  common::ObString result("SELECT *, (pow(pagerank, 0.6) / (pow(pagerank, 0.6) + pow(40, 0.6))) as _score FROM doc_table WHERE pagerank IS NOT NULL ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);

  common::ObString req_str1 = R"({
    "query": {
      "rank_feature": {
        "field" : "pagerank",
        "saturation": {"pivot": 40, "positive_score_impact": false}
      }
    }
  })";
  common::ObString result1("SELECT *, (40 / (pagerank + 40)) as _score FROM doc_table WHERE pagerank IS NOT NULL ORDER BY _score DESC LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str1, result1);

    common::ObString req_str2 = R"({
    "query": {
      "rank_feature": {
        "field" : "pagerank",
        "saturation": {"pivot": 40}
      }
    }
  })";
  common::ObString result2("SELECT *, (pagerank / (pagerank + 40)) as _score FROM doc_table WHERE pagerank IS NOT NULL ORDER BY _score DESC LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str2, result2);

  common::ObString req_str3 = R"({
    "query": {
      "rank_feature": {
        "field" : "pagerank",
        "linear": {}
      }
    }
  })";
  common::ObString result3("SELECT *, pagerank as _score FROM doc_table WHERE pagerank IS NOT NULL ORDER BY _score DESC LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str3, result3);

  common::ObString req_str4 = R"({
    "query": {
      "rank_feature": {
        "field" : "pagerank",
        "linear": {"positive_score_impact": false}
      }
    }
  })";
  common::ObString result4("SELECT *, (1 / pagerank) as _score FROM doc_table WHERE pagerank IS NOT NULL ORDER BY _score DESC LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str4, result4);

  common::ObString req_str5 = R"({
      "query": {
        "rank_feature": {
          "field" : "pagerank",
          "sigmoid": {"pivot": 40, "exponent": 0.6, "positive_score_impact": false}
        }
      }
    })";
  common::ObString result5("SELECT *, (pow(40, 0.6) / (pow(pagerank, 0.6) + pow(40, 0.6))) as _score FROM doc_table WHERE pagerank IS NOT NULL ORDER BY _score DESC LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str5, result5);

  common::ObString req_str6 = R"({
    "query": {
      "rank_feature": {
        "field" : "pagerank",
        "log": {"scaling_factor" : 4}
      }
    }
  })";
  common::ObString result6("SELECT *, ln(pagerank + 4) as _score FROM doc_table WHERE pagerank IS NOT NULL ORDER BY _score DESC LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str6, result6);

  // only positive score impact supported
  common::ObString req_str7 = R"({
    "query": {
      "rank_feature": {
        "field" : "pagerank",
        "log": {"scaling_factor" : 4, "positive_score_impact": false}
      }
    }
  })";
  common::ObString result7("SELECT *, ln(pagerank + 4) as _score FROM doc_table WHERE pagerank IS NOT NULL ORDER BY _score DESC LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str7, result7);

}

TEST_F(TestHybridSearch, match_knn)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "bool": {
          "must": [
            {"match": {"query": "database or oceanBase"}},
            {"match": { "content": "Elasticsearch" }}
          ]
        }
      },
       "knn" : {
          "field": "vector",
          "k": 5,
          "num_candidates": 10,
          "query_vector": [1,2,3],
          "boost": 0.7
      }
    })";
  common::ObString result("SELECT *, (ifnull(_fts._keyword_score, 0) + ifnull(_vs._semantic_score, 0)) as _score FROM ((SELECT /*+ opt_param('hidden_column_visible', 'true') */__pk_increment, (match(query) against('database or oceanBase' in natural language mode) + match(content) against('Elasticsearch' in natural language mode)) as _keyword_score FROM doc_table WHERE match(query) against('database or oceanBase' in natural language mode) AND match(content) against('Elasticsearch' in natural language mode) ORDER BY _keyword_score DESC LIMIT 200) _fts full join (SELECT /*+ opt_param('hidden_column_visible', 'true') */*, l2_distance(vector, '[1, 2, 3]') as _distance, __pk_increment, (round(1 / (1 + l2_distance(vector, '[1, 2, 3]')), 8) * 0.7) as _semantic_score FROM doc_table ORDER BY _distance APPROXIMATE LIMIT 5) _vs on _fts.__pk_increment = _vs.__pk_increment) ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, match_knn_output)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "bool": {
          "must": [
            {"match": {"query": "database or oceanBase"}},
            {"match": { "content": "Elasticsearch" }}
          ]
        }
      },
       "knn" : {
          "field": "vector",
          "k": 5,
          "num_candidates": 10,
          "query_vector": [1,2,3],
          "boost": 0.7
      },
      "_source" : ["query", "content", "vector", "_keyword_score", "_semantic_score"]
    })";
  common::ObString result("SELECT ifnull(_fts.query, _vs.query) as query, ifnull(_fts.content, _vs.content) as content, ifnull(_fts.vector, _vs.vector) as vector, _keyword_score, _semantic_score, (ifnull(_fts._keyword_score, 0) + ifnull(_vs._semantic_score, 0)) as _score FROM ((SELECT /*+ opt_param('hidden_column_visible', 'true') */__pk_increment, query, content, vector, (match(query) against('database or oceanBase' in natural language mode) + match(content) against('Elasticsearch' in natural language mode)) as _keyword_score FROM doc_table WHERE match(query) against('database or oceanBase' in natural language mode) AND match(content) against('Elasticsearch' in natural language mode) ORDER BY _keyword_score DESC LIMIT 200) _fts full join (SELECT /*+ opt_param('hidden_column_visible', 'true') */l2_distance(vector, '[1, 2, 3]') as _distance, __pk_increment, query, content, vector, (round(1 / (1 + l2_distance(vector, '[1, 2, 3]')), 8) * 0.7) as _semantic_score FROM doc_table ORDER BY _distance APPROXIMATE LIMIT 5) _vs on _fts.__pk_increment = _vs.__pk_increment) ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, match_knn_output_wrap)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "bool": {
          "must": [
            {"match": {"query": "database or oceanBase"}},
            {"match": { "content": "Elasticsearch" }}
          ]
        }
      },
       "knn" : {
          "field": "vector",
          "k": 5,
          "num_candidates": 10,
          "query_vector": [1,2,3],
          "boost": 0.7
      },
      "_source" : ["query", "content", "_keyword_score", "_semantic_score"]
    })";
  common::ObString result("SELECT json_arrayagg(json_object('query', query, 'content', content, '_keyword_score', _keyword_score, '_semantic_score', _semantic_score, '_score', _score)) as hits FROM (SELECT ifnull(_fts.query, _vs.query) as query, ifnull(_fts.content, _vs.content) as content, _keyword_score, _semantic_score, (ifnull(_fts._keyword_score, 0) + ifnull(_vs._semantic_score, 0)) as _score FROM ((SELECT /*+ opt_param('hidden_column_visible', 'true') */__pk_increment, query, content, (match(query) against('database or oceanBase' in natural language mode) + match(content) against('Elasticsearch' in natural language mode)) as _keyword_score FROM test.doc_table WHERE match(query) against('database or oceanBase' in natural language mode) AND match(content) against('Elasticsearch' in natural language mode) ORDER BY _keyword_score DESC LIMIT 200) _fts full join (SELECT /*+ opt_param('hidden_column_visible', 'true') */l2_distance(vector, '[1, 2, 3]') as _distance, __pk_increment, query, content, (round(1 / (1 + l2_distance(vector, '[1, 2, 3]')), 8) * 0.7) as _semantic_score FROM test.doc_table ORDER BY _distance APPROXIMATE LIMIT 5) _vs on _fts.__pk_increment = _vs.__pk_increment) ORDER BY _score DESC LIMIT 10) ");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, true, "test");
}

TEST_F(TestHybridSearch, query_string_without_type)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "fields": ["title^3", "content^2.5", "tags"],
          "query": "elasticsearch^2 database^0.8"
        }
      }
    })";

  common::ObString result(
    "SELECT *, "
    "(GREATEST(match(title) against('elasticsearch' in natural language mode) * 3, match(content) against('elasticsearch' in natural language mode) * 2.5, match(tags) against('elasticsearch' in natural language mode)) * 2 +"
    " GREATEST(match(title) against('database' in natural language mode) * 3, match(content) against('database' in natural language mode) * 2.5, match(tags) against('database' in natural language mode)) * 0.8)"
    " as _score FROM doc_table WHERE"
    " match(title) against('elasticsearch' in natural language mode) OR"
    " match(title) against('database' in natural language mode) OR"
    " match(content) against('elasticsearch' in natural language mode) OR"
    " match(content) against('database' in natural language mode) OR"
    " match(tags) against('elasticsearch' in natural language mode) OR"
    " match(tags) against('database' in natural language mode)"
    " ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(tmp_allocator, &table_name);
  SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
    MEMSET(buf, 0, sizeof(buf));
    int64_t res_len = 0;
    const int64_t start_ts = ObClockGenerator::getClock();
    ret = parser.parse(req_str, req);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
    const int64_t translate_cost_time = ObClockGenerator::getClock() - start_ts;
    std::cout << "translate cost time: " << translate_cost_time << std::endl;
    std::cout << "translate sql:" << std::endl << buf << std::endl;
    std::cout << "expect sql:" << std::endl << result.ptr() << std::endl;
    ObString trans_res(res_len, buf);
    ASSERT_EQ(0, result.case_compare(trans_res));
  }
}

TEST_F(TestHybridSearch, query_string_best_fields)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "type": "best_fields",
          "fields": ["title^3", "content^2.5", "tags^1.5"],
          "query": "elasticsearch^2 database^0.8 tutorial^1.2",
          "boost": 1.5
        }
      }
    })";

  common::ObString result(
    "SELECT *, "
    "((GREATEST(match(title) against('elasticsearch' in natural language mode) * 3, match(content) against('elasticsearch' in natural language mode) * 2.5, match(tags) against('elasticsearch' in natural language mode) * 1.5) * 2 +"
    " GREATEST(match(title) against('database' in natural language mode) * 3, match(content) against('database' in natural language mode) * 2.5, match(tags) against('database' in natural language mode) * 1.5) * 0.8 +"
    " GREATEST(match(title) against('tutorial' in natural language mode) * 3, match(content) against('tutorial' in natural language mode) * 2.5, match(tags) against('tutorial' in natural language mode) * 1.5) * 1.2) * 1.5)"
    " as _score FROM doc_table WHERE"
    " match(title) against('elasticsearch' in natural language mode) OR"
    " match(title) against('database' in natural language mode) OR"
    " match(title) against('tutorial' in natural language mode) OR"
    " match(content) against('elasticsearch' in natural language mode) OR"
    " match(content) against('database' in natural language mode) OR"
    " match(content) against('tutorial' in natural language mode) OR"
    " match(tags) against('elasticsearch' in natural language mode) OR"
    " match(tags) against('database' in natural language mode) OR"
    " match(tags) against('tutorial' in natural language mode)"
    " ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(tmp_allocator, &table_name);
  SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
    MEMSET(buf, 0, sizeof(buf));
    int64_t res_len = 0;
    const int64_t start_ts = ObClockGenerator::getClock();
    ret = parser.parse(req_str, req);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
    const int64_t translate_cost_time = ObClockGenerator::getClock() - start_ts;
    std::cout << "translate cost time: " << translate_cost_time << std::endl;
    std::cout << "translate sql:" << std::endl << buf << std::endl;
    std::cout << "expect sql:" << std::endl << result.ptr() << std::endl;
    ObString trans_res(res_len, buf);
    ASSERT_EQ(0, result.case_compare(trans_res));
  }
}

TEST_F(TestHybridSearch, query_string_best_fields_with_minimum_should_match)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "type": "best_fields",
        "fields": ["title^3", "content^2.5", "tags^1.5"],
        "query": "elasticsearch^2 database^0.8 tutorial^1.2",
        "boost": 1.5,
        "minimum_should_match": 2
      }
    }
  })";

  common::ObString result(

    "SELECT *, _fts0._score FROM "
    "(SELECT *, "
    "GREATEST(match(title) against('elasticsearch' in natural language mode) * 3, "
    "match(content) against('elasticsearch' in natural language mode) * 2.5, "
    "match(tags) against('elasticsearch' in natural language mode) * 1.5) * 2 as _word_score_0, "
    "GREATEST(match(title) against('database' in natural language mode) * 3, "
    "match(content) against('database' in natural language mode) * 2.5, "
    "match(tags) against('database' in natural language mode) * 1.5) * 0.8 as _word_score_1, "
    "GREATEST(match(title) against('tutorial' in natural language mode) * 3, "
    "match(content) against('tutorial' in natural language mode) * 2.5, "
    "match(tags) against('tutorial' in natural language mode) * 1.5) * 1.2 as _word_score_2, "
    "((GREATEST(match(title) against('elasticsearch' in natural language mode) * 3, "
    "match(content) against('elasticsearch' in natural language mode) * 2.5, "
    "match(tags) against('elasticsearch' in natural language mode) * 1.5) * 2 + "
    "GREATEST(match(title) against('database' in natural language mode) * 3, "
    "match(content) against('database' in natural language mode) * 2.5, "
    "match(tags) against('database' in natural language mode) * 1.5) * 0.8 + "
    "GREATEST(match(title) against('tutorial' in natural language mode) * 3, "
    "match(content) against('tutorial' in natural language mode) * 2.5, "
    "match(tags) against('tutorial' in natural language mode) * 1.5) * 1.2) * 1.5) as _score "
    "FROM doc_table "
    "WHERE "
    "match(title) against('elasticsearch' in natural language mode) OR "
    "match(title) against('database' in natural language mode) OR "
    "match(title) against('tutorial' in natural language mode) OR "
    "match(content) against('elasticsearch' in natural language mode) OR "
    "match(content) against('database' in natural language mode) OR "
    "match(content) against('tutorial' in natural language mode) OR "
    "match(tags) against('elasticsearch' in natural language mode) OR "
    "match(tags) against('database' in natural language mode) OR "
    "match(tags) against('tutorial' in natural language mode)) _fts0 "
    "WHERE "
    "(_word_score_0 > 0) + (_word_score_1 > 0) + (_word_score_2 > 0) >= 2 "
    "ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, query_string_best_fields_simple)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "type": "best_fields",
          "fields": ["title^3"],
          "query": "elasticsearch^2.89",
          "boost": 3.1415926
        }
      }
    })";

  common::ObString result(
    "SELECT *, "
    "(match(title) against('elasticsearch' in natural language mode) * 3 * 2.89 * 3.1415926)"
    " as _score FROM doc_table WHERE"
    " match(title) against('elasticsearch' in natural language mode)"
    " ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(tmp_allocator, &table_name);
  SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
    MEMSET(buf, 0, sizeof(buf));
    int64_t res_len = 0;
    const int64_t start_ts = ObClockGenerator::getClock();
    ret = parser.parse(req_str, req);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
    const int64_t translate_cost_time = ObClockGenerator::getClock() - start_ts;
    std::cout << "translate cost time: " << translate_cost_time << std::endl;
    std::cout << "translate sql:" << std::endl << buf << std::endl;
    std::cout << "expect sql:" << std::endl << result.ptr() << std::endl;
    ObString trans_res(res_len, buf);
    ASSERT_EQ(0, result.case_compare(trans_res));
  }
}

TEST_F(TestHybridSearch, query_string_most_fields)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "type": "most_fields",
          "fields": ["title^3", "content^2.5", "tags^1.5"],
          "query": "elasticsearch^2 database^0.8 tutorial^1.2",
          "boost": 0.8
        }
      }
    })";

  common::ObString result(
    "SELECT *, "
    "(((match(title) against('elasticsearch' in natural language mode) * 3 + match(content) against('elasticsearch' in natural language mode) * 2.5 + match(tags) against('elasticsearch' in natural language mode) * 1.5) * 2 +"
    " (match(title) against('database' in natural language mode) * 3 + match(content) against('database' in natural language mode) * 2.5 + match(tags) against('database' in natural language mode) * 1.5) * 0.8 +"
    " (match(title) against('tutorial' in natural language mode) * 3 + match(content) against('tutorial' in natural language mode) * 2.5 + match(tags) against('tutorial' in natural language mode) * 1.5) * 1.2) * 0.8)"
    " as _score FROM doc_table WHERE"
    " match(title) against('elasticsearch' in natural language mode) OR"
    " match(title) against('database' in natural language mode) OR"
    " match(title) against('tutorial' in natural language mode) OR"
    " match(content) against('elasticsearch' in natural language mode) OR"
    " match(content) against('database' in natural language mode) OR"
    " match(content) against('tutorial' in natural language mode) OR"
    " match(tags) against('elasticsearch' in natural language mode) OR"
    " match(tags) against('database' in natural language mode) OR"
    " match(tags) against('tutorial' in natural language mode)"
    " ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(tmp_allocator, &table_name);
  SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
    MEMSET(buf, 0, sizeof(buf));
    int64_t res_len = 0;
    const int64_t start_ts = ObClockGenerator::getClock();
    ret = parser.parse(req_str, req);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
    const int64_t translate_cost_time = ObClockGenerator::getClock() - start_ts;
    std::cout << "translate cost time: " << translate_cost_time << std::endl;
    std::cout << "translate sql:" << std::endl << buf << std::endl;
    std::cout << "expect sql:" << std::endl << result.ptr() << std::endl;
    ObString trans_res(res_len, buf);
    ASSERT_EQ(0, result.case_compare(trans_res));
  }
}

TEST_F(TestHybridSearch, query_string_most_fields_with_minimum_should_match)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "type": "most_fields",
        "fields": ["title^3", "content^2.5", "tags^1.5"],
        "query": "elasticsearch^2 database^0.8 tutorial^1.2",
        "boost": 0.8,
        "minimum_should_match": 2
      }
    }
  })";

  common::ObString result(
    "SELECT *, _fts0._score "
    "FROM "
    "(SELECT *, "
    "(match(title) against('elasticsearch' in natural language mode) * 3 + match(content) against('elasticsearch' in natural language mode) * 2.5 + "
    "match(tags) against('elasticsearch' in natural language mode) * 1.5) * 2 as _word_score_0, "
    "(match(title) against('database' in natural language mode) * 3 + match(content) against('database' in natural language mode) * 2.5 + "
    "match(tags) against('database' in natural language mode) * 1.5) * 0.8 as _word_score_1, "
    "(match(title) against('tutorial' in natural language mode) * 3 + match(content) against('tutorial' in natural language mode) * 2.5 + "
    "match(tags) against('tutorial' in natural language mode) * 1.5) * 1.2 as _word_score_2, "
    "(((match(title) against('elasticsearch' in natural language mode) * 3 + match(content) against('elasticsearch' in natural language mode) * 2.5 + "
    "match(tags) against('elasticsearch' in natural language mode) * 1.5) * 2 + (match(title) against('database' in natural language mode) * 3 + "
    "match(content) against('database' in natural language mode) * 2.5 + match(tags) against('database' in natural language mode) * 1.5) * 0.8 + "
    "(match(title) against('tutorial' in natural language mode) * 3 + match(content) against('tutorial' in natural language mode) * 2.5 + "
    "match(tags) against('tutorial' in natural language mode) * 1.5) * 1.2) * 0.8) as _score "
    "FROM doc_table "
    "WHERE match(title) against('elasticsearch' in natural language mode) OR "
    "match(title) against('database' in natural language mode) OR "
    "match(title) against('tutorial' in natural language mode) OR "
    "match(content) against('elasticsearch' in natural language mode) OR "
    "match(content) against('database' in natural language mode) OR "
    "match(content) against('tutorial' in natural language mode) OR "
    "match(tags) against('elasticsearch' in natural language mode) OR "
    "match(tags) against('database' in natural language mode) OR "
    "match(tags) against('tutorial' in natural language mode)) _fts0 "
    "WHERE (_word_score_0 > 0) + (_word_score_1 > 0) + (_word_score_2 > 0) >= 2 "
    "ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, query_string_phrase_no_weight)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "type": "phrase",
          "fields": ["title^3", "content^2.5", "tags^1.5"],
          "query": "elasticsearch database tutorial",
          "boost": 1.2
        }
      }
    })";

  common::ObString result(
    "SELECT *, "
    "(GREATEST(match(title) against('elasticsearch database tutorial' in match phrase mode) * 3, "
    "match(content) against('elasticsearch database tutorial' in match phrase mode) * 2.5, "
    "match(tags) against('elasticsearch database tutorial' in match phrase mode) * 1.5) * 1.2)"
    " as _score FROM doc_table WHERE"
    " match(title) against('elasticsearch database tutorial' in match phrase mode) OR"
    " match(content) against('elasticsearch database tutorial' in match phrase mode) OR"
    " match(tags) against('elasticsearch database tutorial' in match phrase mode)"
    " ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(tmp_allocator, &table_name);
  SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
    MEMSET(buf, 0, sizeof(buf));
    int64_t res_len = 0;
    const int64_t start_ts = ObClockGenerator::getClock();
    ret = parser.parse(req_str, req);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
    const int64_t translate_cost_time = ObClockGenerator::getClock() - start_ts;
    std::cout << "translate cost time: " << translate_cost_time << std::endl;
    std::cout << "translate sql:" << std::endl << buf << std::endl;
    std::cout << "expect sql:" << std::endl << result.ptr() << std::endl;
    ObString trans_res(res_len, buf);
    ASSERT_EQ(0, result.case_compare(trans_res));
  }
}

TEST_F(TestHybridSearch, query_string_phrase_no_weight_with_minimum_should_match)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "type": "phrase",
        "fields": ["title^3", "content^2.5", "tags^1.5"],
        "query": "elasticsearch database tutorial",
        "boost": 1.2,
        "minimum_should_match": 2
      }
    }
  })";
  common::ObString result(
    "SELECT *, "
    "(GREATEST(match(title) against('elasticsearch database tutorial' in match phrase mode) * 3, "
    "match(content) against('elasticsearch database tutorial' in match phrase mode) * 2.5, "
    "match(tags) against('elasticsearch database tutorial' in match phrase mode) * 1.5) * 1.2)"
    " as _score FROM doc_table WHERE"
    " match(title) against('elasticsearch database tutorial' in match phrase mode) OR"
    " match(content) against('elasticsearch database tutorial' in match phrase mode) OR"
    " match(tags) against('elasticsearch database tutorial' in match phrase mode)"
    " ORDER BY _score DESC LIMIT 10");
  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

// in phrase mode, if keyword weight is designated, the query string is parsed as best_fields
TEST_F(TestHybridSearch, query_string_phrase_with_weight)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "type": "phrase",
          "fields": ["title^3", "content^2.5", "tags^1.5"],
          "query": "elasticsearch^2 database^0.8 tutorial^1.2",
          "boost": 0.9
        }
      }
    })";

  common::ObString result(
    "SELECT *, "
    "((GREATEST(match(title) against('elasticsearch' in natural language mode) * 3, match(content) against('elasticsearch' in natural language mode) * 2.5, match(tags) against('elasticsearch' in natural language mode) * 1.5) * 2 +"
    " GREATEST(match(title) against('database' in natural language mode) * 3, match(content) against('database' in natural language mode) * 2.5, match(tags) against('database' in natural language mode) * 1.5) * 0.8 +"
    " GREATEST(match(title) against('tutorial' in natural language mode) * 3, match(content) against('tutorial' in natural language mode) * 2.5, match(tags) against('tutorial' in natural language mode) * 1.5) * 1.2) * 0.9)"
    " as _score FROM doc_table WHERE"
    " match(title) against('elasticsearch' in natural language mode) OR"
    " match(title) against('database' in natural language mode) OR"
    " match(title) against('tutorial' in natural language mode) OR"
    " match(content) against('elasticsearch' in natural language mode) OR"
    " match(content) against('database' in natural language mode) OR"
    " match(content) against('tutorial' in natural language mode) OR"
    " match(tags) against('elasticsearch' in natural language mode) OR"
    " match(tags) against('database' in natural language mode) OR"
    " match(tags) against('tutorial' in natural language mode)"
    " ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(tmp_allocator, &table_name);
  SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
    MEMSET(buf, 0, sizeof(buf));
    int64_t res_len = 0;
    const int64_t start_ts = ObClockGenerator::getClock();
    ret = parser.parse(req_str, req);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
    const int64_t translate_cost_time = ObClockGenerator::getClock() - start_ts;
    std::cout << "translate cost time: " << translate_cost_time << std::endl;
    std::cout << "translate sql:" << std::endl << buf << std::endl;
    std::cout << "expect sql:" << std::endl << result.ptr() << std::endl;
    ObString trans_res(res_len, buf);
    ASSERT_EQ(0, result.case_compare(trans_res));
  }
}

TEST_F(TestHybridSearch, query_string_cross_fields_no_default_operator)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "type": "cross_fields",
          "fields": ["title^3", "content^2.5", "tags^1.5"],
          "query": "elasticsearch database tutorial"
        }
      }
    })";

  common::ObString result(
    "SELECT *, "
    "(GREATEST(match(title) against('elasticsearch' in natural language mode) * 3, match(content) against('elasticsearch' in natural language mode) * 2.5, match(tags) against('elasticsearch' in natural language mode) * 1.5) +"
    " GREATEST(match(title) against('database' in natural language mode) * 3, match(content) against('database' in natural language mode) * 2.5, match(tags) against('database' in natural language mode) * 1.5) +"
    " GREATEST(match(title) against('tutorial' in natural language mode) * 3, match(content) against('tutorial' in natural language mode) * 2.5, match(tags) against('tutorial' in natural language mode) * 1.5))"
    " as _score FROM doc_table WHERE"
    " match(title) against('elasticsearch' in natural language mode) OR"
    " match(title) against('database' in natural language mode) OR"
    " match(title) against('tutorial' in natural language mode) OR"
    " match(content) against('elasticsearch' in natural language mode) OR"
    " match(content) against('database' in natural language mode) OR"
    " match(content) against('tutorial' in natural language mode) OR"
    " match(tags) against('elasticsearch' in natural language mode) OR"
    " match(tags) against('database' in natural language mode) OR"
    " match(tags) against('tutorial' in natural language mode)"
    " ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(tmp_allocator, &table_name);
  SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
    MEMSET(buf, 0, sizeof(buf));
    int64_t res_len = 0;
    const int64_t start_ts = ObClockGenerator::getClock();
    ret = parser.parse(req_str, req);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
    const int64_t translate_cost_time = ObClockGenerator::getClock() - start_ts;
    std::cout << "translate cost time: " << translate_cost_time << std::endl;
    std::cout << "translate sql:" << std::endl << buf << std::endl;
    std::cout << "expect sql:" << std::endl << result.ptr() << std::endl;
    ObString trans_res(res_len, buf);
    ASSERT_EQ(0, result.case_compare(trans_res));
  }
}

TEST_F(TestHybridSearch, query_string_cross_fields_with_and_operator)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "type": "cross_fields",
          "fields": ["title^3", "content^2.5", "tags^1.5"],
          "query": "elasticsearch database tutorial",
          "default_operator": "AND"
        }
      }
    })";

  common::ObString result(
    "SELECT *, "
    "(GREATEST(match(title) against('elasticsearch' in natural language mode) * 3, match(content) against('elasticsearch' in natural language mode) * 2.5, match(tags) against('elasticsearch' in natural language mode) * 1.5) +"
    " GREATEST(match(title) against('database' in natural language mode) * 3, match(content) against('database' in natural language mode) * 2.5, match(tags) against('database' in natural language mode) * 1.5) +"
    " GREATEST(match(title) against('tutorial' in natural language mode) * 3, match(content) against('tutorial' in natural language mode) * 2.5, match(tags) against('tutorial' in natural language mode) * 1.5))"
    " as _score FROM doc_table WHERE"
    " (match(title) against('elasticsearch' in natural language mode) OR match(content) against('elasticsearch' in natural language mode) OR match(tags) against('elasticsearch' in natural language mode)) AND"
    " (match(title) against('database' in natural language mode) OR match(content) against('database' in natural language mode) OR match(tags) against('database' in natural language mode)) AND"
    " (match(title) against('tutorial' in natural language mode) OR match(content) against('tutorial' in natural language mode) OR match(tags) against('tutorial' in natural language mode))"
    " ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(tmp_allocator, &table_name);
  SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
    MEMSET(buf, 0, sizeof(buf));
    int64_t res_len = 0;
    const int64_t start_ts = ObClockGenerator::getClock();
    ret = parser.parse(req_str, req);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, req->translate(buf, OB_MAX_SQL_LENGTH, res_len));
    const int64_t translate_cost_time = ObClockGenerator::getClock() - start_ts;
    std::cout << "translate cost time: " << translate_cost_time << std::endl;
    std::cout << "translate sql:" << std::endl << buf << std::endl;
    std::cout << "expect sql:" << std::endl << result.ptr() << std::endl;
    ObString trans_res(res_len, buf);
    ASSERT_EQ(0, result.case_compare(trans_res));
  }
}

TEST_F(TestHybridSearch, query_string_phrase_with_weight_and_minimum_should_match_and_default_operator)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "type": "phrase",
        "fields": ["title^3", "content^2.5", "tags^1.5"],
        "query": "elasticsearch database tutorial^1.2",
        "boost": 0.77,
        "minimum_should_match": 2,
        "default_operator": "AND"
      }
    }
  })";

  common::ObString result(
    "SELECT *, ((GREATEST(match(title) against('elasticsearch database' in match phrase mode) * 3, "
    "match(content) against('elasticsearch database' in match phrase mode) * 2.5, "
    "match(tags) against('elasticsearch database' in match phrase mode) * 1.5) + "
    "GREATEST(match(title) against('tutorial' in natural language mode) * 3, "
    "match(content) against('tutorial' in natural language mode) * 2.5, "
    "match(tags) against('tutorial' in natural language mode) * 1.5) * 1.2) * 0.77) as _score "
    "FROM doc_table WHERE "
    "(match(title) against('elasticsearch database' in match phrase mode) AND "
    "match(title) against('tutorial' in natural language mode)) OR "
    "(match(content) against('elasticsearch database' in match phrase mode) AND "
    "match(content) against('tutorial' in natural language mode)) OR "
    "(match(tags) against('elasticsearch database' in match phrase mode) AND "
    "match(tags) against('tutorial' in natural language mode)) "
    "ORDER BY _score DESC LIMIT 10");

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
}

TEST_F(TestHybridSearch, query_string_invalid_weights)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "fields": ["title^-3", "content^0", "tags^invalid"],
          "query": "elasticsearch^-2 tutorial^0 database^invalid"
        }
      }
    })";

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(tmp_allocator, &table_name);
  ret = parser.parse(req_str, req);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST_F(TestHybridSearch, offset_size)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
      "knn" : {
          "field": "vector",
          "k": 5,
          "num_candidates": 10,
          "query_vector": [1,2,3],
          "filter" : [
            {"range": {"c1": {"gte" : 2}}}
          ],
          "boost": 0.7
      },
      "from" : 1,
      "size" : 4
    })";

  common::ObString req_str1 = R"({
      "knn" : {
          "field": "vector",
          "k": 5,
          "num_candidates": 10,
          "query_vector": [1,2,3],
          "filter" : [
            {"range": {"c1": {"gte" : 2}}}
          ],
          "boost": 0.7
      },
      "from" : 2,
      "size" : 7
    })";
  ObString table_name("doc_table");
  common::ObString result("SELECT *, l2_distance(vector, '[1, 2, 3]') as _distance, (round(1 / (1 + l2_distance(vector, '[1, 2, 3]')), 8) * 0.7) as _score FROM doc_table WHERE c1 >= 2 ORDER BY _distance APPROXIMATE LIMIT 1, 4");
  TestHybridSearchHelp::runtest(table_name, req_str, result);
  common::ObString res1("SELECT *, l2_distance(vector, '[1, 2, 3]') as _distance, (round(1 / (1 + l2_distance(vector, '[1, 2, 3]')), 8) * 0.7) as _score FROM doc_table WHERE c1 >= 2 ORDER BY _distance APPROXIMATE LIMIT 2, 5");
  TestHybridSearchHelp::runtest(table_name, req_str1, res1);

  common::ObString req_str2 = R"({
      "query": {
        "bool": {
          "must": [
            {"match": {"query": "database or oceanBase"}},
            {"match": { "content": "Elasticsearch" }}
          ]
        }
      },
      "from" : 2,
      "size" : 7
    })";
  common::ObString res2("SELECT *, (match(query) against('database or oceanBase' in natural language mode) + match(content) against('Elasticsearch' in natural language mode)) as _score FROM doc_table WHERE match(query) against('database or oceanBase' in natural language mode) AND match(content) against('Elasticsearch' in natural language mode) ORDER BY _score DESC LIMIT 2, 7");
  TestHybridSearchHelp::runtest(table_name, req_str2, res2);
}

TEST_F(TestHybridSearch, hybrid_search_with_minimum_should_match)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "must": [
          {"match": {"product_name": "Aura"}},
          {"match": {"description": "sound"}},
        ],
        "filter": [
          {"term": {"brand": "AudioPhile"}}
        ],
        "must_not": [
          {"match": {"tags": "premium"}}
        ],
        "should": [
          {"match": {"product_name": "System"}},
          {"match": {"description": "Electronics"}}
        ],
        "minimum_should_match" : 2
      }
    },
    "knn" : {
      "field": "vec",
      "k": 5,
      "query_vector": [0.8, 0.1, 0.8, 0.2],
      "similarity" : 0.5
    }
  })";

  ObString table_name("products");
  common::ObString result = (
    "SELECT *, "
    "(ifnull(_fts._keyword_score, 0) + ifnull(_vs._semantic_score, 0)) as _score "
    "FROM "
    "((SELECT __pk_increment, _fts0._score as _keyword_score "
    "FROM "
    "(SELECT /*+ opt_param('hidden_column_visible', 'true') */*, "
    "match(product_name) against('System' in natural language mode) as _word_score_0, "
    "match(description) against('Electronics' in natural language mode) as _word_score_1, "
    "__pk_increment, "
    "(match(product_name) against('Aura' in natural language mode) + "
    "match(description) against('sound' in natural language mode) + "
    "match(product_name) against('System' in natural language mode) + "
    "match(description) against('Electronics' in natural language mode)) as _score "
    "FROM products "
    "WHERE match(product_name) against('Aura' in natural language mode) AND "
    "match(description) against('sound' in natural language mode) AND "
    "brand = 'AudioPhile' AND "
    "match(product_name) against('System' in natural language mode) AND "
    "match(description) against('Electronics' in natural language mode) AND NOT "
    "match(tags) against('premium' in natural language mode)) _fts0 "
    "WHERE (_word_score_0 > 0) + (_word_score_1 > 0) >= 2 "
    "ORDER BY _keyword_score DESC LIMIT 200) _fts full join "
    "(SELECT *, _score as _semantic_score FROM "
    "(SELECT /*+ opt_param('hidden_column_visible', 'true') */*, "
    "l2_distance(vec, '[0.8, 0.1, 0.8, 0.2]') as _distance, "
    "__pk_increment, round(1 / (1 + l2_distance(vec, '[0.8, 0.1, 0.8, 0.2]')), 8) as _score "
    "FROM products "
    "ORDER BY _distance APPROXIMATE LIMIT 5) _vs0 WHERE _vs0._distance <= 0.5) _vs "
    "on _fts.__pk_increment = _vs.__pk_increment) ORDER BY _score DESC LIMIT 10");

  TestHybridSearchHelp::runtest(table_name, req_str, result);

  common::ObString req_str2 = R"({
    "knn" : {
      "field": "vec",
      "k": 5,
      "query_vector": [0.8, 0.1, 0.8, 0.2],
      "similarity" : 0.5
    }
  })";

  common::ObString result2("SELECT * FROM (SELECT *, l2_distance(vec, '[0.8, 0.1, 0.8, 0.2]') as _distance, round(1 / (1 + l2_distance(vec, '[0.8, 0.1, 0.8, 0.2]')), 8) as _score FROM products ORDER BY _distance APPROXIMATE LIMIT 5) _vs0 WHERE _vs0._distance <= 0.5 LIMIT 10");
  TestHybridSearchHelp::runtest(table_name, req_str2, result2);
}

// ============ ES mode testcases ============
TEST_F(TestHybridSearch, should_with_minimum_should_match_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "should": [
          {"match": {"query": "database or oceanBase"}},
          {"match": { "content": "Elasticsearch" }},
          {"term": {"book_id": "1"}},
          {"term": {"age": "2"}}
        ],
        "minimum_should_match": 3
      }
    }
  })";

  common::ObString result(
    "SELECT *, _fts0._score "
    "FROM "
    "(SELECT *, "
    "match(query) against('database or oceanBase' in natural language mode) as _word_score_0, "
    "match(content) against('Elasticsearch' in natural language mode) as _word_score_1, "
    "book_id = '1' as _word_score_2, "
    "age = '2' as _word_score_3, "
    "(match(query) against('database or oceanBase' in natural language mode) + "
    "match(content) against('Elasticsearch' in natural language mode) + "
    "(book_id = '1') + "
    "(age = '2')) as _score "
    "FROM doc_table "
    "WHERE match(query) against('database or oceanBase' in natural language mode) OR "
    "match(content) against('Elasticsearch' in natural language mode) "
    "OR book_id = '1' "
    "OR age = '2') _fts0 "
    "WHERE (_word_score_0 > 0) + (_word_score_1 > 0) + (_word_score_2 > 0) + (_word_score_3 > 0) >= 3 "
    "ORDER BY _score DESC LIMIT 10");

  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, should_with_must_and_minimum_should_match_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "bool": {
        "must": [
          {"match": {"query": "database or oceanBase"}},
          {"match": { "content": "Elasticsearch" }}
        ],
        "should": [
          {"match": {"book_name": "c ++ programming"}},
          {"match": { "content": "Elasticsearch" }}
        ],
        "minimum_should_match": 1
      }
    }
  })";

  common::ObString result(
    "SELECT *, (match(query) against('database or oceanBase' in natural language mode) + "
    "match(content) against('Elasticsearch' in natural language mode) + "
    "match(book_name) against('c ++ programming' in natural language mode) + "
    "match(content) against('Elasticsearch' in natural language mode)) as _score "
    "FROM doc_table WHERE "
    "match(query) against('database or oceanBase' in natural language mode) AND "
    "match(content) against('Elasticsearch' in natural language mode) AND "
    "(match(book_name) against('c ++ programming' in natural language mode) OR "
    "match(content) against('Elasticsearch' in natural language mode)) "
    "ORDER BY _score DESC LIMIT 10");

  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_without_type_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query", "content"]
      }
    }
  })";

  common::ObString result("SELECT *, score() as _score FROM doc_table WHERE MATCH(query^1, content^1, 'database or oceanBase', 'operator=or;boost=1;type=best_fields') ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_best_fields_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query", "content"],
        "type": "best_fields"
      }
    }
  })";

  common::ObString result("SELECT *, score() as _score FROM doc_table WHERE MATCH(query^1, content^1, 'database or oceanBase', 'operator=or;boost=1;type=best_fields') ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_most_fields_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query^12.663798", "content^1", "tags^0"],
        "type": "most_fields"
      }
    }
  })";

  common::ObString result("SELECT *, score() as _score FROM doc_table WHERE MATCH(query^12.663798, content^1, tags^0, 'database or oceanBase', 'operator=or;boost=1;type=most_fields') ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_cross_fields_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query", "content"],
        "type": "cross_fields"
      }
    }
  })";

  common::ObString result("SELECT *, (GREATEST(match(query) against('database' in natural language mode), match(content) against('database' in natural language mode)) + GREATEST(match(query) against('or' in natural language mode), match(content) against('or' in natural language mode)) + GREATEST(match(query) against('oceanBase' in natural language mode), match(content) against('oceanBase' in natural language mode))) as _score FROM doc_table WHERE match(query) against('database' in natural language mode) OR match(query) against('or' in natural language mode) OR match(query) against('oceanBase' in natural language mode) OR match(content) against('database' in natural language mode) OR match(content) against('or' in natural language mode) OR match(content) against('oceanBase' in natural language mode) ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_phrase_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query", "content"],
        "type": "phrase"
      }
    }
  })";

  common::ObString result("SELECT *, GREATEST(match(query) against('database or oceanBase' in match phrase mode), match(content) against('database or oceanBase' in match phrase mode)) as _score FROM doc_table WHERE match(query) against('database or oceanBase' in match phrase mode) OR match(content) against('database or oceanBase' in match phrase mode) ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_best_fields_with_minimum_should_match_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query", "content"],
        "type": "best_fields",
        "minimum_should_match": 2
      }
    }
  })";

  common::ObString result("SELECT *, score() as _score FROM doc_table WHERE MATCH(query^1, content^1, 'database or oceanBase', 'operator=or;boost=1;minimum_should_match=2;type=best_fields') ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_best_fields_simple_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query", "content"],
        "type": "best_fields"
      }
    }
  })";

  common::ObString result("SELECT *, score() as _score FROM doc_table WHERE MATCH(query^1, content^1, 'database or oceanBase', 'operator=or;boost=1;type=best_fields') ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_most_fields_with_minimum_should_match_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query", "content"],
        "type": "most_fields",
        "minimum_should_match": 2
      }
    }
  })";

  common::ObString result("SELECT *, score() as _score FROM doc_table WHERE MATCH(query^1, content^1, 'database or oceanBase', 'operator=or;boost=1;minimum_should_match=2;type=most_fields') ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_phrase_no_weight_with_minimum_should_match_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query", "content"],
        "type": "phrase",
        "minimum_should_match": 2
      }
    }
  })";

  common::ObString result("SELECT *, GREATEST(match(query) against('database or oceanBase' in match phrase mode), match(content) against('database or oceanBase' in match phrase mode)) as _score FROM doc_table WHERE match(query) against('database or oceanBase' in match phrase mode) OR match(content) against('database or oceanBase' in match phrase mode) ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_phrase_with_weight_and_minimum_should_match_and_default_operator_es_mode)
{
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query^2", "content^1.5"],
        "type": "phrase",
        "minimum_should_match": 2,
        "default_operator": "AND"
      }
    }
  })";

  common::ObString result("SELECT *, GREATEST(match(query) against('database or oceanBase' in match phrase mode) * 2, match(content) against('database or oceanBase' in match phrase mode) * 1.5) as _score FROM doc_table WHERE match(query) against('database or oceanBase' in match phrase mode) OR match(content) against('database or oceanBase' in match phrase mode) ORDER BY _score DESC LIMIT 10");
  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_invalid_weights_es_mode)
{
  int ret = OB_SUCCESS;
  common::ObString req_str = R"({
    "query": {
      "query_string": {
        "query": "database or oceanBase",
        "fields": ["query^0", "content^-1", "tags^2.5"],
        "type": "best_fields"
      }
    }
  })";

  ObArenaAllocator tmp_allocator;
  ObString table_name("doc_table");
  ObQueryReqFromJson *req = nullptr;
  ObESQueryParser parser(tmp_allocator, &table_name);
  ret = parser.parse(req_str, req);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST_F(TestHybridSearch, query_string_cross_fields_no_default_operator_es_mode)
{
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "type": "cross_fields",
          "fields": ["title^3", "content^2.5", "tags^1.5"],
          "query": "elasticsearch database tutorial"
        }
      }
    })";

  common::ObString result(
    "SELECT *, "
    "(GREATEST(match(title) against('elasticsearch' in natural language mode) * 3, match(content) against('elasticsearch' in natural language mode) * 2.5, match(tags) against('elasticsearch' in natural language mode) * 1.5) +"
    " GREATEST(match(title) against('database' in natural language mode) * 3, match(content) against('database' in natural language mode) * 2.5, match(tags) against('database' in natural language mode) * 1.5) +"
    " GREATEST(match(title) against('tutorial' in natural language mode) * 3, match(content) against('tutorial' in natural language mode) * 2.5, match(tags) against('tutorial' in natural language mode) * 1.5))"
    " as _score FROM doc_table WHERE"
    " match(title) against('elasticsearch' in natural language mode) OR"
    " match(title) against('database' in natural language mode) OR"
    " match(title) against('tutorial' in natural language mode) OR"
    " match(content) against('elasticsearch' in natural language mode) OR"
    " match(content) against('database' in natural language mode) OR"
    " match(content) against('tutorial' in natural language mode) OR"
    " match(tags) against('elasticsearch' in natural language mode) OR"
    " match(tags) against('database' in natural language mode) OR"
    " match(tags) against('tutorial' in natural language mode)"
    " ORDER BY _score DESC LIMIT 10");

  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

TEST_F(TestHybridSearch, query_string_cross_fields_with_and_operator_es_mode)
{
  common::ObString req_str = R"({
      "query": {
        "query_string": {
          "type": "cross_fields",
          "fields": ["title^3", "content^2.5", "tags^1.5"],
          "query": "elasticsearch database tutorial",
          "default_operator": "AND"
        }
      }
    })";

  common::ObString result(
    "SELECT *, "
    "(GREATEST(match(title) against('elasticsearch' in natural language mode) * 3, match(content) against('elasticsearch' in natural language mode) * 2.5, match(tags) against('elasticsearch' in natural language mode) * 1.5) +"
    " GREATEST(match(title) against('database' in natural language mode) * 3, match(content) against('database' in natural language mode) * 2.5, match(tags) against('database' in natural language mode) * 1.5) +"
    " GREATEST(match(title) against('tutorial' in natural language mode) * 3, match(content) against('tutorial' in natural language mode) * 2.5, match(tags) against('tutorial' in natural language mode) * 1.5))"
    " as _score FROM doc_table WHERE"
    " (match(title) against('elasticsearch' in natural language mode) OR match(content) against('elasticsearch' in natural language mode) OR match(tags) against('elasticsearch' in natural language mode)) AND"
    " (match(title) against('database' in natural language mode) OR match(content) against('database' in natural language mode) OR match(tags) against('database' in natural language mode)) AND"
    " (match(title) against('tutorial' in natural language mode) OR match(content) against('tutorial' in natural language mode) OR match(tags) against('tutorial' in natural language mode))"
    " ORDER BY _score DESC LIMIT 10");

  ObString table_name("doc_table");
  TestHybridSearchHelp::runtest(table_name, req_str, result, false, "", true);
}

} // namespace common
} // namespace oceanbase

enum TestMode {
  ALL_TESTS,
  ES_MODE_ONLY,
  OLD_MODE_ONLY
};

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  TestMode test_mode = ALL_TESTS;
  bool show_help = false;
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--es") == 0 || strcmp(argv[i], "-e") == 0) {
      test_mode = ES_MODE_ONLY;
    } else if (strcmp(argv[i], "--old") == 0 || strcmp(argv[i], "-o") == 0) {
      test_mode = OLD_MODE_ONLY;
    } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
      show_help = true;
    }
  }
  if (show_help) {
    std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --es, -e     Run ES mode tests only" << std::endl;
    std::cout << "  --old, -o    Run old mode tests only (non-ES mode)" << std::endl;
    std::cout << "  --help, -h   Show this help message" << std::endl;
    std::cout << "  (no args)    Run all tests" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << argv[0] << "              # Run all tests" << std::endl;
    std::cout << "  " << argv[0] << " --es         # Run ES mode tests only" << std::endl;
    std::cout << "  " << argv[0] << " -e           # Run ES mode tests only" << std::endl;
    std::cout << "  " << argv[0] << " --old        # Run old mode tests only" << std::endl;
    std::cout << "  " << argv[0] << " -o           # Run old mode tests only" << std::endl;
    return 0;
  }
  switch (test_mode) {
    case ES_MODE_ONLY:
      ::testing::GTEST_FLAG(filter) = "*es_mode*";
      std::cout << "=== Running ES mode tests only ===" << std::endl;
      std::cout << "Filter: " << ::testing::GTEST_FLAG(filter) << std::endl;
      std::cout << "=================================" << std::endl;
      break;
    case OLD_MODE_ONLY:
      ::testing::GTEST_FLAG(filter) = "-*es_mode*";
      std::cout << "=== Running old mode tests only ===" << std::endl;
      std::cout << "Filter: " << ::testing::GTEST_FLAG(filter) << std::endl;
      std::cout << "==================================" << std::endl;
      break;
    case ALL_TESTS:
    default:
      std::cout << "=== Running all tests ===" << std::endl;
      break;
  }
  return RUN_ALL_TESTS();
}