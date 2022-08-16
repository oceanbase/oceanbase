
let $cluster_is_ob = 1;
let $cluster_is_mysql = 0;
let $collation_count = query_get_value(select count(1) as cnt from information_schema.COLLATIONS, cnt, 1);
if ($collation_count > 100) {
  let $cluster_is_ob = 0;
  let $cluster_is_mysql = 1;
}

