ls result/* resolver/* optimizer/* | grep "tmp$" | sed -E 's|(.*)tmp$|cp \1tmp \1result|g' | sh
ls result/* resolver/* optimizer/* | grep "result$" | sed -E 's|(.*)result$|cp \1result ../../../../unittest/sql/memory_usage/\1result|g' | sh
sh analyze_memory_usage.sh