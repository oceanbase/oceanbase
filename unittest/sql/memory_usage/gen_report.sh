for t in 'memory usage' 'max memory usage'; do
    echo "# ${t}"
    echo "| suite | query | parser | resolve | transform | optimize | total |"
    echo "| --- | --- | --- | --- | --- | --- | --- |"
    for p in tpch tpcds big_and_or big_union many_partition big_insert; do
        sed -n "/^${t}$/,/^$/p" result/*${p}*.result  | grep '^|' | grep -Ev '^\|\s*query\s*\||---' | sed "s/^| /| ${p} | /" | tr -d ','
    done
    echo
done
