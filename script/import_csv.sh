#!/bin/bash
set -euC

# フラグファイルがなければなにもしない
if [[ "$(ls -1 /import/admin_import/do_import_flag | wc -l)" == "0" ]]; then
    echo "import csv skipped."
    return
fi

# データを全削除
echo "delete database started."
rm -rf /var/lib/neo4j/data/databases
rm -rf /var/lib/neo4j/data/transactions
echo "delete database finished."

# CSVインポート
echo "importing csv started."
/var/lib/neo4j/bin/neo4j-admin import \
  --id-type=INTEGER \
  --nodes="/import/admin_import/nodes_header.csv,/import/admin_import/nodes_data_[0-9]+.csv" \
  --relationships="/import/admin_import/relations_header.csv,/import/admin_import/relations_data_[0-9]+.csv"
echo "importing csv finished."

