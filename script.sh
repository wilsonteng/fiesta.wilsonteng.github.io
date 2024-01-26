#!/bin/bash

set -eo pipefail

cd /home/wilsonteng/git/proleak.github.io/

/usr/bin/python3 main.py
/usr/bin/python3 read_sql_and_output_json.py

git add assets/data.json
git add assets/date_created.json
git add assets/version_list.json
git commit -m "Update data $(date +%F)"
git push
