#! usr/env/bin bash

for index in {0..4}
do
    python src/node.py config.json $index
done
