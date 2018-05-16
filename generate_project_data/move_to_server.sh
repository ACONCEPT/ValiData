#! /usr/bin/env bash
scp create_tables.py datagen:~/repos/generate_data
scp queries.py datagen:~/repos/generate_data
scp postgres_cursor.py datagen:~/repos/generate_data
scp -r json_data datagen:~/repos/generate_data
scp requirements.txt datagen:~/repos/generate_data
scp test_faker.py datagen:~/repos/generate_data

