SCHEMA_FILENAME = db.schema

build:
	gcc main.c -o main

run:
	./main $(SCHEMA_FILENAME)

clean-build:
	rm -f main

clean-data:
	rm -f data/*.table

clean: clean-build clean-data

test: build
	python3 test.py

format:
	clang-format -style=Google -i *.c
