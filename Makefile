DB_FILENAME = test.db

build:
	gcc main.c -o main

run:
	./main $(DB_FILENAME)

clean:
	rm -f main $(DB_FILENAME)

test: build
	python3 test.py

format:
	clang-format -style=Google -i *.c
