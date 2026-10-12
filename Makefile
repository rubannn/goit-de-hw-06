.PHONY: build run clean

build:
	docker compose build

run:
	docker compose up --build

clean:
	docker compose down --rmi all -v
	-rm -rf checkpoints
