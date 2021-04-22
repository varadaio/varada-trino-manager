clean:
	rm -rf .env

env: clean
	python3 -m venv ./.env
	.env/bin/pip3 install .[dev]

check:
	.env/bin/flake8 varada_trino_manager/