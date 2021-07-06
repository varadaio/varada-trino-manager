clean:
	rm -rf .env

env: clean
	python3 -m venv ./.env
	.env/bin/pip3 install .[dev]

static-check:
	.env/bin/pyflakes varada_trino_manager/
	.env/bin/flake8 varada_trino_manager/