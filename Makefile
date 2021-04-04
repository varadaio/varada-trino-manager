clean:
	rm -rf .env

env: clean
	python3 -m venv ./.env
	.env/bin/pip3 install .[dev]
