venv:
	rm -rf .venv
	python -m venv .venv
	source .venv/bin/activate; \
	pip install --upgrade pip setuptools wheel; \
	pip install -r dev-requirements.txt -e .
