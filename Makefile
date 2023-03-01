SHELL=/bin/bash -O globstar
CONDA_ACTIVATE = source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate

.PHONY: conda-update
conda-update:
	($(CONDA_ACTIVATE) base && (conda remove --all -n saturnfs -y || true) && conda create -n saturnfs -y && $(CONDA_ACTIVATE) saturnfs)
	conda env update -n saturnfs --file environment.yaml
	conda env update -n saturnfs --file environment.test.yaml

.PHONY: lint
lint: black pylint mypy

.PHONY: black
black:
	@echo -e '\nChecking formatting with Black/ISort...'
	# If you make changes here, also edit .pre-commit-config.yaml to match
	black --check --diff ./
	isort --check ./

.PHONY: mypy
mypy:
	@echo -e '\nLinting with MyPy...'
	mypy --config-file mypy.ini ./saturnfs/

.PHONY: pylint
pylint:
	@echo -e '\nLinting with PyLint...'
	pylint ./saturnfs/

.PHONY: format
format:
	@echo -e '\nFormatting with Black/ISort...'
	black --line-length 100 --exclude '/(\.vscode)/' ./
	isort ./
