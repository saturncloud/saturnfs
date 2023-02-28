SHELL=/bin/bash -O globstar
CONDA_ACTIVATE = source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate

.PHONY: conda-update
conda-update:
	($(CONDA_ACTIVATE) base && (conda remove --all -n saturnfs -y || true) && conda create -n saturnfs -y && $(CONDA_ACTIVATE) saturnfs)
	conda env update -n saturnfs --file environment.yaml
	conda env update -n saturnfs --file environment.test.yaml

.PHONY: mypy
mypy:
	mypy --config-file mypy.ini ./