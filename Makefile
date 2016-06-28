#!/usr/bin/make -f

export PYTHONPATH=$(shell pwd)
PYTEST:=env PYTHONPATH=$(shell pwd) python /home/smurf/nnis/devenv/lib/python3.4/site-packages/pytest.py

test: test.cfg
	@rm -f test.log
	$(PYTEST) -x
t: test.cfg
	@rm -f test.log
	$(PYTEST) -s -x -v
# --cov-report term-missing --cov-config .coveragerc --cov=qbroker.unit --cov=qbroker.proto --assert=plain

test.cfg:
	@cp test.cfg.sample $@
	@echo "Warning: copied test.cfg.sample to $@" >&2

update:
	@sh utils/update_boilerplate

