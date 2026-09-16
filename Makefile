# StreamCheck -- static detection of exactly-once defects in payment pipelines.
#
# `make reproduce` is the clean-room path: broker up, every experiment, figures
# regenerated from the raw results, paper rebuilt, claims verified.

PY  ?= ./.venv/bin/python
PIP ?= ./.venv/bin/pip

.PHONY: help setup up down experiments fast figures paper test verify reproduce clean distclean

help:
	@grep -E '^[a-z-]+:.*?##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | column -t -s "$$(printf '\t')"

.venv:
	python3 -m venv .venv
	$(PIP) install -q --upgrade pip
	$(PIP) install -q -r requirements.txt

setup: .venv ## create the virtualenv

up: ## start the Kafka broker used by the execution arm
	docker compose up -d
	@for i in $$(seq 1 60); do \
	  docker exec streamcheck-broker /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break; \
	  sleep 1; \
	done
	@echo "broker ready on localhost:19092"

down: ## stop the broker
	docker compose down

fast: setup ## everything except the live execution arm
	$(PY) experiments/e1_detection.py
	$(PY) experiments/e2_witness.py
	$(PY) experiments/e4_artifacts.py
	$(PY) experiments/e5_random.py
	$(PY) experiments/e6_scale.py
	$(PY) experiments/e7_prevalence.py

experiments: fast up ## every experiment, including the ones needing a broker
	$(PY) experiments/e8_broker.py
	$(PY) experiments/e9_example.py
	$(PY) experiments/e3_live.py

figures: ## regenerate every figure from results/
	$(PY) figures/make_figures.py

paper: figures ## build the PDF
	cd paper && latexmk -pdf -interaction=nonstopmode streamcheck.tex

test: setup ## unit tests and sanity checks
	$(PY) -m pytest tests/

verify: ## check every number in the paper against results/
	$(PY) scripts/verify_claims.py

reproduce: distclean setup up experiments figures paper test verify ## clean-room end to end
	@echo
	@echo "reproduction complete -- results/ figures/ paper/streamcheck.pdf"

clean:
	rm -rf paper/*.aux paper/*.log paper/*.out paper/*.fls paper/*.fdb_latexmk
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

distclean: clean
	rm -f results/*.json figures/*.pdf paper/streamcheck.pdf
