# StreamCheck -- static detection of exactly-once defects in payment pipelines.
#
# `make reproduce` is the clean-room path: broker up, every experiment, figures
# regenerated from the raw results, paper rebuilt, claims verified.

PY  ?= ./.venv/bin/python
PIP ?= ./.venv/bin/pip

.PHONY: help preflight setup up down experiments fast figures tables paper test arxiv verify reproduce clean distclean

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
	  docker exec "$$(docker compose ps -q broker)" \
	    /opt/kafka/bin/kafka-topics.sh \
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
# E5 prices a fault-injection trial against a real kill-and-restart, which only
# E3 measures. `fast` necessarily runs E5 first, so on a clean tree E5 fell
# back to its estimate and `make reproduce` disagreed with the committed
# results. Re-running it here makes the artifact deterministic and measured.
	$(PY) experiments/e5_random.py

tables: ## regenerate the generated table rows from results/
	$(PY) scripts/make_live_table.py

figures: tables ## regenerate every figure and table from results/
	$(PY) figures/make_figures.py

paper: figures ## build the PDF
	cd paper && latexmk -pdf -interaction=nonstopmode streamcheck.tex

test: setup ## unit tests and sanity checks
	$(PY) -m pytest tests/

arxiv: paper ## build and verify the self-contained arXiv package
	$(PY) scripts/make_arxiv.py

verify: ## check every number in the paper against results/
	$(PY) scripts/make_live_table.py --check
	$(PY) scripts/verify_claims.py
	$(PY) scripts/check_provenance.py
	$(PY) scripts/check_prose_numbers.py

preflight: ## check every tool the reproduction needs, before anything is deleted
	python3 scripts/preflight.py

# `preflight` comes first and `distclean` second, on purpose. This target used
# to lead with distclean, so a reader missing Docker or a TeX distribution lost
# every committed result file and found out twenty minutes later.
#
# Budget about 25 minutes. Most of it is E3: 48 live executions that really
# kill a process and wait for the consumer group to evict it.
reproduce: preflight distclean setup up experiments figures paper test verify ## clean-room end to end (~25 min)
	@echo
	@echo "reproduction complete -- results/ figures/ paper/streamcheck.pdf"

clean:
	rm -rf paper/*.aux paper/*.log paper/*.out paper/*.fls paper/*.fdb_latexmk
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

distclean: clean
	rm -f results/*.json figures/*.pdf paper/streamcheck.pdf
