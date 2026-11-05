# oteadero: single entry points. `make check` is the project check command
# (format, vet, unit tests, oracle boundary) and is what CI runs.

GO_FILES := $(shell git ls-files '*.go')
PY_TEST_DIRS := oracle scripts

.PHONY: check build criteria refs

check:
	@set -e; \
	if [ -n "$(GO_FILES)" ]; then \
		unformatted="$$(gofmt -l $(GO_FILES))"; \
		if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi; \
	fi; \
	if [ -n "$$(go list ./... 2>/dev/null)" ]; then \
		go vet ./...; \
		go test -count=1 ./...; \
	fi; \
	for d in $(PY_TEST_DIRS); do \
		if [ -n "$$(git ls-files "$$d/test_*.py")" ]; then \
			python3 -m unittest discover -s "$$d" -p 'test_*.py'; \
		fi; \
	done; \
	if [ -f scripts/crit_oracle_imports.py ]; then python3 scripts/crit_oracle_imports.py; fi; \
	echo "check: ok"

build:
	go build -trimpath -o bin/ ./cmd/... ./harness/cmd/...

criteria:
	python3 scripts/run_criteria.py

refs:
	python3 scripts/fetch_refs.py
