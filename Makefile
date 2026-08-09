REGISTRY ?= ghcr.io/kelos-dev
VERSION ?= latest
CONTROLLER_IMAGE ?= $(REGISTRY)/open-actions-controller:$(VERSION)
RUNNER_IMAGE ?= $(REGISTRY)/open-actions-runner:$(VERSION)
FIXTURE_IMAGE ?= $(REGISTRY)/open-actions-fixture:$(VERSION)
IMAGE_DIRS ?= cmd/open-actions-controller cmd/open-actions-runner
TEST_FLAGS ?=
E2E_PROCS ?= 1

LOCALBIN ?= $(shell pwd)/bin
GINKGO ?= $(LOCALBIN)/ginkgo

CONTROLLER_GEN_VERSION ?= v0.20.0
CONTROLLER_GEN := go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
HELM_VERSION ?= v3.20.1
HELM := go run helm.sh/helm/v3/cmd/helm@$(HELM_VERSION)
CHART_DIR := internal/manifests/charts/open-actions
CHART_CRD_DIR := $(CHART_DIR)/templates/crds

.PHONY: all build fmt generate manifests update verify test image test-e2e ginkgo

all: build

build: $(LOCALBIN)
	CGO_ENABLED=0 go build -trimpath -o $(LOCALBIN)/ ./$(or $(WHAT),cmd/...)

fmt:
	gofmt -w $$(find api cmd internal test -name '*.go' -type f)

generate:
	$(CONTROLLER_GEN) object paths=./api/...

manifests:
	mkdir -p $(CHART_CRD_DIR)
	rm -f $(CHART_CRD_DIR)/*.yaml
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=$(CHART_CRD_DIR)

update: fmt generate manifests
	go mod tidy

verify:
	@test -z "$$(gofmt -l $$(find api cmd internal test -name '*.go' -type f))"
	go mod tidy
	git diff --exit-code -- go.mod go.sum
	$(CONTROLLER_GEN) object paths=./api/...
	$(MAKE) manifests
	git diff --exit-code -- api/v1alpha1/zz_generated.deepcopy.go $(CHART_CRD_DIR)
	$(HELM) lint $(CHART_DIR)
	$(HELM) template open-actions $(CHART_DIR) --namespace open-actions-system >/dev/null
	$(HELM) template open-actions $(CHART_DIR) --namespace open-actions-system --values config/e2e/values.yaml >/dev/null
	$(HELM) template open-actions $(CHART_DIR) --namespace open-actions-system --set service.type=NodePort --set service.nodePort=30082 >/dev/null
	@if $(HELM) template open-actions $(CHART_DIR) --namespace open-actions-system --set service.type=ClusterIP --set service.nodePort=30082 >/dev/null 2>&1; then \
		echo "Helm accepted service.nodePort for a ClusterIP Service" >&2; \
		exit 1; \
	fi
	go vet ./...

test:
	CGO_ENABLED=$(if $(findstring -race,$(TEST_FLAGS)),1,$(shell go env CGO_ENABLED)) go test $(TEST_FLAGS) ./...

image:
	@set -e; for dir in $(or $(WHAT),$(IMAGE_DIRS)); do \
		case "$${dir#./}" in \
			cmd/open-actions-controller) target=controller; image="$(CONTROLLER_IMAGE)" ;; \
			cmd/open-actions-runner) target=runner; image="$(RUNNER_IMAGE)" ;; \
			test/fixture/github) target=fixture; image="$(FIXTURE_IMAGE)" ;; \
			*) echo "unsupported image path: $$dir" >&2; exit 1 ;; \
		esac; \
		docker build --target "$$target" -t "$$image" .; \
	done

test-e2e: ginkgo ## Run e2e tests (requires Helm and a cluster).
	$(MAKE) build WHAT=cmd/open-actions
	$(GINKGO) -v --tags=e2e --timeout 30m --procs=$(E2E_PROCS) ./test/e2e/...

ginkgo: $(GINKGO)

$(GINKGO): $(LOCALBIN)
	test -s $(GINKGO) || GOBIN=$(LOCALBIN) go install github.com/onsi/ginkgo/v2/ginkgo

$(LOCALBIN):
	mkdir -p $(LOCALBIN)
