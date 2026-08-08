REGISTRY ?= ghcr.io/kelos-dev
VERSION ?= latest
CONTROLLER_IMAGE ?= $(REGISTRY)/open-actions-controller:$(VERSION)
RUNNER_IMAGE ?= $(REGISTRY)/open-actions-runner:$(VERSION)
FIXTURE_IMAGE ?= $(REGISTRY)/open-actions-fixture:$(VERSION)
TEST_FLAGS ?=
E2E_PROCS ?= 1

LOCALBIN ?= $(shell pwd)/bin
GINKGO ?= $(LOCALBIN)/ginkgo

CONTROLLER_GEN_VERSION ?= v0.20.0
CONTROLLER_GEN := go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
KUSTOMIZE_VERSION ?= v5.7.1
KUSTOMIZE := go run sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION)

.PHONY: all build build-controller build-runner fmt generate manifests update verify test image image-controller image-runner image-fixture image-e2e install test-e2e ginkgo

all: build

build: build-controller build-runner

build-controller:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -o bin/open-actions-controller ./cmd/open-actions-controller

build-runner:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -o bin/open-actions-runner ./cmd/open-actions-runner

fmt:
	gofmt -w $$(find api cmd internal test -name '*.go' -type f)

generate:
	$(CONTROLLER_GEN) object paths=./api/...

manifests:
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd/bases

update: fmt generate manifests
	go mod tidy

verify:
	@test -z "$$(gofmt -l $$(find api cmd internal test -name '*.go' -type f))"
	go mod tidy
	git diff --exit-code -- go.mod go.sum
	$(CONTROLLER_GEN) object paths=./api/...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd/bases
	git diff --exit-code -- api/v1alpha1/zz_generated.deepcopy.go config/crd/bases
	$(KUSTOMIZE) build config/default >/dev/null
	$(KUSTOMIZE) build config/e2e >/dev/null
	go vet ./...

test:
	CGO_ENABLED=$(if $(findstring -race,$(TEST_FLAGS)),1,$(shell go env CGO_ENABLED)) go test $(TEST_FLAGS) ./...

image: image-controller image-runner

image-controller:
	docker build --target controller -t $(CONTROLLER_IMAGE) .

image-runner:
	docker build --target runner -t $(RUNNER_IMAGE) .

image-fixture:
	docker build --target fixture -t $(FIXTURE_IMAGE) .

image-e2e: image image-fixture

install:
	kubectl apply -k config/default

test-e2e: ginkgo ## Run e2e tests (requires cluster).
	$(GINKGO) -v --tags=e2e --timeout 30m --procs=$(E2E_PROCS) ./test/e2e/...

ginkgo: $(GINKGO)

$(GINKGO): $(LOCALBIN)
	test -s $(GINKGO) || GOBIN=$(LOCALBIN) go install github.com/onsi/ginkgo/v2/ginkgo

$(LOCALBIN):
	mkdir -p $(LOCALBIN)
