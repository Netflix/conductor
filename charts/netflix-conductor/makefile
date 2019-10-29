RED    := \033[0;31m
GREEN  := \033[0;32m
CYAN   := \033[0;36m
YELLOW := \033[1;33m
NC     := \033[0m # No Color

CHART_REPO := https://artifactory.cluster.foxsports-gitops-prod.com.au/artifactory/helm/
CHART_REPO_KEY := helm-virtual
CURRENT=$(pwd)
NAME := netflixconductor
NAMESPACE := ${NAME}-test
OS := $(shell uname)
RELEASE_FILENAME := $(NAME)-$(shell sed -n 's/^version: //p' Chart.yaml).tgz
RELEASE_VERSION := $(shell cat VERSION)

usage:
	@printf "${YELLOW}make build	${GREEN}# Pull chart dependencies listed in requirements.yaml ${NC}\n"
	@printf "${YELLOW}make install	${GREEN}# Install helm chart ${NC}\n"
	@printf "${YELLOW}make upgrade	${GREEN}# Upgrade helm chart ${NC}\n"
	@printf "${YELLOW}make delete	${GREEN}# Delete helm chart ${NC}\n"
	@printf "${YELLOW}make clean	${GREEN}# Delete chart dependencies ${NC}\n"
	@printf "${YELLOW}make release	${GREEN}# Package and push chart to chart repo ${NC}\n"
	@printf "${YELLOW}make tag  	${GREEN}# Git tag and push release version ${NC}\n"

build: clean
	rm -rf requirements.lock
	@helm repo add $(CHART_REPO_KEY) $(CHART_REPO)
	helm dependency build
	helm lint

install: clean build
	helm install . --name ${NAME} --namespace ${NAMESPACE}

upgrade: clean build
	helm upgrade --install ${NAME} .

delete:
	helm delete --purge ${NAME}

clean:
	rm -rf charts
	rm -rf ${NAME}*.tgz

release: clean
	@test -z $(CHARTMUSEUM_CREDS) && printf "${RED}Environment variable ${YELLOW}CHARTMUSEUM_CREDS ${RED}not found ${NC}\n" && exit 1 || exit 0
	helm dependency build
	helm lint
	helm init --client-only
	helm package .
	@echo Pushing Chart to Helm repo...
	@curl --fail -u $(CHARTMUSEUM_CREDS) -X PUT $(CHART_REPO)/$(RELEASE_FILENAME) -T $(RELEASE_FILENAME)
	@rm -rf ${NAME}*.tgz

tag:
ifeq ($(OS),Darwin)
	sed -i "" -e "s/version:.*/version: $(RELEASE_VERSION)/" Chart.yaml
	sed -i "" -e "s/tag: .*/tag: $(RELEASE_VERSION)/" values.yaml
else ifeq ($(OS),Linux)
	sed -i -e "s/version:.*/version: $(RELEASE_VERSION)/" Chart.yaml
	sed -i -e "s|repository: .*|repository: $(DOCKER_REGISTRY)\/dat\/$(NAME)|" values.yaml
	sed -i -e "s/tag: .*/tag: $(RELEASE_VERSION)/" values.yaml
else
	echo "platfrom $(OS) not supported to release from"
	exit -1
endif
	git add --all
	git commit -m "release $(RELEASE_VERSION)" --allow-empty # if first release then no verion update is performed
	git tag -fa v$(RELEASE_VERSION) -m "Release version $(RELEASE_VERSION)"
	git push origin v$(RELEASE_VERSION)

delete-wait: delete
	@echo "Waiting for helm delete to complete.\c"
	@while [ `kubectl -n ${NAMESPACE} get po | wc -l |awk '{ print $1 }'` -gt 1 ]; \
	do \
		echo ".\c"; \
		sleep 1; \
	done;
	@echo "Delete completed"

inspect-template:
	helm install --dry-run --debug .|less

print:
	helm template .