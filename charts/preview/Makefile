OS := $(shell uname)

preview: 
ifeq ($(OS),Darwin)
	sed -i "" -e "s/version:.*/version: $(PREVIEW_VERSION)/" Chart.yaml
	sed -i "" -e "s/version:.*/version: $(PREVIEW_VERSION)/" ../*/Chart.yaml
	sed -i "" -e "s/tag:.*/tag: $(PREVIEW_VERSION)/" values.yaml
else ifeq ($(OS),Linux)
	sed -i -e "s/version:.*/version: $(PREVIEW_VERSION)/" Chart.yaml
	sed -i -e "s/version:.*/version: $(PREVIEW_VERSION)/" ../*/Chart.yaml
	sed -i -e "s|preview_repository_server:.*|repository: $(DOCKER_REGISTRY)\/netflix-conductor|" values.yaml # Please do not edit this line! Managed by customize.sh
	sed -i -e "s|preview_repository_ui:.*|repository: $(DOCKER_REGISTRY)\/netflix-conductor|" values.yaml # Please do not edit this line! Managed by customize.sh

	sed -i -e "s/preview_tag_server:.*/tag: ui-$(PREVIEW_VERSION)/" values.yaml
	sed -i -e "s/preview_tag_ui:.*/tag: server-$(PREVIEW_VERSION)/" values.yaml

else
	echo "platfrom $(OS) not supported to release from"
	exit -1
endif
	echo "  version: $(PREVIEW_VERSION)" >> requirements.yaml
	jx step helm build

print: 
	rm -rf requirements.lock
	helm template .
