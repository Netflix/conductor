APP_VERSION = $$(mold -app-version)
export APP_VERSION

.PHONY: build

clean:
	@-docker rm $$(docker ps -aq) 2> /dev/null || true
	@-docker rmi $$(docker images | grep "<n" | awk '{print $$3}') 2> /dev/null || true
	@-docker volume rm $$(docker volume ls -qf 'dangling=true') 2> /dev/null || true
	@-docker network rm $$(docker network ls -q) 2> /dev/null || true

build: clean login
	mold

login:
	@eval $$(aws ecr get-login --no-include-email --region us-west-2)

up:
	@docker-compose -f docker/docker-compose.yaml up -d

down:
	@docker-compose -f docker/docker-compose.yaml down

.convert-deploy-template:
	@if test "$(APP_VERSION)" = "" ; then APP_VERSION=$$(mold -app-version); fi
	@sed -e "s/<APP_VERSION>/${APP_VERSION}/g" -e "s/<ENV_TYPE>/${ENV_TYPE}/g" -e "s#<SERVICE_TAG>#urlprefix-${NAME}.dmlib.${TLDEXT}/#g" -e "s/<TLD>/owf-dev/g" deploy.tmpl.nomad > deploy.nomad

plan: .convert-deploy-template
	-nomad plan deploy.nomad
