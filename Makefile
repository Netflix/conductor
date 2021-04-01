build:
	export DOCKER_BUILDKIT=1 && \
	export COMPOSE_DOCKER_CLI_BUILD=1 && \
	time docker-compose -f build-dockercompose.yml build --no-cache
