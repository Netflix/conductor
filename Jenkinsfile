@Library('ZupSharedLibs@v_orchestrator') _

node {

  try {

    buildWithCompose {
      composeFileName = "docker_zupme/docker-compose-ci.yml"
      composeService = "zupme-conductor"
      composeProjectName = "zupme-conductor"
    }

    buildDockerContainer {
      dockerRepositoryName = "zupme-conductor_server"
      dockerFileLocation = "./docker_zupme"
      team = "gateway"
      dockerRegistryGroup = "ZUPME"
    }

    deployDockerService {
      dockerRepositoryName = "zupme-conductor_server"
      dockerSwarmStack = "zupme-conductor_server"
      dockerService = "api"
      team = "gateway"
      dockerRegistryGroup = "ZUPME"
      dockerSwarmGroup = "ZUPME_QA"
    }

    deployDockerServiceK8s {
      microservice = "zupme-conductor_server"
      dockerk8sGroup = "ZUPME"
    }

    deleteReleaseBranch {}

  } catch (e) {

    notifyBuildStatus {
      buildStatus = "FAILED"
    }
    throw e

  }

}