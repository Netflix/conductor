pipeline {
  agent {
    label "dockerhub-maven"
  }

  environment {
    ORG = 'fsa-streamotion' 
    APP_NAME = 'netflixconductor'
    DOCKER_REGISTRY='kayosportsau'
  }

  stages {
      stage('PR Build + PREVIEW') {
          when {
              branch 'PR-*'
          }
          environment {
              PREVIEW_VERSION = "0.0.0-SNAPSHOT-$BRANCH_NAME-$BUILD_NUMBER"
              PREVIEW_NAMESPACE = "$APP_NAME-$BRANCH_NAME".toLowerCase()
              HELM_RELEASE = "$PREVIEW_NAMESPACE".toLowerCase()
          }
          steps {
              container('maven') {
                  sh "echo **************** PREVIEW_VERSION: $PREVIEW_VERSION , PREVIEW_NAMESPACE: $PREVIEW_NAMESPACE, HELM_RELEASE: $HELM_RELEASE"

                  sh "skaffold version"
                  sh "export VERSION=$PREVIEW_VERSION && skaffold build -f skaffold-server.yaml"
                  sh "export VERSION=$PREVIEW_VERSION && skaffold build -f skaffold-ui.yaml"

                  script {
                      def buildVersion =  readFile "${env.WORKSPACE}/PREVIEW_VERSION"
                      currentBuild.description = "$APP_NAME.$PREVIEW_NAMESPACE"
                  }
              }
          }
      }

      stage('Build Release') {
      when {
        branch 'master'
      }
      steps {
        container('maven') {

        sh "git checkout master"
        sh "git config --global credential.helper store"
        sh "jx step git credentials"

        sh "git clone https://github.com/fsa-streamotion/conductor.git $APP_NAME -b ${params.BUILD_BRANCH}"

        sh "cp skaffold-server.yaml $APP_NAME"
        sh "cp skaffold-ui.yaml $APP_NAME"

          dir("$APP_NAME") {
              // ensure we're not on a detached head

              // so we can retrieve the version in later steps
              // sh "git rev-parse HEAD | cut -c 1-16 > VERSION"
              // sh "jx step tag --version \$(cat VERSION)"
              // sh "skaffold version"
              script {
                GIT_COMMIT_HASH = sh (script: "git log -n 1 --pretty=format:'%H'", returnStdout: true)
                GIT_COMMIT_HASH = GIT_COMMIT_HASH.substring(0,16)
                currentBuild.description = "${params.BUILD_BRANCH} -> $GIT_COMMIT_HASH"
                currentBuild.displayName = "${params.BUILD_BRANCH} -> $GIT_COMMIT_HASH"
              }

              sh "echo commitHash===${GIT_COMMIT_HASH}"
              sh "export VERSION=${GIT_COMMIT_HASH} && skaffold build -f skaffold-server.yaml "
              sh "export VERSION=${GIT_COMMIT_HASH} && skaffold build -f skaffold-ui.yaml "

              // sh "jx step post build --image $DOCKER_REGISTRY/$ORG/$APP_NAME:\$(cat VERSION)"
          }
        }
      }
    }
  }
  post {
        always {
          cleanWs()
        }
  }
}
