pipeline {
    agent {
        label "dockerhub-maven"
    }

    environment {
        APP_NAME = 'netflix-conductor'
        DOCKER_REGISTRY = 'kayosportsau'
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
                    sh "echo $PREVIEW_VERSION > PREVIEW_VERSION"
                    sh "skaffold version"
                    sh "./gradlew build -x test -x :conductor-client:findbugsMain "

                    sh "export VERSION=$PREVIEW_VERSION && skaffold build -f skaffold-server.yaml"
                    sh "export VERSION=$PREVIEW_VERSION && skaffold build -f skaffold-ui.yaml"

                    script {
                        def buildVersion = readFile "${env.WORKSPACE}/PREVIEW_VERSION"
                        currentBuild.description = "${DOCKER_REGISTRY}/netflixconductor:server-${PREVIEW_VERSION}"
                        currentBuild.displayName = "${DOCKER_REGISTRY}/netflixconductor:server-${PREVIEW_VERSION}" + "\n ${DOCKER_REGISTRY}/netflixconductor:ui-${PREVIEW_VERSION}"
                    }

                    dir('charts/preview') {
                      sh "make preview"
                      sh "jx preview --app $APP_NAME --namespace=$PREVIEW_NAMESPACE --dir ../.."
                      sh "make print"
                      sh "sleep 60"
                      sh "kubectl describe pods -n=$PREVIEW_NAMESPACE"
                      sh "kubectl logs -n $PREVIEW_NAMESPACE  deployment/conductor-server --all-containers=true"
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

                    // ensure we're not on a detached head
                    sh "git checkout master"
                    sh "git config --global credential.helper store"
                    sh "jx step git credentials"

                    // so we can retrieve the version in later steps
                    sh "echo \$(jx-release-version) > VERSION"
                    sh "jx step tag --version \$(cat VERSION)"
                    sh "skaffold version"
                    sh "./gradlew build -x test -x :conductor-client:findbugsMain "
                    sh "export VERSION=`cat VERSION` && skaffold build -f skaffold-server.yaml"
                    sh "export VERSION=`cat VERSION` && skaffold build -f skaffold-ui.yaml"

                    script {
                        def buildVersion = readFile "${env.WORKSPACE}/VERSION"
                        currentBuild.description = "$buildVersion"
                        currentBuild.displayName = "$buildVersion"
                    }

                    sh "jx step post build --image kayosportsau/netflixconductor:\$(cat VERSION)"
                }
            }
        }
        stage('Promote to Environments') {
            when {
                branch 'master'
            }
            steps {
                container('maven') {
                    dir("charts/$APP_NAME") {
                        sh "jx step changelog --generate-yaml=false --version v\$(cat ../../VERSION)"

                        // release the helm chart
                        // sh "jx step helm release"
                        // sh "ls -la"
                        sh "echo ************************** HELM ************************** "
                        sh "make print"
                        sh "echo ************************** END HELM ************************** "

                        sh "make release"
                        // promote through all 'Auto' promotion Environments
                        sh "jx promote -b --no-poll=true  --helm-repo-url=$CHART_REPOSITORY --no-poll=true --no-merge=true --no-wait=true --env=staging --version \$(cat ../../VERSION)"
                        sh "jx promote -b --no-poll=true --helm-repo-url=$CHART_REPOSITORY --no-poll=true --no-merge=true --no-wait=true --env=production --version \$(cat ../../VERSION)"
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
