pipeline {
    agent {
        label "jenkins-gradle"
    }

    environment {
        ORG = 'fsa-streamotion'
        APP_NAME = 'netflixconductor'
        DOCKER_REGISTRY = 'kayosportsau'
    }

    stages {
        stage('PR Build + PREVIEW') {
            when {
                branch 'feature/*'
            }
            environment {
                PREVIEW_VERSION = "0.0.0-SNAPSHOT-$BRANCH_NAME-$BUILD_NUMBER"
                PREVIEW_NAMESPACE = "$APP_NAME-$BRANCH_NAME".toLowerCase()
                HELM_RELEASE = "$PREVIEW_NAMESPACE".toLowerCase()
            }
            steps {
                container('gradle') {
                    sh "echo **************** PREVIEW_VERSION: $PREVIEW_VERSION , PREVIEW_NAMESPACE: $PREVIEW_NAMESPACE, HELM_RELEASE: $HELM_RELEASE"
                    sh "echo $PREVIEW_VERSION > PREVIEW_VERSION"
                    sh "gradle build -x test -x :conductor-client:findbugsMain "
                    // sh "skaffold version"
                    sh "export VERSION=$PREVIEW_VERSION && skaffold build -f skaffold-server.yaml"
                    sh "export VERSION=$PREVIEW_VERSION && skaffold build -f skaffold-ui.yaml"

                    script {
                        def buildVersion = readFile "${env.WORKSPACE}/PREVIEW_VERSION"
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

                    // ensure we're not on a detached head
                    sh "git checkout master"
                    sh "git config --global credential.helper store"
                    sh "jx step git credentials"

                    // so we can retrieve the version in later steps
                    sh "echo \$(jx-release-version) > VERSION"
                    sh "jx step tag --version \$(cat VERSION)"
                    sh "skaffold version"
                    sh "export VERSION=`cat VERSION` && skaffold build -f skaffold-server.yaml"
                    sh "export VERSION=`cat VERSION` && skaffold build -f skaffold-ui.yaml"

                    script {
                        def buildVersion = readFile "${env.WORKSPACE}/VERSION"
                        currentBuild.description = "$buildVersion"
                        currentBuild.displayName = "$buildVersion"
                    }

//            sh "jx step post build --image $DOCKER_REGISTRY/$ORG/$APP_NAME:\$(cat VERSION)"
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
