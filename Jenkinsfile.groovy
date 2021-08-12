pipeline {
    agent {
        label "streamotion-docker-in-docker"
    }

    environment {
        GRADLE_OPTS = '-Xmx3g -XX:MaxMetaspaceSize=512m -Dorg.gradle.daemon=false'
        container = 'docker'
    }



    stages {
        stage('Integration-Test') {
            when {
                branch 'PR-*'
            }
            environment {
                /*in the dind container, jenkins-k8s plugin mounts the default /var/run/docker.sock to the k8s node host docker daemon port, lets not mess with that
                * instead the DOCKER im gonna run in this POD will use /var/run/dind.sock to publish docker daemon api*/
                DOCKER_HOST = "unix:///var/run/dind.sock"
                KUBECONFIG = "$HOME/.kube/config"

                PREVIEW_VERSION = "0.0.0-SNAPSHOT-$PREVIEW_NAMESPACE-$BUILD_NUMBER"
            }
            steps {
                container('dind') {
                    sh "env"
                    sh "whoami"
                    sh "echo $HOME"
                    retry(3) { //flacky docker pulls
                        sh 'kill -SIGTERM "$(pgrep dockerd)" || echo "NO dockerd found"'
                        sh "sleep 5"
                        sh "/usr/bin/dockerd -H unix:///var/run/dind.sock &"
                        sh 'sleep 15' //wait for docker to be ready
                        sh "docker ps"
                        sh 'rm -rf $HOME/.kube/config | echo "No previous Kubeconfig found"'
                    }

                    sh "sleep 100000000"
                    sh "docker ps"
                }
            }
            post {

                failure {
                    //kill the docker engine
                    sh "echo FAILED!!! Pls see POD logs"
                    sh "sleep 6000"
//                    sh 'kill -SIGTERM "$(pgrep dockerd)" || echo "dockerd not running"'
                }


            }

        }

    }


    // stages {
    //     stage('Setup Docker in Docker') {
    //         steps {
    //             container('maven') {
    //                 sh '''yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo && \\
    //                   yum update -y && \\
    //                   yum install -y docker-ce docker-ce-cli containerd.io'''

    //                 sh '''update-alternatives --set iptables  /usr/sbin/iptables-legacy || true && \\
    //                   update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy || true && \\
    //                   update-alternatives --set arptables /usr/sbin/arptables-legacy || true'''

    //                 sh '''set -x && \\
    //                   groupadd --system dockremap && \\
    //                   adduser --system -g dockremap dockremap && \\
    //                   echo 'dockremap:165536:65536' >> /etc/subuid && \\
    //                   echo 'dockremap:165536:65536' >> /etc/subgid'''
    //             }
    //         }
    //     }

    //     stage('Compile & Test') {
    //         steps {
    //             container('maven') {
    //                 sh "./gradlew build --info --stacktrace"
    //             }
    //         }
    //     }
    // }

    // post {
    //     always {
    //         container('maven') {
    //             junit(
    //                     testResults: '**/build/test-results/**/TEST-*.xml',
    //                     allowEmptyResults: true
    //             )
    //         }
    //         cleanWs()
    //     }
    // }

//    environment {
//        APP_NAME = 'netflix-conductor'
//        DOCKER_REGISTRY = 'kayosportsau'
//        ORG = 'fsa-streamotion'
//
//    }
//
//    stages {
//        stage('PR Build + PREVIEW') {
//            when {
//                branch 'PR-*'
//            }
//            environment {
//                PREVIEW_VERSION = "0.0.0-SNAPSHOT-$BRANCH_NAME-$BUILD_NUMBER"
//                PREVIEW_NAMESPACE = "$APP_NAME-$BRANCH_NAME".toLowerCase()
//                HELM_RELEASE = "$PREVIEW_NAMESPACE".toLowerCase()
//                CONDUCTOR_API = "http://conductor-server.${PREVIEW_NAMESPACE}.jx.gitops-prod.streamotion.gitops.com.au/api"
//                // When adjust these, mind the resources given to preview. A eco and slow setup might just red the build.
//                EXPECT_WORKFLOW_COUNT                = "200"
//                EXPECT_WORKFLOW_CREATION_TIME_SECS   = "200"
//                EXPECT_WORKFLOW_COMPLETION_TIME_SECS = "1000"
//            }
//            steps {
//                container('maven') {
//                    sh "echo **************** PREVIEW_VERSION: $PREVIEW_VERSION , PREVIEW_NAMESPACE: $PREVIEW_NAMESPACE, HELM_RELEASE: $HELM_RELEASE"
//                    sh "echo $PREVIEW_VERSION > PREVIEW_VERSION"
//                    sh "cp -v server/src/main/resources/kitchensinkpreview.json server/src/main/resources/kitchensink.json"
//                    sh "skaffold version && ./gradlew build -w -x test -x :conductor-client:findbugsMain "
//                    sh "export VERSION=$PREVIEW_VERSION && skaffold build -f skaffold-server.yaml && skaffold build -f skaffold-ui.yaml"
//                    // Comment out skaffold build all
//                    // sh "export VERSION=$PREVIEW_VERSION && skaffold build -f skaffold-all.yaml"
//
//                    script {
//                        def buildVersion = readFile "${env.WORKSPACE}/PREVIEW_VERSION"
//                        currentBuild.description = "${DOCKER_REGISTRY}/netflixconductor:server-${PREVIEW_VERSION}"
//                        currentBuild.displayName = "${DOCKER_REGISTRY}/netflixconductor:server-${PREVIEW_VERSION}" + "\n ${DOCKER_REGISTRY}/netflixconductor:ui-${PREVIEW_VERSION}"
//                    }
//
//                    dir('charts') {
//                        sh "./preview.sh"
//                    }
//
//                    dir('charts/preview') {
//                      sh "make preview && jx preview --app $APP_NAME --namespace=$PREVIEW_NAMESPACE --dir ../.."
//                      // try to sleep through the rolling deployment, we no want to hit the old pod
//                      sh "make print && sleep 360"
//                      sh "kubectl describe pods -n $PREVIEW_NAMESPACE"
//                      sh "echo '************************************************\n' && cat values.yaml"
//                      sh "printenv | sort && kubectl get pods -n $PREVIEW_NAMESPACE"
//                    }
//                }
//            }
//        }
//
//        stage('Component Test') {
//            when {
//                branch 'PR-*'
//            }
//            environment {
//                PREVIEW_NAMESPACE = "$APP_NAME-$BRANCH_NAME".toLowerCase()
//                CONDUCTOR_API = "http://conductor-server.${PREVIEW_NAMESPACE}.jx.gitops-prod.streamotion.gitops.com.au/api"
//            }
//            steps {
//                container('maven') {
//                    dir('client/python') {
//                        // ///DO some loadtest:
//                        // 1. make sure conductor preview up & running
//                        // 2. upload some workflow & tasks in conductor server
//                        // 3. simulate some producers to generate X amount of workflow
//                        // 4. simulate some workers to consume all the tasks (do concurrency)
//                        // 5. assert we dont have any hanging tasks and all workflows completed
//                        // sh "python kitchensink_workers.py > worker.log &"
//                        // sh "python load_test_kitchen_sink.py"
//                        // todo: fix these
//                    }
//                }
//            }
//        }
//
//        stage('Build Release') {
//            when {
//                branch 'master'
//            }
//            steps {
//                container('maven') {
//
//                    // ensure we're not on a detached head
//                    sh "git checkout master"
//                    sh "git config --global credential.helper store"
//                    sh "jx step git credentials"
//
//                    // so we can retrieve the version in later steps
//                    sh "echo \$(jx-release-version) > VERSION"
//                    sh "jx step tag --version \$(cat VERSION)"
//                    sh "skaffold version"
//                    sh "./gradlew build -x test -x :conductor-client:findbugsMain "
//                    sh '''
//                      aws sts assume-role-with-web-identity \
//                      --role-arn $AWS_ROLE_ARN \
//                      --role-session-name ecraccess \
//                      --web-identity-token file://\$AWS_WEB_IDENTITY_TOKEN_FILE \
//                      --duration-seconds 900 > /tmp/ecr-access.txt
//                      '''
//                    sh '''
//                      set +x
//                      export VERSION=$(cat VERSION) && export AWS_ACCESS_KEY_ID=\$(cat /tmp/ecr-access.txt | jq -r '.Credentials.AccessKeyId') \
//                      && export AWS_SECRET_ACCESS_KEY=\$(cat /tmp/ecr-access.txt | jq -r '.Credentials.SecretAccessKey')\
//                      && export AWS_SESSION_TOKEN=\$(cat /tmp/ecr-access.txt | jq -r '.Credentials.SessionToken') && set -x && skaffold version \
//                      && skaffold build -f skaffold-server.yaml \
//                      && skaffold build -f skaffold-ui.yaml
//                    '''
//                    // skaffold-all.yaml builds standalone ui + server image, only be used locally or dev
//                    // commenting it out for the moment because there's a checksum ERROR
//                    //sh "export VERSION=`cat VERSION` && skaffold build -f skaffold-all.yaml"
//                    script {
//                        def buildVersion = readFile "${env.WORKSPACE}/VERSION"
//                        currentBuild.description = "$buildVersion"
//                        currentBuild.displayName = "$buildVersion"
//                    }
//
//                    sh "jx step post build --image kayosportsau/netflixconductor:\$(cat VERSION)"
//                }
//            }
//        }
//        stage('Promote to Environments') {
//            when {
//                branch 'master'
//            }
//            steps {
//                container('maven') {
//                    dir("charts/$APP_NAME") {
//                        sh "jx step changelog --generate-yaml=false --version v\$(cat ../../VERSION)"
//
//                        // release the helm chart
//                        // sh "jx step helm release"
//                        // sh "ls -la"
//                        sh "export VERSION=`cat ../../VERSION` && sed -i 's|2019.0.3|'\$VERSION'|g' values.yaml"
//                        sh "make release && make print"
//                        // promote through all 'Auto' promotion Environments
//                        //sh "jx promote -b --no-poll=true  --helm-repo-url=$CHART_REPOSITORY --no-poll=true --no-merge=true --no-wait=true --env=staging --version \$(cat ../../VERSION)"
//                        //sh "jx promote -b --no-poll=true --helm-repo-url=$CHART_REPOSITORY --no-poll=true --no-merge=true --no-wait=true --env=production --version \$(cat ../../VERSION)"
//                        sh "jx promote -b --no-poll=true  --helm-repo-url=$CHART_REPOSITORY --no-poll=true --no-merge=true --no-wait=true --env=conductor-staging --version \$(cat ../../VERSION)"
//                        sh "jx promote -b --no-poll=true  --helm-repo-url=$CHART_REPOSITORY --no-poll=true --no-merge=true --no-wait=true --env=conductor-production --version \$(cat ../../VERSION)"
//                    }
//                }
//            }
//        }
//    }
//    post {
//        always {
//            cleanWs()
//        }
//    }
}
