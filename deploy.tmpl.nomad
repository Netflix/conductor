job "conductor" {
  region      = "us-west-2"
  datacenters = ["us-west-2"]
  type        = "service"

  // Define which env to deploy service in
  constraint {
    attribute = "${meta.hood}"
    // Options: [ corp | prod | shared ]
    value     = "corp"
  }

  constraint {
    attribute = "${meta.env_type}"
    // Options: [ test | live ]
    value     = "<ENV_TYPE>"
  }

  // Configure the job to do rolling updates
  update {
    stagger      = "15s"
    max_parallel = 1
  }

  group "ui" {

    count = 1

    # Create an individual task (unit of work). This particular
    # task utilizes a Docker container to front a web application.
    task "ui" {
      # Specify the driver to be "docker". Nomad supports
      # multiple drivers.
      driver = "docker"
      # Configuration is specific to each driver.
      config {
        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/conductor:<APP_VERSION>-ui"
        port_map {
          http = 5000
        }
        labels {
          service = "${NOMAD_JOB_NAME}"
        }
        logging {
          type = "syslog"
          config {
            tag = "${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}"
          }
        }
      }

      env {
        WF_SERVER = "http://${NOMAD_JOB_NAME}-server.service.<TLD>:30000/api/"
      }

      # The service block tells Nomad how to register this service
      # with Consul for service discovery and monitoring.
      service {
        name = "${JOB}-${TASK}"
        # This tells Consul to monitor the service on the port
        # labled "http".
        port = "http"

        // Specify the service healthcheck endpoint.
        // Note: if the health check fails, the service
        // WILL NOT get deployed.
        check {
          type     = "http"
          path     = "/"
          interval = "20s"
          timeout  = "2s"
        }
      }
      # Specify the maximum resources required to run the job,
      # include CPU, memory, and bandwidth.
      resources {
        cpu    = 128 # MHz
        memory = 256 # MB

        network {
          mbits = 4
          port "http" {}
        }
      }
    } // END task.ui
  } // END group.conductor

  group "server" {
    count = 1

    task "server" {

      driver = "docker"
      config {
        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/conductor:<APP_VERSION>-server"
        port_map {
          http = 8080
        }
        labels {
          service = "${NOMAD_JOB_NAME}"
        }
        logging {
          type = "syslog"
          config {
            tag = "${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}"
          }
        }
      }

      env {
        STACK = "<TLD>" // Important for redis key
        environment = "<TLD>"

        // Database settings
        db = "redis"
        workflow_dynomite_cluster_name = "${NOMAD_JOB_NAME}"
        workflow_dynomite_cluster_hosts = "${NOMAD_JOB_NAME}-db.service.<TLD>:6379:us-east-1c"

        // Workflow settings
        workflow_namespace_prefix = "${NOMAD_JOB_NAME}.conductor"
        workflow_namespace_queue_prefix = "${NOMAD_JOB_NAME}.conductor.queues"
        decider_sweep_frequency_seconds = "1"

        // Elasticsearch settings
        workflow_elasticsearch_url = "${NOMAD_JOB_NAME}-search.service.<TLD>:9300"
        workflow_elasticsearch_mode = "elasticsearch"
        workflow_elasticsearch_index_name = "${NOMAD_JOB_NAME}.conductor.<TLD>"
        workflow_elasticsearch_cluster_name = "${NOMAD_JOB_NAME}.search"

        // NATS settings
        io_nats_client_url = "nats://events.service.<TLD>:4222"
        conductor_additional_modules = "com.netflix.conductor.contribs.NatsModule"

        // Auth settings
        // TODO: Move client secret to VAULT!
        conductor_auth_url = "https://auth.dmlib.de/v1/tenant/deluxe/auth/token"
        conductor_auth_clientId = "deluxe.conductor"
        conductor_auth_clientSecret = "4ecafd6a-a3ce-45dd-bf05-85f2941413d3"
      }

      service {
        tags = ["urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.dmlib.de/ auth=true"]
        name = "${JOB}-${TASK}"
        port = "http"
        check {
          type     = "http"
          path     = "/"
          interval = "20s"
          timeout  = "2s"
        }
      }

      resources {
        cpu    = 128 # MHz
        memory = 1024 # MB

        network {
          mbits = 2
          port "http" {
            static = 30000
          }
        }
      }
    } // end task
  } // end group

  group "db" {
    count = 1

    task "db" {

      driver = "docker"
      config {
        image = "redis:3.2"
        port_map {
          port6379 = 6379
        }
        labels {
          service = "${NOMAD_JOB_NAME}"
        }
      }

      service {
        name = "${JOB}-${TASK}"
        port = "port6379"

        check {
          type     = "tcp"
          interval = "10s"
          timeout  = "3s"
        }
      }

      resources {
        cpu    = 128 # MHz
        memory = 1024 # MB

        network {
          mbits = 4
          port "port6379" {
            static = 6379
          }
        }
      }
    }
    // end task
  }
  // end group

  group "search" {
    count = 1

    task "search" {

      driver = "docker"
      config {
        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/consul-elasticsearch:0.1.2"
        port_map {
          http = 9200
          tcp = 9300
        }
        labels {
          service = "${NOMAD_JOB_NAME}"
        }
        logging {
          type = "syslog"
          config {
            tag = "${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}"
          }
        }
      }

      env {
        ES_JAVA_OPTS        = "-Xms512m -Xmx512m"
        CONSUL_ADDR         = "consul.service.<TLD>:8500"
        CLUSTER_NAME        = "${NOMAD_JOB_NAME}.${NOMAD_TASK_NAME}"
        PUBLISH_IP          = "${NOMAD_IP_tcp}"
        PUBLISH_PORT        = "${NOMAD_HOST_PORT_tcp}"
        DISCOVERY_MIN_NODES = "1"
        DISCOVERY_HOST      = "${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}"
        DISCOVERY_WAIT      = "30s:60s"
      }

      service {
        name = "${JOB}-${TASK}"
        port = "http"

        check {
          type     = "http"
          path     = "/"
          interval = "10s"
          timeout  = "3s"
        }
      }

      resources {
        cpu    = 128 # MHz
        memory = 1024 # MB

        network {
          mbits = 4
          port "http" {
            static = 9200
          }
          port "tcp" {
            static = 9300
          }
        }
      }
    }
    // end task
  }
  // end group

} // end job
