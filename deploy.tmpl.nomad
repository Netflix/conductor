job "conductor" {
  type        = "service"
  region      = "us-west-2"
  datacenters = ["us-west-2"]

  meta {
    service-class = "platform"
  }

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

  update {
    stagger      = "15s"
    max_parallel = 1
  }

  group "ui" {
    count = 3

    constraint {
      operator  = "distinct_property"
      attribute = "${attr.platform.aws.placement.availability-zone}"
    }

    task "ui" {
      driver = "docker"
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
        WF_SERVICE = "${NOMAD_JOB_NAME}-server.service.<TLD>"
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
        memory = 128 # MB
        network {
          mbits = 4
          port "http" {}
        }
      }
    } // end ui task
  } // end ui group

  group "server" {
    count = 3

    constraint {
      operator  = "distinct_property"
      attribute = "${attr.platform.aws.placement.availability-zone}"
    }

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
        TLD   = "<TLD>"
        STACK = "<ENV_TYPE>"

        // Database settings
        db = "elasticsearch"

        // Workflow settings
        workflow_failure_expandInline   = "false"
        decider_sweep_frequency_seconds = "5"
        workflow_event_processor_refresh_seconds = "5"

        // Elasticsearch settings
        workflow_elasticsearch_mode = "elasticsearch"
        workflow_elasticsearch_service = "${NOMAD_JOB_NAME}-search-tcp.service.<TLD>"
        workflow_elasticsearch_cluster_name = "${NOMAD_JOB_NAME}.search"
        workflow_elasticsearch_initial_sleep_seconds = "30"

        // NATS settings
        io_nats_client_url = "nats://events.service.<TLD>:4222"
        conductor_additional_modules = "com.netflix.conductor.contribs.NatsModule"

        // Auth settings. TODO: Move client secret to VAULT!
        conductor_auth_url = "https://auth.dmlib.de/v1/tenant/deluxe/auth/token"
        conductor_auth_clientId = "deluxe.conductor"
        conductor_auth_clientSecret = "4ecafd6a-a3ce-45dd-bf05-85f2941413d3"

        // Exclude demo workflows
        loadSample = "false"
      }
      service {
        tags = ["urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.dmlib.<DM_TLD>/ auth=true"]
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
        cpu    = 128  # MHz
        memory = 2048 # MB
        network {
          mbits = 2
          port "http" {}
        }
      }
    } // end server task
  } // end server group

  group "search" {
    count = 3

    constraint {
      operator  = "distinct_property"
      attribute = "${attr.platform.aws.placement.availability-zone}"
    }

    task "search" {
      meta {
        product-class = "third-party"
        stack-role = "db"
      }
      driver = "docker"
      config {
        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/consul-elasticsearch:5.6.2-deluxe-0.1"
        port_map {
          http = 9200
          tcp = 9300
        }
        # volume_options.driver_config.options.size is GiB
        mounts = [
          {
            target = "/usr/share/elasticsearch/data"
            source = "${NOMAD_JOB_NAME}.${NOMAD_TASK_NAME}.<ENV_TYPE>"
            readonly = false
            volume_options {
              no_copy = false
              driver_config {
                name = "ebs"
                options = {
                  type = "gp2"
                  size = "16"
                }
              }
            }
          }
        ]        
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
        ES_JAVA_OPTS        = "-Xms1024m -Xmx1024m"
        CONSUL_ADDR         = "consul.service.<TLD>:8500"
        CLUSTER_NAME        = "${NOMAD_JOB_NAME}.${NOMAD_TASK_NAME}"
        PUBLISH_IP          = "${NOMAD_IP_tcp}"
        TCP_PUBLISH_PORT    = "${NOMAD_HOST_PORT_tcp}"
        DISCOVERY_HOST      = "${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}-tcp"
        DISCOVERY_WAIT      = "30s:60s"
        DISCOVERY_MIN_NODES = "2"
      }
      service {
        name = "${JOB}-${TASK}-http"
        port = "http"
        check {
          type     = "http"
          path     = "/"
          interval = "10s"
          timeout  = "3s"
        }
      }
      
      service {
        name = "${JOB}-${TASK}-tcp"
        port = "tcp"
        check {
          type     = "tcp"
          interval = "10s"
          timeout  = "3s"
        }
      }
      resources {
        cpu    = 256  # MHz
        memory = 2048 # MB
        network {
          mbits = 4
          port "http" {}
          port "tcp" {}
        }
      }
    } // end search task
  } // end search group
} // end job
