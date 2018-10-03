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
    value     = "shared"
  }

  constraint {
    attribute = "${meta.env_type}"
    // Options: [ test | int | live ]
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

    # vault declaration
    vault {
      change_mode = "noop"
      env = false
      policies = ["read-secrets"]
    }

    task "ui" {
      meta {
        product-class = "custom"
        stack-role = "ui"
      }
      driver = "docker"
      config {
        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/conductor:<APP_VERSION>-ui"
        port_map {
          http = 5000
        }
        volumes = [
          "local/secrets/conductor-ui.env:/app/config/secrets.env"
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
        TLD = "<TLD>"
        APP_VERSION = "<APP_VERSION>"
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
      # Write secrets to the file that can be mounted as volume
      template {
        data = <<EOF
        {{ with printf "secret/%s" (env "NOMAD_JOB_NAME") | secret }}{{ range $k, $v := .Data }}{{ $k }}={{ $v }}
        {{ end }}{{ end }}
        EOF
        destination   = "local/secrets/conductor-ui.env"
        change_mode   = "signal"
        change_signal = "SIGINT"
      }
      resources {
        cpu    = 256 # MHz
        memory = 512 # MB
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

    # vault declaration
    vault {
      change_mode = "noop"
      env = false
      policies = ["read-secrets"]
    }

    task "server" {
      meta {
        product-class = "custom"
        stack-role = "api"
      }
      driver = "docker"
      config {
        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/conductor:<APP_VERSION>-server"
        port_map {
          http = 8080
        }
        volumes = [
          "local/secrets/conductor-server.env:/app/config/secrets.env"
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
        trigger = "1"
        TLD   = "<TLD>"
        STACK = "<ENV_TYPE>"
        APP_VERSION = "<APP_VERSION>"

        // Database settings
        db = "elasticsearch"

        // Workflow settings
        workflow_auth_validate = "true"
        workflow_failure_expandInline = "false"
        decider_sweep_frequency_seconds = "5"
        workflow_event_processor_refresh_seconds = "30"
        workflow_system_task_worker_poll_frequency = "5000"
        workflow_system_task_worker_queue_size = "300"
        workflow_sweeper_frequency = "5000"
        workflow_sweeper_thread_count = 10

        // Elasticsearch settings
        workflow_elasticsearch_mode = "elasticsearch"
        workflow_elasticsearch_service = "${NOMAD_JOB_NAME}-search-tcp.service.<TLD>"
        workflow_elasticsearch_cluster_name = "${NOMAD_JOB_NAME}.search"
        workflow_elasticsearch_initial_sleep_seconds = "30"

        // NATS settings
        io_nats_streaming_url = "nats://nats.service.<TLD>:4222"
        io_nats_streaming_clusterId = "events-streaming"
        io_nats_streaming_durableName = "conductor-server-<TLD>"
        io_nats_streaming_publishRetryIn = "5,10,15"

        // Additional nats & asset modules
        conductor_additional_modules = "com.netflix.conductor.contribs.NatsStreamModule,com.netflix.conductor.contribs.AssetModule"

        // Exclude demo workflows
        loadSample = "false"
      }
      service {
        tags = ["urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.dmlib.<DM_TLD>/ auth=true","urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.service.<TLD>/", "metrics=${NOMAD_JOB_NAME}"]
        name = "${JOB}-${TASK}"
        port = "http"
        check {
          type     = "http"
          path     = "/"
          interval = "10s"
          timeout  = "3s"
        }
      }

      # Write secrets to the file that can be mounted as volume
      template {
        data = <<EOF
        {{ with printf "secret/%s" (env "NOMAD_JOB_NAME") | secret }}{{ range $k, $v := .Data }}{{ $k }}={{ $v }}
        {{ end }}{{ end }}
        EOF
        destination   = "local/secrets/conductor-server.env"
        change_mode   = "signal"
        change_signal = "SIGINT"
      }

      resources {
        cpu    = 256  # MHz
        memory = 2048 # MB
        network {
          mbits = 4
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
            source = "${NOMAD_JOB_NAME}.${NOMAD_TASK_NAME}.<ENV_TYPE>.${attr.platform.aws.placement.availability-zone}"
            readonly = false
            volume_options {
              no_copy = false
              driver_config {
                name = "<VOLUME_DRIVER>"
                options = {
                  type = "gp2"
                  ebstype = "gp2"
                  size = "16"
                  backing = "relocatable"
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
