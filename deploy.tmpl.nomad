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
    count = 5

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
        decider_sweep_frequency_seconds = "30"
        workflow_event_processor_thread_count = "10"
        workflow_event_processor_refresh_seconds = "30"
        workflow_system_task_worker_poll_count = "50"
        workflow_system_task_worker_poll_timeout = "1000"
        workflow_system_task_worker_poll_frequency = "1000"
        workflow_system_task_worker_queue_size = "300"
        workflow_system_task_http_unack_timeout = "300"
        workflow_sweeper_frequency = "500"
        workflow_sweeper_thread_count = 50
        workflow_sweeper_batch_sherlock_service = "sherlock.service.<TLD>"
        workflow_sweeper_batch_sherlock_worker_count = 100
        workflow_sweeper_batch_names = "sherlock"
        workflow_batch_sherlock_enabled = "true"
        workflow_lazy_decider = "true"

        // Elasticsearch settings.
        workflow_elasticsearch_mode = "elasticsearch"
        workflow_elasticsearch_initial_sleep_seconds = "30"
        workflow_elasticsearch_stale_period_seconds = "300"

        // One MQ settings
        io_shotgun_dns = "shotgun.service.<TLD>"
        io_shotgun_service = "conductor-server-<TLD>"
        io_shotgun_publishRetryIn = "5,10,15"
        io_shotgun_shared = "true"
        com_bydeluxe_onemq_log = "false"

        // NATS settings
        io_nats_streaming_url = "nats://nats.service.<TLD>:4222"
        io_nats_streaming_clusterId = "events-streaming"
        io_nats_streaming_durableName = "conductor-server-<TLD>"
        io_nats_streaming_publishRetryIn = "5,10,15"

        // Additional nats & asset modules
        conductor_additional_modules = "com.netflix.conductor.contribs.NatsStreamModule,com.netflix.conductor.contribs.ShotgunModule,com.netflix.conductor.contribs.AssetModule"

        // Exclude demo workflows
        loadSample = "false"

        // Loggers
        log4j_logger_com_netflix_conductor_dao_es6rest="INFO"
        log4j_logger_com_netflix_conductor_core_events_nats="DEBUG"
        log4j_logger_com_netflix_conductor_core_events_shotgun="DEBUG"
        log4j_logger_com_netflix_conductor_core_events_EventProcessor="INFO"
        log4j_logger_com_netflix_conductor_core_execution_DeciderService="DEBUG"
        log4j_logger_com_netflix_conductor_core_execution_WorkflowExecutor="DEBUG"
        log4j_logger_com_netflix_conductor_contribs_http="DEBUG"
        log4j_logger_com_netflix_conductor_contribs_queue_nats="DEBUG"
        log4j_logger_com_netflix_conductor_contribs_queue_shotgun="DEBUG"

        // The following will be provided by secret/conductor
        //  - conductor_auth_url
        //  - conductor_auth_clientId
        //  - conductor_auth_clientSecret
        //  - workflow_elasticsearch_url
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
        cpu    = 512  # MHz
        memory = 2048 # MB
        network {
          mbits = 4
          port "http" {}
        }
      }
    } // end server task
  } // end server group
} // end job
