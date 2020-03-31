job "conductor" {
  type        = "service"
  region      = "us-west-2"
  datacenters = ["us-west-2"]

  meta {
    service-class = "platform"
  }

  constraint {
    attribute = "${meta.enclave}"
    value     = "shared"
  }

  update {
    max_parallel      = 2
    health_check      = "checks"
    min_healthy_time  = "10s"
    healthy_deadline  = "5m"
    progress_deadline = "10m"
    auto_revert       = true
    stagger           = "30s"
  }

  group "ui" {
    count = 3

    # vault declaration
    vault {
      change_mode = "restart"
      env         = false
      policies    = ["read-secrets"]
    }

    task "ui" {
      meta {
        product-class = "custom"
        stack-role    = "ui"
      }

      driver = "docker"

      config {
        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/conductor:[[.app_version]]-ui"

        port_map {
          http = 5000
        }

        volumes = [
          "local/secrets/conductor-ui.env:/app/config/secrets.env",
        ]

        labels {
          service   = "${NOMAD_JOB_NAME}"
          component = "${NOMAD_TASK_NAME}"
        }

        logging {
          type = "syslog"

          config {
            tag = "${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}"
          }
        }
      }

      env {
        TLD = "${meta.tld}"
        APP_VERSION = "[[.app_version]]"
        WF_SERVICE  = "${NOMAD_JOB_NAME}-server.service.${meta.tld}"
        AUTH_SERVICE_NAME    = "auth.service.${meta.tld}"
        KEYCLOAK_SERVICE_URL = "http://keycloak.service.${meta.tld}"
      }

      service {
        tags = ["urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.dmlib.${meta.public_tld}/ auth=true", "urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.service.${meta.tld}/"]
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
        {{ with printf "secret/conductor/ui" | secret }}{{ range $k, $v := .Data }}{{ $k }}={{ $v }}
        {{ end }}{{ end }}
        EOF

        destination   = "local/secrets/conductor-ui.env"
        change_mode   = "signal"
        change_signal = "SIGINT"
      }

      resources {
        cpu    = 100 # MHz
        memory = 128 # MB

        network {
          mbits = 4
          port  "http"{}
        }
      }
    } // end ui task
  } // end ui group

  group "server" {
    count = 5

    # vault declaration
    vault {
      change_mode = "restart"
      env         = false
      policies    = ["read-secrets"]
    }

    task "server" {
      meta {
        product-class = "custom"
        stack-role    = "api"
      }

      driver = "docker"

      config {
        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/conductor:[[.app_version]]-server"

        port_map {
          http = 8080
        }

        volumes = [
          "local/secrets/conductor-server.env:/app/config/secrets.env",
        ]

        labels {
          service   = "${NOMAD_JOB_NAME}"
          component = "${NOMAD_TASK_NAME}"
        }

        logging {
          type = "syslog"

          config {
            tag = "${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}"
          }
        }
      }

      env {
        TLD         = "${meta.tld}"
        STACK       = "${meta.env}"
        APP_VERSION = "[[.app_version]]"

        // Database settings
        db = "aurora"

        // Workflow settings
        workflow_failure_expandInline                = "false"
        decider_sweep_frequency_seconds              = "60"
        workflow_system_task_worker_thread_count     = "5"
        workflow_system_task_worker_poll_count       = "50"
        workflow_system_task_worker_poll_timeout     = "1000"
        workflow_system_task_worker_poll_frequency   = "1000"
        workflow_system_task_worker_queue_size       = "300"
        workflow_system_task_http_unack_timeout      = "300"
        workflow_sweeper_frequency                   = "500"
        workflow_sweeper_thread_count                = "50"
        workflow_sweeper_pool_timeout                = "1000"
        workflow_sweeper_batch_sherlock_service      = "sherlock.service.${meta.tld}"
        workflow_sweeper_batch_sherlock_worker_count = "100"
        workflow_sweeper_batch_names                 = "sherlock"
        workflow_batch_sherlock_enabled              = "true"
        workflow_lazy_decider                        = "true"

        // Elasticsearch settings.
        workflow_elasticsearch_mode = "none"

        // Auth settings. Rest settings are in vault
        conductor_auth_service  = "auth.service.${meta.tld}"
        conductor_auth_endpoint = "/v1/tenant/deluxe/auth/token"

        // One MQ settings
        io_shotgun_dns            = "shotgun.service.${meta.tld}"
        io_shotgun_service        = "${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}-${meta.tld}"
        io_shotgun_publishRetryIn = "5,10,15"
        io_shotgun_shared         = "false"
        com_bydeluxe_onemq_log    = "false"

        // Additional modules
        conductor_additional_modules = "com.netflix.conductor.contribs.ShotgunModule"

        // Exclude demo workflows
        loadSample = "false"

        // Disable system-level loggers by default
        log4j_logger_com_jayway_jsonpath = "OFF"
        log4j_logger_com_zaxxer_hikari = "INFO"
        log4j_logger_org_eclipse_jetty = "INFO"
        log4j_logger_org_apache_http = "INFO"
        log4j_logger_io_grpc_netty = "INFO"
        log4j_logger_io_swagger = "OFF"
        log4j_logger_tracer = "OFF"

        // DataDog Integration
        DD_AGENT_HOST = "datadog-apm.service.${meta.tld}"
        DD_SERVICE_NAME = "conductor.server.webapi"
        DD_SERVICE_MAPPING = "postgresql:conductor.server.postgresql"
        DD_TRACE_GLOBAL_TAGS = "env:${meta.tld}"
        DD_LOGS_INJECTION = "true"
      }

      service {
        tags = ["urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.dmlib.${meta.public_tld}/ auth=true trace=true", "urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.service.${meta.tld}/ trace=true", "metrics=${NOMAD_JOB_NAME}"]
        name = "${JOB}-${TASK}"
        port = "http"

        check {
          type     = "http"
          path     = "/v1/health"
          interval = "10s"
          timeout  = "3s"
          check_restart {
            limit           = 3
            grace           = "180s"
            ignore_warnings = false
          }
        }
      }

      # Write secrets to the file that can be mounted as volume
      template {
        data = <<EOF
        {{ with printf "secret/conductor" | secret }}{{ range $k, $v := .Data }}{{ $k }}={{ $v }}
        {{ end }}{{ end }}
        EOF

        destination   = "local/secrets/conductor-server.env"
        change_mode   = "signal"
        change_signal = "SIGINT"
      }

      resources {
        cpu    = 256  # MHz
        memory = 1024 # MB

        network {
          mbits = 100
          port  "http"{}
        }
      }
    } // end server task
  } // end server group
} // end job
