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
      change_mode = "noop"
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
        TLD         = "${meta.tld}"
        APP_VERSION = "[[.app_version]]"
        WF_SERVICE  = "${NOMAD_JOB_NAME}-server.service.${meta.tld}"

        // Auth settings. Rest settings are in vault
        conductor_auth_service  = "auth.service.${meta.tld}"
        conductor_auth_endpoint = "/v1/tenant/deluxe/auth/token"
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
          port  "http"{}
        }
      }
    } // end ui task
  } // end ui group
  group "ui-pg" {
    count = 3

    # vault declaration
    vault {
      change_mode = "noop"
      env         = false
      policies    = ["read-secrets"]
    }

    task "ui-pg" {
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
        TLD         = "${meta.tld}"
        APP_VERSION = "[[.app_version]]"
        WF_SERVICE  = "${NOMAD_JOB_NAME}-server-pg.service.${meta.tld}"

        // Auth settings. Rest settings are in vault
        conductor_auth_service  = "auth.service.${meta.tld}"
        conductor_auth_endpoint = "/v1/tenant/deluxe/auth/token"
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
          port  "http"{}
        }
      }
    } // end ui task
  } // end ui group

  group "server" {
    count = 5

    # vault declaration
    vault {
      change_mode = "noop"
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
        db = "elasticsearch"

        // Workflow settings
        workflow_failure_expandInline                = "false"
        decider_sweep_frequency_seconds              = "30"
        workflow_event_processor_thread_count        = "10"
        workflow_event_processor_refresh_seconds     = "30"
        workflow_system_task_worker_poll_count       = "50"
        workflow_system_task_worker_poll_timeout     = "1000"
        workflow_system_task_worker_poll_frequency   = "1000"
        workflow_system_task_worker_queue_size       = "300"
        workflow_system_task_http_unack_timeout      = "300"
        workflow_sweeper_frequency                   = "500"
        workflow_sweeper_thread_count                = 50
        workflow_sweeper_batch_sherlock_service      = "sherlock.service.${meta.tld}"
        workflow_sweeper_batch_sherlock_worker_count = 100
        workflow_sweeper_batch_names                 = "sherlock"
        workflow_batch_sherlock_enabled              = "true"
        workflow_lazy_decider                        = "true"

        // Elasticsearch settings.
        workflow_elasticsearch_mode                  = "elasticsearch"
        workflow_elasticsearch_initial_sleep_seconds = "30"
        workflow_elasticsearch_stale_period_seconds  = "300"

        // Auth settings. Rest settings are in vault
        conductor_auth_service  = "auth.service.${meta.tld}"
        conductor_auth_endpoint = "/v1/tenant/deluxe/auth/token"

        // One MQ settings
        io_shotgun_dns            = "shotgun.service.${meta.tld}"
        io_shotgun_service        = "conductor-server-${meta.tld}"
        io_shotgun_publishRetryIn = "5,10,15"
        io_shotgun_shared         = "false"
        io_shotgun_manualAck      = "true"
        com_bydeluxe_onemq_log    = "false"

        // NATS settings
        io_nats_streaming_url            = "nats://nats.service.${meta.tld}:4222"
        io_nats_streaming_clusterId      = "events-streaming"
        io_nats_streaming_durableName    = "conductor-server-${meta.tld}"
        io_nats_streaming_publishRetryIn = "5,10,15"

        // Additional nats & asset modules
        conductor_additional_modules = "com.netflix.conductor.contribs.NatsStreamModule,com.netflix.conductor.contribs.ShotgunModule"

        // Exclude demo workflows
        loadSample = "false"

        // The following will be provided by secret/conductor
        //  - conductor_auth_url
        //  - conductor_auth_clientId
        //  - conductor_auth_clientSecret
        //  - workflow_elasticsearch_url
      }

      service {
        tags = ["urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.dmlib.${meta.public_tld}/ auth=true", "urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.service.${meta.tld}/"]
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
          port  "http"{}
        }
      }
    } // end server task
  } // end server group
  group "server-pg" {
    count = 5

    # vault declaration
    vault {
      change_mode = "noop"
      env         = false
      policies    = ["read-secrets"]
    }

    task "server-pg" {
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
        decider_sweep_frequency_seconds              = "30"
        workflow_event_processor_thread_count        = "10"
        workflow_event_processor_refresh_seconds     = "30"
        workflow_system_task_worker_poll_count       = "50"
        workflow_system_task_worker_poll_timeout     = "1000"
        workflow_system_task_worker_poll_frequency   = "1000"
        workflow_system_task_worker_queue_size       = "300"
        workflow_system_task_http_unack_timeout      = "300"
        workflow_sweeper_frequency                   = "500"
        workflow_sweeper_thread_count                = 50
        workflow_sweeper_batch_sherlock_service      = "sherlock.service.${meta.tld}"
        workflow_sweeper_batch_sherlock_worker_count = 100
        workflow_sweeper_batch_names                 = "sherlock"
        workflow_batch_sherlock_enabled              = "true"
        workflow_lazy_decider                        = "true"

        // Auth settings. Rest settings are in vault
        conductor_auth_service  = "auth.service.${meta.tld}"
        conductor_auth_endpoint = "/v1/tenant/deluxe/auth/token"

        // One MQ settings
        io_shotgun_dns            = "shotgun.service.${meta.tld}"
        io_shotgun_service        = "conductor-server-${meta.tld}-pg"
        io_shotgun_publishRetryIn = "5,10,15"
        io_shotgun_shared         = "false"
        io_shotgun_manualAck      = "true"
        com_bydeluxe_onemq_log    = "false"

        // NATS settings
        io_nats_streaming_url            = "nats://nats.service.${meta.tld}:4222"
        io_nats_streaming_clusterId      = "events-streaming"
        io_nats_streaming_durableName    = "conductor-server-${meta.tld}-pg"
        io_nats_streaming_publishRetryIn = "5,10,15"

        // Additional nats & asset modules
        conductor_additional_modules = "com.netflix.conductor.contribs.NatsStreamModule,com.netflix.conductor.contribs.ShotgunModule"

        // Exclude demo workflows
        loadSample = "false"

        // Disable logging for system level libraries
        log4j_logger_io_swagger        = "INFO"
        log4j_logger_io_grpc_netty     = "INFO"
        log4j_logger_org_apache_http   = "INFO"
        log4j_logger_org_eclipse_jetty = "INFO"
        log4j_logger_com_zaxxer_hikari = "INFO"
        log4j_logger_com_jayway_jsonpath_internal_path_CompiledPath = "INFO"
        log4j_logger_com_netflix_conductor_core_execution_tasks_SystemTaskWorkerCoordinator = "INFO"
      }

      service {
        tags = ["urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.dmlib.${meta.public_tld}/ auth=true", "urlprefix-${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}.service.${meta.tld}/"]
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
          port  "http"{}
        }
      }
    } // end server task
  } // end server group
} // end job

//job "conductor-archiver" {
//  type        = "batch"
//  region      = "us-west-2"
//  datacenters = ["us-west-2"]
//
//  periodic {
//    cron             = "@daily"
//    time_zone        = "America/Los_Angeles"
//    prohibit_overlap = true
//  }
//
//  meta {
//    service-class = "platform"
//  }
//
//  constraint {
//    attribute = "${meta.enclave}"
//    value     = "shared"
//  }
//
//  group "archiver" {
//    count = 1
//
//    # vault declaration
//    vault {
//      change_mode = "noop"
//      env         = false
//      policies    = ["read-secrets"]
//    }
//
//    task "archiver" {
//      meta {
//        product-class = "custom"
//        stack-role    = "daemon"
//      }
//
//      driver = "docker"
//
//      config {
//        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/conductor:[[.app_version]]-archiver"
//
//        volumes = [
//          "local/secrets/conductor-archiver.env:/app/config/secrets.env",
//        ]
//
//        labels {
//          service   = "${NOMAD_JOB_NAME}"
//          component = "${NOMAD_TASK_NAME}"
//        }
//
//        logging {
//          type = "syslog"
//
//          config {
//            tag = "${NOMAD_JOB_NAME}-${NOMAD_TASK_NAME}"
//          }
//        }
//      }
//
//      env {
//        env_type = "${meta.env}"
//      }
//
//      # Write secrets to the file that can be mounted as volume
//      template {
//        data = <<EOF
//        {{ with printf "secret/conductor" | secret }}{{ range $k, $v := .Data }}{{ $k }}={{ $v }}
//        {{ end }}{{ end }}
//        EOF
//
//        destination   = "local/secrets/conductor-archiver.env"
//        change_mode   = "signal"
//        change_signal = "SIGINT"
//      }
//
//      resources {
//        cpu    = 512  # MHz
//        memory = 2048 # MB
//
//        network {
//          mbits = 4
//        }
//      }
//    } // end archiver task
//  } // end archiver group
//} // end archiver job
