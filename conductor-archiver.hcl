job "conductor-archiver" {
  type        = "batch"
  region      = "us-west-2"
  datacenters = ["us-west-2"]

  periodic {
    cron             = "@daily"
    time_zone        = "America/Los_Angeles"
    prohibit_overlap = true
  }

  meta {
    service-class = "platform"
  }

  constraint {
    attribute = "${meta.enclave}"
    value     = "shared"
  }

  group "archiver" {
    count = 1

    # vault declaration
    vault {
      change_mode = "noop"
      env         = false
      policies    = ["read-secrets"]
    }

    task "archiver" {
      meta {
        product-class = "custom"
        stack-role    = "daemon"
      }

      driver = "docker"

      config {
        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/conductor:[[.app_version]]-archiver"

        volumes = [
          "local/secrets/conductor-archiver.env:/app/config/secrets.env",
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
        env_type = "${meta.env}"
      }

      # Write secrets to the file that can be mounted as volume
      template {
        data = <<EOF
        {{ with printf "secret/conductor" | secret }}{{ range $k, $v := .Data }}{{ $k }}={{ $v }}
        {{ end }}{{ end }}
        EOF

        destination   = "local/secrets/conductor-archiver.env"
        change_mode   = "signal"
        change_signal = "SIGINT"
      }

      resources {
        cpu    = 512  # MHz
        memory = 2048 # MB

        network {
          mbits = 4
        }
      }
    } // end archiver task
  } // end archiver group
} // end archiver job