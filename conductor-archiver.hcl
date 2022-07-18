variable "app_version" {
  default = "latest"
}

variable "env" {
  default = "dev"
}

variable "conductor_archiver_count" {
    type = map(string)
    default = {
        dev = 1
        int = 1
        uat = 1
        live = 1
    }
}

variable "conductor_archiver_cpu" {
    type = map(string)
    default = {
        dev = 128
        int = 128
        uat = 128
        live = 128
    }
}

variable "conductor_archiver_mem" {
    type = map(string)
    default = {
        dev = 1024
        int = 1024
        uat = 1024
        live = 1024
    }
}

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
    repository = "git@github.com:d3sw/conductor.git"
    job_file = "conductor-archiver.hcl"
    service-class = "platform"
  }

  constraint {
    attribute = "${meta.enclave}"
    value     = "shared"
  }

  group "archiver" {
   count = lookup(var.conductor_archiver_count, var.env, 1)

    network {
       mode = "bridge"
    }

    # vault declaration
    vault {
      change_mode = "restart"
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
        image = "583623634344.dkr.ecr.us-west-2.amazonaws.com/conductor:${var.app_version}-archiver"

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
        {{ with printf "kv/conductor" | secret }}{{ range $k, $v := .Data.data }}{{ $k }}={{ $v }}
        {{ end }}{{ end }}
        EOF

        destination   = "local/secrets/conductor-archiver.env"
        change_mode   = "signal"
        change_signal = "SIGINT"
      }

      resources {
        cpu    = lookup(var.conductor_archiver_cpu, var.env, 128)  # MHz
        memory = lookup(var.conductor_archiver_mem, var.env, 1024) # MB
      }
    } // end archiver task
  } // end archiver group
} // end archiver job
