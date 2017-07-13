job "conductor" {
  region      = "us-west-2"
  datacenters = ["us-west-2"]
  type        = "service"

  // Define which env to deploy service in
  constraint {
      attribute = "${meta.hood}"
      // Options: [ corp | prod | shared ]
      value     = "shared"
  }

  constraint {
      attribute = "${meta.env_type}"
      // Options: [ test | live ]
      value     = "test"
  }

  // Configure the job to do rolling updates
  update {
   stagger      = "15s"
   max_parallel = 1
  }

  # Create a 'conductor' group. Each task in the group will be scheduled onto the same machine.
  group "server" {
    # Specify the number of tasks/instances to launch.
    count = 1

    # Create an individual task (unit of work). This particular
    # task utilizes a Docker container to front a web application.
    task "server" {
      # Specify the driver to be "docker". Nomad supports
      # multiple drivers.
      driver = "docker"
      # Configuration is specific to each driver.
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
            tag = "conductor-server"
          }
        }
      }

      # The service block tells Nomad how to register this service
      # with Consul for service discovery and monitoring.
      service {
        name = "${JOB}-server"
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
        cpu    = 200 # MHz
        memory = 1000 # MB

        network {
          mbits = 1
          port "http" {
            static = 30000
          }
        }
      }
    } // END task.conductor_task
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
            tag = "conductor-ui"
          }
        }
      }

      env {
        WF_SERVER = "http://conductor-server.service:30000/api/"
      }

      # The service block tells Nomad how to register this service
      # with Consul for service discovery and monitoring.
      service {
        name = "${JOB}-ui"
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
        cpu    = 200 # MHz
        memory = 128 # MB

        network {
          mbits = 1
          port "http" {}
        }
      }
    } // END task.ui
  } // END group.conductor
}
