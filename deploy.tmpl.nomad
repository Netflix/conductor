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
        db = "dynomite"
        workflow_dynomite_cluster_hosts = "${NOMAD_JOB_NAME}-db.service.<TLD>:8102:us-east-1c"
        workflow_elasticsearch_mode = "memory"
        decider_sweep_frequency_seconds = "1"
        //workflow_sweeper_delay_seconds = "3"
        //workflow_sweeper_mode = "direct"

        // Uncomment for NATS
        io_nats_client_url = "nats://events.service.owf-dev:4222"
        conductor_additional_modules = "com.netflix.conductor.contribs.NatsModule"

        // Uncomment for NAT Streaming
        //io_nats_streaming_url = "nats://${NOMAD_JOB_NAME}-nats.service.<TLD>:4222:us-east-1c"
        //io_nats_streaming_clusterId = "test-cluster"
        //io_nats_streaming_clientId = "nomad"
        //conductor_additional_modules = "com.netflix.conductor.contribs.NatsStreamModule"
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
        image = "v1r3n/dynomite"
        port_map {
            port8102 = 8102
            port22122 = 22122
            port22222 = 22222
        }
        labels {
            service = "${NOMAD_JOB_NAME}"
        }        
      }

      service {
        name = "${JOB}-${TASK}"
        port = "port8102"

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
          port "port8102" {
            static = 8102
          }
          port "port22122" {
            static = 22122
          }
          port "port22222" {
            static = 22222
          }
        }
      }
    } // end task
  } // end group
}
