# Terraform RDS
- This is the Terraform config to deploy RDS resources in AWS.
    - PLEASE DO NOT MODIFY FILES UNDER THIS DIRECTORY UNLESS YOU KNOW WHAT YOU ARE DOING.
- This config deploys an RDS instance in the specified `env` for the **Conductor** service.

## Components


- The deployment is broken down into the following files:

    - **Makefile**: acts as a wrapper for Terraform commands and contains specific targets to deploy resources.  
    - **outputs**: contains the values that get written to `stdout` upon successful Terraform commands.
    - **provider**: defines the provider type. In this case:
      - aws
      - vault
    - **rds**: contains the configs pertaining to RDS and its dependencies.
    - **vars**: contains variables that all resources in the directory can reference.

Usage
-----

- Take a look at the `Makefile` for details on the targets.

### Stage Resources
Follow the auto complete targets for MAKE command in the `Makefile`
- `make plan-db-corp-int`
- `make plan-db-corp-live`
- `make plan-db-corp-test`
- `make plan-db-shared-int`
- `make plan-db-shared-live`
- `make plan-db-shared-test`
- `make plan-rotate-pass-db-shared-int`
- `make plan-rotate-pass-db-shared-live`
- `make plan-rotate-pass-db-shared-test`

### Deploy Resources

- `make apply-db-corp-int`
- `make apply-db-corp-live`
- `make apply-db-corp-test`
- `make apply-db-shared-int`
- `make apply-db-shared-live`
- `make apply-db-shared-test`

> - **Warning**: Terraform will also apply any changes it detects between RDS configuration in code compared to the configuration in tfstate . first run `plan` to see the potential changes.

- `make apply-rotate-pass-db-shared-int` 
- `make apply-rotate-pass-db-shared-live`
- `make apply-rotate-pass-db-shared-test`

### Destroy Resources

- `make destroy-db-corp-int`
- `make destroy-db-corp-live`
- `make destroy-db-corp-test`
- `make destroy-db-shared-int`
- `make destroy-db-shared-live`
- `make destroy-db-shared-test`

## Inputs

| Name                 | Description                                                                                                                                                                                                                                                                                                                                                                                                |  Type  |        Default        | Required |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----: | :-------------------: | :------: |
| allowed\_subnets     | Allowed subnets for security groups.                                                                                                                                                                                                                                                                                                                                                                       |  map   |        `<map>`        |    no    |
| apply\_immediately   | (Optional) A list of EC2 Availability Zones for the DB cluster storage where DB cluster instances can be created. <br  /> RDS automatically assigns 3 AZs if less than 3 AZs are configured, which will show as a difference requiring resource recreation next Terraform apply.<br  />  It is recommended to specify 3 AZs or use the lifecycle configuration block ignore_changes argument if necessary. | string |       `"true"`        |    no    |
| availability\_zones  | (Optional) A list of EC2 Availability Zones for the DB cluster storage where DB cluster instances can be created. <br  />                                                                                                                                                                                                                                                                                  |  list  |       `<list>`        |    no    |
| cluster\_size        | min 1                                                                                                                                                                                                                                                                                                                                                                                                      | string |         `"1"`         |    no    |
| database\_name       | check vault                                                                                                                                                                                                                                                                                                                                                                                                |  map   |        `<map>`        |    no    |
| database\_password   | check vault                                                                                                                                                                                                                                                                                                                                                                                                |  map   |        `<map>`        |    no    |
| database\_port       | default postgressql port                                                                                                                                                                                                                                                                                                                                                                                   | string |       `"5432"`        |    no    |
| database\_user       | check vault                                                                                                                                                                                                                                                                                                                                                                                                |  map   |        `<map>`        |    no    |
| enclave              | <shared\|corp>                                                                                                                                                                                                                                                                                                                                                                                             | string |      `"shared"`       |    no    |
| engine               | (Optional) The name of the database engine to be used for this DB cluster<br  />. Defaults to aurora. Valid Values: aurora, aurora-mysql, aurora-postgresql                                                                                                                                                                                                                                                | string | `"aurora-postgresql"` |    no    |
| engine\_version      | (Optional) The database engine version.<br  /> Updating this argument results in an outage.<br  /> See the Aurora MySQL and Aurora Postgres documentation for your configured engine to determine this value.<br  /> For example with Aurora MySQL 2, a potential value for this argument is 5.7.mysql_aurora.2.03.2.<br  />                                                                               | string |       `"10.6"`        |    no    |
| env                  | environment <test\|int\|live>                                                                                                                                                                                                                                                                                                                                                                                               | string |         `""`          |   yes    |
| instance\_class      | default set to the min db instance_class available                                                                                                                                                                                                                                                                                                                                                         | string |    `"db.r5.large"`    |    no    |
| owner                | name of the service+_+service                                                                                                                                                                                                                                                                                                                                                                              | string | `"conductor_service"` |    no    |
| publicly\_accessible | default set to false                                                                                                                                                                                                                                                                                                                                                                                       | string |       `"false"`       |    no    |
| region               | region                                                                                                                                                                                                                                                                                                                                                                                                     | string |     `"us-west-2"`     |    no    |
| service              | name of the service                                                                                                                                                                                                                                                                                                                                                                                        | string |     `"conductor"`     |    no    |

**Maintainer**: Platform-Engineering team