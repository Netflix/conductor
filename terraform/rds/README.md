# Conductor-RDS
## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|:----:|:-----:|:-----:|
| allowed\_subnets | Allowed subnets for security groups. | map | `<map>` | no |
| apply\_immediately |  (Optional) A list of EC2 Availability Zones for the DB cluster storage where DB cluster instances can be created. <br  /> RDS automatically assigns 3 AZs if less than 3 AZs are configured, which will show as a difference requiring resource recreation next Terraform apply.<br  />  It is recommended to specify 3 AZs or use the lifecycle configuration block ignore_changes argument if necessary. | string | `"true"` | no |
| availability\_zones | (Optional) A list of EC2 Availability Zones for the DB cluster storage where DB cluster instances can be created. <br  />  | list | `<list>` | no |
| cluster\_size | min 2 | string | `"2"` | no |
| database\_name | check vault | map | `<map>` | no |
| database\_password | check vault | map | `<map>` | no |
| database\_port | default postgressql port | string | `"5432"` | no |
| database\_user | check vault | map | `<map>` | no |
| enclave | <shared\|corp> | string | `"shared"` | no |
| engine |(Optional) The name of the database engine to be used for this DB cluster<br  />. Defaults to aurora. Valid Values: aurora, aurora-mysql, aurora-postgresql  | string | `"aurora-postgresql"` | no |
| engine\_version | (Optional) The database engine version.<br  /> Updating this argument results in an outage.<br  /> See the Aurora MySQL and Aurora Postgres documentation for your configured engine to determine this value.<br  /> For example with Aurora MySQL 2, a potential value for this argument is 5.7.mysql_aurora.2.03.2.<br  /> | string | `"10.6"` | no |
| env | environment | string | `""` | yes |
| instance\_class | default set to the min db instance_class available | string | `"db.r5.large"` | no |
| owner | name of the service+_+service | string | `"conductor_service"` | no |
| publicly\_accessible | default set to false | string | `"false"` | no |
| region | region | string | `"us-west-2"` | no |
| service | name of the service | string | `"conductor"` | no |


***WIP***