SERVICE = conductor
REGION = us-west-2
TFSTATE = $(SERVICE)/$(REGION)-$(ENV)-aurora-postgres.tfstate

# Target Specific Variables
## corp
### enclave
plan-db-corp-% apply-db-corp-% destroy-db-corp-%: ENCLAVE=corp
plan-db-shared-% apply-db-shared-% destroy-db-shared-%: ENCLAVE=shared
### env and tld
%-db-corp-test: ENV=test
%-db-corp-live: ENV=live
%-db-corp-int: ENV=int

%-db-shared-test: ENV=test
%-db-shared-live: ENV=live
%-db-shared-int: ENV=int
# clean terraform modules
clean:
		rm -rf ./.terraform

creds:
		# ensure there is an active aws-vault session
		@aws-vault exec -j deluxemediaplatform >/dev/null

show-vars:
			@echo "REGION:\t\t\t$(REGION)"
			@echo "ENV:\t\t\t$(ENV)"
			@echo ""

check-vars: show-vars
	@if [ -z "$(ENV)" ]; then echo ENV not set!; exit 1; fi
	@echo "ARE THESE PARAMETERS CORRECT? [yes/no]" && read ans && [ $$ans == yes ]

init: check-vars creds
	@if [ -f .terraform/terraform.tfstate ]; then rm .terraform/terraform.tfstate; fi
	terraform init -backend-config="key=$(TFSTATE)"


# dry run
plan-db-%: clean show-vars check-vars init
	terraform plan \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var env="$(ENV)" \
		-target=aws_rds_cluster.rds_cluster \
		-target=aws_rds_cluster_instance.rds_cluster_instances \
		-target=aws_db_subnet_group.rds_db_subnet_group \
		-target=aws_security_group.conductor-sg \
		-target=aws_elasticache_subnet_group.conductor-subnet-grp

# plan db pass
plan-pass-%: clean show-vars check-vars init
	terraform plan \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var env="$(ENV)" \
		-target=random_string.password


# deploy resources
apply-db-%: clean show-vars check-vars init
	terraform apply \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var env="$(ENV)" \
		-target=aws_rds_cluster.rds_cluster \
		-target=aws_rds_cluster_instance.rds_cluster_instances \
		-target=aws_db_subnet_group.rds_db_subnet_group \
		-target=aws_security_group.conductor-sg \
		-target=aws_elasticache_subnet_group.conductor-subnet-grp

# destroy resources
destroy-db-%: clean show-vars check-vars init
	terraform destroy \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var env="$(ENV)" \
		-target=aws_rds_cluster.rds_cluster \
		-target=aws_rds_cluster_instance.rds_cluster_instances \
		-target=aws_db_subnet_group.rds_db_subnet_group \
		-target=aws_security_group.conductor-sg \
		-target=aws_elasticache_subnet_group.conductor-subnet-grp
# dummy target for autocompletion

plan-db-corp-test:
apply-db-corp-test:
destroy-db-corp-test:

plan-db-corp-int:
apply-db-corp-int:
destroy-db-corp-int:


plan-db-corp-live:
apply-db-corp-live:
destroy-db-corp-live:

plan-db-shared-test:
apply-db-shared-test:
destroy-db-shared-test:

plan-db-shared-int:
apply-db-shared-int:
destroy-db-shared-int:


plan-db-shared-live:
apply-db-shared-live:
destroy-db-shared-live:

plan-pass: