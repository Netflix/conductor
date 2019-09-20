SERVICE = conductor
REGION = us-west-2
ENCLAVE = shared
TFSTATE = $(SERVICE)/$(REGION)-$(ENV)-aurora-postgres.tfstate
GENERATE_NEW_PASSOWRD = "$$RANDOM"

# Target Specific Variables
## corp
### enclave

%-db-test: ENV=test
%-db-int: ENV=int
%-db-uat: ENV=uat
%-db-live: ENV=live

# clean terraform modules
clean:
		rm -rf ./.terraform

creds:
		# ensure there is an active aws-vault session
		@aws-vault exec -j deluxemediaplatform >/dev/null

show-vars:
			@echo "REGION:\t\t\t$(REGION)"
			@echo "ENV:\t\t\t$(ENV)"
			@echo "SERVICE:\t\t$(SERVICE)"
			@echo "RANDOM:\t\t\t$(GENERATE_NEW_PASSOWRD)"
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
		-target=aws_elasticache_subnet_group.conductor-subnet-grp \
		-target=random_string.password \
		-target=vault_generic_secret.db-password

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
		-target=aws_elasticache_subnet_group.conductor-subnet-grp \
		-target=random_string.password \
		-target=vault_generic_secret.db-password

## destroy resources
destroy-db-%: clean show-vars check-vars init
	terraform plan \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var env="$(ENV)" \
		-target=aws_rds_cluster.rds_cluster \
		-target=aws_rds_cluster_instance.rds_cluster_instances \
		-target=aws_db_subnet_group.rds_db_subnet_group \
		-target=aws_security_group.conductor-sg \
		-target=aws_elasticache_subnet_group.conductor-subnet-grp \
		-target=random_string.password \
		-target=vault_generic_secret.db-password

# WARNING USE WITH CAUTION . THIS MAY CAUSE OUTAGE
# plan db password change
plan-rotate-pass-%: clean show-vars check-vars init
	terraform plan \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var env="$(ENV)" \
		-var generate_new_password="$(GENERATE_NEW_PASSOWRD)" \
		-target=random_string.password \
		-target=vault_generic_secret.db-password

# apply db password change
apply-rotate-pass-%: clean show-vars check-vars init
	terraform apply \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var env="$(ENV)" \
		-var generate_new_password="$(GENERATE_NEW_PASSOWRD)" \
		-target=random_string.password \
		-target=vault_generic_secret.db-password
# dummy target for autocompletion
plan-db-test:
apply-db-test:
destroy-db-test:

plan-db-int:
apply-db-int:
destroy-db-int:

plan-db-uat:
apply-db-uat:
destroy-db-uat:

plan-db-live:
apply-db-live:
destroy-db-live:

plan-rotate-pass-db-test:
apply-rotate-pass-db-test:

plan-rotate-pass-db-int:
apply-rotate-pass-db-int:

plan-rotate-pass-db-uat:
apply-rotate-pass-db-uat:

plan-rotate-pass-db-live:
apply-rotate-pass-db-live: