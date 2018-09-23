REGION = us-west-2

# target specific variables
## shared
### enclave
plan-conductor-elastic-shared-% apply-conductor-elastic-shared-% destroy-conductor-elastic-shared-%: ENCLAVE=shared
### env
%-elastic-shared-test: ENV=test
%-elastic-shared-int: ENV=int
%-elastic-shared-live: ENV=live

### domain
%-conductor-elastic-shared-test: DOMAIN=conductor-aes-shared-test
%-conductor-elastic-shared-int: DOMAIN=conductor-aes-shared-int
%-conductor-elastic-shared-live: DOMAIN=conductor-aes-shared-live

### service
%-conductor-elastic-shared-test %-conductor-elastic-shared-int %-conductor-elastic-shared-live: SERVICE=conductor

# clean terraform modules
clean:
	rm -rf ./.terraform

check:
	@if test "$(ENV)" = "" ; then echo "ENV_TYPE is not set"; exit 1; fi
	@if test "$(ENCLAVE)" = "" ; then echo "ENCLAVE is not set"; exit 1; fi
	@if test "$(DOMAIN)" = "" ; then echo "DOMAIN is not set"; exit 1; fi
	@echo "ARE THESE PARAMETERS CORRECT? [yes/no]" && read ans && [ $$ans == yes ]

show-vars:
	@echo "REGION:\t\t\t$(REGION)"
	@echo "ENCLAVE:\t\t$(ENCLAVE)"
	@echo "ENV:\t\t$(ENV)"
	@echo "DOMAIN:\t\t$(DOMAIN)"
	@echo ""

init:
	@if [ -f .terraform/terraform.tfstate ]; then rm .terraform/terraform.tfstate; fi
	terraform init -backend-config="key=$(SERVICE)/$(REGION)-$(ENCLAVE)-$(ENV)-$(DOMAIN).tfstate"

# conductor
# different targets for different environments
# LIVE
## plan
plan-conductor-elastic-shared-live: clean show-vars check init
	terraform plan \
		-var domain="$(DOMAIN)" \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var owner="conductor_elastic_shared-$(ENV)" \
		-var D_TAG="$(DOMAIN)" \
		-var env="$(ENV)" \
		-var service="${SERVICE}" \
		-target=aws_elasticsearch_domain.es-live \
		-target=aws_security_group.conductor-aes-sg

## apply
apply-conductor-elastic-shared-live: clean show-vars check init
	terraform apply \
		-var domain="$(DOMAIN)" \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var owner="conductor_elastic_shared-$(ENV)" \
		-var D_TAG="$(DOMAIN)" \
		-var env="$(ENV)" \
		-var service="${SERVICE}" \
		-target=aws_elasticsearch_domain.es-live \
		-target=aws_security_group.conductor-aes-sg

## destroy
destroy-conductor-elastic-shared-live: clean show-vars check init
	terraform destroy \
		-var domain="$(DOMAIN)" \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var owner="conductor_elastic_shared-$(ENV)" \
		-var D_TAG="$(DOMAIN)" \
		-var env="$(ENV)" \
		-var service="${SERVICE}" \
		-target=aws_elasticsearch_domain.es-live \
		-target=aws_security_group.conductor-aes-sg

# INT
## plan
plan-conductor-elastic-shared-int: clean show-vars check init
	terraform plan \
		-var domain="$(DOMAIN)" \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var owner="conductor_elastic_shared-$(ENV)" \
		-var D_TAG="$(DOMAIN)" \
		-var env="$(ENV)" \
		-var service="${SERVICE}" \
		-target=aws_elasticsearch_domain.es-dev \
		-target=aws_security_group.conductor-aes-sg

## apply
apply-conductor-elastic-shared-int: clean show-vars check init
	terraform apply \
		-var domain="$(DOMAIN)" \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var owner="conductor_elastic_shared-$(ENV)" \
		-var D_TAG="$(DOMAIN)" \
		-var env="$(ENV)" \
		-var service="${SERVICE}" \
		-target=aws_elasticsearch_domain.es-dev \
		-target=aws_security_group.conductor-aes-sg

## destroy
destroy-conductor-elastic-shared-int: clean show-vars check init
	terraform destroy \
		-var domain="$(DOMAIN)" \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var owner="conductor_elastic_shared-$(ENV)" \
		-var D_TAG="$(DOMAIN)" \
		-var env="$(ENV)" \
		-var service="${SERVICE}" \
		-target=aws_elasticsearch_domain.es-dev \
		-target=aws_security_group.conductor-aes-sg

# TEST
## plan
plan-conductor-elastic-shared-test: clean show-vars check init
	terraform plan \
		-var domain="$(DOMAIN)" \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var owner="conductor_elastic_shared-$(ENV)" \
		-var D_TAG="$(DOMAIN)" \
		-var env="$(ENV)" \
		-var service="${SERVICE}" \
		-target=aws_elasticsearch_domain.es-dev \
		-target=aws_security_group.conductor-aes-sg

## apply
apply-conductor-elastic-shared-test: clean show-vars check init
	terraform apply \
		-var domain="$(DOMAIN)" \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var owner="conductor_elastic_shared-$(ENV)" \
		-var D_TAG="$(DOMAIN)" \
		-var env="$(ENV)" \
		-var service="${SERVICE}" \
		-target=aws_elasticsearch_domain.es-dev \
		-target=aws_security_group.conductor-aes-sg

## destroy
destroy-conductor-elastic-shared-test: clean show-vars check init
	terraform destroy \
		-var domain="$(DOMAIN)" \
		-var region="$(REGION)" \
		-var enclave="$(ENCLAVE)" \
		-var owner="conductor_elastic_shared-$(ENV)" \
		-var D_TAG="$(DOMAIN)" \
		-var env="$(ENV)" \
		-var service="${SERVICE}" \
		-target=aws_elasticsearch_domain.es-dev \
		-target=aws_security_group.conductor-aes-sg

# dummy target for autocompletion
#conductor
plan-conductor-elastic-shared-test:
plan-conductor-elastic-shared-live:
plan-conductor-elastic-shared-int:
apply-conductor-elastic-shared-test:
apply-conductor-elastic-shared-live:
apply-conductor-elastic-shared-int:
destroy-conductor-elastic-shared-test:
destroy-conductor-elastic-shared-live:
destroy-conductor-elastic-shared-int: