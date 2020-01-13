# Do not modify any of these below.  These vars are all derived or static.
REGION = us-west-2
ENCLAVE = storage
SERVICE = conductor
TFSTATE = $(SERVICE)/vault-dynamic-$(REGION)-$(ENV).tfstate

clean:
	@rm -rf ./.terraform

creds:
	@aws-vault exec -j deluxemediaplatform >/dev/null

show-vars:
	@echo "REGION:\t\t$(REGION)"
	@echo "ENCLAVE:\t$(ENCLAVE)"
	@echo "ENV:\t\t$(ENV)"

check-vars:
	@if [ -z "$(ENV)" ]; then echo ENV not set!; exit 1; fi
	@echo ""
	@echo "NOTE: To run these terraform configs you MUST have the VAULT_TOKEN env variable set to a admin vault token in $(ENCLAVE)-$(ENV)."
	@echo ""
	@echo "ARE THESE PARAMETERS CORRECT AND IS VAULT_TOKEN SET? [yes/no]" && read ans && [ $$ans == yes ]

init: creds
	@if [ -f .terraform/terraform.tfstate ]; then rm .terraform/terraform.tfstate; fi
	terraform init -backend-config="key=$(TFSTATE)"

## Env
%.test: ENV=test
%.int: ENV=int
%.uat: ENV=uat
%.live: ENV=live

plan.%: show-vars check-vars init
	terraform plan \
	-var region="$(REGION)" \
	-var enclave="$(ENCLAVE)" \
	-var env="$(ENV)"

apply.%: show-vars check-vars init
	terraform apply \
	-var region="$(REGION)" \
	-var enclave="$(ENCLAVE)" \
	-var env="$(ENV)"

destroy.%: show-vars check-vars init
	terraform destroy \
	-var region="$(REGION)" \
	-var enclave="$(ENCLAVE)" \
	-var env="$(ENV)"

