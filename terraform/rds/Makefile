SERVICE = conductor
REGION = us-west-2
ENV = test
TFSTATE = $(SERVICE)/$(REGION)-$(ENV)-aurora-postgres.tfstate


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