MFA_DEVICE_ARN := 
BRANCH_NAME := $(shell git rev-parse --abbrev-ref HEAD)

define ImportCredentials
$(eval export AWS_ACCESS_KEY_ID=$(shell jq -r '.Credentials.AccessKeyId' .cache/credentials.json 2>/dev/null))
$(eval export AWS_SECRET_ACCESS_KEY=$(shell jq -r '.Credentials.SecretAccessKey' .cache/credentials.json 2>/dev/null))
$(eval export AWS_SESSION_TOKEN=$(shell jq -r '.Credentials.SessionToken' .cache/credentials.json 2>/dev/null))
$(eval export AWS_DEFAULT_REGION=ap-northeast-1)
$(eval export AWS_DEFAULT_OUTPUT=json)
endef

build:
	docker builder prune --force
	docker build -t testglue:latest .

run:
	docker run --rm -it -v ./:/home/glue_user/workspace/jupyter_workspace testglue:latest

clean:
	-docker rmi $(shell docker images -qf reference=testglue:latest)

sync: sync-confs sync-jobs sync-catalogs

sync-confs:
	-rm -rf confs
	mkdir -p confs
	$(call ImportCredentials)
	aws s3 cp s3://amsd-$(BRANCH_NAME)-datalake-conf ./confs --recursive

sync-jobs:
	test -f .cache/credentials.json
	-rm -f .cache/list-jobs-*.json
	-rm -rf jobs cracked_jobs
	$(call ImportCredentials)
	./tools/fetch-all-jobs.sh

sync-catalogs:
	-rm -rf catalogs
	./tools/fetch-glue-catalogs.sh
	./tools/fetch-athena-catalogs.sh

mfa-%:
	mkdir -p .cache
	aws sts get-session-token \
		--serial-number $(MFA_DEVICE_ARN) \
		--token-code ${@:mfa-%=%} | tee ./.cache/credentials.json
