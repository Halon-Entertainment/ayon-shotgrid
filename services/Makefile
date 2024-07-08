ADDON_VERSION := $(shell grep "version" ../package.py | cut -d' ' -f3 | tr -d '"')
SERVICE = $(error Please specify the service to build with 'SERVICE', for example: 'make build SERVICE=leecher')

default:
	@echo ""
	@echo "Ayon Shotgrid $(ADDON_VERSION) Service Builder"
	@echo ""
	@echo "Usage: make SERVICE=[service-name] [target]"
	@echo ""
	@echo "Passing SERVICE is required for any of the targets to work, possible services:"
	@echo ""
	@echo "  leecher - Fetch Shotgrid Events into AYON."
	@echo "  processor - Process 'shotgrid.event's in AYON."
	@echo "  transmitter - Push AYON events to Shotgrid."
	@echo ""
	@echo "Targets:"
	@echo "  build        Build docker image."
	@echo "  build-all    Build docker image for 'leecher', 'processor', and 'transmitter'."
	@echo "  clean        Remove local images."
	@echo "  clean-build  Remove local images and build without docker cache."
	@echo "  dev          Run a service locally"

build:
	docker build --network=host -t ayon-shotgrid-$(SERVICE):$(ADDON_VERSION) -f $(SERVICE)/Dockerfile .

build-all:
	$(foreach service,leecher processor transmitter, docker build --network=host -t ayon-shotgrid-$(service):$(ADDON_VERSION) -f $(service)/Dockerfile . &)

clean:
	if docker images "ayon-shotgrid-$(SERVICE)"; then \
		docker rmi ayon-shotgrid-$(SERVICE); \
	fi

clean-build: clean
	docker build --network=host --no-cache -t ayon-shotgrid-$(SERVICE):$(ADDON_VERSION) -f $(SERVICE)/Dockerfile .

clean-build-all:
	$(foreach service,leecher processor transmitter, docker build --network=host --no-cache -t ayon-shotgrid-$(service):$(ADDON_VERSION) -f $(service)/Dockerfile . &)

dev:
	-ln -s $(CURDIR)/shotgrid_common/* $(CURDIR)/$(SERVICE)
	-docker run --rm -u ayonuser -ti \
		-v $(CURDIR)/shotgrid_common:$(CURDIR)/shotgrid_common:Z \
		-v $(CURDIR)/$(SERVICE):/service:Z \
		--env-file $(CURDIR)/$(SERVICE)/.env \
		--env AYON_ADDON_NAME=shotgrid \
		--env AYON_ADDON_VERSION=$(ADDON_VERSION) \
		--attach=stdin \
		--attach=stdout \
		--attach=stderr \
		--network=host \
		ayon-shotgrid-$(SERVICE):$(ADDON_VERSION) python -m $(SERVICE)

	find $(CURDIR)/$(SERVICE) -type l -delete

shell:
	docker run --rm -u ayonuser -ti -v $(CURDIR)/$(SERVICE)/$(SERVICE):/service:Z ayon-shotgrid-$(SERVICE):$(ADDON_VERSION) /bin/sh