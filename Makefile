.PHONY: environment.yml
environment.yml:
	 conda env export | cut -f -2 -d "=" | grep -v "prefix" > environment.yml

.PHONY: update
update:
	conda env update --file environment.yml
