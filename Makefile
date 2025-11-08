.PHONY: build down shell seed dbt-run dbt-debug

build:
	docker-compose up -d --build

down:
	docker-compose down

shell:
	docker-compose exec analytics-env bash

load:
	docker-compose exec analytics-env bash -c "cd scripts && python loader.py --filename '../input/country_and_industry/*' --table country_and_industry --write-disposition truncate_append --force-load"
	docker-compose exec analytics-env bash -c "cd scripts && python loader.py --filename '../input/events/*/*' --table events --write-disposition truncate_append --force-load"

seed:
	docker-compose exec analytics-env bash -c "cd dbt_project && dbt seed --profiles-dir ../profiles"

dbt-run:
	docker-compose exec analytics-env bash -c "cd dbt_project && dbt run --profiles-dir ../profiles"

dbt-debug:
	docker-compose exec analytics-env bash -c "cd dbt_project && dbt debug --profiles-dir ../profiles"
