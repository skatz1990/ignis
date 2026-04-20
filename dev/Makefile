IMAGE   := ignis-spark-app:latest
JOB     := ignis-skew-job
NS      := ignis-dev
LOG_DIR := /tmp/ignis-spark-logs

.PHONY: build apply wait logs analyze clean run

build:
	docker build -t $(IMAGE) app/

apply:
	kubectl apply -f k8s/namespace.yaml
	kubectl delete job $(JOB) -n $(NS) --ignore-not-found
	kubectl apply -f k8s/spark-job.yaml

wait:
	kubectl wait --for=condition=complete job/$(JOB) -n $(NS) --timeout=300s

logs:
	@POD=$$(kubectl get pods -n $(NS) -l job-name=$(JOB) -o jsonpath='{.items[0].metadata.name}'); \
	kubectl logs -f $$POD

analyze:
	@LOG=$$(ls -t $(LOG_DIR) | grep -v '\.inprogress' | head -1); \
	echo "→ analyzing $(LOG_DIR)/$$LOG"; \
	cd .. && .venv/bin/ignis analyze $(LOG_DIR)/$$LOG

clean:
	kubectl delete job $(JOB) -n $(NS) --ignore-not-found
	rm -rf $(LOG_DIR)

run: apply wait analyze
