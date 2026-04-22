JOB     := ignis-skew-job
NS      := ignis-dev
LOG_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))/output

.PHONY: apply fetch logs analyze clean run

apply:
	rm -rf $(LOG_DIR)
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/configmap.yaml
	kubectl delete job $(JOB) -n $(NS) --ignore-not-found
	kubectl apply -f k8s/spark-job.yaml

fetch:
	@mkdir -p $(LOG_DIR)
	@echo "Waiting for pod to be created..."
	@until kubectl get pods -n $(NS) -l job-name=$(JOB) -o jsonpath='{.items[0].metadata.name}' 2>/dev/null | grep -q .; do sleep 1; done
	@POD=$$(kubectl get pods -n $(NS) -l job-name=$(JOB) -o jsonpath='{.items[0].metadata.name}'); \
	echo "Pod: $$POD"; \
	kubectl wait --for=condition=ready pod/$$POD -n $(NS) --timeout=120s; \
	echo "Waiting for Spark to finish (SPARK_DONE)..."; \
	until kubectl logs -n $(NS) $$POD 2>/dev/null | grep -q "SPARK_DONE"; do sleep 2; done; \
	echo "Copying log..."; \
	kubectl cp $(NS)/$$POD:/mnt/spark-logs/. $(LOG_DIR)/; \
	kubectl delete job $(JOB) -n $(NS) --ignore-not-found; \
	for zst in $(LOG_DIR)/eventlog_v2_*/events_*.zstd; do \
		[ -f "$$zst" ] || continue; \
		name=$$(basename $$(dirname $$zst) | sed 's/eventlog_v2_//'); \
		zstd -d -c "$$zst" > "$(LOG_DIR)/$$name" && echo "Decompressed: $(LOG_DIR)/$$name"; \
	done

logs:
	@POD=$$(kubectl get pods -n $(NS) -l job-name=$(JOB) -o jsonpath='{.items[0].metadata.name}'); \
	kubectl logs -f -n $(NS) $$POD

analyze:
	@LOG=$$(ls -t $(LOG_DIR) | grep -v '\.inprogress' | grep -v 'eventlog_v2_' | head -1); \
	echo "→ analyzing $(LOG_DIR)/$$LOG"; \
	cd .. && .venv/bin/ignis analyze "$(LOG_DIR)/$$LOG"

clean:
	kubectl delete job $(JOB) -n $(NS) --ignore-not-found
	rm -rf $(LOG_DIR)

run: apply fetch analyze