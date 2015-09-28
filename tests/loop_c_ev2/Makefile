# Citrusleaf Tools
# Makefile

.PHONY: default
default: all
	@echo "done."

clean:
	rm -rf obj/*
	rm -f bin/*

%:
	$(MAKE) -C src $@
