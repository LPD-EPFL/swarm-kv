DEPDIR := ../.deps
TIDYDIR := ../.clang-tidy-builds

ifneq ($(DORY_BUILD_VERBOSITY),)
	SILENCE =
else
	SILENCE = @
endif

.NOTPARALLEL:

# This rule is first, as it's the default rule
$(DEPDIR)/Makefile.generated : ../targets.yaml
	$(SILENCE)mkdir -p $(DEPDIR) && ./generate-makefile.py $@

clean:
	$(SILENCE) rm -rf $(DEPDIR)

buildclean:
	$(SILENCE) scripts/dory_rm_build_dirs "$(PWD)"

tidyclean:
	$(SILENCE) rm -rf $(TIDYDIR)/*

distclean: clean tidyclean buildclean
	$(SILENCE) conan remove -f "dory-*"
