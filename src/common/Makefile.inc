# Create template specific variables
UC_DIR := COMMON
LC_DIR := common

SRCS := common/utils.cc \
        common/perf_tracker.cc \
        common/internal_ids.cc

EXES := 

DEPS := $(PROTO_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

