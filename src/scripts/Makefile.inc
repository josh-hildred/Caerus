# Create template specific variables
UC_DIR := SCRIPTS
LC_DIR := scripts

SRCS := scripts/script_utils.cc

EXES := scripts/cluster.cc \
        scripts/calvindb_server.cc \
        scripts/lowlatency_calvindb_server.cc \
        scripts/caerus_server.cc

DEPS := $(PROTO_OBJS) $(COMMON_OBJS) $(BACKEND_OBJS) $(APPLICATIONS_OBJS) $(MACHINE_OBJS) $(LOG_OBJS) $(SCHEDULER_OBJS) \
$(CAERUS_SEQUENCER_OBJS) $(SLOG_SEQUENCER_OBJS) $(CALVIN_SEQUENCER_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

