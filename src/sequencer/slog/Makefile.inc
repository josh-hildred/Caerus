# Create template specific variables
UC_DIR := SLOG_SEQUENCER
LC_DIR := sequencer/slog

SRCS := sequencer/slog/local_paxos.cc \
        sequencer/slog/lowlatency_sequencer.cc

EXES := 

DEPS := $(PROTO_OBJS) $(COMMON_OBJS) $(LOG_OBJS) $(BACKEND_OBJS) $(MACHINE_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
