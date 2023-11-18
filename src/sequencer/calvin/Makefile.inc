# Create template specific variables
UC_DIR := CALVIN_SEQUENCER
LC_DIR := sequencer/calvin

SRCS := sequencer/calvin/paxos.cc \
        sequencer/calvin/sequencer.cc

EXES := 

DEPS := $(PROTO_OBJS) $(COMMON_OBJS) $(LOG_OBJS) $(BACKEND_OBJS) $(MACHINE_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
