# Create template specific variables
UC_DIR := CAERUS_SEQUENCER
LC_DIR := sequencer/caerus

SRCS := sequencer/caerus/conflict_graph.cc \
        sequencer/caerus/merger.cc \
        sequencer/caerus/partial_sequencer.cc

EXES := 

DEPS := $(PROTO_OBJS) $(COMMON_OBJS) $(LOG_OBJS) $(BACKEND_OBJS) $(MACHINE_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
