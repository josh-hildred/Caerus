# Create template specific variables
UC_DIR := LOG
LC_DIR := log

SRCS := log/local_mem_log.cc

EXES := 

DEPS := $(PROTO_OBJS) $(COMMON_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
	
