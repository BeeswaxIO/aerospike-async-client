#Citrusleaf Tools
# Makefile

DEPTH = ../../..
include Makefile.in

DIR_INCLUDE = ../include $(DEPTH)/include
DIR_OBJECT = ../obj
DIR_TARGET = ../bin

HEADERS = loop.h shash.h
SOURCES = main.c loop.c shash.c
TARGET = loop_c_ev

INCLUDES = $(DIR_INCLUDE:%=-I%) 
#LIBRARIES = -lev2citrusleaf  -lssl -lrt -levent
LIBRARIES = -lev2citrusleaf -levent -lssl -lrt -lcrypto
LDFLAGS += -L$(DEPTH)/lib

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: loop_c_ev2

.PHONY: clean
clean:
	/bin/rm -f $(OBJECTS) $(DIR_TARGET)/$(TARGET)

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES)

.PHONY: loop_c_ev2
loop_c_ev2: $(OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)

-include $(DEPENDENCIES)

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c $(INCLUDES) $<
