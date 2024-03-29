d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc store.cc server.cc)

PROTOS += $(addprefix $(d), weak-proto.proto)

OBJS-weak-client := $(LIB-message) $(LIB-udptransport) $(LIB-request) $(LIB-store-common) $(LIB-store-frontend) \
									$(o)weak-proto.o $(o)shardclient.o $(o)client.o 

LIB-weak-store := $(LIB-message) $(LIB-udptransport) $(LIB-request) \
									$(LIB-store-common) $(LIB-store-backend) \
									$(o)weak-proto.o $(o)store.o $(o)server.o

