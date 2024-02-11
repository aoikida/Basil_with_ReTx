d := $(dir $(lastword $(MAKEFILE_LIST)))

#SRCS += $(addprefix $(d), app.cc replica.cc slots.cc common.cc server.cc shardclient.cc client.cc testreplica.cc testclient.cc)
SRCS += $(addprefix $(d), app.cc replica.cc slots.cc common.cc server.cc shardclient.cc client.cc)

PROTOS += $(addprefix $(d), pbft-proto.proto server-proto.proto)

# HotStuff static libraries
LIB-hotstuff-interface := store/hotstuffstore/libhotstuff/examples/libindicus_interface.a store/hotstuffstore/libhotstuff/salticidae/libsalticidae.a store/hotstuffstore/libhotstuff/libhotstuff.a store/hotstuffstore/libhotstuff/secp256k1/.libs/libsecp256k1.a

LIB-hotstuff-store := $(o)common.o $(o)slots.o $(o)replica.o $(o)server.o \
	$(o)pbft-proto.o $(o)server-proto.o $(o)app.o $(o)shardclient.o \
	$(o)client.o $(LIB-crypto) $(LIB-configuration) $(LIB-store-common) \
	$(LIB-transport) $(LIB-store-backend) $(LIB-hotstuff-interface)
