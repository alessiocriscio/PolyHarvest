# Polymarket executor — Ubuntu 24 / g++13 / ARM64 / macOS
# Dependencies:
#   Ubuntu: sudo apt install nlohmann-json3-dev libcurl4-openssl-dev libssl-dev -y
#   macOS:  brew install nlohmann-json curl openssl@3

UNAME_S := $(shell uname -s)

CXX      = g++
CXXFLAGS = -std=c++20 -O3 -Wall -Wextra -march=native -Wno-deprecated-declarations
LDFLAGS  = -lcurl -lssl -lcrypto

ifeq ($(UNAME_S), Darwin)
    # macOS specific paths via Homebrew
    OPENSSL_PREFIX = $(shell brew --prefix openssl@3 2>/dev/null || echo "/opt/homebrew/opt/openssl@3")
    CURL_PREFIX = $(shell brew --prefix curl 2>/dev/null || echo "/opt/homebrew/opt/curl")
    JSON_PREFIX = $(shell brew --prefix nlohmann-json 2>/dev/null || echo "/opt/homebrew/opt/nlohmann-json")
    
    CXXFLAGS += -I$(OPENSSL_PREFIX)/include -I$(JSON_PREFIX)/include
    LDFLAGS  += -L$(OPENSSL_PREFIX)/lib
    
    ifneq ($(wildcard $(CURL_PREFIX)/include),)
        CXXFLAGS += -I$(CURL_PREFIX)/include
    endif
    ifneq ($(wildcard $(CURL_PREFIX)/lib),)
        LDFLAGS  += -L$(CURL_PREFIX)/lib
    endif
else
    # Linux (Ubuntu 24) standard paths
    CXXFLAGS += -I/usr/include/nlohmann
endif

TARGET   = executor
SRC      = executor.cpp

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)
