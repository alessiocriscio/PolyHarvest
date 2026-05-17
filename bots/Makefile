# Polymarket executor — Ubuntu 24 / g++13 / ARM64
# Dependencies:
#   sudo apt install nlohmann-json3-dev libcurl4-openssl-dev libssl-dev -y

CXX      = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -march=native -I/usr/include/nlohmann
LDFLAGS  = -lcurl -lssl -lcrypto

TARGET   = executor
SRC      = executor.cpp

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)
