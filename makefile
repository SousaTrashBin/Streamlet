JAVAC = javac

SRC_DIR = src
OUT_DIR = out

SRC_FILES = $(shell find $(SRC_DIR) -name "*.java")

all: compile

$(OUT_DIR):
	mkdir -p $(OUT_DIR)

compile: $(OUT_DIR)
	$(JAVAC) -d $(OUT_DIR) $(SRC_FILES)

clean:
	rm -rf out
	rm -rf output
