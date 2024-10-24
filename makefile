SRC_DIR = src
BUILD_DIR = build

all: clean build

clean:
	@echo "Cleaning build directory..."
	@if [ -d "$(BUILD_DIR)" ]; then rm -rf $(BUILD_DIR); fi

build: clean
	@echo "Compiling CoffeeScript files..."
	@mkdir -p $(BUILD_DIR)
	@coffee -c --output $(BUILD_DIR) $(SRC_DIR)
	@echo "Build complete."

watch:
	@echo "Watching for changes..."
	@$(COFFEE) --watch --compile --output $(BUILD_DIR) $(SRC_DIR)

.PHONY: all clean build watch

