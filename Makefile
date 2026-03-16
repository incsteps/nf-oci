# Build the plugin
assemble:
	./gradlew assemble

clean:
	rm -rf .nextflow*
	rm -rf work
	rm -rf build
	./gradlew clean

# Run plugin unit tests
test:
	./gradlew test

# Install the plugin into local nextflow plugins dir
install:
	./gradlew install

# Publish the plugin
release:
	./gradlew releasePlugin

e2e:
	./gradlew installPlugin -Pversion=99.99.99
	cd src/e2e; OCI_PLUGIN_VERSION=99.99.99 ./nf-test test ${test}
