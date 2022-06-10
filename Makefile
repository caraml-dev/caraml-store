format-java:
	./gradlew spotlessApply

lint-java:
	./gradlew spotlessCheck

compile:
	./gradlew clean compileJava

test-all:
	./gradlew test

dist:
	./gradlew core:clean core:bootJar
