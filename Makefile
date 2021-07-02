build:
	if [ -d ./dist ]; then rm -r ./dist; fi;
	mkdir -p ./dist/src && cp -r ./src/* ./dist/src/
	cd dist && zip -r ../dist.zip *