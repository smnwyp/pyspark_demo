# how to run
```commandline
$ mkdir res
$ chmod -R 777 res
$ docker build -t pyspark_demo .
$ docker run -v "$PWD/res":"/home/jovyan/res/" pyspark_demo
```
should you be running on mac chip unfortunately lol:
```commandline
$ mkdir res
$ chmod -R 777 res
$ docker buildx build --platform linux/arm64/v8 -t pyspark_demo .
$ docker run --platform linux/arm64/v8 -v "$PWD/res":"/home/jovyan/res/" pyspark_demo
```
result csv could be found in `$PWD/res` folder.