# build librdkafka and export the according pkg config so it will be found

if [ -z $TMP_DIR ]
then
  TMP_DIR=$(mktemp -d)
fi

SOURCE_DIR=$(dirname ${BASH_SOURCE[0]})/..
LIB_RDKAFKA_DIR=$SOURCE_DIR/vendor/github.com/edenhill/librdkafka
cd $LIB_RDKAFKA_DIR
./configure --prefix=$TMP_DIR
make
make install

export PKG_CONFIG_PATH=$TMP_DIR/lib/pkgconfig

if [ -z $LD_LIBRARY_PATH ]
then
	export LD_LIBRARY_PATH=$TMP_DIR/lib
else
	export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$TMP_DIR/lib"
fi

cd $OLDPWD
