if [ "$1" == "download" ]
then
    cd ./resource/
    wget http://files.inria.fr/corese/distrib/corese-server-4.2.3c.jar
fi

cd ./resource/

java -Xmx12G -jar corese-server-4.2.3c.jar -p 8082 -su -rdfstar
