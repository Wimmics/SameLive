if [ "$1" == "download" ]
then
    cd ./resource/
    wget https://files.inria.fr/corese/distrib/corese-server-4.1.6d.jar
fi

cd ./resource/

java -Xmx12G -jar corese-server-4.1.6d.jar -p 8082 -su -rdfstar
