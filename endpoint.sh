if [ "$1" == "download" ]
then
    cd ./resource/
    wget https://files.inria.fr/corese/distrib/corese-server-4.1.6d.jar
    # wget http://files.inria.fr/corese/distrib/corese-server-4.2.3a.jar
    # wget https://raw.githubusercontent.com/Wimmics/corese/master/corese-server/src/main/resources/webapp/data/myprofile.ttl
fi

cd ./resource/

# java -Xmx12G -jar corese-server-4.2.3a.jar -pp myprofile.ttl -p 8082 -su -rdfstar
java -Xmx12G -jar corese-server-4.1.6d.jar -pp myprofile.ttl -p 8082 -su -rdfstar
# java -Xmx12G -jar corese-server-4.1.6d.jar -p 8082 -su -rdfstar
