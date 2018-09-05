mvn protobuf:compile protobuf:compile-custom

The version is incremented any time the file is created, modified, or deleted.

Once a crashed metadata store is restored, then it should come back up-to-date within 5 seconds. 
(Hint: the leader should try to append entries to the logs every 500 milliseconds)



target/surfstore/bin/runBlockServer ../configs/configCentralized.txt

target/surfstore/bin/runMetadataStore ../configs/configCentralized.txt

target/surfstore/bin/runClient ../configs/configCentralized.txt




target/surfstore/bin/runBlockServer ../configs/configDistributed.txt

target/surfstore/bin/runMetadataStore -n 1 ../configs/configDistributed.txt

target/surfstore/bin/runMetadataStore -n 2 ../configs/configDistributed.txt

target/surfstore/bin/runMetadataStore -n 3 ../configs/configDistributed.txt


target/surfstore/bin/runClient ../configs/configDistributed.txt