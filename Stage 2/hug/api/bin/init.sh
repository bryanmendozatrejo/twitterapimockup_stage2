#!/bin/sh
sqlite-utils insert ./var/users.db users --csv ./share/users.csv --detect-types --pk=id
sqlite-utils create-index ./var/users.db users username bio email password --unique
sqlite-utils insert ./var/users.db followers --csv ./share/followers.csv --detect-types --pk=id
sqlite-utils create-index ./var/users.db followers username friend_username --unique
sqlite-utils insert ./var/users.db likes --csv ./share/likes.csv --detect-types --pk=id
sqlite-utils create-index ./var/users.db likes posts_id username --unique
sqlite-utils insert ./var/posts.db posts --csv ./share/posts.csv --detect-types --pk=id
sqlite-utils create-index ./var/posts.db posts username message timestamp repost --unique
sqlite-utils insert ./var/services.db services --csv ./share/services.csv --detect-types --pk=id
sqlite-utils create-index ./var/services.db services service_name url http --unique
sqlite-utils insert ./var/healths.db healths --csv ./share/healths.csv --detect-types --pk=id
sqlite-utils create-index ./var/healths.db healths service_name http url status_code --unique
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb | python3 createPollsTable.py
