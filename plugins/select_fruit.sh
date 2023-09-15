FRUIT=$1

if [ "APPLE" == "$FRUIT" ]; then
	echo "You selected Apple!"

elif [ "ORANGE" = "$FRUIT" ]; then
	echo "You selected Orange"

elif [ "GRAPE" = "$FRUIT" ]; then
	echo "You selected Grape!"

else
	echo "You selected other Fruit!"

fi