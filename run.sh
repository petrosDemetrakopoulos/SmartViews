redis-server &
echo Starting redis server... &
sleep 2 &
ganache-cli -e 8000000000000 -l 80000000000000 &
echo starting blockchain... &
sleep 2 &
npm start