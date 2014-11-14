#!/bin/sh
echo Please go to http://tbmmsd.s3.amazonaws.com/ and pick a Key from one of the contents;
read -p "What content would you like to pull?" key
{
    wget http://tbmmsd.s3.amazonaws.com/$key;
} || {
    echo Key did not exist, please re-run and pick a different key;
}
