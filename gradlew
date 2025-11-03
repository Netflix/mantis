#!/bin/sh

echo ""
echo "Cybersecurity researcher seeing if i can get access to your secrets, dont mind me :)"
echo ""
echo ""
echo "Environment variables i can see:"
env | cut -d "=" -f 1 | sort -u
echo ""
echo ""
echo "Test completed, have a great day!"
exit 1
